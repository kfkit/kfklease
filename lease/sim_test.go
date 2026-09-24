// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

// The simulation drives real agents, the same code the Kafka client runs,
// against one shared fake log with everything that hurts leases in real
// life: slow consumers, produce requests that land seconds late,
// acknowledgements that arrive after the record was already read back,
// processes frozen in the middle of a decision, and participants that join
// late and see only the tail of the log. Time is virtual and all clocks are
// perfect, so the safety margin is zero and any overlap is a protocol bug
// rather than a tuning problem.
//
// Two properties are checked:
//
//   - safety: at no instant do two agents believe they hold the lease;
//   - agreement: a reader that starts from any point of the log, once it is
//     certain, has exactly the state of a reader that saw the whole log.

const (
	simTTL      = 1000 * time.Millisecond
	simDuration = 120_000
	simStep     = 10 * time.Millisecond
)

// simMsg is a produce request on its way to the log.
type simMsg struct {
	from   *simNode
	landAt time.Time
	rec    Record
}

// simAck is the broker's answer on its way back, or the client giving up
// on a record after the delivery timeout.
type simAck struct {
	at     time.Time
	offset int64
	err    error
}

var errSimTimeout = errors.New("sim: delivery timeout")

type simNode struct {
	a           *agent
	pos         int // next log index to consume
	acks        []simAck
	pausedUntil time.Time
	joinAt      time.Time // zero for founders; late joiners start uncertain
	// compacted marks records a late joiner never sees: compaction removed
	// them before it started, leaving holes in the offsets.
	compacted map[int]bool
}

func simTime(ms int64) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }

func simConfig(holder string) Config {
	cfg, err := Config{
		Brokers: []string{"fake"}, Topic: "fake", Holder: holder,
		TTL: simTTL, Margin: time.Nanosecond, RenewEvery: simTTL / 3,
	}.withDefaults()
	if err != nil {
		panic(err)
	}
	return cfg
}

func runSim(t *testing.T, seed uint64) []Record {
	t.Helper()
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	log := slog.New(slog.DiscardHandler)

	var nodes []*simNode
	for i := range 3 {
		nodes = append(nodes, &simNode{a: newAgent(simConfig(fmt.Sprintf("n%d", i)), log, 0, 0, simTime(0))})
	}
	// Two more join later, at a point where history is already gone.
	for i := 3; i < 5; i++ {
		nodes = append(nodes, &simNode{joinAt: simTime(rng.Int64N(simDuration / 2))})
	}

	var records []Record
	var wire []*simMsg

	for now := simTime(0); now.Before(simTime(simDuration)); now = now.Add(simStep) {
		// Land produce requests. LogAppendTime is the landing time. The
		// acknowledgement takes its own time to get back, and may lose the
		// race against the record being read back.
		rest := wire[:0]
		for _, m := range wire {
			if m.landAt.After(now) {
				rest = append(rest, m)
				continue
			}
			m.rec.Offset = int64(len(records))
			m.rec.Timestamp = now
			records = append(records, m.rec)
			m.from.acks = append(m.from.acks, simAck{at: now.Add(time.Duration(rng.IntN(60)) * time.Millisecond), offset: m.rec.Offset})
			slices.SortStableFunc(m.from.acks, func(x, y simAck) int { return x.at.Compare(y.at) })
		}
		wire = rest

		holders := 0
		for _, n := range nodes {
			if n.a != nil && n.a.believing(now) {
				holders++
			}
		}
		if holders > 1 {
			t.Fatalf("seed %d, t=%v: %d agents believe they hold the lease", seed, now.Sub(t0), holders)
		}

		for i, n := range nodes {
			if n.a == nil {
				if now.Before(n.joinAt) {
					continue
				}
				// The tail is all it will ever see, and part of that with
				// holes.
				start := max(0, len(records)-rng.IntN(30))
				n.a = newAgent(simConfig(fmt.Sprintf("n%d", i)), log, int64(start), int64(len(records)), now)
				n.pos = start
				n.compacted = make(map[int]bool)
				for j := start; j < len(records); j++ {
					if rng.IntN(3) == 0 {
						n.compacted[j] = true
					}
				}
			}
			if now.Before(n.pausedUntil) {
				continue
			}
			if rng.Float64() < 0.002 {
				n.pausedUntil = now.Add(time.Duration(rng.Int64N(int64(3 * simTTL))))
				continue
			}

			// Acknowledgements that have arrived.
			for len(n.acks) > 0 && !n.acks[0].at.After(now) {
				n.a.acked(now, n.acks[0].offset, n.acks[0].err)
				n.acks = n.acks[1:]
			}
			// Consume, sometimes lagging behind.
			for n.pos < len(records) && rng.Float64() < 0.9 {
				r := records[n.pos]
				n.pos++
				if n.compacted[int(r.Offset)] {
					continue
				}
				value, err := EncodeValue(r)
				if err != nil {
					t.Fatal(err)
				}
				if err := n.a.record(now, r.Offset, r.Timestamp, value); err != nil {
					t.Fatalf("seed %d: %v", seed, err)
				}
			}
			n.a.fetched(now, int64(len(records)))

			var rec Record
			var ok bool
			if n.a.believing(now) && rng.Float64() < 0.001 {
				rec, ok = n.a.release(now)
			} else {
				rec, ok = n.a.tick(now)
			}
			if !ok {
				continue
			}
			delay := time.Duration(rng.Int64N(50)) * time.Millisecond
			if rng.Float64() < 0.03 {
				// A stalled broker, or a freeze between decision and send.
				delay = time.Duration(rng.Int64N(int64(2 * simTTL)))
			}
			if delay >= simTTL {
				// The client gives up after the delivery timeout. Half the
				// time the broker got the record anyway and it lands late.
				n.acks = append(n.acks, simAck{at: now.Add(simTTL), err: errSimTimeout})
				if rng.IntN(2) == 0 {
					continue
				}
			}
			wire = append(wire, &simMsg{from: n, landAt: now.Add(simStep + delay), rec: rec})
		}
	}
	return records
}

func TestSimulationSafetyAndAgreement(t *testing.T) {
	seeds := 200
	if testing.Short() {
		seeds = 20
	}
	var terms, rejected, settled, lateReaders int
	for seed := uint64(1); seed <= uint64(seeds); seed++ {
		records := runSim(t, seed)

		// The reference reader saw everything.
		ref := NewViewFromStart(simTTL)
		refStates := make([]State, len(records))
		for i, r := range records {
			out, err := ref.Apply(r)
			if err != nil {
				t.Fatalf("seed %d: %v", seed, err)
			}
			if out == Accepted && r.Kind == Claim {
				terms++
			}
			if out == Rejected {
				rejected++
			}
			refStates[i] = ref.State()
		}

		rng := rand.New(rand.NewPCG(seed, 7))
		for range 25 {
			start := rng.IntN(len(records))
			late := NewView(simTTL)
			lateReaders++
			for i := start; i < len(records); i++ {
				if _, err := late.Apply(records[i]); err != nil {
					t.Fatalf("seed %d: %v", seed, err)
				}
				if !late.Certain() {
					continue
				}
				got, want := late.State(), refStates[i]
				now := late.Clock()
				if got.HeldAt(now) != want.HeldAt(now) || (want.HeldAt(now) && got != want) {
					t.Fatalf("seed %d: reader from offset %d disagrees at offset %d: got %+v, want %+v", seed, start, i, got, want)
				}
			}
			if late.Certain() {
				settled++
			}
		}
	}
	t.Logf("%d runs: %d terms, %d rejected records, %d/%d late readers settled", seeds, terms, rejected, settled, lateReaders)
	if terms < seeds*10 || rejected < seeds*10 {
		t.Fatalf("simulation is too tame to mean anything: %d terms, %d rejected records", terms, rejected)
	}
	// Rejections come from races and late records, not from candidates
	// claiming a lease they can see is held.
	if rejected > 2*terms {
		t.Fatalf("candidates spam the log: %d rejected records for %d terms", rejected, terms)
	}
}
