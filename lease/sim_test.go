// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"
)

// The simulation runs a few participants that follow the writer rules from
// docs/protocol.md against one shared log, with everything that hurts leases
// in real life: slow consumers, produce requests that land seconds late, and
// processes frozen in the middle of a decision. Time is in milliseconds and
// all clocks are perfect, so the safety margin is zero and any overlap is a
// protocol bug rather than a tuning problem.
//
// Two properties are checked:
//
//   - safety: at no instant do two participants believe they hold the lease;
//   - agreement: a reader that starts from any point of the log, once it is
//     certain, has exactly the state of a reader that saw the whole log.

const (
	simTTL      = 1000 // ms
	simRenew    = simTTL / 3
	simDuration = 120_000
	simStep     = 10
)

type simMsg struct {
	landAt int64
	sentAt int64
	rec    Record
}

type simNode struct {
	id   string
	view *View
	pos  int // next log index to consume

	// Belief: the node considers itself holder of epoch until deadline.
	epoch    int64
	deadline int64

	// At most one produce request in flight; its send time is the basis of
	// the deadline once the node's own fold accepts the record.
	inflight *simMsg
	lastSend int64

	pausedUntil int64
}

func simTime(ms int64) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }

func runSim(t *testing.T, seed uint64) []Record {
	t.Helper()
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))

	nodes := make([]*simNode, 3)
	for i := range nodes {
		nodes[i] = &simNode{id: fmt.Sprintf("n%d", i), view: NewViewFromStart(simTTL * time.Millisecond), epoch: -1}
	}
	var log []Record
	var wire []*simMsg

	for now := int64(0); now < simDuration; now += simStep {
		// Land produce requests. LogAppendTime is the landing time.
		rest := wire[:0]
		for _, m := range wire {
			if m.landAt > now {
				rest = append(rest, m)
				continue
			}
			m.rec.Offset = int64(len(log))
			m.rec.Timestamp = simTime(now)
			log = append(log, m.rec)
		}
		wire = rest

		holders := 0
		for _, n := range nodes {
			if now < n.deadline {
				holders++
			}
		}
		if holders > 1 {
			t.Fatalf("seed %d, t=%dms: %d participants believe they hold the lease", seed, now, holders)
		}

		for _, n := range nodes {
			if now < n.pausedUntil {
				continue
			}
			if rng.Float64() < 0.002 {
				n.pausedUntil = now + rng.Int64N(3*simTTL)
				continue
			}

			// Consume, sometimes lagging behind.
			for n.pos < len(log) && rng.Float64() < 0.9 {
				r := log[n.pos]
				n.pos++
				out, err := n.view.Apply(r)
				if err != nil {
					t.Fatalf("seed %d: %v", seed, err)
				}
				if n.inflight == nil || r.Holder != n.id {
					continue
				}
				// Own record read back: only now may the belief change.
				if out == Accepted {
					switch r.Kind {
					case Claim:
						n.epoch = r.Offset
						n.deadline = n.inflight.sentAt + simTTL
					case Renew:
						n.deadline = n.inflight.sentAt + simTTL
					case Release:
						n.deadline = 0
					}
				}
				n.inflight = nil
			}

			if n.inflight != nil {
				continue
			}
			var rec Record
			switch {
			case now < n.deadline && rng.Float64() < 0.001:
				// Giving up the lease starts with no longer believing in it.
				n.deadline = 0
				rec = Record{Kind: Release, Holder: n.id, Epoch: n.epoch}
			case now < n.deadline && now-n.lastSend >= simRenew:
				rec = Record{Kind: Renew, Holder: n.id, Epoch: n.epoch, TTL: simTTL * time.Millisecond}
			case now >= n.deadline && rng.Float64() < 0.02:
				// Claims are fired blindly, held or not: the fold sorts it out.
				rec = Record{Kind: Claim, Holder: n.id, TTL: simTTL * time.Millisecond}
			default:
				continue
			}
			delay := rng.Int64N(50)
			if rng.Float64() < 0.03 {
				// A stalled broker, or a freeze between decision and send.
				delay = rng.Int64N(2 * simTTL)
			}
			m := &simMsg{landAt: now + 1 + delay, sentAt: now, rec: rec}
			n.inflight, n.lastSend = m, now
			wire = append(wire, m)
		}
	}
	return log
}

func TestSimulationSafetyAndAgreement(t *testing.T) {
	seeds := 200
	if testing.Short() {
		seeds = 20
	}
	var terms, rejected, settled, lateReaders int
	for seed := uint64(1); seed <= uint64(seeds); seed++ {
		log := runSim(t, seed)

		// The reference reader saw everything.
		ref := NewViewFromStart(simTTL * time.Millisecond)
		refStates := make([]State, len(log))
		for i, r := range log {
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
		for k := 0; k < 25; k++ {
			start := rng.IntN(len(log))
			late := NewView(simTTL * time.Millisecond)
			lateReaders++
			for i := start; i < len(log); i++ {
				if _, err := late.Apply(log[i]); err != nil {
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
}
