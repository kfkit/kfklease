// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"cmp"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

// The simulation drives real agents, the same code the Kafka client runs,
// against one shared fake log with everything that hurts leases in real
// life: slow consumers, produce requests that land seconds late,
// acknowledgements that arrive after the record was already read back,
// processes frozen in the middle of a decision, nodes cut off from the log
// for a while, and participants that join late and see only the tail of
// the log. Time is virtual and all clocks are
// perfect, so the safety margin is zero and any overlap is a protocol bug
// rather than a tuning problem.
//
// Two properties are checked:
//
//   - safety: at no instant do two agents believe they hold the lease, nor
//     do agents of two sites believe their site holds it (siteHolding: the
//     holder, or another agent of its site that reads the holder's term
//     from the log, as a scaler replica answers KEDA);
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

// simParams are the imperfections of one run. The defaults are the harsh
// but honest case: perfect clocks, zero margin, so that any overlap is a
// protocol bug.
type simParams struct {
	// brokerSkew bounds the offset of a partition leader's clock from real
	// time; each new leader draws its own offset from [-brokerSkew, +brokerSkew].
	brokerSkew time.Duration
	// clientDrift bounds the rate error of a node's clock: a node's clock
	// runs at a rate drawn from [1-clientDrift, 1+clientDrift].
	clientDrift float64
	margin      time.Duration
	// health turns the health gate on, with workloads that go up and down.
	health bool
	// sites puts the nodes in two sites, with a takeover delay: delay, or
	// simDelay when zero.
	sites bool
	delay time.Duration
	// standby switches nodes in and out of standby every twenty TTLs or
	// so.
	standby bool
}

const (
	simFailAfter = 3 * simTTL
	simCooldown  = 5 * simTTL
	simDelay     = 3 * simTTL
)

// config is node i's: holder "n<i>", or "s<i%2>/n<i>" with sites.
func (p simParams) config(i int) Config {
	holder := fmt.Sprintf("n%d", i)
	var delay time.Duration
	if p.sites {
		holder = fmt.Sprintf("s%d/%s", i%2, holder)
		delay = cmp.Or(p.delay, simDelay)
	}
	cfg, err := Config{
		Brokers: []string{"fake"}, Topic: "fake", Holder: holder, TakeoverDelay: delay,
		TTL: simTTL, Margin: max(p.margin, time.Nanosecond), RenewEvery: simTTL / 3,
	}.withDefaults()
	if p.health {
		cfg.Health = HealthGate{FailAfter: simFailAfter, Cooldown: simCooldown}
	}
	if err != nil {
		panic(err)
	}
	return cfg
}

type simNode struct {
	a           *agent
	pos         int // next log index to consume
	acks        []simAck
	pausedUntil time.Time
	// partitionedUntil: cut off from the log, the node reads nothing and
	// its records never land, but its clock and its deadline keep going.
	partitionedUntil time.Time
	// rate is how fast this node's clock runs relative to real time.
	rate   float64
	joinAt time.Time // zero for founders; late joiners start uncertain
	// compacted marks records a late joiner never sees: compaction removed
	// them before it started, leaving holes in the offsets.
	compacted map[int]bool

	// The health gate, observed from outside on the node's clock:
	// workloadUp is whether its workload would pass a probe, heldFrom and
	// droppedAt when the node last started and stopped believing,
	// lastReport its last healthy report, trippedAt when the gate last
	// took the lease away.
	workloadUp bool
	believed   bool
	heldFrom   time.Time
	droppedAt  time.Time
	lastReport time.Time
	trippedAt  time.Time

	standby bool
}

// simStats counts what happened in one run.
type simStats struct {
	gateReleases int
	// standbyReleases are releases sent in standby.
	standbyReleases int
	// siteAnswers counts steps at which an agent that is not the holder
	// believed its site holds the lease.
	siteAnswers int
}

// checkStandby holds a node in standby to its rules after a tick at local
// time: it does not believe and does not claim.
func (n *simNode) checkStandby(t *testing.T, seed uint64, local time.Time, rec Record, sent bool, st *simStats) {
	t.Helper()
	if !n.standby {
		return
	}
	if n.a.believing(local) {
		t.Fatalf("seed %d, t=%v: %s believes in standby", seed, local.Sub(t0), n.a.cfg.Holder)
	}
	switch {
	case sent && rec.Kind == Claim:
		t.Fatalf("seed %d, t=%v: %s claims in standby", seed, local.Sub(t0), n.a.cfg.Holder)
	case sent && rec.Kind == Release:
		st.standbyReleases++
	}
}

// checkGate holds a node to the health gate after a tick at local time:
// it believes only while its workload was healthy within FailAfter, counted
// from the start of its term, and it claims nothing within the cooldown
// after the gate took the lease away. Only the gate makes tick return a
// release, and the belief went at the tick that tripped it, which may be
// well before the release goes out (a record in flight, an uncertain view).
func (n *simNode) checkGate(t *testing.T, seed uint64, local time.Time, rec Record, sent bool, st *simStats) {
	t.Helper()
	believing := n.a.believing(local)
	if believing && !n.believed {
		n.heldFrom = local
	}
	if !believing && n.believed {
		n.droppedAt = local
	}
	n.believed = believing
	since := n.heldFrom
	if n.lastReport.After(since) {
		since = n.lastReport
	}
	if believing && local.Sub(since) >= simFailAfter+simStep {
		t.Fatalf("seed %d, t=%v: holder unhealthy for %v still believes", seed, local.Sub(t0), local.Sub(since))
	}
	if !sent {
		return
	}
	switch {
	case rec.Kind == Release:
		n.trippedAt = n.droppedAt
		st.gateReleases++
	case rec.Kind == Claim && !n.trippedAt.IsZero() && local.Before(n.trippedAt.Add(simCooldown)):
		t.Fatalf("seed %d, t=%v: claim %v after the gate tripped", seed, local.Sub(t0), local.Sub(n.trippedAt))
	}
}

func simTime(ms int64) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }

// local is the node's own reading of the clock at real time now. Agents
// only ever see local time; the log and the wire run on real time.
func (n *simNode) local(now time.Time) time.Time {
	return t0.Add(time.Duration(float64(now.Sub(t0)) * n.rate))
}

func simRate(rng *rand.Rand, p simParams) float64 {
	return 1 + (2*rng.Float64()-1)*p.clientDrift
}

// runSim returns the log of one run and the real time of the first instant
// at which two agents believed they held the lease, or zero.
func runSim(t *testing.T, seed uint64, p simParams) (records []Record, overlapAt time.Time) {
	records, overlapAt, _ = runSimStats(t, seed, p)
	return records, overlapAt
}

func runSimStats(t *testing.T, seed uint64, p simParams) (records []Record, overlapAt time.Time, st simStats) {
	t.Helper()
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	log := slog.New(slog.DiscardHandler)

	var nodes []*simNode
	for i := range 3 {
		n := &simNode{rate: simRate(rng, p)}
		n.a = newAgent(p.config(i), log, 0, 0, n.local(simTime(0)))
		nodes = append(nodes, n)
	}
	// The partition leader's clock: an offset that changes with the leader.
	var leaderOffset time.Duration
	nextLeaderChange := simTime(0)
	// Two more join later, at a point where history is already gone.
	for i := 3; i < 5; i++ {
		nodes = append(nodes, &simNode{joinAt: simTime(rng.Int64N(simDuration / 2))})
	}

	var wire []*simMsg

	for now := simTime(0); now.Before(simTime(simDuration)); now = now.Add(simStep) {
		if !now.Before(nextLeaderChange) {
			leaderOffset = time.Duration((2*rng.Float64() - 1) * float64(p.brokerSkew))
			nextLeaderChange = now.Add(time.Duration(rng.Int64N(int64(20 * simTTL))))
		}
		// Land produce requests. LogAppendTime is the landing time by the
		// leader's clock. The acknowledgement takes its own time to get
		// back, and may lose the race against the record being read back.
		rest := wire[:0]
		for _, m := range wire {
			if m.landAt.After(now) {
				rest = append(rest, m)
				continue
			}
			m.rec.Offset = int64(len(records))
			m.rec.Timestamp = now.Add(leaderOffset)
			records = append(records, m.rec)
			m.from.acks = append(m.from.acks, simAck{at: now.Add(time.Duration(rng.IntN(60)) * time.Millisecond), offset: m.rec.Offset})
			slices.SortStableFunc(m.from.acks, func(x, y simAck) int { return x.at.Compare(y.at) })
		}
		wire = rest

		holders := 0
		sites := map[string]bool{}
		for _, n := range nodes {
			if n.a == nil {
				continue
			}
			local := n.local(now)
			if n.a.believing(local) {
				holders++
			}
			if n.a.siteHolding(local) {
				sites[n.a.cfg.Site] = true
				if !n.a.believing(local) {
					st.siteAnswers++
				}
			}
		}
		if (holders > 1 || len(sites) > 1) && overlapAt.IsZero() {
			overlapAt = now
		}

		for i, n := range nodes {
			if n.a == nil {
				if now.Before(n.joinAt) {
					continue
				}
				// The tail is all it will ever see, and part of that with
				// holes.
				n.rate = simRate(rng, p)
				start := max(0, len(records)-rng.IntN(30))
				n.a = newAgent(p.config(i), log, int64(start), int64(len(records)), n.local(now))
				n.a.setStandby(n.local(now), n.standby)
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
			local := n.local(now)
			if p.health {
				// Workloads come and go every twenty TTLs or so, and are
				// probed every step: a report counts only while up.
				if rng.Float64() < 0.0005 {
					n.workloadUp = !n.workloadUp
				}
				if n.workloadUp {
					n.a.healthy(local)
					n.lastReport = local
				}
			}
			if p.standby && rng.Float64() < 0.0005 {
				n.standby = !n.standby
				n.a.setStandby(local, n.standby)
			}
			if rng.Float64() < 0.001 {
				n.partitionedUntil = now.Add(time.Duration(rng.Int64N(int64(2 * simTTL))))
			}

			// Acknowledgements that have arrived.
			for len(n.acks) > 0 && !n.acks[0].at.After(now) {
				n.a.acked(local, n.acks[0].offset, n.acks[0].err)
				n.acks = n.acks[1:]
			}
			if now.Before(n.partitionedUntil) {
				n.a.fetchFailed(errSimTimeout)
				rec, ok := n.a.tick(local)
				if p.health {
					n.checkGate(t, seed, local, rec, ok, &st)
				}
				n.checkStandby(t, seed, local, rec, ok, &st)
				if ok {
					// Sent into the void; the client gives up after a ttl.
					n.acks = append(n.acks, simAck{at: now.Add(simTTL), err: errSimTimeout})
				}
				continue
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
				if err := n.a.record(local, r.Offset, r.Timestamp, value); err != nil {
					t.Fatalf("seed %d: %v", seed, err)
				}
			}
			n.a.fetched(local, int64(len(records)))

			var rec Record
			var ok bool
			if n.a.believing(local) && rng.Float64() < 0.001 {
				rec, ok = n.a.release(local)
			} else {
				rec, ok = n.a.tick(local)
				if p.health {
					n.checkGate(t, seed, local, rec, ok, &st)
				}
				n.checkStandby(t, seed, local, rec, ok, &st)
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
	return records, overlapAt, st
}

func TestSimulationSafetyAndAgreement(t *testing.T) {
	seeds := 200
	if testing.Short() {
		seeds = 20
	}
	var terms, rejected, settled, lateReaders int
	for seed := uint64(1); seed <= uint64(seeds); seed++ {
		records, overlapAt := runSim(t, seed, simParams{})
		if !overlapAt.IsZero() {
			t.Fatalf("seed %d, t=%v: two agents believe they hold the lease", seed, overlapAt.Sub(t0))
		}

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

// Imperfect clocks: a partition leader whose clock is off lets a takeover
// happen early by the difference between two leaders' offsets, and a slow
// client clock stretches the belief. The margin has to cover both:
//
//	margin >= 2 * brokerSkew + ttl * clientDrift
//
// The test shows the model bites (zero margin, overlap in some run) and
// that the bound holds (that margin, no overlap in any run).
func TestSimulationClockSkewNeedsMargin(t *testing.T) {
	seeds := 200
	if testing.Short() {
		seeds = 40
	}
	cases := []struct {
		name string
		p    simParams
	}{
		{"broker skew", simParams{brokerSkew: simTTL / 5}},
		{"client drift", simParams{clientDrift: 0.1}},
		{"both", simParams{brokerSkew: simTTL / 10, clientDrift: 0.05}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bound := 2*tc.p.brokerSkew + time.Duration(float64(simTTL)*tc.p.clientDrift)
			unsafe, safe := tc.p, tc.p
			safe.margin = bound
			overlaps := 0
			for seed := uint64(1); seed <= uint64(seeds); seed++ {
				if _, at := runSim(t, seed, unsafe); !at.IsZero() {
					overlaps++
				}
				if _, at := runSim(t, seed, safe); !at.IsZero() {
					t.Fatalf("seed %d, t=%v: overlap with margin %v", seed, at.Sub(t0), bound)
				}
			}
			t.Logf("margin 0: overlap in %d of %d runs; margin %v: none", overlaps, seeds, bound)
			if overlaps == 0 {
				t.Fatalf("the imperfect clocks never caused an overlap: the model does not bite")
			}
		})
	}
}

// The health gate on top of everything else: workloads that go down and
// come back, holders that release because of it and stay out for the
// cooldown. Safety still holds, an unhealthy holder lets go within
// FailAfter (checkGate), and the gate actually fires.
func TestSimulationHealthGate(t *testing.T) {
	seeds := 100
	if testing.Short() {
		seeds = 20
	}
	var releases, terms int
	for seed := uint64(1); seed <= uint64(seeds); seed++ {
		records, overlapAt, st := runSimStats(t, seed, simParams{health: true})
		if !overlapAt.IsZero() {
			t.Fatalf("seed %d, t=%v: two agents believe they hold the lease", seed, overlapAt.Sub(t0))
		}
		releases += st.gateReleases
		ref := NewViewFromStart(simTTL)
		for _, r := range records {
			out, err := ref.Apply(r)
			if err != nil {
				t.Fatalf("seed %d: %v", seed, err)
			}
			if out == Accepted && r.Kind == Claim {
				terms++
			}
		}
	}
	t.Logf("%d runs: %d terms, %d releases by the health gate", seeds, terms, releases)
	if releases < seeds*5 || terms < seeds*10 {
		t.Fatalf("the gate is too tame to mean anything: %d releases, %d terms", releases, terms)
	}
}

// Sites: two of them, a takeover delay, and everything else as above. Safety
// still holds; a site never holds the lease within the delay after the end
// of another site's term, by the broker's clock: a claim that lands that
// early (sent in time, landed late) is released and never renewed; and a
// site's own nodes do take over within it, after a release or a term that
// ran out.
func TestSimulationSites(t *testing.T) {
	seeds := 100
	if testing.Short() {
		seeds = 20
	}
	var sameSite, crossSite, early int
	minCross := time.Duration(math.MaxInt64)
	for seed := uint64(1); seed <= uint64(seeds); seed++ {
		records, overlapAt := runSim(t, seed, simParams{sites: true})
		if !overlapAt.IsZero() {
			t.Fatalf("seed %d, t=%v: two agents believe they hold the lease", seed, overlapAt.Sub(t0))
		}
		ref := NewViewFromStart(simTTL)
		earlyTerms := map[int64]bool{}
		for _, r := range records {
			prev := ref.State()
			out, err := ref.Apply(r)
			if err != nil {
				t.Fatalf("seed %d: %v", seed, err)
			}
			if out == Accepted && r.Kind == Renew && earlyTerms[r.Epoch] {
				t.Fatalf("seed %d, offset %d: %s renewed term %d, which started within the takeover delay", seed, r.Offset, r.Holder, r.Epoch)
			}
			if out != Accepted || r.Kind != Claim || prev.Expires.IsZero() {
				continue
			}
			gap := r.Timestamp.Sub(prev.Expires)
			if SiteOf(prev.Last) == SiteOf(r.Holder) {
				if gap < simDelay {
					sameSite++
				}
				continue
			}
			if gap < simDelay {
				earlyTerms[r.Offset] = true
				early++
				continue
			}
			crossSite++
			minCross = min(minCross, gap)
		}
	}
	t.Logf("%d runs: %d takeovers by the same site within the delay, %d by another site, the earliest %v after the end; %d claims landed early and were given up",
		seeds, sameSite, crossSite, minCross, early)
	if sameSite < seeds*5 || crossSite < seeds {
		t.Fatalf("too tame to mean anything: %d same-site takeovers, %d cross-site", sameSite, crossSite)
	}
}

// Standby: nodes in two sites switch standby on and off. Safety holds, a
// node in standby never believes and never claims, and standby does give
// terms up.
func TestSimulationStandby(t *testing.T) {
	seeds := 100
	if testing.Short() {
		seeds = 20
	}
	var releases, terms int
	for seed := uint64(1); seed <= uint64(seeds); seed++ {
		records, overlapAt, st := runSimStats(t, seed, simParams{sites: true, standby: true})
		if !overlapAt.IsZero() {
			t.Fatalf("seed %d, t=%v: two agents believe they hold the lease", seed, overlapAt.Sub(t0))
		}
		releases += st.standbyReleases
		ref := NewViewFromStart(simTTL)
		for _, r := range records {
			out, err := ref.Apply(r)
			if err != nil {
				t.Fatalf("seed %d: %v", seed, err)
			}
			if out == Accepted && r.Kind == Claim {
				terms++
			}
		}
	}
	t.Logf("%d runs: %d terms, %d releases in standby", seeds, terms, releases)
	if releases < seeds || terms < seeds*5 {
		t.Fatalf("too tame to mean anything: %d releases in standby, %d terms", releases, terms)
	}
}

// Site answers: every agent of a site that is not the holder believes its
// site holds the lease while it reads the holder's term from the log, as a
// second scaler replica answers KEDA. Safety covers them: agents of two
// sites never believe their sites hold the lease at once, through pauses,
// partitions, lagging reads and late joiners. With a takeover delay of half
// a TTL the delay covers little of a stale read, so only the freshness rule
// keeps a reader that wakes up from a pause from answering for a term long
// gone; the case shows that rule bites.
func TestSimulationSiteAnswers(t *testing.T) {
	seeds := 100
	if testing.Short() {
		seeds = 20
	}
	for _, delay := range []time.Duration{simDelay, simTTL / 2} {
		t.Run(delay.String(), func(t *testing.T) {
			var answers int
			for seed := uint64(1); seed <= uint64(seeds); seed++ {
				_, overlapAt, st := runSimStats(t, seed, simParams{sites: true, delay: delay})
				if !overlapAt.IsZero() {
					t.Fatalf("seed %d, t=%v: two sites believe they hold the lease", seed, overlapAt.Sub(t0))
				}
				answers += st.siteAnswers
			}
			t.Logf("%d runs: %v of answers for the site by agents that do not hold the lease", seeds, time.Duration(answers)*simStep)
			if answers < seeds*int(10*simTTL/simStep) {
				t.Fatalf("too tame to mean anything: %d steps of site answers", answers)
			}
		})
	}
}
