// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"errors"
	"log/slog"
	"testing"
	"time"
)

// Deterministic checks of the writer rules on one agent, for the cases the
// simulation reaches rarely or not at all.

const (
	agMargin = 2 * time.Second
	agRenew  = 3 * time.Second
)

type agentHarness struct {
	t   *testing.T
	a   *agent
	log []Record
	// broker time runs a fixed offset ahead of the agent's clock; the
	// protocol must not care.
	skew time.Duration
}

func newHarness(t *testing.T, start int64) *agentHarness {
	t.Helper()
	cfg, err := Config{Brokers: []string{"x"}, Topic: "x", Holder: "me", TTL: testTTL, Margin: agMargin, RenewEvery: agRenew}.withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	return &agentHarness{t: t, a: newAgent(cfg, slog.New(slog.DiscardHandler), start, start, at(0)), skew: time.Hour}
}

// land appends a record at broker time now+skew and returns its offset.
func (h *agentHarness) land(now time.Time, rec Record) int64 {
	rec.Offset = int64(len(h.log))
	rec.Timestamp = now.Add(h.skew)
	h.log = append(h.log, rec)
	return rec.Offset
}

// read feeds the agent every landed record it has not seen, in order.
func (h *agentHarness) read(now time.Time) {
	h.t.Helper()
	for _, r := range h.log {
		if r.Offset < h.a.nextOffset {
			continue
		}
		value, err := EncodeValue(r)
		if err != nil {
			h.t.Fatal(err)
		}
		if err := h.a.record(now, r.Offset, r.Timestamp, value); err != nil {
			h.t.Fatal(err)
		}
	}
	h.a.fetched(now, int64(len(h.log)))
}

func (h *agentHarness) tick(now time.Time) (Record, bool) {
	h.t.Helper()
	return h.a.tick(now)
}

func (h *agentHarness) wantSend(now time.Time, kind Kind) Record {
	h.t.Helper()
	rec, ok := h.tick(now)
	if !ok || rec.Kind != kind {
		h.t.Fatalf("at %v: tick = (%v, %v), want %v", now.Sub(t0), rec.Kind, ok, kind)
	}
	return rec
}

func (h *agentHarness) wantSilent(now time.Time) {
	h.t.Helper()
	if rec, ok := h.tick(now); ok {
		h.t.Fatalf("at %v: unexpected %v", now.Sub(t0), rec.Kind)
	}
}

func (h *agentHarness) wantBelief(now time.Time, believing bool) {
	h.t.Helper()
	if got := h.a.believing(now); got != believing {
		h.t.Fatalf("at %v: believing = %v, want %v", now.Sub(t0), got, believing)
	}
}

// acquire walks the agent through a claim and returns its epoch.
func (h *agentHarness) acquire(now time.Time) int64 {
	h.t.Helper()
	rec := h.wantSend(now, Claim)
	off := h.land(now.Add(100*time.Millisecond), rec)
	h.a.acked(now.Add(150*time.Millisecond), off, nil)
	h.read(now.Add(200 * time.Millisecond))
	h.wantBelief(now.Add(200*time.Millisecond), true)
	return off
}

func TestAgentDeadlineCountsFromSendTime(t *testing.T) {
	h := newHarness(t, 0)
	sent := at(0)
	rec := h.wantSend(sent, Claim)
	// The record takes a while to land and even longer to be read back.
	off := h.land(at(5), rec)
	h.a.acked(at(6), off, nil)
	h.read(at(7))
	h.wantBelief(at(7), true)
	// send + ttl - margin = 8s, whatever the landing and read-back times.
	h.wantBelief(at(7.999), true)
	h.wantBelief(at(8), false)
	if _, ok := h.tick(at(8)); ok {
		t.Fatal("a renew was sent past the deadline")
	}
}

func TestAgentAckAndReadBackInEitherOrder(t *testing.T) {
	for _, ackFirst := range []bool{true, false} {
		h := newHarness(t, 0)
		rec := h.wantSend(at(0), Claim)
		off := h.land(at(0.1), rec)
		if ackFirst {
			h.a.acked(at(0.2), off, nil)
			h.wantBelief(at(0.2), false) // not before the fold saw it
			h.read(at(0.3))
		} else {
			h.read(at(0.2))
			h.wantBelief(at(0.2), false) // not before the offset is known
			h.a.acked(at(0.3), off, nil)
		}
		h.wantBelief(at(0.3), true)
		if h.a.epoch != off {
			t.Fatalf("ackFirst=%v: epoch = %d, want %d", ackFirst, h.a.epoch, off)
		}
	}
}

func TestAgentIgnoresRecordItGaveUpOn(t *testing.T) {
	h := newHarness(t, 0)
	// The first claim times out on the client...
	stale := h.wantSend(at(0), Claim)
	h.a.acked(at(10), 0, errors.New("delivery timeout"))
	h.wantBelief(at(10), false)
	// ...so the agent claims again, and the new claim is in flight when the
	// stale one turns out to have landed after all and is read back.
	fresh := h.wantSend(at(13.1), Claim)
	h.land(at(13.2), stale)
	h.read(at(13.3))
	h.wantBelief(at(13.3), false)
	// The fresh claim lands next and is rejected: the lease is held by the
	// stale term. The agent must not believe on the strength of either.
	off := h.land(at(13.4), fresh)
	h.a.acked(at(13.5), off, nil)
	h.read(at(13.6))
	h.wantBelief(at(13.6), false)
	if s := h.a.view.State(); s.Holder != "me" || s.Epoch != 0 {
		t.Fatalf("log state = %+v, want the stale term", s)
	}
}

func TestAgentDropsBeliefWhenLogSaysTermEnded(t *testing.T) {
	h := newHarness(t, 0)
	epoch := h.acquire(at(0))
	// Someone else's claim lands after the term ran out in the log, before
	// this agent's deadline (say its clock stalled for a bit).
	h.land(at(10.5), recordOf(Claim, "other", 0))
	h.read(at(7.9))
	h.wantBelief(at(7.9), false)
	// A renew of the dead term is rejected by the fold too.
	h.a.epoch = epoch
	h.a.deadline = at(9)
	renew := h.wantSend(at(8), Renew)
	off := h.land(at(11), renew)
	h.a.acked(at(8.5), off, nil)
	h.read(at(8.6))
	h.wantBelief(at(8.6), false)
}

func TestAgentRenewsWhileBelievingAndNotAfter(t *testing.T) {
	h := newHarness(t, 0)
	h.acquire(at(0))
	h.wantSilent(at(2.9))
	renew := h.wantSend(at(3), Renew)
	if renew.Epoch != 0 {
		t.Fatalf("renew epoch = %d", renew.Epoch)
	}
	h.wantSilent(at(4)) // one request in flight at a time
	off := h.land(at(3.1), renew)
	h.a.acked(at(3.2), off, nil)
	h.read(at(3.3))
	h.wantBelief(at(10.9), true) // 3 + 10 - 2
	h.wantBelief(at(11), false)
	// Past the deadline the agent does not renew, and does not claim
	// either while the log still shows a live term: until 13.1 by the
	// broker's clock, which the agent estimates from the read-back time
	// of the last record (3.3), so it waits until 13.3.
	h.wantSilent(at(11))
	h.wantSilent(at(13.2))
	h.wantSend(at(13.4), Claim)
}

func TestAgentStaysQuietWhileUncertain(t *testing.T) {
	h := newHarness(t, 40) // history before offset 40 is gone
	h.read(at(0))
	if h.a.view.Certain() {
		t.Fatal("certain without evidence")
	}
	for s := 0.0; s < 11; s += 0.5 {
		h.wantSilent(at(s))
	}
	// A whole ttl plus margin of silence at the end of the log settles it.
	h.wantSend(at(12.1), Claim)
}

func TestAgentSilenceRestartsAfterFetchFailure(t *testing.T) {
	h := newHarness(t, 40)
	h.read(at(0))
	h.wantSilent(at(6))
	h.a.fetchFailed(errors.New("connection refused"))
	h.wantSilent(at(12.1)) // silence during the outage proves nothing
	h.read(at(12.2))       // fetching again
	h.wantSilent(at(20))
	h.wantSend(at(24.3), Claim)
}

func TestAgentHoleMakesViewUncertain(t *testing.T) {
	h := newHarness(t, 0)
	h.acquire(at(0))
	// Compaction ate a record between what the agent read and what it
	// reads next: offset 1 never arrives, offset 2 does.
	skipped := recordOf(Renew, "me", 0)
	value, err := EncodeValue(skipped)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.a.record(at(1.1), 2, at(1).Add(h.skew), value); err != nil {
		t.Fatal(err)
	}
	if h.a.view.Certain() {
		t.Fatal("view certain across a hole")
	}
	// The belief stands until its deadline, but nothing is written on an
	// uncertain view: not even a renew.
	h.wantBelief(at(3), true)
	h.wantSilent(at(3))
}

// A stop: the belief goes first, the release second, and nothing is claimed
// afterwards.
func TestAgentStopDropsBeliefFirstThenReleases(t *testing.T) {
	h := newHarness(t, 0)
	epoch := h.acquire(at(0))
	h.a.stop(at(1))
	h.wantBelief(at(1), false)
	if h.a.stopDone(at(1)) {
		t.Fatal("done before the release was written")
	}
	rec := h.wantSend(at(1), Release)
	if rec.Epoch != epoch {
		t.Fatalf("released epoch %d, want %d", rec.Epoch, epoch)
	}
	if h.a.stopDone(at(1.05)) {
		t.Fatal("done before the release was acknowledged")
	}
	off := h.land(at(1.1), rec)
	if err := h.a.acked(at(1.15), off, nil); err != nil {
		t.Fatal(err)
	}
	if !h.a.stopDone(at(1.15)) {
		t.Fatal("not done after the release was acknowledged")
	}
	// The log says free, and a stopping agent still claims nothing.
	h.read(at(1.2))
	h.wantSilent(at(1.3))
}

// Rule 2 holds for a stop too: the release waits for the renew in flight,
// and goes out once that was read back and accepted.
func TestAgentStopWaitsForTheRecordInFlight(t *testing.T) {
	h := newHarness(t, 0)
	epoch := h.acquire(at(0))
	renew := h.wantSend(at(3), Renew)
	h.a.stop(at(3.5))
	h.wantBelief(at(3.5), false)
	h.wantSilent(at(3.5))
	if h.a.stopDone(at(3.5)) {
		t.Fatal("done with a release owed")
	}
	off := h.land(at(3.6), renew)
	if err := h.a.acked(at(3.65), off, nil); err != nil {
		t.Fatal(err)
	}
	h.read(at(3.7))
	if rec := h.wantSend(at(3.7), Release); rec.Epoch != epoch {
		t.Fatalf("released epoch %d, want %d", rec.Epoch, epoch)
	}
}

// A renew that is still in flight when the belief would have run out
// leaves the stop nothing to write: the release would land behind a renew
// nobody read back, and two such records look like an anchor to a reader
// that starts there. The term runs out by itself.
func TestAgentStopWritesNoReleaseBehindALateRenew(t *testing.T) {
	h := newHarness(t, 0)
	h.acquire(at(0)) // the belief lasts until 8, the term until 10.1
	renew := h.wantSend(at(3), Renew)
	h.a.stop(at(3.5))
	h.wantSilent(at(7.9))
	if h.a.stopDone(at(7.9)) {
		t.Fatal("done while the release may still be written")
	}
	if !h.a.stopDone(at(8)) {
		t.Fatal("not done once the belief would have run out")
	}
	h.wantSilent(at(8.1))
	// The renew lands after the term ran out. Read back and rejected, it
	// still brings no release.
	off := h.land(at(15), renew)
	if err := h.a.acked(at(15.05), off, nil); err != nil {
		t.Fatal(err)
	}
	h.read(at(15.1))
	h.wantSilent(at(15.2))
}

// The same for every release: standby gave the belief up with a renew in
// flight, the renew landed after the term ran out and was rejected, and no
// release follows it.
func TestAgentNoReleaseAfterARejectedRenew(t *testing.T) {
	h := newHarness(t, 0)
	h.acquire(at(0))
	renew := h.wantSend(at(3), Renew)
	h.a.setStandby(at(4), true)
	h.wantBelief(at(4), false)
	h.wantSilent(at(4.1)) // rule 2
	off := h.land(at(12), renew)
	if err := h.a.acked(at(12.05), off, nil); err != nil {
		t.Fatal(err)
	}
	h.read(at(12.1))
	h.wantSilent(at(12.2))
}

// A release is written only before the deadline the belief had: standby
// gave the belief up while the view was uncertain, and by the time the
// view is certain again the term may be over.
func TestAgentNoReleasePastTheDeadline(t *testing.T) {
	h := newHarness(t, 0)
	h.acquire(at(0))
	h.a.view.Gap()
	h.a.setStandby(at(1), true)
	h.wantSilent(at(1.1)) // uncertain
	h.a.view = NewViewFromStart(testTTL)
	h.a.nextOffset = 0
	h.read(at(8.5)) // certain again, the term is this agent's, the deadline (8) has passed
	h.wantSilent(at(8.6))
}

// A renew the client gave up on may still land, unread. The term is
// sealed: no further renew and no release, so that two records of one term
// always mean the first was read back. The belief lasts until its
// deadline.
func TestAgentSealsTheTermAfterARenewItGaveUpOn(t *testing.T) {
	h := newHarness(t, 0)
	h.acquire(at(0))
	h.wantSend(at(3), Renew)
	if err := h.a.acked(at(4), 0, errors.New("delivery timeout")); err != nil {
		t.Fatal(err)
	}
	h.wantBelief(at(7.9), true)
	h.wantSilent(at(7)) // no second renew
	h.a.stop(at(7.5))
	h.wantBelief(at(7.5), false)
	h.wantSilent(at(7.5)) // and no release
	if !h.a.stopDone(at(7.5)) {
		t.Fatal("a sealed term owes no release")
	}

	// The other order: the belief given up first, the renew in flight
	// given up on after.
	h = newHarness(t, 0)
	h.acquire(at(0))
	h.wantSend(at(3), Renew)
	h.a.setStandby(at(3.5), true)
	if err := h.a.acked(at(4), 0, errors.New("delivery timeout")); err != nil {
		t.Fatal(err)
	}
	h.wantSilent(at(4.1))
}

// landAt appends a record with a broker time of its own: a partition
// leader whose clock is off.
func (h *agentHarness) landAt(broker time.Time, rec Record) int64 {
	rec.Offset = int64(len(h.log))
	rec.Timestamp = broker.Add(h.skew)
	h.log = append(h.log, rec)
	return rec.Offset
}

// A release is written only for a term the log still shows as live and
// this participant's, whatever the deadline says: a new partition leader
// whose clock runs ahead ends a term early.
func TestAgentNoReleaseForATermTheLogShowsOver(t *testing.T) {
	t.Run("the renew in flight was rejected", func(t *testing.T) {
		h := newHarness(t, 0)
		h.acquire(at(0)) // the term runs until 10.1 by the broker
		renew := h.wantSend(at(3), Renew)
		h.a.setStandby(at(3.5), true)
		// It lands at 3.6 on a leader that says 11.6.
		off := h.landAt(at(11.6), renew)
		if err := h.a.acked(at(3.65), off, nil); err != nil {
			t.Fatal(err)
		}
		h.read(at(3.7))
		h.wantSilent(at(3.8)) // well before the deadline of 8
	})
	t.Run("another holds", func(t *testing.T) {
		h := newHarness(t, 0)
		h.acquire(at(0))
		h.a.view.Gap() // the belief is given up on a view that is not certain
		h.a.setStandby(at(1), true)
		h.wantSilent(at(1.1))
		h.a.view, h.a.nextOffset = NewViewFromStart(testTTL), 0
		// On a leader that says 11, another participant's claim is
		// accepted at 2.
		h.landAt(at(11), recordOf(Claim, "other", 0))
		h.read(at(2.1))
		h.wantSilent(at(2.2))
	})
}

// A stop with a claim in flight waits for it: accepted, the term is
// released at once and never believed.
func TestAgentStopReleasesAClaimInFlight(t *testing.T) {
	h := newHarness(t, 0)
	claim := h.wantSend(at(0), Claim)
	h.a.stop(at(0.05))
	if h.a.stopDone(at(0.05)) {
		t.Fatal("done with a claim in flight")
	}
	off := h.land(at(0.1), claim)
	if err := h.a.acked(at(0.15), off, nil); err != nil {
		t.Fatal(err)
	}
	h.read(at(0.2))
	h.wantBelief(at(0.2), false)
	if rec := h.wantSend(at(0.2), Release); rec.Epoch != off {
		t.Fatalf("released epoch %d, want %d", rec.Epoch, off)
	}
}

// A participant claims only when it has read the log up to its end: before
// that it knows an old state of the lease, or none.
func TestAgentClaimsOnlyWhenCaughtUp(t *testing.T) {
	t.Run("nothing read yet", func(t *testing.T) {
		// A compacted topic starts at offset 0 whatever was removed, so the
		// view is certain from the start: of a lease that is free.
		h := newSiteHarness(t, 0, 30*time.Second)
		h.a.highWater = 5000
		h.wantSilent(at(0.9))
	})
	t.Run("replaying", func(t *testing.T) {
		// Its own site released long ago; a thousand records follow.
		h := newSiteHarness(t, 0, 30*time.Second)
		off := h.land(at(0), recordOf(Claim, "eu/old", 0))
		h.land(at(1), Record{Kind: Release, Holder: "eu/old", Epoch: off, TTL: testTTL})
		h.read(at(100))
		h.a.fetched(at(100), 1002)
		h.wantSilent(at(100.1))
	})
	t.Run("caught up", func(t *testing.T) {
		h := newSiteHarness(t, 0, 30*time.Second)
		off := h.land(at(0), recordOf(Claim, "eu/old", 0))
		h.land(at(1), Record{Kind: Release, Holder: "eu/old", Epoch: off, TTL: testTTL})
		h.read(at(100))
		h.wantSend(at(100.1), Claim)
	})
}

// A release ends the term in the log, but a replica of the holder's site
// that read the last renewal and missed the release answers "my site
// holds" until the term's natural end. Another site waits that out,
// however short its takeover delay.
func TestAgentTakeoverWaitsOutAReleasedTermsNaturalEnd(t *testing.T) {
	const delay = 3 * time.Second // shorter than the ttl
	sibling := newSiteHarness(t, 0, delay)
	other := newSiteHarness(t, 0, delay)
	other.a.cfg.Site, other.a.cfg.Holder = "us", "us/them"
	for _, h := range []*agentHarness{sibling, other} {
		h.land(at(0), recordOf(Claim, "eu/holder", 0))
		h.read(at(0.1))
		h.tick(at(0.1))
		h.land(at(3), recordOf(Renew, "eu/holder", 0)) // the term runs until 13
		h.tick(at(2.9))
		h.read(at(3.1))
		h.tick(at(3.1))
	}
	// The holder releases at 4. The sibling is cut off and never reads it.
	other.land(at(4), Record{Kind: Release, Holder: "eu/holder", Epoch: 0, TTL: testTTL})
	other.read(at(4.1))
	if !sibling.a.siteHolding(at(10.9)) {
		t.Fatal("the sibling does not answer for its site")
	}
	// 4 + delay has long passed; the sibling answers until 11.1 (the read
	// at 3.1, the term's 10 s, less the margin).
	for s := 7.1; s < 14.4; s += 0.5 {
		other.wantSilent(at(s))
		if sibling.a.siteHolding(at(s)) && s > 13 {
			t.Fatalf("at %vs the sibling still answers", s)
		}
	}
	// 13 (the natural end) plus two poll intervals of 0.75 s.
	other.wantSend(at(14.7), Claim)
}

// A gap of a TTL between two records proves the lease free only on top of
// what the brokers' clocks may be apart: the record before the gap may
// carry the time of a leader running behind.
func TestViewSilenceAllowsForBrokerSkew(t *testing.T) {
	// The whole log: a holds from 0 and renews at 9.5 on a leader whose
	// clock runs 1 s ahead of the next one's.
	log := []Record{
		claim(0, 0, "a"),
		renew(1, 9.5, "a", 0),
		claim(2, 8.6, "b"), // rejected; 9.6 by the old leader
		renew(3, 18.7, "a", 0),
	}
	full := NewViewFromStart(testTTL)
	for _, r := range log {
		if _, err := full.Apply(r); err != nil {
			t.Fatal(err)
		}
	}
	if s := full.State(); s.Holder != "a" || !s.HeldAt(full.Clock()) {
		t.Fatalf("full history: %+v, want a holding", s)
	}
	for _, tc := range []struct {
		skew time.Duration
		free bool
	}{{0, true}, {time.Second, false}} {
		late := NewView(testTTL)
		late.AllowSkew(tc.skew)
		for _, r := range log[2:] {
			if _, err := late.Apply(r); err != nil {
				t.Fatal(err)
			}
		}
		if free := late.Certain() && late.State().Holder == ""; free != tc.free {
			t.Fatalf("skew %v: reader from offset 2 calls the lease free = %v, want %v", tc.skew, free, tc.free)
		}
	}
}

// The log this participant follows was replaced by another: the topic was
// deleted and created again, or acknowledged records were lost.
func TestAgentNoticesAReplacedLog(t *testing.T) {
	value := func(r Record) []byte {
		t.Helper()
		v, err := EncodeValue(r)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	t.Run("a newer record at an offset already passed", func(t *testing.T) {
		h := newHarness(t, 0)
		h.acquire(at(0))
		for i := 1; i <= 3; i++ {
			h.renewAt(float64(3 * i))
		}
		rec := value(recordOf(Claim, "other", 0))
		if err := h.a.record(at(20), 0, at(20).Add(h.skew), rec); !errors.Is(err, ErrLogReplaced) {
			t.Fatalf("record at offset 0 with a later timestamp: %v, want ErrLogReplaced", err)
		}
	})
	t.Run("a record read twice", func(t *testing.T) {
		h := newHarness(t, 0)
		h.acquire(at(0))
		first := h.log[0]
		if err := h.a.record(at(1), first.Offset, first.Timestamp, value(first)); err != nil {
			t.Fatalf("duplicate: %v", err)
		}
		h.wantBelief(at(1), true)
	})
	t.Run("an own record acknowledged where another was read", func(t *testing.T) {
		h := newHarness(t, 0)
		h.acquire(at(0))
		h.renewAt(3)
		h.wantSend(at(6), Renew)
		// The new log is empty: the renew lands at offset 0.
		if err := h.a.acked(at(6.1), 0, nil); !errors.Is(err, ErrLogReplaced) {
			t.Fatalf("acknowledged at offset 0, read up to %d: %v, want ErrLogReplaced", h.a.nextOffset, err)
		}
	})
	t.Run("read back before the acknowledgement", func(t *testing.T) {
		h := newHarness(t, 0)
		h.acquire(at(0))
		rec := h.wantSend(at(3), Renew)
		off := h.land(at(3.1), rec)
		h.read(at(3.2))
		if err := h.a.acked(at(3.3), off, nil); err != nil {
			t.Fatalf("the usual race: %v", err)
		}
		h.wantBelief(at(10.9), true)
	})
}

func TestAgentBacksOffAfterRejectedClaim(t *testing.T) {
	h := newHarness(t, 0)
	h.land(at(0), recordOf(Claim, "other", 0))
	// The agent has not read the log yet, so it claims blindly...
	rec := h.wantSend(at(0.1), Claim)
	off := h.land(at(0.2), rec)
	h.a.acked(at(0.3), off, nil)
	h.read(at(0.4))
	h.wantBelief(at(0.4), false)
	// ...and then waits for the other term to run out instead of spamming.
	for s := 0.5; s < 10; s += 0.5 {
		h.wantSilent(at(s))
	}
	h.wantSend(at(10.3), Claim)
}

func newGatedHarness(t *testing.T, gate HealthGate) *agentHarness {
	t.Helper()
	h := newHarness(t, 0)
	h.a.cfg.Health = gate
	return h
}

func TestAgentHealthGateOffByDefault(t *testing.T) {
	h := newHarness(t, 0)
	h.acquire(at(0))
	// Nobody ever reports the workload healthy, and the holder keeps
	// renewing.
	for s := 3.0; s < 60; s += 3 {
		renew := h.wantSend(at(s), Renew)
		off := h.land(at(s+0.1), renew)
		h.a.acked(at(s+0.2), off, nil)
		h.read(at(s + 0.3))
	}
	h.wantBelief(at(60), true)
}

// renewAt walks one renew through the log.
func (h *agentHarness) renewAt(s float64) {
	h.t.Helper()
	renew := h.wantSend(at(s), Renew)
	off := h.land(at(s+0.1), renew)
	h.a.acked(at(s+0.2), off, nil)
	h.read(at(s + 0.3))
}

func TestAgentHealthGateReleasesAndCoolsDown(t *testing.T) {
	h := newGatedHarness(t, HealthGate{FailAfter: 20 * time.Second, Cooldown: 60 * time.Second})
	epoch := h.acquire(at(0)) // the term starts when the claim settles, at 0.2
	// Healthy at 5, then never again: the gate counts from there.
	h.a.healthy(at(5))
	for _, s := range []float64{3, 6, 9, 12, 15, 18, 21, 24} {
		h.renewAt(s)
	}
	h.wantBelief(at(24.9), true)
	rel := h.wantSend(at(25), Release)
	if rel.Epoch != epoch {
		t.Fatalf("release epoch = %d, want %d", rel.Epoch, epoch)
	}
	h.wantBelief(at(25), false)
	if s, _, _ := h.a.status(at(25)); !s.CooldownUntil.Equal(at(85)) {
		t.Fatalf("cooldown until %v, want 85s", s.CooldownUntil.Sub(t0))
	}
	off := h.land(at(25.1), rel)
	h.a.acked(at(25.2), off, nil)
	h.read(at(25.3))
	if h.a.view.State().HeldAt(h.a.view.Clock()) {
		t.Fatal("the release did not end the term in the log")
	}
	// The lease is free, but the participant stays out until the cooldown
	// is over, whatever the workload does meanwhile.
	h.a.healthy(at(30))
	for s := 25.5; s < 85; s += 0.5 {
		h.wantSilent(at(s))
	}
	if s, _, _ := h.a.status(at(85)); !s.CooldownUntil.IsZero() {
		t.Fatalf("cooldown still reported after it ended: %v", s.CooldownUntil.Sub(t0))
	}
	h.wantSend(at(85.1), Claim)
}

func TestAgentHealthGateGivesANewTermTimeToComeUp(t *testing.T) {
	h := newGatedHarness(t, HealthGate{FailAfter: 10 * time.Second, Cooldown: time.Minute})
	// A healthy report from long before the term counts for nothing, and
	// does not trip the gate at once either.
	h.a.healthy(at(0))
	h.land(at(0), recordOf(Claim, "other", 0))
	h.read(at(0.1))
	h.acquire(at(10.2)) // settles at 10.4
	h.renewAt(13.2)
	h.renewAt(16.2)
	h.renewAt(19.2)
	h.wantBelief(at(20.3), true)
	if rec, ok := h.tick(at(20.4)); !ok || rec.Kind != Release {
		t.Fatalf("at 20.4: tick = (%v, %v), want a release ten seconds into the term", rec.Kind, ok)
	}
}

func TestAgentHealthGateKeepsAHealthyHolder(t *testing.T) {
	h := newGatedHarness(t, HealthGate{FailAfter: 10 * time.Second, Cooldown: time.Minute})
	h.acquire(at(0))
	for s := 3.0; s < 60; s += 3 {
		h.a.healthy(at(s - 1))
		h.renewAt(s)
	}
	h.wantBelief(at(60), true)
}

func TestAgentHealthGateReleaseWaitsForTheRecordInFlight(t *testing.T) {
	h := newGatedHarness(t, HealthGate{FailAfter: 5 * time.Second, Cooldown: time.Minute})
	h.acquire(at(0)) // the term starts at 0.2
	renew := h.wantSend(at(3), Renew)
	// The gate trips at 5.2 while the renew is still in flight: the belief
	// goes at once, the release waits for its turn.
	h.wantSilent(at(5.3))
	h.wantBelief(at(5.3), false)
	off := h.land(at(5.4), renew)
	h.a.acked(at(5.5), off, nil)
	h.read(at(5.6))
	h.wantBelief(at(5.6), false) // the accepted renew does not bring it back
	h.wantSend(at(5.7), Release)
	h.wantSilent(at(5.8))
}

// newSiteHarness is an agent of site "eu" that waits delay after another
// site's term.
func newSiteHarness(t *testing.T, start int64, delay time.Duration) *agentHarness {
	t.Helper()
	h := newHarness(t, start)
	cfg := h.a.cfg
	cfg.Site, cfg.Holder, cfg.TakeoverDelay = "eu", "eu/me", delay
	h.a = newAgent(cfg, slog.New(slog.DiscardHandler), start, start, at(0))
	return h
}

// ownSiteReleased puts a term of another participant of the agent's site
// into the log before at(0), released, and reads it: the agent may claim at
// once.
func (h *agentHarness) ownSiteReleased() {
	h.t.Helper()
	epoch := h.land(at(-1), recordOf(Claim, "eu/old", 0))
	h.land(at(-0.5), recordOf(Release, "eu/old", epoch))
	h.read(at(-0.4))
}

func TestAgentTakeoverDelay(t *testing.T) {
	const delay = 30 * time.Second
	claim := func(h *agentHarness, sec float64, holder string) {
		h.land(at(sec), recordOf(Claim, holder, 0))
	}
	release := func(h *agentHarness, sec float64, holder string) {
		h.land(at(sec), recordOf(Release, holder, 0))
	}
	// The broker's clock is estimated from the read-back time of the last
	// record, read 0.2 s after it landed: a term that ended at broker time
	// e may be taken over by another site from e + 30.2.
	cases := []struct {
		name string
		// start is where the agent's log begins; 1 hides the claim that
		// started the term, so the view settles by silence alone.
		start int64
		setup func(h *agentHarness)
		// silent is the last moment checked for silence, 0 for none.
		silent, sendsAt float64
	}{
		{"another site released", 0, func(h *agentHarness) { claim(h, 0, "us/x"); release(h, 1, "us/x") }, 31.1, 31.3},
		{"the own site released", 0, func(h *agentHarness) { claim(h, 0, "eu/old"); release(h, 1, "eu/old") }, 0, 1.3},
		{"another site's term expired", 0, func(h *agentHarness) { claim(h, 0, "us/x"); claim(h, 1, "us/y") }, 40.1, 40.3},
		{"the own site's term expired", 0, func(h *agentHarness) { claim(h, 0, "eu/old"); claim(h, 1, "us/y") }, 10.1, 10.3},
		// Nothing was read for a ttl plus margin after the renew at 1: the
		// unknown term ended by 13 at the latest, a ttl after the renew and
		// the margin for the brokers' clocks on top.
		{"unknown holder, settled by silence", 1, func(h *agentHarness) {
			claim(h, 0, "eu/old")
			h.land(at(1), recordOf(Renew, "eu/old", 0))
		}, 43.1, 43.3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newSiteHarness(t, tc.start, delay)
			tc.setup(h)
			last := h.log[len(h.log)-1].Timestamp.Add(-h.skew)
			h.read(last.Add(200 * time.Millisecond))
			for s := last.Sub(t0).Seconds() + 0.3; s < tc.silent; s += 0.5 {
				h.wantSilent(at(s))
			}
			if tc.silent > 0 {
				h.wantSilent(at(tc.silent))
			}
			h.wantSend(at(tc.sendsAt), Claim)
		})
	}

	// Not one record left of a history that existed: the delay counts from
	// the silence that settled the view, on the agent's own clock.
	h := newSiteHarness(t, 40, delay)
	h.read(at(0))
	h.wantSilent(at(12.1))
	h.wantSilent(at(42.1))
	h.wantSend(at(42.2), Claim)

	// Without a delay, another site's term is no different.
	h = newSiteHarness(t, 0, 0)
	claim(h, 0, "us/x")
	release(h, 1, "us/x")
	h.read(at(1.2))
	h.wantSend(at(1.3), Claim)
}

// An empty log read from its start may be a topic deleted and created
// again, whose holder on the old one still runs its workload: a term of an
// unknown holder, as after silence. The delay counts from the moment the
// view became certain, on the agent's own clock, whatever the agent's site.
func TestAgentTakeoverDelayOnAnEmptyLog(t *testing.T) {
	const delay = 30 * time.Second
	h := newSiteHarness(t, 0, delay)
	for s := 0.0; s <= 30; s += 0.5 {
		h.read(at(s))
		h.wantSilent(at(s))
	}
	h.wantSend(at(30.1), Claim)

	// Certain from the start, though created later: the delay counts from
	// then.
	h = newSiteHarness(t, 0, delay)
	h.a = newAgent(h.a.cfg, slog.New(slog.DiscardHandler), 0, 0, at(5))
	h.wantSilent(at(35))
	h.wantSend(at(35.1), Claim)

	// A term in the log is judged by the rules for it: here its own site's,
	// released, which it takes over at once.
	h = newSiteHarness(t, 0, delay)
	h.land(at(1), recordOf(Claim, "eu/old", 0))
	h.land(at(2), recordOf(Release, "eu/old", 0))
	h.read(at(2.2))
	h.wantSend(at(2.3), Claim)

	// A log with records in it, read from its start, is no empty log.
	h = newSiteHarness(t, 0, delay)
	h.land(at(0), recordOf(Claim, "eu/old", 0))
	h.land(at(1), recordOf(Release, "eu/old", 0))
	h.a = newAgent(h.a.cfg, slog.New(slog.DiscardHandler), 0, int64(len(h.log)), at(1.1))
	h.read(at(1.2))
	h.wantSend(at(1.3), Claim)

	// Without a delay, an empty log is claimed at once.
	h = newSiteHarness(t, 0, 0)
	h.wantSend(at(0), Claim)
}

func TestAgentReadyOnceCertain(t *testing.T) {
	ready := func(h *agentHarness) bool { s, _, _ := h.a.status(at(0)); return s.Ready }
	if h := newHarness(t, 0); !ready(h) {
		t.Fatal("not ready on an empty log")
	}
	// A log read from its start is certain at once, but says nothing until
	// it has been read to its end.
	h := newHarness(t, 0)
	h.land(at(0), recordOf(Claim, "other", 0))
	h.land(at(1), recordOf(Renew, "other", 0))
	h.a = newAgent(h.a.cfg, slog.New(slog.DiscardHandler), 0, int64(len(h.log)), at(1.1))
	if ready(h) {
		t.Fatal("ready before reading a log with records in it")
	}
	h.read(at(1.2))
	if !ready(h) {
		t.Fatal("not ready on a log read to its end")
	}
	h = newHarness(t, 40)
	h.read(at(0))
	if ready(h) {
		t.Fatal("ready before the view was ever certain")
	}
	h.wantSend(at(12.1), Claim) // silence settles it
	if !ready(h) {
		t.Fatal("not ready on a certain view")
	}
	h.a.view.Gap()
	if !ready(h) {
		t.Fatal("ready is not sticky: a hole later makes it unknown again")
	}
}

func TestAgentGivesUpAClaimThatLandedEarly(t *testing.T) {
	h := newSiteHarness(t, 0, 30*time.Second)
	h.land(at(0), recordOf(Claim, "us/x", 0))
	h.land(at(1), recordOf(Release, "us/x", 0))
	h.read(at(1.2))
	rec := h.wantSend(at(31.3), Claim)
	// While the claim is on its way, the other site takes the lease back
	// for a moment: its own nodes need not wait.
	h.land(at(31.4), recordOf(Claim, "us/y", 0))
	h.land(at(31.5), recordOf(Release, "us/y", 2))
	off := h.land(at(31.6), rec)
	h.a.acked(at(31.7), off, nil)
	h.read(at(31.8))
	if s := h.a.view.State(); s.Holder != "eu/me" || s.Epoch != off {
		t.Fatalf("log state = %+v, want the claim accepted", s)
	}
	// Accepted in the log, 0.1 s after the other site's term: not believed,
	// and released at once.
	h.wantBelief(at(31.8), false)
	if r := h.wantSend(at(31.9), Release); r.Epoch != off {
		t.Fatalf("release of epoch %d, want %d", r.Epoch, off)
	}
}

func TestAgentTakesOverOnlyWhenCaughtUp(t *testing.T) {
	h := newSiteHarness(t, 0, 30*time.Second)
	h.land(at(0), recordOf(Claim, "us/x", 0))
	h.land(at(1), recordOf(Release, "us/x", 0))
	h.read(at(1.2))
	// The log has grown, but the record has not arrived yet: it may be a
	// new term of the other site.
	h.land(at(31), recordOf(Claim, "us/y", 0))
	h.a.fetched(at(31.3), int64(len(h.log)))
	h.wantSilent(at(31.3))
	h.read(at(31.4))
	h.wantSilent(at(31.5))
	// The own site waits for the record too: it may be a new term of
	// anyone. Here it is a renew of the term that was released, rejected.
	h = newSiteHarness(t, 0, 30*time.Second)
	h.land(at(0), recordOf(Claim, "eu/x", 0))
	h.land(at(1), recordOf(Release, "eu/x", 0))
	h.read(at(1.2))
	h.a.fetched(at(1.3), int64(len(h.log))+1)
	h.wantSilent(at(1.3))
	h.land(at(1.31), recordOf(Renew, "eu/x", 0))
	h.read(at(1.4))
	h.wantSend(at(1.4), Claim)
}

func TestAgentStandbyNeverClaims(t *testing.T) {
	h := newHarness(t, 0)
	h.a.setStandby(at(0), true)
	// The lease is free and the view certain, for a long while.
	for s := 0.0; s < 60; s += 0.5 {
		h.wantSilent(at(s))
	}
	if s, _, _ := h.a.status(at(60)); !s.Standby || !s.Ready {
		t.Fatalf("status = %+v, want standby and ready", s)
	}
	// It follows the log all the same.
	h.land(at(60), recordOf(Claim, "other", 0))
	h.read(at(60.1))
	if s := h.a.view.State(); s.Holder != "other" {
		t.Fatalf("log state = %+v, want other holding", s)
	}
	h.a.setStandby(at(61), false)
	h.wantSilent(at(61)) // held by other
	h.wantSend(at(70.5), Claim)
}

func TestAgentStandbyReleasesATermHeld(t *testing.T) {
	h := newHarness(t, 0)
	epoch := h.acquire(at(0))
	h.renewAt(3)
	h.a.setStandby(at(4), true)
	// The belief goes at once, the release follows at the next tick.
	h.wantBelief(at(4), false)
	rel := h.wantSend(at(4), Release)
	if rel.Epoch != epoch {
		t.Fatalf("release epoch = %d, want %d", rel.Epoch, epoch)
	}
	off := h.land(at(4.1), rel)
	h.a.acked(at(4.2), off, nil)
	h.read(at(4.3))
	if h.a.view.State().HeldAt(h.a.view.Clock()) {
		t.Fatal("the release did not end the term in the log")
	}
	// Free, and its own site's: still no claim.
	for s := 4.5; s < 60; s += 0.5 {
		h.wantSilent(at(s))
	}
}

func TestAgentStandbyReleaseWaitsForTheRecordInFlight(t *testing.T) {
	h := newHarness(t, 0)
	h.acquire(at(0))
	renew := h.wantSend(at(3), Renew)
	h.a.setStandby(at(3.1), true)
	h.wantBelief(at(3.1), false)
	h.wantSilent(at(3.2))
	off := h.land(at(3.3), renew)
	h.a.acked(at(3.4), off, nil)
	h.read(at(3.5))
	h.wantBelief(at(3.5), false) // the accepted renew does not bring it back
	h.wantSend(at(3.6), Release)
	h.wantSilent(at(3.7))
}

// A claim sent before standby was switched on and accepted after: never
// believed, not even for a moment, and released.
func TestAgentStandbyGivesUpAClaimInFlight(t *testing.T) {
	h := newHarness(t, 0)
	claim := h.wantSend(at(0), Claim)
	h.a.setStandby(at(0.05), true)
	off := h.land(at(0.1), claim)
	h.a.acked(at(0.15), off, nil)
	h.read(at(0.2))
	h.wantBelief(at(0.2), false)
	if s, own, _ := h.a.status(at(0.2)); !own.deadline.IsZero() || s.Holding {
		t.Fatalf("status = %+v, deadline %v: reports holding", s, own.deadline)
	}
	if r := h.wantSend(at(0.3), Release); r.Epoch != off {
		t.Fatalf("release of epoch %d, want %d", r.Epoch, off)
	}
}

// siteRecord lands a record of another participant at sec, with a tick just
// before and the read just after, as the loop of a reader at the end of the
// log does.
func (h *agentHarness) siteRecord(sec float64, rec Record) int64 {
	h.t.Helper()
	h.tick(at(sec - 0.1))
	off := h.land(at(sec), rec)
	h.read(at(sec + 0.1))
	return off
}

func (h *agentHarness) wantSite(now time.Time, holding bool, epoch int64) {
	h.t.Helper()
	_, _, site := h.a.status(now)
	if got := h.a.siteHolding(now); got != holding || (holding && site.epoch != epoch) {
		h.t.Fatalf("at %v: site holding = %v, epoch %d; want %v, epoch %d", now.Sub(t0), got, site.epoch, holding, epoch)
	}
}

func TestAgentSiteBelief(t *testing.T) {
	const delay = 30 * time.Second
	newSibling := func(t *testing.T, delay time.Duration) *agentHarness {
		h := newSiteHarness(t, 0, delay)
		h.a.nextClaimAt = at(1000) // a reader, not a candidate, for now
		return h
	}

	t.Run("follows a term of its site", func(t *testing.T) {
		h := newSibling(t, delay)
		epoch := h.siteRecord(1, recordOf(Claim, "eu/b", 0))
		// Read at 1.1: the term counts from there for ttl - margin.
		h.wantSite(at(9.09), true, epoch)
		h.wantSite(at(9.1), false, 0)
		h.siteRecord(4, recordOf(Renew, "eu/b", epoch))
		h.wantSite(at(12.09), true, epoch)
		h.wantBelief(at(12.09), false)
		h.siteRecord(5, recordOf(Release, "eu/b", epoch))
		h.wantSite(at(5.1), false, 0)
	})

	t.Run("a stale read extends nothing", func(t *testing.T) {
		h := newSibling(t, delay)
		epoch := h.siteRecord(1, recordOf(Claim, "eu/b", 0))
		// Frozen from 1.1 to 8: the renew of 4 is read at 8, far from the
		// last tick, and might be any age.
		h.land(at(4), recordOf(Renew, "eu/b", epoch))
		h.read(at(8))
		h.wantSite(at(9.09), true, epoch)
		h.wantSite(at(9.1), false, 0)
		// So does a read behind the end of the log: the tick at 9.9 saw
		// the renew of 9.5 landed but not read.
		h.land(at(9.5), recordOf(Renew, "eu/b", epoch))
		h.a.fetched(at(9.6), int64(len(h.log)))
		h.tick(at(9.9))
		h.read(at(10))
		h.wantSite(at(10), false, 0)
		// The next fresh renew does.
		h.siteRecord(12, recordOf(Renew, "eu/b", epoch))
		h.wantSite(at(20.09), true, epoch)
	})

	t.Run("not for another site, without a delay, or in standby", func(t *testing.T) {
		h := newSibling(t, delay)
		h.siteRecord(1, recordOf(Claim, "us/x", 0))
		h.wantSite(at(1.2), false, 0)

		h = newSibling(t, 0)
		h.siteRecord(1, recordOf(Claim, "eu/b", 0))
		h.wantSite(at(1.2), false, 0)

		h = newSibling(t, delay)
		epoch := h.siteRecord(1, recordOf(Claim, "eu/b", 0))
		h.wantSite(at(1.2), true, epoch)
		h.a.setStandby(at(1.3), true)
		h.wantSite(at(1.3), false, 0)
		h.siteRecord(4, recordOf(Renew, "eu/b", epoch))
		h.wantSite(at(4.2), false, 0)
	})

	t.Run("the own term is the site's", func(t *testing.T) {
		h := newSiteHarness(t, 0, delay)
		h.ownSiteReleased()
		epoch := h.acquire(at(0))
		if !h.a.siteHolding(at(0.2)) {
			t.Fatal("the holder does not count its own term as its site's")
		}
		if _, own, site := h.a.status(at(0.2)); own.epoch != epoch || !site.deadline.IsZero() {
			t.Fatalf("own %+v, site %+v: want the own belief only", own, site)
		}
	})

	t.Run("takes over for its site at once", func(t *testing.T) {
		h := newSiteHarness(t, 0, delay)
		h.a.nextClaimAt = at(5)
		epoch := h.siteRecord(1, recordOf(Claim, "eu/b", 0))
		h.siteRecord(6, recordOf(Release, "eu/b", epoch))
		h.wantSend(at(6.2), Claim)
	})
}

// The health gate on a participant whose site another one holds: it probes
// the same workload, and when that has been unhealthy for FailAfter it
// claims nothing for the cooldown, so the holder's release moves the lease
// to another site rather than around this one.
func TestAgentHealthGateOfASibling(t *testing.T) {
	gate := HealthGate{FailAfter: 10 * time.Second, Cooldown: time.Minute}
	for _, healthy := range []bool{false, true} {
		h := newSiteHarness(t, 0, 30*time.Second)
		h.a.cfg.Health = gate
		h.a.nextClaimAt = at(1000)
		epoch := h.siteRecord(1, recordOf(Claim, "eu/b", 0)) // the site's term starts at 1.1
		h.a.healthy(at(2))
		for s := 4.0; s < 15; s += 3 {
			if healthy {
				h.a.healthy(at(s - 0.5))
			}
			h.siteRecord(s, recordOf(Renew, "eu/b", epoch))
		}
		s, _, _ := h.a.status(at(13))
		if tripped := !s.CooldownUntil.IsZero(); tripped == healthy {
			t.Fatalf("healthy=%v: cooldown until %v", healthy, s.CooldownUntil.Sub(t0))
		}
		// Tripped, it still answers for its site while the holder holds.
		h.wantSite(at(13), true, epoch)
		h.a.nextClaimAt = time.Time{}
		h.siteRecord(14, recordOf(Release, "eu/b", epoch))
		if healthy {
			h.wantSend(at(14.2), Claim)
			continue
		}
		for s := 14.2; s < 72; s += 0.5 {
			h.wantSilent(at(s))
		}
	}
}

// A term of the site that follows another within a TTL continues the run
// the health gate counts from: taking over for an unhealthy site buys its
// workload no time.
func TestAgentHealthGateCountsAcrossTheSite(t *testing.T) {
	h := newSiteHarness(t, 0, 30*time.Second)
	h.a.cfg.Health = HealthGate{FailAfter: 10 * time.Second, Cooldown: time.Minute}
	h.a.nextClaimAt = at(1000)
	epoch := h.siteRecord(1, recordOf(Claim, "eu/b", 0)) // the run starts at 1.1
	h.siteRecord(4, recordOf(Release, "eu/b", epoch))
	h.a.nextClaimAt = time.Time{}
	h.acquire(at(4.2))
	h.renewAt(7.2)
	h.renewAt(10.2)
	h.wantBelief(at(11), true)
	if rec, ok := h.tick(at(11.2)); !ok || rec.Kind != Release {
		t.Fatalf("at 11.2: tick = (%v, %v), want the release ten seconds into the site's run", rec.Kind, ok)
	}
}

// A yield from the site of the term that ended last ends the takeover
// delay for another site: from the yield, but not before the term would
// have run out unrenewed plus two poll intervals (0.75 s each here). Any
// other yield changes nothing. Records are read 0.2 s after the last one
// landed, so the broker's clock is estimated 0.2 s behind the agent's.
func TestAgentYield(t *testing.T) {
	const delay = 30 * time.Second
	type landed struct {
		sec float64
		rec Record
	}
	cases := []struct {
		name string
		log  []landed
		// silent is the last moment checked for silence; sendsAt the claim.
		silent, sendsAt float64
	}{
		{"yield after a release", []landed{{0, recordOf(Claim, "us/x", 0)}, {3, recordOf(Renew, "us/x", 0)}, {4, recordOf(Release, "us/x", 0)}, {6, recordOf(Yield, "us/y", 0)}},
			14.6, 14.8}, // the renew at 3 runs out at 13, + 1.5
		{"yield later than the natural end", []landed{{0, recordOf(Claim, "us/x", 0)}, {1, recordOf(Release, "us/x", 0)}, {20, recordOf(Yield, "us/x", 0)}},
			0, 20.3},
		{"yield after an expiry", []landed{{0, recordOf(Claim, "us/x", 0)}, {12, recordOf(Yield, "us/x", 0)}},
			12.1, 12.3}, // 10 + 1.5 = 11.5 has passed by the time it is read
		{"yield by another site", []landed{{0, recordOf(Claim, "us/x", 0)}, {1, recordOf(Release, "us/x", 0)}, {2, recordOf(Yield, "ap/z", 0)}},
			31.1, 31.3},
		{"yield by the claimant's own site", []landed{{0, recordOf(Claim, "us/x", 0)}, {1, recordOf(Release, "us/x", 0)}, {2, recordOf(Yield, "eu/z", 0)}},
			31.1, 31.3},
		{"yield of another term", []landed{{0, recordOf(Claim, "us/x", 0)}, {1, recordOf(Release, "us/x", 0)}, {2, recordOf(Yield, "us/x", 5)}},
			31.1, 31.3},
		{"yield while the term was held", []landed{{0, recordOf(Claim, "us/x", 0)}, {1, recordOf(Yield, "us/x", 0)}, {2, recordOf(Release, "us/x", 0)}},
			32.1, 32.3},
		// The yield names the term before the last: it lands late.
		{"yield after a new term", []landed{{0, recordOf(Claim, "us/x", 0)}, {1, recordOf(Release, "us/x", 0)}, {2, recordOf(Claim, "us/y", 0)}, {3, recordOf(Release, "us/y", 2)}, {4, recordOf(Yield, "us/x", 0)}},
			33.1, 33.3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newSiteHarness(t, 0, delay)
			for _, l := range tc.log {
				h.land(at(l.sec), l.rec)
			}
			last := at(tc.log[len(tc.log)-1].sec)
			h.read(last.Add(200 * time.Millisecond))
			for s := last.Sub(t0).Seconds() + 0.3; s < tc.silent; s += 0.5 {
				h.wantSilent(at(s))
			}
			if tc.silent > 0 {
				h.wantSilent(at(tc.silent))
			}
			claim := h.wantSend(at(tc.sendsAt), Claim)
			// A claim that lands after the yield took effect is no early
			// claim: it is believed, not given up.
			off := h.land(at(tc.sendsAt+0.1), claim)
			h.a.acked(at(tc.sendsAt+0.15), off, nil)
			h.read(at(tc.sendsAt + 0.2))
			h.wantBelief(at(tc.sendsAt+0.2), true)
		})
	}

	// A hole hides a renewal, so the term ran longer than the claim
	// said: the view anchors again on the renew and the release, but the
	// term's natural end is not known, and the yield does not count.
	h := newSiteHarness(t, 0, delay)
	h.land(at(0), recordOf(Claim, "us/x", 0))
	h.land(at(3), recordOf(Renew, "us/x", 0)) // compacted away
	h.land(at(5), recordOf(Renew, "us/x", 0))
	h.land(at(6), recordOf(Release, "us/x", 0))
	h.land(at(7), recordOf(Yield, "us/x", 0))
	for _, r := range h.log {
		if r.Offset == 1 {
			continue
		}
		value, err := EncodeValue(r)
		if err != nil {
			t.Fatal(err)
		}
		if err := h.a.record(at(7.2), r.Offset, r.Timestamp, value); err != nil {
			t.Fatal(err)
		}
	}
	h.a.fetched(at(7.2), int64(len(h.log)))
	if !h.a.view.Certain() {
		t.Fatal("the view did not anchor again")
	}
	h.wantSilent(at(17.2)) // 15 + 1.5 has passed, but the yield does not count
	h.wantSilent(at(36.1))
	h.wantSend(at(36.3), Claim)

	// A yield read on a view that is not certain does not count: the
	// claim that started the term is gone, so the view settles by silence
	// (at 2.2 + ttl + margin), on a term that ended by 1 + ttl + margin at
	// the latest, and the delay counts from there.
	h = newSiteHarness(t, 1, delay)
	h.land(at(0), recordOf(Claim, "us/x", 0))
	h.land(at(1), recordOf(Release, "us/x", 0))
	h.land(at(2), recordOf(Yield, "us/x", 0))
	h.read(at(2.2))
	h.wantSilent(at(14.3))
	h.wantSilent(at(43.1))
	h.wantSend(at(43.3), Claim)
}

// The handover on the side that yields: in standby, once its site's term
// has ended, the agent reports it, and writes a yield for it on request,
// once, and only while the handover still stands.
func TestAgentHandover(t *testing.T) {
	const delay = 30 * time.Second
	handover := func(h *agentHarness, now time.Time) (int64, bool) {
		s, _, _ := h.a.status(now)
		if s.Handover != (s.HandoverEpoch >= 0) {
			h.t.Fatalf("status = %+v: handover and its epoch disagree", s)
		}
		return s.HandoverEpoch, s.Handover
	}
	standbyAfterTerm := func(t *testing.T) (*agentHarness, int64) {
		h := newSiteHarness(t, 0, delay)
		h.ownSiteReleased()
		epoch := h.acquire(at(0))
		if _, ok := handover(h, at(1)); ok {
			t.Fatal("handover while holding")
		}
		h.a.setStandby(at(2), true)
		rel := h.wantSend(at(2), Release)
		off := h.land(at(2.1), rel)
		h.a.acked(at(2.15), off, nil)
		h.read(at(2.2))
		return h, epoch
	}

	t.Run("yields once on request", func(t *testing.T) {
		h, epoch := standbyAfterTerm(t)
		if e, ok := handover(h, at(2.3)); !ok || e != epoch {
			t.Fatalf("handover = %d, %v; want %d", e, ok, epoch)
		}
		h.wantSilent(at(2.3)) // nothing without a request
		h.a.requestYield(at(5), epoch)
		y := h.wantSend(at(5), Yield)
		if y.Epoch != epoch || y.Holder != "eu/me" {
			t.Fatalf("yield = %+v", y)
		}
		off := h.land(at(5.1), y)
		h.a.acked(at(5.15), off, nil)
		h.read(at(5.2))
		if _, ok := handover(h, at(5.3)); ok {
			t.Fatal("handover still due after the yield landed")
		}
		h.a.requestYield(at(5.3), epoch)
		h.wantSilent(at(5.3))
	})
	t.Run("a request for another term", func(t *testing.T) {
		h, epoch := standbyAfterTerm(t)
		h.a.requestYield(at(3), epoch+1)
		h.wantSilent(at(3))
	})
	t.Run("standby off before the yield goes out", func(t *testing.T) {
		h, epoch := standbyAfterTerm(t)
		h.a.requestYield(at(3), epoch)
		h.a.setStandby(at(3), false)
		if rec, ok := h.tick(at(3)); ok && rec.Kind == Yield {
			t.Fatal("yield sent out of standby")
		}
	})
	t.Run("not after the delay", func(t *testing.T) {
		h, epoch := standbyAfterTerm(t)
		// The release landed at 2 on the agent's clock; the broker's clock
		// is estimated from its read-back at 2.2.
		if _, ok := handover(h, at(32.1)); !ok {
			t.Fatal("no handover just before the delay ran out")
		}
		if _, ok := handover(h, at(32.2)); ok {
			t.Fatal("handover after the delay ran out")
		}
		h.a.requestYield(at(32.2), epoch)
		h.wantSilent(at(32.2))
	})
	t.Run("not while another site holds", func(t *testing.T) {
		h, _ := standbyAfterTerm(t)
		epoch := h.land(at(3), recordOf(Claim, "eu/other", 0))
		h.read(at(3.1))
		if _, ok := handover(h, at(3.2)); ok {
			t.Fatal("handover while its own site holds again")
		}
		h.land(at(4), recordOf(Release, "eu/other", epoch))
		h.read(at(4.1))
		if e, ok := handover(h, at(4.2)); !ok || e != epoch {
			t.Fatalf("handover = %d, %v; want the new term %d", e, ok, epoch)
		}
		h.land(at(5), recordOf(Claim, "us/x", 0))
		h.read(at(5.1))
		if _, ok := handover(h, at(5.2)); ok {
			t.Fatal("handover while another site holds")
		}
	})
	t.Run("not out of standby", func(t *testing.T) {
		h := newSiteHarness(t, 0, delay)
		h.land(at(0), recordOf(Claim, "eu/other", 0))
		h.land(at(1), recordOf(Release, "eu/other", 0))
		h.a.nextClaimAt = at(1000)
		h.read(at(1.1))
		if _, ok := handover(h, at(1.2)); ok {
			t.Fatal("handover out of standby")
		}
		h.a.setStandby(at(1.3), true)
		if _, ok := handover(h, at(1.3)); !ok {
			t.Fatal("no handover in standby")
		}
	})
}

// The projection from what an agent published to the Status reported at a
// later moment, as Candidate.Status calls it: with a clock the loop has not
// reached and a standby switch it has not seen.
func TestStatusAt(t *testing.T) {
	own, site := belief{at(8), 5}, belief{at(6), 3}
	known := Status{Certain: true, Ready: true, CooldownUntil: at(7), Handover: true, HandoverEpoch: 5}
	for _, tc := range []struct {
		name      string
		now       time.Time
		standby   bool
		own, site belief
		want      Status
	}{
		{"own term: the site's term too", at(1), false, own, belief{},
			Status{Holding: true, Epoch: 5, Deadline: at(8), SiteHolding: true, SiteEpoch: 5, SiteDeadline: at(8), CooldownUntil: at(7), HandoverEpoch: -1}},
		{"own term before the site's", at(1), false, own, site,
			Status{Holding: true, Epoch: 5, Deadline: at(8), SiteHolding: true, SiteEpoch: 5, SiteDeadline: at(8), CooldownUntil: at(7), HandoverEpoch: -1}},
		{"site's term only", at(1), false, belief{}, site,
			Status{SiteHolding: true, SiteEpoch: 3, SiteDeadline: at(6), CooldownUntil: at(7), HandoverEpoch: -1}},
		{"site's term outlived", at(6), false, belief{}, site,
			Status{CooldownUntil: at(7), HandoverEpoch: -1}},
		{"own term and cooldown outlived", at(8), false, own, belief{},
			Status{HandoverEpoch: -1}},
		{"standby: no belief, and the handover", at(1), true, own, site,
			Status{Standby: true, CooldownUntil: at(7), Handover: true, HandoverEpoch: 5}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.want.Certain, tc.want.Ready = true, true
			got, gotOwn, gotSite := statusAt(tc.now, tc.standby, known, tc.own, tc.site)
			if got != tc.want {
				t.Errorf("status:\n got %+v\nwant %+v", got, tc.want)
			}
			// Projected again from its own result, it stays what it is.
			if again, _, _ := statusAt(tc.now, tc.standby, got, gotOwn, gotSite); again != got {
				t.Errorf("projected twice:\n got %+v\nwant %+v", again, got)
			}
		})
	}
}
