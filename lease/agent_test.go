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
	agTTL    = 10 * time.Second
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
	cfg, err := Config{Brokers: []string{"x"}, Topic: "x", Holder: "me", TTL: agTTL, Margin: agMargin, RenewEvery: agRenew}.withDefaults()
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
	h.land(at(10.5), Record{Kind: Claim, Holder: "other", TTL: agTTL})
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
	skipped := Record{Kind: Renew, Holder: "me", Epoch: 0, TTL: agTTL}
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

func TestAgentReleaseDropsBeliefFirst(t *testing.T) {
	h := newHarness(t, 0)
	epoch := h.acquire(at(0))
	rec, ok := h.a.release(at(1))
	if !ok || rec.Kind != Release || rec.Epoch != epoch {
		t.Fatalf("release = (%+v, %v)", rec, ok)
	}
	h.wantBelief(at(1), false)
	if _, ok := h.a.release(at(1)); ok {
		t.Fatal("released twice")
	}
	// Not believing: the next tick claims again once the log says free.
	h.land(at(1.1), rec)
	h.read(at(1.2))
	h.wantSend(at(1.3), Claim)
}

func TestAgentBacksOffAfterRejectedClaim(t *testing.T) {
	h := newHarness(t, 0)
	h.land(at(0), Record{Kind: Claim, Holder: "other", TTL: agTTL})
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
	h.land(at(0), Record{Kind: Claim, Holder: "other", TTL: agTTL})
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
	h.a.cfg.Site, h.a.cfg.Holder, h.a.cfg.TakeoverDelay = "eu", "eu/me", delay
	return h
}

func TestAgentTakeoverDelay(t *testing.T) {
	const delay = 30 * time.Second
	claim := func(h *agentHarness, sec float64, holder string) {
		h.land(at(sec), Record{Kind: Claim, Holder: holder, TTL: agTTL})
	}
	release := func(h *agentHarness, sec float64, holder string) {
		h.land(at(sec), Record{Kind: Release, Holder: holder, Epoch: 0})
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
		// unknown term ended by 11 at the latest.
		{"unknown holder, settled by silence", 1, func(h *agentHarness) {
			claim(h, 0, "eu/old")
			h.land(at(1), Record{Kind: Renew, Holder: "eu/old", Epoch: 0, TTL: agTTL})
		}, 41.1, 41.3},
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

func TestAgentReadyOnceCertain(t *testing.T) {
	ready := func(h *agentHarness) bool { s, _, _ := h.a.status(at(0)); return s.Ready }
	if h := newHarness(t, 0); !ready(h) {
		t.Fatal("not ready on a log read from its start")
	}
	h := newHarness(t, 40)
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
	h.land(at(0), Record{Kind: Claim, Holder: "us/x", TTL: agTTL})
	h.land(at(1), Record{Kind: Release, Holder: "us/x", Epoch: 0})
	h.read(at(1.2))
	rec := h.wantSend(at(31.3), Claim)
	// While the claim is on its way, the other site takes the lease back
	// for a moment: its own nodes need not wait.
	h.land(at(31.4), Record{Kind: Claim, Holder: "us/y", TTL: agTTL})
	h.land(at(31.5), Record{Kind: Release, Holder: "us/y", Epoch: 2})
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
	h.land(at(0), Record{Kind: Claim, Holder: "us/x", TTL: agTTL})
	h.land(at(1), Record{Kind: Release, Holder: "us/x", Epoch: 0})
	h.read(at(1.2))
	// The log has grown, but the record has not arrived yet: it may be a
	// new term of the other site.
	h.land(at(31), Record{Kind: Claim, Holder: "us/y", TTL: agTTL})
	h.a.fetched(at(31.3), int64(len(h.log)))
	h.wantSilent(at(31.3))
	h.read(at(31.4))
	h.wantSilent(at(31.5))
	// The own site is not held to it.
	h = newSiteHarness(t, 0, 30*time.Second)
	h.land(at(0), Record{Kind: Claim, Holder: "eu/x", TTL: agTTL})
	h.land(at(1), Record{Kind: Release, Holder: "eu/x", Epoch: 0})
	h.read(at(1.2))
	h.a.fetched(at(1.3), int64(len(h.log))+1)
	h.wantSend(at(1.3), Claim)
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
	h.land(at(60), Record{Kind: Claim, Holder: "other", TTL: agTTL})
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
	if s, deadline, _ := h.a.status(at(0.2)); !deadline.IsZero() || s.Holding {
		t.Fatalf("status = %+v, deadline %v: reports holding", s, deadline)
	}
	if r := h.wantSend(at(0.3), Release); r.Epoch != off {
		t.Fatalf("release of epoch %d, want %d", r.Epoch, off)
	}
}
