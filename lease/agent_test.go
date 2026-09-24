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
