// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"log/slog"
	"time"
)

// agent is the decision core of a participant: the writer rules from
// docs/protocol.md on top of a View. It does no I/O and reads no clock. The
// Kafka client (Candidate) feeds it records, fetch results and produce
// acknowledgements with the current time and sends what tick returns; the
// simulation in sim_test.go drives the same code against a fake log.
type agent struct {
	cfg Config
	log *slog.Logger

	view *View
	// nextOffset is the offset the next record must have; anything higher
	// is a hole.
	nextOffset int64
	// highWater is the log's end offset as last reported.
	highWater int64
	// ownOutcomes remembers verdicts on this participant's records read
	// back before the acknowledgement told us their offset.
	ownOutcomes map[int64]Outcome

	// Belief: this participant holds epoch until deadline (monotonic).
	epoch    int64
	deadline time.Time

	inflight    *inflight
	lastSend    time.Time
	nextClaimAt time.Time

	// Silence bookkeeping, all on the monotonic clock.
	connected    bool
	fetchedOnce  bool
	caughtUpAt   time.Time
	lastRecordAt time.Time
	// lastRecordAt is paired with the broker time of that record, to
	// estimate the broker's clock between records.
	lastRecordBroker time.Time
}

type inflight struct {
	kind   Kind
	sentAt time.Time
	// offset is -1 until the broker acknowledged the record.
	offset int64
}

// newAgent starts an agent on a log whose retained records span
// [start, end). A start of 0 means the whole history is there.
func newAgent(cfg Config, log *slog.Logger, start, end int64, now time.Time) *agent {
	a := &agent{
		cfg:         cfg,
		log:         log,
		view:        NewView(cfg.TTL),
		nextOffset:  start,
		highWater:   end,
		ownOutcomes: make(map[int64]Outcome),
		epoch:       -1,
		connected:   true,
		caughtUpAt:  now,
	}
	if start == 0 {
		// Nothing was ever deleted, so the reader sees everything. Holes
		// left by compaction are caught record by record.
		a.view = NewViewFromStart(cfg.TTL)
	}
	return a
}

// fetchFailed reports that the log could not be read. Silence measured
// before this moment means nothing.
func (a *agent) fetchFailed(err error) {
	if a.connected {
		// A topic created a moment ago has no leader yet, which is not
		// worth a warning.
		level := slog.LevelWarn
		if !a.fetchedOnce {
			level = slog.LevelDebug
		}
		a.log.Log(context.Background(), level, "fetch failed", "err", err)
	}
	a.connected = false
}

// fetched reports a successful read of the log, with its end offset.
func (a *agent) fetched(now time.Time, highWater int64) {
	if !a.connected {
		a.connected = true
		a.caughtUpAt = now
		if a.fetchedOnce {
			a.log.Info("fetching again")
		}
	}
	a.fetchedOnce = true
	a.highWater = max(a.highWater, highWater)
}

// record applies the next record of the log: its offset, its broker
// timestamp and its value. Duplicates are skipped, holes make the view
// uncertain, and an undecodable value is logged and ignored.
func (a *agent) record(now time.Time, offset int64, ts time.Time, value []byte) error {
	if offset < a.nextOffset {
		return nil
	}
	if offset > a.nextOffset {
		a.log.Warn("hole in the log", "expected", a.nextOffset, "got", offset)
		a.view.Gap()
		a.caughtUpAt = now
	}
	a.nextOffset = offset + 1
	a.lastRecordAt = now
	a.lastRecordBroker = ts

	rec, err := DecodeValue(value)
	if err != nil {
		a.log.Warn("skipping record", "offset", offset, "err", err)
		return nil
	}
	rec.Offset = offset
	rec.Timestamp = ts
	out, err := a.view.Apply(rec)
	if err != nil {
		return err
	}
	a.log.Debug("record", "offset", rec.Offset, "kind", rec.Kind, "by", rec.Holder, "epoch", rec.Epoch, "outcome", out)

	if rec.Holder == a.cfg.Holder {
		a.ownRecord(now, rec.Offset, rec.Kind, out)
	}
	if a.believing(now) && a.view.Certain() {
		s := a.view.State()
		if s.Holder != a.cfg.Holder || s.Epoch != a.epoch || !s.HeldAt(a.view.Clock()) {
			a.log.Warn("term ended in the log", "epoch", a.epoch, "log_holder", s.Holder, "log_epoch", s.Epoch)
			a.dropBelief()
		}
	}
	return nil
}

// ownRecord handles a record written by this participant. Only the record
// in flight can change the belief, and only once its offset is known from
// the acknowledgement; anything else is a record we gave up on.
func (a *agent) ownRecord(now time.Time, offset int64, kind Kind, out Outcome) {
	in := a.inflight
	if in == nil || in.kind != kind {
		return
	}
	if in.offset == -1 {
		a.ownOutcomes[offset] = out
		return
	}
	if in.offset == offset {
		a.settle(now, out)
	}
}

// acked reports the broker's answer to the record in flight: its offset,
// or an error when it did not land within the delivery timeout.
func (a *agent) acked(now time.Time, offset int64, err error) {
	in := a.inflight
	if in == nil {
		return
	}
	if err != nil {
		a.log.Warn("produce failed", "kind", in.kind, "err", err)
		a.inflight = nil
		if in.kind == Claim {
			a.nextClaimAt = now.Add(a.cfg.RenewEvery)
		}
		return
	}
	in.offset = offset
	if out, ok := a.ownOutcomes[offset]; ok {
		a.settle(now, out)
	}
}

// settle applies the fold's verdict on the record in flight to the belief.
func (a *agent) settle(now time.Time, out Outcome) {
	in := a.inflight
	a.inflight = nil
	clear(a.ownOutcomes)
	switch {
	case out == Pending:
		// The view lost certainty while the record was in flight. The
		// record may well be valid, but the belief needs proof.
		a.log.Warn("own record unjudged", "kind", in.kind, "offset", in.offset)
	case in.kind == Claim && out == Accepted:
		a.epoch = in.offset
		a.extendBelief(in.sentAt)
		a.log.Info("acquired", "epoch", a.epoch)
	case in.kind == Claim:
		s := a.view.State()
		wait := min(max(s.Expires.Sub(a.brokerNow(now)), a.cfg.pollEvery()), a.cfg.TTL)
		a.nextClaimAt = now.Add(wait)
		a.log.Debug("claim rejected", "log_holder", s.Holder, "log_epoch", s.Epoch, "retry_in", wait)
	case in.kind == Renew && out == Accepted:
		if a.believing(now) {
			a.extendBelief(in.sentAt)
		}
	case in.kind == Renew:
		a.log.Warn("renew rejected", "epoch", a.epoch)
		a.dropBelief()
	}
}

func (a *agent) believing(now time.Time) bool { return now.Before(a.deadline) }

// extendBelief sets the deadline from the send time of an accepted record:
// the record landed after it was sent, so the term in the log outlives the
// belief by at least the margin.
func (a *agent) extendBelief(sentAt time.Time) {
	a.deadline = sentAt.Add(a.cfg.TTL - a.cfg.Margin)
}

func (a *agent) dropBelief() {
	if a.deadline.IsZero() {
		return
	}
	a.deadline = time.Time{}
	a.log.Info("no longer holder", "epoch", a.epoch)
}

// brokerNow estimates the broker's clock from the last record seen.
func (a *agent) brokerNow(now time.Time) time.Time {
	if a.lastRecordAt.IsZero() {
		return a.view.Clock()
	}
	return a.lastRecordBroker.Add(now.Sub(a.lastRecordAt))
}

// tick measures silence, expires the belief and decides what to write. The
// returned record, if any, is to be produced now; the agent counts it as
// sent at now.
func (a *agent) tick(now time.Time) (Record, bool) {
	if !a.believing(now) && !a.deadline.IsZero() {
		a.log.Warn("deadline passed without renewal", "epoch", a.epoch)
		a.dropBelief()
	}

	if a.connected && a.nextOffset >= a.highWater && !a.view.Certain() {
		since := a.caughtUpAt
		if a.lastRecordAt.After(since) {
			since = a.lastRecordAt
		}
		if now.Sub(since) >= a.cfg.TTL+a.cfg.Margin && a.view.Quiet(a.cfg.TTL) {
			a.log.Info("lease is free: nothing written for a ttl")
		}
	}

	if a.inflight != nil || !a.view.Certain() {
		return Record{}, false
	}
	switch {
	case a.believing(now):
		if now.Sub(a.lastSend) >= a.cfg.RenewEvery {
			return a.send(now, Renew), true
		}
	case now.After(a.nextClaimAt) && !a.view.State().HeldAt(a.brokerNow(now)):
		return a.send(now, Claim), true
	}
	return Record{}, false
}

func (a *agent) send(now time.Time, kind Kind) Record {
	a.inflight = &inflight{kind: kind, sentAt: now, offset: -1}
	a.lastSend = now
	return a.newRecord(kind, a.epoch)
}

// release gives the lease up: the belief goes first, and the returned
// record, if any, goes second. A release that never lands only costs the
// others a wait of one TTL.
func (a *agent) release(now time.Time) (Record, bool) {
	if !a.believing(now) {
		return Record{}, false
	}
	epoch := a.epoch
	a.dropBelief()
	return a.newRecord(Release, epoch), true
}

func (a *agent) newRecord(kind Kind, epoch int64) Record {
	rec := Record{Kind: kind, Holder: a.cfg.Holder, TTL: a.cfg.TTL}
	if kind != Claim {
		rec.Epoch = epoch
	}
	return rec
}

// status is what the agent knows and believes, with the deadline that
// bounds the belief.
func (a *agent) status(now time.Time) (s Status, deadline time.Time, epoch int64) {
	s = Status{Certain: a.view.Certain(), AsOf: a.view.Clock()}
	if s.Certain {
		s.Lease = a.view.State()
	}
	if a.believing(now) {
		deadline = a.deadline
	}
	return s, deadline, a.epoch
}
