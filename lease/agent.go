// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// ErrLogReplaced means the log this participant follows is not the one it
// read before: an offset it has passed now holds another record. The topic
// was deleted and created again, or the partition lost acknowledged
// records. Nothing the participant knows can be trusted; Run returns the
// error and a new process starts over from the log as it is.
var ErrLogReplaced = errors.New("lease: the log was replaced or truncated")

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
	ownOutcomes map[int64]verdict

	// Belief: this participant holds epoch until deadline (monotonic).
	epoch    int64
	deadline time.Time
	// Site belief: another participant of this site holds siteEpoch until
	// siteDeadline (monotonic), as this participant reads the log. See
	// followSite.
	siteEpoch    int64
	siteDeadline time.Time

	inflight    *inflight
	lastSend    time.Time
	nextClaimAt time.Time

	// The health gate, on the monotonic clock. heldSince is when this
	// site's current run of terms began, as far as this participant saw
	// it, lastSiteHeld the last tick at which the site held, and
	// lastHealthy the last time the workload was reported healthy.
	// cooldownUntil holds claims back after the gate tripped.
	heldSince     time.Time
	lastSiteHeld  time.Time
	lastHealthy   time.Time
	cooldownUntil time.Time

	// releaseBy is set while the term epoch is owed a release: the belief
	// was given up (the health gate, standby, a stop, a claim that must not
	// be kept) and the record waits for its turn under the
	// one-request-in-flight rule. It is the deadline that belief had: past
	// it nothing is written for the term, as for a renew. Zero for none.
	releaseBy time.Time
	// sealed: a record of the term epoch was given up on and may still
	// land, unread. Nothing more is written for the term, so that two
	// records of one term in the log always mean the writer read the first
	// one back; the belief lasts until its deadline.
	sealed bool
	// stopping: the participant was told to stop. It claims nothing, and
	// what is left to do is the release it owes, see stop.
	stopping bool

	// ready is set once the view has been certain with the log read up to
	// its end offset: from then on the agent knows who holds the lease, or
	// knew it last.
	ready bool

	// standby: follow the log, never claim, release a term held.
	standby bool

	// The handover, see yieldFrom. termEnd is the broker time at which the
	// latest term would have run out unrenewed, by the last claim or renew
	// of it this participant folded on a certain view: a release ends the
	// term earlier in the log, but a reader of its site that missed the
	// release still answers for the site until about then. yielded is the
	// latest yield that counts for the term of the view's state. yieldDue
	// is the epoch of a yield the site's workload is down for, waiting for
	// its turn under the one-request-in-flight rule; -1 for none.
	termEnd  epochAt
	yielded  epochAt
	yieldDue int64

	// lastTickAt is the last tick, and caughtUpAtTick whether the agent sat
	// at the end of the log on a certain view then: what tells a record
	// that just landed from one read late. See freshRead.
	lastTickAt     time.Time
	caughtUpAtTick bool

	// Silence bookkeeping, all on the monotonic clock.
	connected    bool
	fetchedOnce  bool
	caughtUpAt   time.Time
	lastRecordAt time.Time
	// lastRecordAt is paired with the broker time of that record, to
	// estimate the broker's clock between records.
	lastRecordBroker time.Time
	// maxBroker is the latest broker time of any record read, for telling a
	// record seen twice from another record at an offset already passed.
	maxBroker time.Time
}

// verdict is the fold's outcome of an own record, and for an accepted
// claim whether it landed too early: within the takeover delay after
// another site's term, which a late landing can do although the claim was
// sent in time.
type verdict struct {
	out   Outcome
	early bool
}

// epochAt is a broker time tied to a term; epoch -1 for none.
type epochAt struct {
	epoch int64
	at    time.Time
}

var noEpoch = epochAt{epoch: -1}

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
		ownOutcomes: make(map[int64]verdict),
		epoch:       -1,
		siteEpoch:   -1,
		termEnd:     noEpoch,
		yielded:     noEpoch,
		yieldDue:    -1,
		standby:     cfg.Standby,
		connected:   true,
		caughtUpAt:  now,
	}
	if start == 0 {
		// Nothing was ever deleted, so the reader sees everything. Holes
		// left by compaction are caught record by record.
		a.view = NewViewFromStart(cfg.TTL)
		a.noteReady()
	}
	// The margin covers the skew between the brokers' clocks, among other
	// things; see docs/protocol.md.
	a.view.AllowSkew(cfg.Margin)
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
	a.noteReady()
}

// caughtUp reports whether every record up to the log's end offset, as last
// reported, was read.
func (a *agent) caughtUp() bool { return a.nextOffset >= a.highWater }

// noteReady sets ready once the view is certain and has caught up with the
// log. A view of the whole history is certain before it has read a single
// record, and knows nothing yet.
func (a *agent) noteReady() {
	a.ready = a.ready || (a.view.Certain() && a.caughtUp())
}

// record applies the next record of the log: its offset, its broker
// timestamp and its value. Duplicates are skipped, holes make the view
// uncertain, and an undecodable value is logged and ignored. A record at an
// offset already passed that is newer than everything read so far is not a
// duplicate: the log was replaced (ErrLogReplaced).
func (a *agent) record(now time.Time, offset int64, ts time.Time, value []byte) error {
	if offset < a.nextOffset {
		if ts.After(a.maxBroker) {
			return fmt.Errorf("%w: offset %d, read up to %d before, holds a record of %v, later than any read (%v)",
				ErrLogReplaced, offset, a.nextOffset, ts, a.maxBroker)
		}
		return nil
	}
	a.maxBroker = later(a.maxBroker, ts)
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
	before, wasCertain := a.view.State(), a.view.Certain()
	var takeoverAt time.Time
	if wasCertain {
		takeoverAt = a.takeoverFrom(before)
	}
	out, err := a.view.Apply(rec)
	if err != nil {
		return err
	}
	a.followTerm(rec, out)
	a.log.Debug("record", "offset", rec.Offset, "kind", rec.Kind, "by", rec.Holder, "epoch", rec.Epoch, "outcome", out)

	fresh := a.freshRead(now)
	a.noteReady()
	if rec.Holder == a.cfg.Holder {
		early := rec.Kind == Claim && out == Accepted && wasCertain && ts.Before(takeoverAt)
		a.ownRecord(now, rec.Offset, rec.Kind, verdict{out, early})
	}
	if a.believing(now) && a.view.Certain() {
		s := a.view.State()
		if s.Holder != a.cfg.Holder || s.Epoch != a.epoch || !s.HeldAt(a.view.Clock()) {
			a.log.Warn("term ended in the log", "epoch", a.epoch, "log_holder", s.Holder, "log_epoch", s.Epoch)
			a.dropBelief()
		}
	}
	a.followSite(now, rec.Kind, out, fresh)
	return nil
}

// followTerm keeps what the handover needs after a record was applied: the
// natural end of the latest term, and the latest yield for it that counts.
// On a view that is not certain both are forgotten.
func (a *agent) followTerm(rec Record, out Outcome) {
	if !a.view.Certain() {
		a.forgetTerm()
		return
	}
	s := a.view.State()
	switch {
	case rec.Kind == Yield:
		at := later(rec.Timestamp, a.view.Clock())
		if a.yieldCounts(rec, s, at) {
			a.yielded = epochAt{s.Epoch, at}
			a.log.Debug("yield", "by", rec.Holder, "epoch", rec.Epoch)
		}
	case out == Accepted && s.Holder != "":
		a.termEnd = epochAt{s.Epoch, s.Expires}
	}
}

// yieldCounts reports whether a yield read at broker time at speaks for the
// term of s: written by a participant of the site that held it, naming it,
// after it ended, and with the term's natural end known.
func (a *agent) yieldCounts(y Record, s State, at time.Time) bool {
	return s.Epoch >= 0 && y.Epoch == s.Epoch && s.Last != "" && SiteOf(y.Holder) == SiteOf(s.Last) &&
		!s.HeldAt(at) && a.termEnd.epoch == s.Epoch
}

func (a *agent) forgetTerm() {
	a.termEnd, a.yielded = noEpoch, noEpoch
}

func later(x, y time.Time) time.Time {
	if x.After(y) {
		return x
	}
	return y
}

// freshRead reports whether a record read at now landed just before: the
// agent sat at the end of a certain log at its last tick, and that tick was
// recent, so the record arrived since, give or take the latency of a fetch.
// A record read after a freeze, a lost connection or from a backlog may be
// old and is not fresh.
func (a *agent) freshRead(now time.Time) bool {
	return a.caughtUpAtTick && a.connected && !a.lastTickAt.IsZero() && now.Sub(a.lastTickAt) <= 2*a.cfg.pollEvery()
}

// followSite keeps the site belief after a record was applied: another
// participant of this site holds the lease. A fresh, accepted claim or renew
// of such a term starts or extends the belief by the term the log gives it,
// counted from the moment the record was read and less the margin, as the
// holder counts its own from the send; anything that ends the term in the
// log ends the belief. It takes a takeover delay: another site claims only
// that long after the term ended, which also covers the time a record took
// to reach this reader. In standby there is no site belief.
func (a *agent) followSite(now time.Time, kind Kind, out Outcome, fresh bool) {
	s := a.view.State()
	if !a.siteDeadline.IsZero() && (!a.view.Certain() || s.Holder == "" || s.Epoch != a.siteEpoch || !s.HeldAt(a.view.Clock())) {
		a.dropSite()
	}
	if out != Accepted || kind == Release || !fresh || !a.view.Certain() || !a.followsSite(s) {
		return
	}
	if a.siteDeadline.IsZero() {
		a.siteStarted(now)
		a.log.Debug("site holds", "by", s.Holder, "epoch", s.Epoch)
	}
	a.siteEpoch = s.Epoch
	a.siteDeadline = now.Add(s.Expires.Sub(a.view.Clock()) - a.cfg.Margin)
}

// followsSite reports whether a term of s's holder counts as this site's
// for the site belief.
func (a *agent) followsSite(s State) bool {
	return a.cfg.TakeoverDelay > 0 && !a.standby && s.Holder != a.cfg.Holder && SiteOf(s.Holder) == a.cfg.Site
}

func (a *agent) dropSite() {
	if a.siteDeadline.IsZero() {
		return
	}
	a.siteDeadline = time.Time{}
	a.log.Debug("site no longer holds", "epoch", a.siteEpoch)
}

// siteHolding reports whether this participant's site holds the lease, as
// far as it may act on: it holds the lease itself, or it believes another
// participant of its site does.
func (a *agent) siteHolding(now time.Time) bool {
	return a.believing(now) || now.Before(a.siteDeadline)
}

// siteStarted notes that a term of this site began at now, this
// participant's or another's. The health gate counts from the start of the
// site's run of terms: a term that follows one of the same site within a
// TTL continues it, so that passing the lease around a site gives a
// workload that cannot run no extra time.
func (a *agent) siteStarted(now time.Time) {
	if a.lastSiteHeld.IsZero() || now.Sub(a.lastSiteHeld) > a.cfg.TTL {
		a.heldSince = now
	}
	a.lastSiteHeld = now
}

// ownRecord handles a record written by this participant. Only the record
// in flight can change the belief, and only once its offset is known from
// the acknowledgement; anything else is a record we gave up on.
func (a *agent) ownRecord(now time.Time, offset int64, kind Kind, v verdict) {
	in := a.inflight
	if in == nil || in.kind != kind {
		return
	}
	if in.offset == -1 {
		a.ownOutcomes[offset] = v
		return
	}
	if in.offset == offset {
		a.settle(now, v)
	}
}

// acked reports the broker's answer to the record in flight: its offset,
// or an error when the client gave up on it. A record given up on may
// still land; see sealed. An offset already passed at which this record
// was not read means the log was replaced (ErrLogReplaced).
func (a *agent) acked(now time.Time, offset int64, err error) error {
	in := a.inflight
	if in == nil {
		return nil
	}
	if err != nil {
		a.log.Warn("produce failed", "kind", in.kind, "err", err)
		a.inflight = nil
		switch in.kind {
		case Claim:
			a.nextClaimAt = now.Add(a.cfg.RenewEvery)
		case Renew:
			a.sealed = true
		}
		return nil
	}
	in.offset = offset
	if v, ok := a.ownOutcomes[offset]; ok {
		a.settle(now, v)
		return nil
	}
	if offset < a.nextOffset {
		return fmt.Errorf("%w: own %v acknowledged at offset %d, read up to %d before, and the record read there was another",
			ErrLogReplaced, in.kind, offset, a.nextOffset)
	}
	return nil
}

// settle applies the fold's verdict on the record in flight to the belief.
func (a *agent) settle(now time.Time, v verdict) {
	in := a.inflight
	a.inflight = nil
	clear(a.ownOutcomes)
	out := v.out
	switch {
	case out == Pending:
		// The view lost certainty while the record was in flight. The
		// record may well be valid, but the belief needs proof.
		a.log.Warn("own record unjudged", "kind", in.kind, "offset", in.offset)
	case in.kind == Claim && out == Accepted && v.early:
		// Another site held a short term while the claim was on its way,
		// and its workload may still run. The term is ours in the log, so
		// end it at once rather than let it block everyone for a TTL.
		a.newTerm(in)
		a.releaseBy = a.beliefFrom(in.sentAt)
		a.log.Warn("claim landed within the takeover delay after another site's term, releasing", "epoch", in.offset)
	case in.kind == Claim && out == Accepted && (a.standby || a.stopping):
		// Sent before standby was switched on, or before the stop. Never
		// believed, so that nothing reports holding for a moment; released
		// at once.
		a.newTerm(in)
		a.releaseBy = a.beliefFrom(in.sentAt)
		a.log.Info("claim landed in standby or after the stop, releasing", "epoch", in.offset)
	case in.kind == Claim && out == Accepted:
		a.newTerm(in)
		a.extendBelief(in.sentAt)
		a.siteStarted(now)
		a.log.Info("acquired", "epoch", a.epoch)
	case in.kind == Claim:
		s := a.view.State()
		wait := min(max(s.Expires.Sub(a.brokerNow(now)), a.cfg.pollEvery()), a.cfg.TTL)
		a.nextClaimAt = now.Add(wait)
		a.log.Debug("claim rejected", "log_holder", s.Holder, "log_epoch", s.Epoch, "retry_in", wait)
	case in.kind == Renew && out == Accepted:
		switch {
		case a.believing(now):
			a.extendBelief(in.sentAt)
		case !a.releaseBy.IsZero():
			// The belief was given up while the renew was on its way; the
			// renew extended the term, and the release it is owed with it.
			a.releaseBy = a.beliefFrom(in.sentAt)
		}
	case in.kind == Renew:
		a.log.Warn("renew rejected", "epoch", a.epoch)
		a.dropBelief()
	}
}

// newTerm notes that the claim in flight started a term of this
// participant.
func (a *agent) newTerm(in *inflight) {
	a.epoch = in.offset
	a.sealed = false
}

func (a *agent) believing(now time.Time) bool { return now.Before(a.deadline) }

// beliefFrom is the deadline of a belief that rests on an accepted record
// sent at sentAt: the record landed after it was sent, so the term in the
// log outlives the belief by at least the margin.
func (a *agent) beliefFrom(sentAt time.Time) time.Time {
	return sentAt.Add(a.cfg.TTL - a.cfg.Margin)
}

func (a *agent) extendBelief(sentAt time.Time) {
	a.deadline = a.beliefFrom(sentAt)
}

// giveUp ends the belief at once and owes its term a release: the record
// lets the others take over without waiting for the term to run out. tick
// writes it when rule 2 lets it, if it still may; see mayRelease.
func (a *agent) giveUp() {
	a.releaseBy = a.deadline
	a.dropBelief()
}

// mayRelease reports whether the release owed until by may be written at
// now. Like a renew it goes out only before the deadline of the belief it
// follows, and only for a term the log shows as this participant's and
// live as of the last record read, with no record of the term given up on
// (sealed). So a release always follows a record of its term that its
// writer read back and found accepted, which is what the readers' anchor
// rule takes two records of one term to mean.
func (a *agent) mayRelease(now, by time.Time) bool {
	s := a.view.State()
	return now.Before(by) && !a.sealed &&
		s.Holder == a.cfg.Holder && s.Epoch == a.epoch && s.HeldAt(a.view.Clock())
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

// healthy reports that the workload was seen healthy at now.
func (a *agent) healthy(now time.Time) {
	if now.After(a.lastHealthy) {
		a.lastHealthy = now
	}
}

// unhealthyFor is how long the site's workload has gone without a healthy
// report, counted from the later of the start of the site's terms and the
// last report: a workload gets FailAfter to come up where it was moved.
func (a *agent) unhealthyFor(now time.Time) time.Duration {
	since := a.heldSince
	if a.lastHealthy.After(since) {
		since = a.lastHealthy
	}
	return now.Sub(since)
}

// checkHealth applies the health gate while the site holds: when its
// workload has gone unhealthy for FailAfter, this participant claims nothing
// for Cooldown, and if it is the holder it stops believing at once and owes
// the log a release. Every participant of the site probes the same
// workload, so the others do not take the lease over for the site when the
// holder lets it go.
func (a *agent) checkHealth(now time.Time) {
	gate := a.cfg.Health
	if gate.FailAfter <= 0 || !a.siteHolding(now) || now.Before(a.cooldownUntil) {
		return
	}
	d := a.unhealthyFor(now)
	if d < gate.FailAfter {
		return
	}
	a.cooldownUntil = now.Add(gate.Cooldown)
	if !a.believing(now) {
		a.log.Warn("workload unhealthy, claiming nothing for this site", "site_epoch", a.siteEpoch, "unhealthy_for", d, "cooldown", gate.Cooldown)
		return
	}
	a.log.Warn("workload unhealthy, releasing", "epoch", a.epoch, "unhealthy_for", d, "cooldown", gate.Cooldown)
	a.giveUp()
}

// setStandby switches standby on or off. In standby the agent follows the
// log and never claims; a term it holds ends at once: the belief goes now,
// the release when rule 2 lets it, as with the health gate.
func (a *agent) setStandby(now time.Time, on bool) {
	if on == a.standby {
		return
	}
	a.standby = on
	if !on {
		a.log.Info("standby off")
		return
	}
	a.log.Info("standby on")
	a.dropSite()
	if a.believing(now) {
		a.giveUp()
	}
}

// stop tells the agent that its participant is going away: it claims
// nothing from now on, and a term it holds ends at once, the belief now
// and the release when rule 2 lets it. The caller keeps feeding the agent
// and sending what tick returns until stopDone.
func (a *agent) stop(now time.Time) {
	a.stopping = true
	if a.believing(now) {
		a.giveUp()
	}
}

// stopDone reports whether a stopping agent has nothing left to wait for:
// no release it owes and may still write, no claim in flight whose term it
// would have to release, and no release on its way that the broker has not
// acknowledged. A renew in flight is waited for only for the release that
// follows it.
func (a *agent) stopDone(now time.Time) bool {
	if now.Before(a.releaseBy) {
		return false
	}
	in := a.inflight
	switch {
	case in == nil:
		return true
	case in.kind == Release:
		return in.offset >= 0
	case in.kind == Claim:
		return !now.Before(a.beliefFrom(in.sentAt))
	}
	return true
}

// tick measures silence, expires the belief and decides what to write. The
// returned record, if any, is to be produced now; the agent counts it as
// sent at now.
func (a *agent) tick(now time.Time) (Record, bool) {
	if !a.believing(now) && !a.deadline.IsZero() {
		a.log.Warn("deadline passed without renewal", "epoch", a.epoch)
		a.dropBelief()
	}
	if !a.siteDeadline.IsZero() && !now.Before(a.siteDeadline) {
		a.dropSite()
	}
	a.checkHealth(now)
	if a.siteHolding(now) {
		a.lastSiteHeld = now
	}
	defer func() {
		a.lastTickAt = now
		a.caughtUpAtTick = a.connected && a.view.Certain() && a.caughtUp()
	}()

	if a.connected && a.caughtUp() && !a.view.Certain() {
		since := a.caughtUpAt
		if a.lastRecordAt.After(since) {
			since = a.lastRecordAt
		}
		if now.Sub(since) >= a.cfg.TTL+a.cfg.Margin && a.view.Quiet(a.cfg.TTL) {
			a.log.Info("lease is free: nothing written for a ttl")
			a.ready = true
			if a.view.State().Expires.IsZero() && a.cfg.TakeoverDelay > 0 {
				// Not one record left of a history that existed: whoever
				// held the lease last, its term ended during this silence
				// at the latest. Count the takeover delay from now.
				if at := now.Add(a.cfg.TakeoverDelay); at.After(a.nextClaimAt) {
					a.nextClaimAt = at
				}
			}
		}
	}

	if a.inflight != nil || !a.view.Certain() {
		return Record{}, false
	}
	switch {
	case !a.releaseBy.IsZero():
		// The belief is gone already; the record only lets the others
		// take over without waiting for the term to run out.
		by := a.releaseBy
		a.releaseBy = time.Time{}
		if a.mayRelease(now, by) {
			return a.send(now, Release), true
		}
	case a.yieldDue >= 0:
		due := a.yieldDue
		a.yieldDue = -1
		if epoch, ok := a.handover(now); ok && epoch == due {
			a.log.Info("yielding", "epoch", epoch)
			return a.sendFor(now, Yield, epoch), true
		}
	case a.believing(now):
		if !a.sealed && now.Sub(a.lastSend) >= a.cfg.RenewEvery {
			return a.send(now, Renew), true
		}
	case !a.standby && !a.stopping && now.After(a.nextClaimAt) && !now.Before(a.cooldownUntil) && a.mayClaim(now):
		return a.send(now, Claim), true
	}
	return Record{}, false
}

// mayClaim is the claim rule on a certain view that has caught up with the
// log: the lease is free by the broker's clock, and the term that ended
// last was this participant's site's or ended TakeoverDelay ago. A site's
// own participants take over at once, so a restarted scaler keeps its
// site's workload; another site waits until that workload has had time to
// stop. A reader that is behind knows an old state of the lease: it may
// have missed a new term, of that site or of any other, and a view of the
// whole history that has read nothing yet has missed all of them. A term
// the view does not know the holder of (silence) counts as another site's.
func (a *agent) mayClaim(now time.Time) bool {
	s := a.view.State()
	brokerNow := a.brokerNow(now)
	if !a.caughtUp() || s.HeldAt(brokerNow) {
		return false
	}
	from := a.takeoverFrom(s)
	return from.IsZero() || !brokerNow.Before(from)
}

// takeoverFrom is the broker time from which this participant may take the
// lease over after the term s describes: zero, at once, after a term of its
// own site, on a log with no term yet, or without a delay; otherwise
// TakeoverDelay after the end of the term and not while a replica of that
// site may still answer for it (answeredUntil), or earlier with a yield,
// see yieldFrom.
func (a *agent) takeoverFrom(s State) time.Time {
	if s.Expires.IsZero() || (s.Last != "" && SiteOf(s.Last) == a.cfg.Site) || a.cfg.TakeoverDelay == 0 {
		return time.Time{}
	}
	from := later(s.Expires.Add(a.cfg.TakeoverDelay), a.answeredUntil(s))
	if y, ok := a.yieldFrom(s); ok && y.Before(from) {
		from = y
	}
	return from
}

// answeredUntil is the latest broker time at which a participant of the
// site that held the term s describes may still answer for it (followSite):
// the term's natural end, the end its last accepted claim or renew gave it,
// plus the two poll intervals a read may lag and still count as fresh. A
// release ends the term earlier in the log, but a replica of that site that
// read the last renewal and missed the release answers "my site holds"
// until then, and two sites must not say so at once: no other site claims
// before, whatever the takeover delay. A view that does not know the
// natural end (it became certain on the release itself) takes the latest
// it can be, a TTL after the release; after silence the end the view
// knows is that latest moment already.
func (a *agent) answeredUntil(s State) time.Time {
	end := s.Expires
	switch {
	case s.Epoch < 0:
	case a.termEnd.epoch == s.Epoch:
		end = a.termEnd.at
	default:
		end = s.Expires.Add(a.cfg.TTL)
	}
	return end.Add(2 * a.cfg.pollEvery())
}

// yieldFrom is the end of the takeover delay after the term s describes
// when its site has yielded it: the site's workload has stopped, so
// another site may start its own without waiting for it. Not before the
// yield, and not before a replica of that site may still answer for the
// term (answeredUntil).
func (a *agent) yieldFrom(s State) (time.Time, bool) {
	if s.Epoch < 0 || a.yielded.epoch != s.Epoch || a.termEnd.epoch != s.Epoch {
		return time.Time{}, false
	}
	return later(a.yielded.at, a.answeredUntil(s)), true
}

// handover reports the term this participant's site owes the other sites a
// yield for: in standby, on a certain view caught up with the log, the
// term that ended last was this site's, it has ended, no yield for it is
// in the log, and the takeover delay after its end has not passed yet;
// after that a yield would shorten nothing. The site's workload is to stop
// now; once it has, requestYield.
func (a *agent) handover(now time.Time) (int64, bool) {
	s := a.view.State()
	if !a.standby || a.cfg.TakeoverDelay == 0 || !a.ready || !a.view.Certain() || !a.caughtUp() ||
		s.Epoch < 0 || s.Last == "" || SiteOf(s.Last) != a.cfg.Site || a.yielded.epoch == s.Epoch {
		return -1, false
	}
	brokerNow := a.brokerNow(now)
	if s.HeldAt(brokerNow) || !brokerNow.Before(s.Expires.Add(a.cfg.TakeoverDelay)) {
		return -1, false
	}
	return s.Epoch, true
}

// requestYield asks for a yield of epoch: the caller saw the site's
// workload down after handover reported epoch. tick writes it if handover
// still reports the same term then.
func (a *agent) requestYield(now time.Time, epoch int64) {
	if e, ok := a.handover(now); ok && e == epoch {
		a.yieldDue = epoch
	}
}

func (a *agent) send(now time.Time, kind Kind) Record {
	return a.sendFor(now, kind, a.epoch)
}

func (a *agent) sendFor(now time.Time, kind Kind, epoch int64) Record {
	a.inflight = &inflight{kind: kind, sentAt: now, offset: -1}
	a.lastSend = now
	return a.newRecord(kind, epoch)
}

func (a *agent) newRecord(kind Kind, epoch int64) Record {
	rec := Record{Kind: kind, Holder: a.cfg.Holder, TTL: a.cfg.TTL}
	if kind != Claim {
		rec.Epoch = epoch
	}
	return rec
}

// belief is a term to act on until deadline, on the monotonic clock; a zero
// deadline is no belief.
type belief struct {
	deadline time.Time
	epoch    int64
}

// status is what the agent knows, with its beliefs: its own term, and its
// site's term held by another participant.
func (a *agent) status(now time.Time) (s Status, own, site belief) {
	s = Status{Certain: a.view.Certain(), AsOf: a.view.Clock(), Ready: a.ready, Standby: a.standby}
	if s.Certain {
		s.Lease = a.view.State()
	}
	if a.believing(now) {
		own = belief{a.deadline, a.epoch}
	}
	if now.Before(a.siteDeadline) {
		site = belief{a.siteDeadline, a.siteEpoch}
	}
	if now.Before(a.cooldownUntil) {
		s.CooldownUntil = a.cooldownUntil
	}
	s.HandoverEpoch = -1
	if epoch, ok := a.handover(now); ok {
		s.Handover, s.HandoverEpoch = true, epoch
	}
	return s, own, site
}
