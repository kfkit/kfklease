// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

// Package lease implements the kfklease protocol: a lease whose state is a
// deterministic fold over a Kafka log.
//
// This file is the pure core. It does no I/O and reads no clocks: records go
// in, the state comes out. Every participant that folds the same records
// reaches the same state, which is what replaces a compare-and-swap that
// Kafka does not have. See docs/protocol.md for the reasoning.
package lease

import (
	"fmt"
	"time"
)

// State is the lease as of the last applied record.
type State struct {
	// Holder is empty when the lease is free.
	Holder string
	// Epoch is the offset of the claim that started the current term. It
	// grows with every new term and serves as a fencing token.
	Epoch int64
	// Expires is on the broker's clock, like every timestamp in the protocol:
	// when the current term ends unless renewed, or when the latest term
	// ended (by expiry or release). For a lease proven free by silence it is
	// the latest moment the unknown term before it can have ended; zero
	// when no term is known to have existed.
	Expires time.Time
	// Last is the holder of the latest term, kept after the term ended;
	// empty when the view does not know it, as after silence.
	Last string
}

// HeldAt reports whether the lease is held at broker time t.
func (s State) HeldAt(t time.Time) bool {
	return s.Holder != "" && t.Before(s.Expires)
}

// heldBy reports whether the term epoch of holder is the one that holds the
// lease at broker time t: what a renew or a release of that term needs to
// take effect, and what a writer needs to see before it writes either.
func (s State) heldBy(holder string, epoch int64, t time.Time) bool {
	return s.Holder == holder && s.Epoch == epoch && s.HeldAt(t)
}

// LastSite returns the site of the holder of the latest term, and false
// when the view does not know that holder, as after silence.
func (s State) LastSite() (site string, ok bool) {
	return SiteOf(s.Last), s.Last != ""
}

// Outcome is the verdict on an applied record.
type Outcome uint8

const (
	// Pending means the view does not know the lease state yet and cannot
	// judge the record. The record may be judged later, when an anchor turns
	// up, but its outcome is not reported again.
	Pending Outcome = iota
	// Accepted means the record took effect.
	Accepted
	// Rejected means the record had no effect: a claim on a held lease, a
	// renew of a term that is over, a TTL outside the allowed range. A
	// yield is always rejected: it is for the writers, not the fold.
	Rejected
)

func (o Outcome) String() string {
	switch o {
	case Pending:
		return "pending"
	case Accepted:
		return "accepted"
	case Rejected:
		return "rejected"
	}
	return fmt.Sprintf("outcome(%d)", uint8(o))
}

// View folds the records of one lease into its State.
//
// A view rarely sees the log from its beginning: the topic is compacted, so
// history is truncated. A view therefore starts out uncertain and becomes
// certain only through a state it can prove:
//
//   - start: the caller guarantees the full history (NewViewFromStart);
//   - silence: no claim or renew for a whole TTL means the lease is free,
//     either between two consecutive records (by broker timestamps, plus
//     the skew between two leaders' clocks, see AllowSkew) or at the end of
//     the log (Quiet);
//   - anchor: a record of a term followed by a renew or release of the same
//     term was valid, because a holder writes those only after its own fold
//     accepted the earlier record and only while it still believes the term is
//     live.
//
// Participants must act (claim, renew, release, consider themselves holder)
// only on a certain view.
//
// A View is not safe for concurrent use.
type View struct {
	ttl time.Duration
	// skew is how far apart the clocks of two partition leaders may be;
	// see AllowSkew.
	skew time.Duration

	certain bool
	state   State

	// clock is the latest broker timestamp seen, forced to be monotone:
	// partition leaders change and their clocks differ.
	clock      time.Time
	lastOffset int64
	seen       bool

	// pending holds records seen while uncertain, with clamped timestamps.
	pending []Record
}

// NewView returns an uncertain view of a lease. ttl is the TTL of the lease:
// it is a property of the lease, identical for all participants, and records
// asking for more are rejected by everyone.
func NewView(ttl time.Duration) *View {
	if ttl <= 0 {
		panic("lease: ttl must be positive")
	}
	return &View{ttl: ttl, lastOffset: -1}
}

// NewViewFromStart returns a view that is certain the lease is free. The
// caller guarantees that every record of the lease ever written will be
// applied, which holds only when the partition is read without gaps from
// offset 0.
func NewViewFromStart(ttl time.Duration) *View {
	v := NewView(ttl)
	v.certain = true
	v.state.Epoch = -1
	return v
}

// AllowSkew tells the view how far apart the clocks of two partition
// leaders may be: twice the offset of one broker's clock from real time.
// Timestamps step when the leader changes, so the gap between two records
// can read longer than it was by that much, and only a gap of a TTL plus
// d proves the lease free. Participants pass their margin, which has to
// cover the skew anyway. The default is none: perfect broker clocks.
func (v *View) AllowSkew(d time.Duration) {
	if d < 0 {
		panic("lease: negative skew")
	}
	v.skew = d
}

// Certain reports whether State can be trusted.
func (v *View) Certain() bool { return v.certain }

// State returns the lease state as of the last applied record. It is
// meaningful only when Certain is true.
func (v *View) State() State { return v.state }

// Clock returns the latest broker time the view has seen.
func (v *View) Clock() time.Time { return v.clock }

// Gap tells the view that records may have been skipped: the reader saw a
// hole in the partition offsets (compaction) or had to seek. The view falls
// back to uncertain.
func (v *View) Gap() {
	v.certain = false
	v.state = State{}
	v.pending = nil
	v.seen = false
}

// Quiet tells the view that the reader sat at the end of the log for d of
// real time and no record of this lease arrived. If d covers a whole TTL the
// lease is free. The caller owns the safety margin for clock drift.
func (v *View) Quiet(d time.Duration) bool {
	if d < v.ttl {
		return false
	}
	var endBy time.Time
	if v.seen {
		endBy = v.clock.Add(v.ttl + v.skew)
	}
	v.setFree(endBy)
	return true
}

// setFree settles the view on a free lease after silence. endBy is the
// latest moment the term before the silence, unknown to an uncertain view,
// can have ended.
func (v *View) setFree(endBy time.Time) {
	if !v.certain {
		v.state = State{Epoch: -1, Expires: endBy}
	}
	v.state.Holder = ""
	v.certain = true
	v.pending = nil
}

// Apply folds the next record of the lease. Records must come in offset
// order; an error means the caller broke that contract and the view was left
// unchanged.
func (v *View) Apply(r Record) (Outcome, error) {
	if r.Offset <= v.lastOffset {
		return Rejected, fmt.Errorf("lease: offset %d after %d", r.Offset, v.lastOffset)
	}
	switch _, known := kinds[r.Kind]; {
	case !known:
		return Rejected, fmt.Errorf("lease: cannot apply %v", r.Kind)
	case r.Kind == Yield:
		// Not part of the fold: it neither moves the clock nor breaks a
		// silence, so the state is what it would be without it.
		return Rejected, nil
	}

	prev, hadPrev := v.clock, v.seen
	if r.Timestamp.Before(v.clock) {
		r.Timestamp = v.clock
	}
	v.clock = r.Timestamp
	v.lastOffset = r.Offset
	v.seen = true

	// Silence between two consecutive records: whatever term there was, it
	// got no renewal for a whole TTL, and for the skew on top: the record
	// before may carry the time of a leader whose clock ran behind the one
	// that stamped the term's last renewal.
	if !v.certain && hadPrev && r.Timestamp.Sub(prev) >= v.ttl+v.skew {
		v.setFree(prev.Add(v.ttl + v.skew))
	}

	if v.certain {
		return v.fold(r), nil
	}
	return v.applyUncertain(r), nil
}

func (v *View) applyUncertain(r Record) Outcome {
	if r.Kind != Claim {
		holder, epoch := r.term()
		for i, p := range v.pending {
			if p.Kind == Release {
				continue
			}
			if h, e := p.term(); h != holder || e != epoch {
				continue
			}
			// p is proven valid, so the state right after it is known.
			// Replay everything seen since.
			v.state = State{Holder: holder, Epoch: epoch, Expires: p.Timestamp.Add(p.TTL), Last: holder}
			v.certain = true
			rest := v.pending[i+1:]
			v.pending = nil
			for _, q := range rest {
				v.fold(q)
			}
			return v.fold(r)
		}
	}
	if v.validTTL(r) {
		v.pending = append(v.pending, r)
	}
	v.prunePending()
	return Pending
}

// prunePending drops records too old to matter: an anchor older than a TTL
// proves a term that has expired since, and silence will settle that case.
func (v *View) prunePending() {
	horizon := v.clock.Add(-2 * v.ttl)
	i := 0
	for i < len(v.pending) && v.pending[i].Timestamp.Before(horizon) {
		i++
	}
	v.pending = v.pending[i:]
}

func (v *View) validTTL(r Record) bool {
	return r.Kind == Release || (r.TTL > 0 && r.TTL <= v.ttl)
}

// fold applies r to a certain state.
func (v *View) fold(r Record) Outcome {
	if !v.validTTL(r) {
		return Rejected
	}
	switch r.Kind {
	case Claim:
		if v.state.HeldAt(r.Timestamp) {
			return Rejected
		}
		v.state = State{Holder: r.Holder, Epoch: r.Offset, Expires: r.Timestamp.Add(r.TTL), Last: r.Holder}
	case Renew:
		if !v.state.heldBy(r.Holder, r.Epoch, r.Timestamp) {
			return Rejected
		}
		v.state.Expires = r.Timestamp.Add(r.TTL)
	case Release:
		if !v.state.heldBy(r.Holder, r.Epoch, r.Timestamp) {
			return Rejected
		}
		v.state.Holder = ""
		v.state.Expires = r.Timestamp
	}
	return Accepted
}
