// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

// HandoverAction is one thing a handover driver does to its site's
// workload, or for it.
type HandoverAction uint8

const (
	// HandoverNothing: nothing more at this look.
	HandoverNothing HandoverAction = iota
	// HandoverLift: remove the pause from the workload, and say how it
	// went with Lifted.
	HandoverLift
	// HandoverPause: pause the workload for the handover of Epoch, and say
	// how it went with Paused.
	HandoverPause
	// HandoverCount: the workload is paused for the handover of Epoch;
	// count what remains of it and say so with Remaining. A count that
	// failed ends the look.
	HandoverCount
	// HandoverYield: nothing of the workload remains; ask for the yield of
	// Epoch (Candidate.Yield). That ends the look.
	HandoverYield
)

// HandoverStep is what a handover driver does next, as HandoverDriver tells
// it.
type HandoverStep struct {
	Do HandoverAction
	// Epoch is the term whose handover the pause, the count and the yield
	// are for. With HandoverLift it is the term this driver paused the
	// workload for, -1 when it did not.
	Epoch int64
	// Rising is set with HandoverLift when the pause is lifted because the
	// participant started to say that its site holds the lease.
	Rising bool
}

// HandoverDriver is the decisions of whoever drives a site's workload
// through a handover (rule 11 of docs/protocol.md), and what it remembers
// between its looks at the lease. It does no I/O and reads no clock: the
// scaler's Handover carries its steps out against KEDA, and the simulation
// against a workload of its own, so both follow the rules written here.
//
// A look starts with Look, which takes the participant's Status and returns
// the first step. The driver carries a step out, reports the outcome with
// the method the step names, and gets the next step of the same look, until
// HandoverNothing or HandoverYield. The rules:
//
//   - the pause is lifted when the participant starts to say that its site
//     holds the lease (Status.SiteHolding turned true since the last look):
//     once per start, and not for as long as that goes on, so that a replica
//     answering for a term from a stale read does not undo a pause set
//     after it started;
//   - the pause is lifted whenever another site holds the latest term, if
//     it may be in place: until this driver has lifted it once it may be,
//     left by a previous life that stopped half-way through a handover. A
//     latest term whose holder the view does not know, as after silence,
//     is not another site's;
//   - a lift that failed is tried again at the next look;
//   - otherwise, while the lease reports a handover (Status.Handover), the
//     workload is paused, once per term; a pause that failed is tried again
//     at the next look, and nothing is counted before it is in place;
//   - once paused, what remains of the workload is counted at every look,
//     and when nothing remains the yield of the term is asked for, again at
//     every look while the handover stands: a yield that did not land is
//     simply asked for once more.
//
// The pause is never lifted while the site's own term is the latest and
// nothing of the site says it holds: the yield promised the other sites
// that the workload stays down.
type HandoverDriver struct {
	site           string
	wasSiteHolding bool
	// mayBePaused is false only after this driver lifted the pause; a
	// previous life may have left it.
	mayBePaused bool
	// pausedFor is the term this driver paused the workload for, -1 for
	// none.
	pausedFor int64
	// epoch is the term of the handover the look in progress is about.
	epoch int64
}

// NewHandoverDriver returns the driver of site's workload: what
// Config.Holder's site is for its participant (SiteOf).
func NewHandoverDriver(site string) *HandoverDriver {
	return &HandoverDriver{site: site, mayBePaused: true, pausedFor: -1, epoch: -1}
}

// Look starts a look at the lease with the participant's status and returns
// its first step.
func (d *HandoverDriver) Look(s Status) HandoverStep {
	rising := s.SiteHolding && !d.wasSiteHolding
	d.wasSiteHolding = s.SiteHolding
	lastSite, known := s.Lease.LastSite()
	otherHolds := s.Certain && known && lastSite != d.site
	switch {
	case rising || (otherHolds && d.mayBePaused):
		return HandoverStep{Do: HandoverLift, Epoch: d.pausedFor, Rising: rising}
	case s.Handover:
		d.mayBePaused = true
		d.epoch = s.HandoverEpoch
		if d.pausedFor != d.epoch {
			return HandoverStep{Do: HandoverPause, Epoch: d.epoch}
		}
		return HandoverStep{Do: HandoverCount, Epoch: d.epoch}
	}
	return HandoverStep{Epoch: -1}
}

// Lifted reports how a HandoverLift went and ends the look. A lift that
// failed is asked for again at the next look.
func (d *HandoverDriver) Lifted(ok bool) HandoverStep {
	if ok {
		d.mayBePaused, d.pausedFor = false, -1
	} else {
		d.wasSiteHolding = false
	}
	return HandoverStep{Epoch: -1}
}

// Paused reports how a HandoverPause went. With the pause in place the next
// step is the count; without it the look ends, and the pause is asked for
// again at the next one.
func (d *HandoverDriver) Paused(ok bool) HandoverStep {
	if !ok {
		return HandoverStep{Epoch: -1}
	}
	d.pausedFor = d.epoch
	return HandoverStep{Do: HandoverCount, Epoch: d.epoch}
}

// Remaining reports what a HandoverCount found: 0 once the workload is down
// and stays down, anything else a positive count. The yield is asked for
// only when nothing remains.
func (d *HandoverDriver) Remaining(n int) HandoverStep {
	if n > 0 {
		return HandoverStep{Epoch: -1}
	}
	return HandoverStep{Do: HandoverYield, Epoch: d.epoch}
}
