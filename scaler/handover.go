// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package scaler

import (
	"context"
	"log/slog"
	"time"

	"github.com/kfkit/kfklease/lease"
)

// HandoverSource is what the handover needs from a lease participant.
// *lease.Candidate satisfies it.
type HandoverSource interface {
	Source
	Yield(epoch int64)
}

var _ HandoverSource = (*lease.Candidate)(nil)

// Workload is how the handover stops and watches a site's workload through
// its ScaledObject. *Kube satisfies it.
type Workload interface {
	Pause(ctx context.Context, scaledObject string) error
	Unpause(ctx context.Context, scaledObject string) error
	// Remaining is 0 once the workload is down and stays down: its target
	// is scaled to zero and none of its pods is left, terminating ones
	// included. Anything else is a positive count.
	Remaining(ctx context.Context, scaledObject string) (int, error)
}

var _ Workload = (*Kube)(nil)

// Handover stops a site's workload at once when the site gives a lease up
// in standby, and tells the other sites when it is down, so that another
// site takes over without waiting for its takeover delay. See Run.
type Handover struct {
	Source   HandoverSource
	Workload Workload
	// ScaledObject scales the lease's workload in this namespace.
	ScaledObject string
	// Site is this participant's site.
	Site string
	// Interval is how often Run looks at the lease and, during a handover,
	// at what remains of the workload.
	Interval time.Duration
	Log      *slog.Logger
}

// Run drives the handover until ctx is done:
//
//   - while the lease reports a handover (lease.Status.Handover: in standby,
//     the site's term ended and no yield for it is in the log yet), it sets
//     KEDA's paused-replicas annotation to "0", so the workload goes now
//     rather than after the ScaledObject's cooldownPeriod, waits until
//     KEDA has scaled the target to zero and none of its pods is left,
//     terminating ones included, and then has the lease write a yield;
//   - it removes the annotation when this participant starts to say that
//     its site holds the lease, and whenever another site holds the latest
//     term, so that a scaler stopped half-way through a handover cannot
//     leave the workload paused for good.
//
// It never lifts the pause while the site's own term is the latest and
// another participant of the site may still answer KEDA for it: the yield
// promised the other sites that the workload stays down.
func (h Handover) Run(ctx context.Context) {
	ticker := time.NewTicker(h.Interval)
	defer ticker.Stop()
	// What to do at each look is lease.HandoverDriver's to say; Run does it
	// to the workload and tells it how that went.
	driver := lease.NewHandoverDriver(h.Site)
	// yieldedFor is the term whose yield was last logged.
	var yieldedFor int64 = -1
	for {
		s := h.Source.Status()
		for step := driver.Look(s); step.Do != lease.HandoverNothing; {
			step = h.do(ctx, driver, step, s, &yieldedFor)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// do carries one step of a look out and returns the next one.
func (h Handover) do(ctx context.Context, driver *lease.HandoverDriver, step lease.HandoverStep, s lease.Status, yieldedFor *int64) lease.HandoverStep {
	epoch := step.Epoch
	switch step.Do {
	case lease.HandoverLift:
		err := h.Workload.Unpause(ctx, h.ScaledObject)
		switch {
		case err != nil:
			h.Log.Warn("handover: cannot lift the pause", "scaled_object", h.ScaledObject, "err", err)
		case epoch >= 0 || step.Rising:
			h.Log.Info("handover: pause lifted", "scaled_object", h.ScaledObject, "site_holding", s.SiteHolding)
		}
		return driver.Lifted(err == nil)
	case lease.HandoverPause:
		err := h.Workload.Pause(ctx, h.ScaledObject)
		if err != nil {
			h.Log.Warn("handover: cannot pause the workload; the other sites wait for the takeover delay", "scaled_object", h.ScaledObject, "epoch", epoch, "err", err)
		} else {
			h.Log.Info("handover: workload paused", "scaled_object", h.ScaledObject, "epoch", epoch)
		}
		return driver.Paused(err == nil)
	case lease.HandoverCount:
		n, err := h.Workload.Remaining(ctx, h.ScaledObject)
		if err != nil {
			h.Log.Warn("handover: cannot count the workload's pods", "scaled_object", h.ScaledObject, "epoch", epoch, "err", err)
			break
		}
		return driver.Remaining(n)
	case lease.HandoverYield:
		if *yieldedFor != epoch {
			h.Log.Info("handover: workload down, yielding", "scaled_object", h.ScaledObject, "epoch", epoch)
			*yieldedFor = epoch
		}
		h.Source.Yield(epoch)
	}
	return lease.HandoverStep{}
}
