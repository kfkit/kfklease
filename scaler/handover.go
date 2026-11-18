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
	Pods(ctx context.Context, scaledObject string) (int, error)
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
	// counts the workload's pods.
	Interval time.Duration
	Log      *slog.Logger
}

// Run drives the handover until ctx is done:
//
//   - while the lease reports a handover (lease.Status.Handover: in standby,
//     the site's term ended and no yield for it is in the log yet), it sets
//     KEDA's paused-replicas annotation to "0", so the workload goes now
//     rather than after the ScaledObject's cooldownPeriod, counts the
//     target's pods until there are none left, terminating ones included,
//     and then has the lease write a yield;
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
	var (
		wasSiteHolding bool
		// mayBePaused is false only after this run removed the
		// annotation; a previous life may have left it.
		mayBePaused       = true
		pausedFor   int64 = -1
		yieldedFor  int64 = -1
	)
	for {
		s := h.Source.Status()
		rising := s.SiteHolding && !wasSiteHolding
		otherHolds := s.Certain && s.Lease.Last != "" && lease.SiteOf(s.Lease.Last) != h.Site
		switch {
		case rising || (otherHolds && mayBePaused):
			if err := h.Workload.Unpause(ctx, h.ScaledObject); err != nil {
				h.Log.Warn("handover: cannot lift the pause", "scaled_object", h.ScaledObject, "err", err)
				s.SiteHolding = false // try again at the next look
				break
			}
			if pausedFor >= 0 || rising {
				h.Log.Info("handover: pause lifted", "scaled_object", h.ScaledObject, "site_holding", s.SiteHolding)
			}
			mayBePaused, pausedFor = false, -1
		case s.Handover:
			mayBePaused = true
			h.stop(ctx, s.HandoverEpoch, &pausedFor, &yieldedFor)
		}
		wasSiteHolding = s.SiteHolding
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// stop pauses the workload for the handover of epoch and asks for the yield
// once no pod is left.
func (h Handover) stop(ctx context.Context, epoch int64, pausedFor, yieldedFor *int64) {
	if *pausedFor != epoch {
		if err := h.Workload.Pause(ctx, h.ScaledObject); err != nil {
			h.Log.Warn("handover: cannot pause the workload; the other sites wait for the takeover delay", "scaled_object", h.ScaledObject, "epoch", epoch, "err", err)
			return
		}
		h.Log.Info("handover: workload paused", "scaled_object", h.ScaledObject, "epoch", epoch)
		*pausedFor = epoch
	}
	n, err := h.Workload.Pods(ctx, h.ScaledObject)
	switch {
	case err != nil:
		h.Log.Warn("handover: cannot count the workload's pods", "scaled_object", h.ScaledObject, "epoch", epoch, "err", err)
		return
	case n > 0:
		return
	}
	if *yieldedFor != epoch {
		h.Log.Info("handover: workload down, yielding", "scaled_object", h.ScaledObject, "epoch", epoch)
		*yieldedFor = epoch
	}
	// Again at every look while the handover stands: a yield that did not
	// land is simply asked for once more.
	h.Source.Yield(epoch)
}
