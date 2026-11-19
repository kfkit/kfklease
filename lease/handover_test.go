// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"fmt"
	"strings"
	"testing"
)

// handoverLook carries one look of d out on a workload that answers as
// told, and returns the steps the driver asked for, in order: "lift",
// "lift rising", "lift after 7" (the term this driver paused for), "pause 7",
// "count 7", "yield 7".
func handoverLook(d *HandoverDriver, s Status, liftOK, pauseOK bool, remaining int) string {
	var steps []string
	for step := d.Look(s); step.Do != HandoverNothing; {
		switch step.Do {
		case HandoverLift:
			name := "lift"
			if step.Rising {
				name += " rising"
			}
			if step.Epoch >= 0 {
				name += fmt.Sprintf(" after %d", step.Epoch)
			}
			steps = append(steps, name)
			step = d.Lifted(liftOK)
		case HandoverPause:
			steps = append(steps, fmt.Sprintf("pause %d", step.Epoch))
			step = d.Paused(pauseOK)
		case HandoverCount:
			steps = append(steps, fmt.Sprintf("count %d", step.Epoch))
			step = d.Remaining(remaining)
		case HandoverYield:
			steps = append(steps, fmt.Sprintf("yield %d", step.Epoch))
			step = HandoverStep{}
		}
	}
	return strings.Join(steps, ", ")
}

// The rules of the handover driver, one script each: the driver of site
// "eu" looks at one status after another, on a workload whose pause and
// lift go through unless the look says otherwise.
func TestHandoverDriver(t *testing.T) {
	// The site's own term 7 is the latest and has ended; the participant
	// is in standby.
	ownEnded := Status{Certain: true, Ready: true, Standby: true, Lease: State{Epoch: 7, Last: "eu/old"}, HandoverEpoch: -1}
	handover := func(epoch int64) Status {
		s := ownEnded
		s.Lease.Epoch, s.Handover, s.HandoverEpoch = epoch, true, epoch
		return s
	}
	holding := Status{Certain: true, Ready: true, SiteHolding: true, SiteEpoch: 9, Lease: State{Holder: "eu/x", Epoch: 9, Last: "eu/x"}, HandoverEpoch: -1}
	other := Status{Certain: true, Ready: true, Standby: true, Lease: State{Holder: "us/x", Epoch: 9, Last: "us/x"}, HandoverEpoch: -1}
	// Certain by silence: the lease is free and the view does not know who
	// held the latest term.
	silence := Status{Certain: true, Ready: true, Standby: true, HandoverEpoch: -1}
	uncertain := other
	uncertain.Certain = false

	type look struct {
		s                   Status
		failLift, failPause bool
		remaining           int
		want                string
	}
	tests := []struct {
		name  string
		looks []look
	}{
		{"pauses once per term, counts at every look, yields at every look once nothing remains", []look{
			{s: handover(7), remaining: 2, want: "pause 7, count 7"},
			{s: handover(7), remaining: 1, want: "count 7"},
			{s: handover(7), want: "count 7, yield 7"},
			{s: handover(7), want: "count 7, yield 7"},
			{s: handover(7), remaining: 1, want: "count 7"},
			// The yield landed: no handover any more, and the pause stays.
			{s: ownEnded, want: ""},
			{s: handover(11), want: "pause 11, count 11, yield 11"},
		}},
		{"nothing is counted before the pause is in place, and a failed pause is tried again", []look{
			{s: handover(7), failPause: true, want: "pause 7"},
			{s: handover(7), failPause: true, want: "pause 7"},
			{s: handover(7), want: "pause 7, count 7, yield 7"},
			{s: handover(7), want: "count 7, yield 7"},
		}},
		{"the site's own ended term never lifts the pause", []look{
			{s: ownEnded, want: ""},
			{s: ownEnded, want: ""},
		}},
		{"lifts when the site starts to hold, once per start", []look{
			{s: ownEnded, want: ""},
			{s: holding, want: "lift rising"},
			{s: holding, want: ""},
			{s: ownEnded, want: ""},
			{s: holding, want: "lift rising"},
			{s: holding, want: ""},
		}},
		{"a pause of its own is lifted when the site starts to hold", []look{
			{s: handover(7), want: "pause 7, count 7, yield 7"},
			{s: holding, want: "lift rising after 7"},
			{s: holding, want: ""},
			// Lifted: the next handover of the same term pauses again.
			{s: handover(7), want: "pause 7, count 7, yield 7"},
		}},
		{"a failed lift is tried again at the next look", []look{
			{s: holding, failLift: true, want: "lift rising"},
			{s: holding, failLift: true, want: "lift rising"},
			{s: holding, want: "lift rising"},
			{s: holding, want: ""},
		}},
		{"a pause a previous life may have left is lifted once another site holds, once", []look{
			{s: other, failLift: true, want: "lift"},
			{s: other, want: "lift"},
			{s: other, want: ""},
			{s: other, want: ""},
		}},
		{"a pause of its own is lifted once another site holds, once", []look{
			{s: other, want: "lift"},
			{s: handover(7), want: "pause 7, count 7, yield 7"},
			{s: other, want: "lift after 7"},
			{s: other, want: ""},
		}},
		// The guard on the last holder: a view settled by silence knows no
		// holder of the latest term, and "" is not another site.
		{"a latest term whose holder is unknown is not another site's", []look{
			{s: silence, want: ""},
			{s: handover(7), want: "pause 7, count 7, yield 7"},
			{s: silence, want: ""},
		}},
		{"an uncertain view lifts nothing", []look{
			{s: uncertain, want: ""},
			{s: other, want: "lift"},
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewHandoverDriver("eu")
			for i, l := range tc.looks {
				if got := handoverLook(d, l.s, !l.failLift, !l.failPause, l.remaining); got != l.want {
					t.Fatalf("look %d: the driver did %q, want %q", i, got, l.want)
				}
			}
		})
	}
}
