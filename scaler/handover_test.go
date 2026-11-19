// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package scaler

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/kfkit/kfklease/lease"
)

type yieldSource struct {
	*fakeSource
	ymu    sync.Mutex
	yields []int64
}

func (y *yieldSource) Yield(epoch int64) {
	y.ymu.Lock()
	y.yields = append(y.yields, epoch)
	y.ymu.Unlock()
}

func (y *yieldSource) yielded() []int64 {
	y.ymu.Lock()
	defer y.ymu.Unlock()
	return slices.Clone(y.yields)
}

// fakeWorkload records the calls and fails them on request.
type fakeWorkload struct {
	mu       sync.Mutex
	left     int // what Remaining reports
	fail     error
	calls    []string
	podCalls int
}

func (f *fakeWorkload) Pause(context.Context, string) error   { return f.call("pause") }
func (f *fakeWorkload) Unpause(context.Context, string) error { return f.call("unpause") }
func (f *fakeWorkload) Remaining(context.Context, string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.podCalls++
	return f.left, f.fail
}

func (f *fakeWorkload) call(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return f.fail
	}
	f.calls = append(f.calls, name)
	return nil
}

func (f *fakeWorkload) set(left int, fail error) {
	f.mu.Lock()
	f.left, f.fail = left, fail
	f.mu.Unlock()
}

func (f *fakeWorkload) got() ([]string, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls), f.podCalls
}

func startHandover(t *testing.T, st lease.Status) (*yieldSource, *fakeWorkload) {
	t.Helper()
	wl := &fakeWorkload{left: 2}
	return startHandoverOf(t, st, wl), wl
}

// startHandoverOf runs the handover of site "eu" for ScaledObject "conn" on
// wl until the test ends.
func startHandoverOf(t *testing.T, st lease.Status, wl Workload) *yieldSource {
	t.Helper()
	src := &yieldSource{fakeSource: newFakeSource()}
	src.setStatus(st)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	h := Handover{Source: src, Workload: wl, ScaledObject: "conn", Site: "eu", Interval: 2 * time.Millisecond,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	go func() { h.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return src
}

// settle lets the driver look a few more times.
func settle() { time.Sleep(20 * time.Millisecond) }

var ownTermEnded = lease.Status{Certain: true, Ready: true, Standby: true, Lease: lease.State{Epoch: 7, Last: "eu/old"}, HandoverEpoch: -1}

func TestHandoverPausesWaitsAndYields(t *testing.T) {
	st := ownTermEnded
	st.Handover, st.HandoverEpoch = true, 7
	src, wl := startHandover(t, st)
	eventually(t, "pods counted", func() bool { _, n := wl.got(); return n >= 3 })
	if calls, _ := wl.got(); !slices.Equal(calls, []string{"pause"}) {
		t.Fatalf("calls = %q, want one pause", calls)
	}
	if y := src.yielded(); len(y) != 0 {
		t.Fatalf("yielded %v with pods left", y)
	}
	wl.set(0, nil)
	eventually(t, "yield", func() bool { return len(src.yielded()) > 0 })
	if y := src.yielded(); y[0] != 7 {
		t.Fatalf("yielded %v, want epoch 7", y)
	}
	// The yield landed: no handover any more, and the pause stays.
	src.setStatus(ownTermEnded)
	settle()
	n := len(src.yielded())
	settle()
	if calls, _ := wl.got(); !slices.Equal(calls, []string{"pause"}) || len(src.yielded()) != n {
		t.Fatalf("calls = %q, yields %v after the handover", calls, src.yielded())
	}
}

// Against the API, with the Kube that ships: no yield while the target's
// scale still asks for replicas, whatever its pod count. No pod at this
// instant is a rollout, a pod restart or a KEDA that has not applied the
// pause yet, and the pod comes back next to the other site's.
func TestHandoverNoYieldWhileReplicasAreWanted(t *testing.T) {
	k, api := newFakeKube(t)
	st := ownTermEnded
	st.Handover, st.HandoverEpoch = true, 7
	src := startHandoverOf(t, st, k)
	// A look is three requests, the first one four. After nine more, at
	// least one whole look at the workload as it is now was made and acted
	// on.
	looks := func() {
		t.Helper()
		from := api.requests()
		eventually(t, "more looks at the workload", func() bool { return api.requests() >= from+9 })
	}
	for _, sc := range []struct{ replicas, pods int }{{3, 3}, {3, 0}, {1, 0}, {1, 2}, {1, 0}} {
		api.setScale(sc.replicas, sc.pods)
		looks()
		if y := src.yielded(); len(y) != 0 {
			t.Fatalf("yielded %v with spec.replicas %d and %d pods", y, sc.replicas, sc.pods)
		}
	}
	api.setScale(0, 0)
	eventually(t, "yield", func() bool { return len(src.yielded()) > 0 })
	if y := src.yielded(); y[0] != 7 {
		t.Fatalf("yielded %v, want epoch 7", y)
	}
}

func TestHandoverWithoutPauseNoYield(t *testing.T) {
	st := ownTermEnded
	st.Handover, st.HandoverEpoch = true, 7
	src, wl := startHandover(t, lease.Status{HandoverEpoch: -1})
	wl.set(0, errors.New("forbidden"))
	src.setStatus(st)
	settle()
	if y := src.yielded(); len(y) != 0 {
		t.Fatalf("yielded %v without a pause", y)
	}
	if _, n := wl.got(); n != 0 {
		t.Fatalf("pods counted %d times without a pause", n)
	}
	wl.set(0, nil)
	eventually(t, "yield after the pause went through", func() bool { return len(src.yielded()) > 0 })
}

// The pause is lifted when the participant starts to say its site holds,
// once per start, and not while that goes on: a replica that answers for
// a term from a stale read must not undo a handover's pause.
func TestHandoverLiftsThePauseWhenTheSiteHolds(t *testing.T) {
	src, wl := startHandover(t, ownTermEnded)
	settle()
	if calls, _ := wl.got(); len(calls) != 0 {
		t.Fatalf("calls = %q while the site's own term is the latest", calls)
	}
	holding := ownTermEnded
	holding.Standby, holding.SiteHolding, holding.SiteEpoch = false, true, 9
	src.setStatus(holding)
	eventually(t, "unpause", func() bool { c, _ := wl.got(); return len(c) == 1 })
	settle()
	if calls, _ := wl.got(); !slices.Equal(calls, []string{"unpause"}) {
		t.Fatalf("calls = %q, want one unpause", calls)
	}
	src.setStatus(ownTermEnded)
	settle()
	src.setStatus(holding)
	eventually(t, "unpause again", func() bool { c, _ := wl.got(); return len(c) == 2 })
}

// A failed unpause is tried again.
func TestHandoverRetriesTheUnpause(t *testing.T) {
	holding := lease.Status{Certain: true, Ready: true, SiteHolding: true, HandoverEpoch: -1}
	src, wl := startHandover(t, lease.Status{HandoverEpoch: -1})
	wl.set(0, errors.New("unavailable"))
	src.setStatus(holding)
	settle()
	wl.set(0, nil)
	eventually(t, "unpause", func() bool { c, _ := wl.got(); return len(c) == 1 })
	settle()
	if calls, _ := wl.got(); len(calls) != 1 {
		t.Fatalf("calls = %q, want one unpause", calls)
	}
}

// Once another site holds the latest term, a pause left by an earlier
// life is lifted, once.
func TestHandoverLiftsThePauseWhenAnotherSiteHolds(t *testing.T) {
	other := lease.Status{Certain: true, Ready: true, Standby: true, Lease: lease.State{Holder: "us/x", Epoch: 9, Last: "us/x"}, HandoverEpoch: -1}
	_, wl := startHandover(t, other)
	eventually(t, "unpause", func() bool { c, _ := wl.got(); return len(c) == 1 })
	settle()
	if calls, _ := wl.got(); !slices.Equal(calls, []string{"unpause"}) {
		t.Fatalf("calls = %q, want one unpause", calls)
	}
}
