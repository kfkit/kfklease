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
	fakeSource
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
	pods     int
	fail     error
	calls    []string
	podCalls int
}

func (f *fakeWorkload) Pause(context.Context, string) error   { return f.call("pause") }
func (f *fakeWorkload) Unpause(context.Context, string) error { return f.call("unpause") }
func (f *fakeWorkload) Pods(context.Context, string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.podCalls++
	return f.pods, f.fail
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

func (f *fakeWorkload) set(pods int, fail error) {
	f.mu.Lock()
	f.pods, f.fail = pods, fail
	f.mu.Unlock()
}

func (f *fakeWorkload) got() ([]string, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls), f.podCalls
}

func startHandover(t *testing.T, st lease.Status) (*yieldSource, *fakeWorkload) {
	t.Helper()
	src := &yieldSource{fakeSource: fakeSource{changed: make(chan struct{}, 1)}}
	src.setStatus(st)
	wl := &fakeWorkload{pods: 2}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	h := Handover{Source: src, Workload: wl, ScaledObject: "conn", Site: "eu", Interval: 2 * time.Millisecond,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	go func() { h.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return src, wl
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
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
