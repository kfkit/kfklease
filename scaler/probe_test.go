// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package scaler

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestProbeReportsSuccessOnlyWhileTheSiteHolds(t *testing.T) {
	var code atomic.Int32
	code.Store(http.StatusOK)
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(int(code.Load()))
	}))
	defer srv.Close()

	src := newFakeSource()
	var healthy atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		Probe{URL: srv.URL, Interval: 5 * time.Millisecond, Timeout: time.Second, Log: slog.New(slog.DiscardHandler)}.
			Run(ctx, src, func() { healthy.Add(1) })
	}()
	defer func() { cancel(); <-done }()

	// Not holding: nothing is probed.
	time.Sleep(30 * time.Millisecond)
	if hits.Load() != 0 || healthy.Load() != 0 {
		t.Fatalf("probed while not holding: %d hits, %d healthy", hits.Load(), healthy.Load())
	}

	src.set(true)
	eventually(t, "a healthy report", func() bool { return healthy.Load() >= 2 })

	// Failing statuses are not reported; a redirect status counts as
	// healthy, as for a Kubernetes probe.
	for _, c := range []int{http.StatusServiceUnavailable, http.StatusNotFound} {
		code.Store(int32(c))
		h0 := hits.Load()
		eventually(t, "the failing status to be seen", func() bool { return hits.Load() >= h0+2 })
		before := healthy.Load()
		h1 := hits.Load()
		eventually(t, "more probes", func() bool { return hits.Load() >= h1+3 })
		if got := healthy.Load(); got != before {
			t.Fatalf("status %d reported healthy (%d -> %d)", c, before, got)
		}
	}
	code.Store(http.StatusNotModified)
	before := healthy.Load()
	eventually(t, "a healthy report after recovery", func() bool { return healthy.Load() > before })

	// Another replica of the site holds the lease: probing goes on.
	src.setSibling(true)
	h1 := hits.Load()
	eventually(t, "probes while the site holds", func() bool { return hits.Load() >= h1+2 })

	// Lease given up: probing stops.
	src.set(false)
	time.Sleep(20 * time.Millisecond)
	h0 := hits.Load()
	time.Sleep(30 * time.Millisecond)
	if hits.Load() != h0 {
		t.Fatalf("probed after giving the lease up: %d more hits", hits.Load()-h0)
	}
}

func TestProbeUnreachableIsNotHealthy(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // nothing listens there now

	src := newFakeSource()
	src.set(true)
	var healthy atomic.Int32
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	Probe{URL: url, Interval: 5 * time.Millisecond, Timeout: 100 * time.Millisecond, Log: slog.New(slog.DiscardHandler)}.
		Run(ctx, src, func() { healthy.Add(1) })
	if healthy.Load() != 0 {
		t.Fatalf("an unreachable workload was reported healthy %d times", healthy.Load())
	}
}
