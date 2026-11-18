// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kfkit/kfklease/lease"
	"github.com/kfkit/kfklease/scaler"
)

type readySource struct{ ready atomic.Bool }

func (r *readySource) Status() lease.Status     { return lease.Status{Ready: r.ready.Load()} }
func (r *readySource) Changed() <-chan struct{} { return nil }

func TestWaitReadyWaitsForEveryLease(t *testing.T) {
	a, b := &readySource{}, &readySource{}
	a.ready.Store(true)
	done := make(chan bool, 1)
	go func() { done <- waitReady(context.Background(), []scaler.Source{a, b}, time.Millisecond) }()
	select {
	case <-done:
		t.Fatal("ready while one lease is not known yet")
	case <-time.After(20 * time.Millisecond):
	}
	b.ready.Store(true)
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("waitReady = false, want true")
		}
	case <-time.After(time.Second):
		t.Fatal("not ready once every lease is known")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if waitReady(ctx, []scaler.Source{&readySource{}}, time.Millisecond) {
		t.Fatal("ready after the context was cancelled")
	}
}
