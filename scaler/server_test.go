// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package scaler

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/kfkit/kfklease/internal/externalscaler"
	"github.com/kfkit/kfklease/lease"
)

type fakeSource struct {
	mu      sync.Mutex
	st      lease.Status
	changed chan struct{}
}

func (f *fakeSource) Status() lease.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.st
}

func (f *fakeSource) Changed() <-chan struct{} { return f.changed }

func (f *fakeSource) set(holding bool) {
	f.mu.Lock()
	f.st = lease.Status{Holding: holding, Epoch: 7}
	f.mu.Unlock()
	select {
	case f.changed <- struct{}{}:
	default:
	}
}

func serve(t *testing.T, src Source, heartbeat time.Duration) externalscaler.ExternalScalerClient {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	externalscaler.RegisterExternalScalerServer(gs, New(src, "demo", heartbeat))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return externalscaler.NewExternalScalerClient(conn)
}

func TestUnaryCalls(t *testing.T) {
	src := &fakeSource{changed: make(chan struct{}, 1)}
	cl := serve(t, src, time.Hour)
	ctx := context.Background()
	ref := &externalscaler.ScaledObjectRef{Name: "singleton", Namespace: "default"}

	spec, err := cl.GetMetricSpec(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if got := spec.GetMetricSpecs(); len(got) != 1 || got[0].GetMetricName() != MetricName || got[0].GetTargetSize() != 1 {
		t.Fatalf("spec: %v", got)
	}

	for _, holding := range []bool{false, true, false} {
		src.set(holding)
		active, err := cl.IsActive(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		if active.GetResult() != holding {
			t.Fatalf("IsActive = %v, want %v", active.GetResult(), holding)
		}
		m, err := cl.GetMetrics(ctx, &externalscaler.GetMetricsRequest{ScaledObjectRef: ref, MetricName: MetricName})
		if err != nil {
			t.Fatal(err)
		}
		var want int64
		if holding {
			want = 1
		}
		if got := m.GetMetricValues(); len(got) != 1 || got[0].GetMetricValue() != want {
			t.Fatalf("metrics = %v, want %d", got, want)
		}
	}
}

func TestTopicMismatch(t *testing.T) {
	src := &fakeSource{changed: make(chan struct{}, 1)}
	cl := serve(t, src, time.Hour)
	ref := &externalscaler.ScaledObjectRef{Name: "x", ScalerMetadata: map[string]string{"topic": "other"}}
	_, err := cl.IsActive(context.Background(), ref)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}
	ref.ScalerMetadata["topic"] = "demo"
	if _, err := cl.IsActive(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
}

func TestStreamPushesChangesAndHeartbeats(t *testing.T) {
	src := &fakeSource{changed: make(chan struct{}, 1)}
	cl := serve(t, src, 200*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := cl.StreamIsActive(ctx, &externalscaler.ScaledObjectRef{Name: "x"})
	if err != nil {
		t.Fatal(err)
	}
	recv := func() bool {
		t.Helper()
		r, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		return r.GetResult()
	}
	if recv() {
		t.Fatal("initial value: want false")
	}
	src.set(true)
	if !recv() {
		t.Fatal("after acquiring: want true")
	}
	// No change: the next message is a heartbeat with the same value.
	t0 := time.Now()
	if !recv() {
		t.Fatal("heartbeat: want true")
	}
	if d := time.Since(t0); d < 150*time.Millisecond || d > 2*time.Second {
		t.Fatalf("heartbeat after %v, want about 200ms", d)
	}
	src.set(false)
	if recv() {
		t.Fatal("after losing: want false")
	}
}
