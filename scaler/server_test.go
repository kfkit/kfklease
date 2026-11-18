// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package scaler

import (
	"context"
	"net"
	"strings"
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
	// unknown makes the source a participant that has not read the log
	// yet; otherwise Status reports Ready.
	unknown bool
}

func (f *fakeSource) Status() lease.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.st
	s.Ready = s.Ready || !f.unknown
	return s
}

func (f *fakeSource) setUnknown(unknown bool) {
	f.mu.Lock()
	f.unknown = unknown
	f.mu.Unlock()
	select {
	case f.changed <- struct{}{}:
	default:
	}
}

func (f *fakeSource) Changed() <-chan struct{} { return f.changed }

// set makes the source the holder, or no participant of its site.
func (f *fakeSource) set(holding bool) {
	f.setStatus(lease.Status{Holding: holding, Epoch: 7, SiteHolding: holding, SiteEpoch: 7})
}

// setSibling makes another participant of the source's site the holder,
// or nobody.
func (f *fakeSource) setSibling(holding bool) {
	f.setStatus(lease.Status{SiteHolding: holding, SiteEpoch: 9})
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

	// The holder, then a replica of the same site that is not: both tell
	// KEDA that the site holds the lease.
	for i, holding := range []bool{false, true, false, true, false} {
		if i < 3 {
			src.set(holding)
		} else {
			src.setSibling(holding)
		}
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

func TestSeveralLeases(t *testing.T) {
	a := &fakeSource{changed: make(chan struct{}, 1)}
	b := &fakeSource{changed: make(chan struct{}, 1)}
	srv, err := NewLeases([]Lease{{Topic: "connect-a", Source: a}, {Topic: "connect-b", Source: b}}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	srv.Register(gs)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	cl := externalscaler.NewExternalScalerClient(conn)
	ctx := context.Background()
	ref := func(md map[string]string) *externalscaler.ScaledObjectRef {
		return &externalscaler.ScaledObjectRef{Name: "x", ScalerMetadata: md}
	}

	a.set(true)
	b.set(false)
	for topic, want := range map[string]bool{"connect-a": true, "connect-b": false} {
		r := ref(map[string]string{"topic": topic})
		active, err := cl.IsActive(ctx, r)
		if err != nil || active.GetResult() != want {
			t.Fatalf("IsActive(%s) = %v, %v; want %v", topic, active.GetResult(), err, want)
		}
		m, err := cl.GetMetrics(ctx, &externalscaler.GetMetricsRequest{ScaledObjectRef: r, MetricName: MetricName})
		if err != nil || m.GetMetricValues()[0].GetMetricValue() != map[bool]int64{true: 1}[want] {
			t.Fatalf("GetMetrics(%s) = %v, %v", topic, m.GetMetricValues(), err)
		}
	}

	// Each stream follows its own lease.
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	stream, err := cl.StreamIsActive(sctx, ref(map[string]string{"topic": "connect-b"}))
	if err != nil {
		t.Fatal(err)
	}
	if r, err := stream.Recv(); err != nil || r.GetResult() {
		t.Fatalf("connect-b initial: %v %v", r.GetResult(), err)
	}
	b.set(true)
	if r, err := stream.Recv(); err != nil || !r.GetResult() {
		t.Fatalf("connect-b after acquiring: %v %v", r.GetResult(), err)
	}

	// With several leases a ScaledObject must name one, and one served here.
	for _, md := range []map[string]string{nil, {"topic": "other"}, {"topic": ""}} {
		_, err := cl.IsActive(ctx, ref(md))
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("metadata %v: err = %v, want InvalidArgument", md, err)
		}
		if md == nil && !strings.Contains(err.Error(), "name one by its topic") {
			t.Fatalf("no topic named: %v, want a request to name one", err)
		}
		if _, err := cl.GetMetricSpec(ctx, ref(md)); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("spec, metadata %v: err = %v, want InvalidArgument", md, err)
		}
	}
}

func TestNewLeasesRefuses(t *testing.T) {
	src := &fakeSource{changed: make(chan struct{}, 1)}
	for name, tc := range map[string]struct {
		leases    []Lease
		heartbeat time.Duration
	}{
		"none":            {nil, time.Second},
		"no heartbeat":    {[]Lease{{Topic: "a", Source: src}}, 0},
		"no source":       {[]Lease{{Topic: "a"}}, time.Second},
		"duplicate topic": {[]Lease{{Topic: "a", Source: src}, {Topic: "a", Source: src}}, time.Second},
		"empty topic":     {[]Lease{{Topic: "a", Source: src}, {Source: src}}, time.Second},
	} {
		if _, err := NewLeases(tc.leases, tc.heartbeat); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

// A scaler that just started does not know who holds the lease: it answers
// with an error, pushes nothing, and answers once it knows.
func TestNotReadyIsAnError(t *testing.T) {
	src := &fakeSource{changed: make(chan struct{}, 1), unknown: true}
	cl := serve(t, src, 100*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ref := &externalscaler.ScaledObjectRef{Name: "singleton"}

	if _, err := cl.IsActive(ctx, ref); status.Code(err) != codes.Unavailable {
		t.Fatalf("IsActive: err = %v, want Unavailable", err)
	}
	if _, err := cl.GetMetrics(ctx, &externalscaler.GetMetricsRequest{ScaledObjectRef: ref, MetricName: MetricName}); status.Code(err) != codes.Unavailable {
		t.Fatalf("GetMetrics: err = %v, want Unavailable", err)
	}
	if _, err := cl.GetMetricSpec(ctx, ref); err != nil {
		t.Fatalf("GetMetricSpec: %v", err)
	}

	stream, err := cl.StreamIsActive(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan bool, 4)
	go func() {
		for {
			m, err := stream.Recv()
			if err != nil {
				close(got)
				return
			}
			got <- m.GetResult()
		}
	}()
	// Several heartbeats go by with nothing pushed.
	select {
	case v := <-got:
		t.Fatalf("pushed %v while not ready", v)
	case <-time.After(350 * time.Millisecond):
	}
	src.setUnknown(false)
	if v := <-got; v {
		t.Fatal("pushed true, want false")
	}
	if a, err := cl.IsActive(ctx, ref); err != nil || a.GetResult() {
		t.Fatalf("IsActive = %v, %v; want false", a.GetResult(), err)
	}
}
