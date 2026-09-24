// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package scaler

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/kfkit/kfklease/lease"
)

func (f *fakeSource) setStatus(s lease.Status) {
	f.mu.Lock()
	f.st = s
	f.mu.Unlock()
	select {
	case f.changed <- struct{}{}:
	default:
	}
}

func get(t *testing.T, srv *httptest.Server, path string) (int, string) {
	t.Helper()
	resp, err := http.Get(srv.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(b)
}

func metric(t *testing.T, body, name string) string {
	t.Helper()
	m := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(name) + ` (\S+)$`).FindStringSubmatch(body)
	if m == nil {
		t.Fatalf("metric %s not found", name)
	}
	return m[1]
}

func TestHTTPStatusAndMetrics(t *testing.T) {
	src := &fakeSource{changed: make(chan struct{}, 1)}
	h := NewHTTP(src, "k3s-a/pod")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Watch(ctx)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	if code, body := get(t, srv, "/healthz"); code != 200 || body != "ok\n" {
		t.Fatalf("healthz: %d %q", code, body)
	}

	// Standby, uncertain.
	code, body := get(t, srv, "/status")
	var st StatusResponse
	if err := json.Unmarshal([]byte(body), &st); err != nil || code != 200 {
		t.Fatalf("status: %d %v %s", code, err, body)
	}
	if st.Holder != "k3s-a/pod" || st.Holding || st.Certain || st.Lease != nil {
		t.Fatalf("status = %+v", st)
	}
	_, body = get(t, srv, "/metrics")
	if metric(t, body, "kfklease_holding") != "0" || metric(t, body, "kfklease_epoch") != "-1" || metric(t, body, "kfklease_certain") != "0" {
		t.Fatalf("standby metrics:\n%s", body)
	}

	// Holding epoch 7 with 8 seconds to go, the log agreeing.
	asOf := time.Now()
	src.setStatus(lease.Status{
		Holding: true, Epoch: 7, Deadline: time.Now().Add(8 * time.Second),
		Certain: true, Lease: lease.State{Holder: "k3s-a/pod", Epoch: 7, Expires: asOf.Add(10 * time.Second)}, AsOf: asOf,
	})
	waitFor(t, func() bool {
		_, b := get(t, srv, "/metrics")
		return metric(t, b, `kfklease_transitions_total{to="holding"}`) == "1"
	})
	_, body = get(t, srv, "/status")
	if err := json.Unmarshal([]byte(body), &st); err != nil {
		t.Fatal(err)
	}
	if !st.Holding || st.Epoch != 7 || st.DeadlineIn < 7 || st.DeadlineIn > 8 || st.Lease == nil || st.Lease.Epoch != 7 {
		t.Fatalf("holding status = %+v", st)
	}
	_, body = get(t, srv, "/metrics")
	if metric(t, body, "kfklease_holding") != "1" || metric(t, body, "kfklease_epoch") != "7" || metric(t, body, "kfklease_lease_held") != "1" {
		t.Fatalf("holding metrics:\n%s", body)
	}
	if v := metric(t, body, "kfklease_belief_remaining_seconds"); !strings.HasPrefix(v, "7.") {
		t.Fatalf("remaining = %s", v)
	}

	// Lost it: the standby transition is counted, the gauges follow.
	src.setStatus(lease.Status{Certain: true, Lease: lease.State{Holder: "other", Epoch: 9, Expires: asOf.Add(20 * time.Second)}, AsOf: asOf})
	waitFor(t, func() bool {
		_, b := get(t, srv, "/metrics")
		return metric(t, b, `kfklease_transitions_total{to="standby"}`) == "1"
	})
	_, body = get(t, srv, "/metrics")
	if metric(t, body, "kfklease_holding") != "0" || metric(t, body, "kfklease_lease_held") != "1" || metric(t, body, "kfklease_belief_remaining_seconds") != "0" {
		t.Fatalf("lost metrics:\n%s", body)
	}
	if code, _ := get(t, srv, "/nope"); code != 404 {
		t.Fatalf("unknown path: %d", code)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
