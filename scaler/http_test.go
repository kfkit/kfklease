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

// noMetric fails if body has any series of the metric name.
func noMetric(t *testing.T, body, name string) {
	t.Helper()
	if regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(name) + `[{ ]`).MatchString(body) {
		t.Fatalf("unexpected %s in:\n%s", name, body)
	}
}

func TestHTTPStatusAndMetrics(t *testing.T) {
	src := &fakeSource{changed: make(chan struct{}, 1), unknown: true}
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
	if st.Holder != "k3s-a/pod" || st.Holding || st.Certain || st.Ready || st.Lease != nil {
		t.Fatalf("status = %+v", st)
	}
	_, body = get(t, srv, "/metrics")
	if metric(t, body, "kfklease_holding") != "0" || metric(t, body, "kfklease_epoch") != "-1" || metric(t, body, "kfklease_certain") != "0" {
		t.Fatalf("standby metrics:\n%s", body)
	}
	noMetric(t, body, "kfklease_lease_holder_info") // not certain

	// Holding epoch 7 with 8 seconds to go, the log agreeing.
	asOf := time.Now()
	src.setStatus(lease.Status{
		Holding: true, Epoch: 7, Deadline: time.Now().Add(8 * time.Second),
		Certain: true, Ready: true, Lease: lease.State{Holder: "k3s-a/pod", Epoch: 7, Expires: asOf.Add(10 * time.Second)}, AsOf: asOf,
	})
	waitFor(t, func() bool {
		_, b := get(t, srv, "/metrics")
		return metric(t, b, `kfklease_transitions_total{to="holding"}`) == "1"
	})
	_, body = get(t, srv, "/status")
	if err := json.Unmarshal([]byte(body), &st); err != nil {
		t.Fatal(err)
	}
	if !st.Holding || !st.Ready || st.Epoch != 7 || st.DeadlineIn < 7 || st.DeadlineIn > 8 || st.Lease == nil || st.Lease.Epoch != 7 {
		t.Fatalf("holding status = %+v", st)
	}
	_, body = get(t, srv, "/metrics")
	if metric(t, body, "kfklease_holding") != "1" || metric(t, body, "kfklease_epoch") != "7" || metric(t, body, "kfklease_lease_held") != "1" {
		t.Fatalf("holding metrics:\n%s", body)
	}
	if metric(t, body, `kfklease_lease_holder_info{holder="k3s-a/pod",site="k3s-a"}`) != "1" {
		t.Fatalf("holder info:\n%s", body)
	}
	if v := metric(t, body, "kfklease_belief_remaining_seconds"); !strings.HasPrefix(v, "7.") {
		t.Fatalf("remaining = %s", v)
	}

	// Lost it: the standby transition is counted, the gauges follow.
	src.setStatus(lease.Status{Certain: true, Ready: true, Lease: lease.State{Holder: "other", Epoch: 9, Expires: asOf.Add(20 * time.Second)}, AsOf: asOf})
	waitFor(t, func() bool {
		_, b := get(t, srv, "/metrics")
		return metric(t, b, `kfklease_transitions_total{to="standby"}`) == "1"
	})
	_, body = get(t, srv, "/metrics")
	if metric(t, body, "kfklease_holding") != "0" || metric(t, body, "kfklease_lease_held") != "1" || metric(t, body, "kfklease_belief_remaining_seconds") != "0" {
		t.Fatalf("lost metrics:\n%s", body)
	}
	// Every participant reports the holder the log names, a foreign one
	// too, and only that one. A holder id without a slash is a site of its
	// own.
	if metric(t, body, `kfklease_lease_holder_info{holder="other",site="other"}`) != "1" || strings.Count(body, "\nkfklease_lease_holder_info{") != 1 {
		t.Fatalf("holder info after losing it:\n%s", body)
	}
	if metric(t, body, "kfklease_health_cooldown_seconds") != "0" {
		t.Fatalf("cooldown without a gate:\n%s", body)
	}

	// The health gate released it and holds claims back for a minute.
	src.setStatus(lease.Status{Certain: true, AsOf: asOf, CooldownUntil: time.Now().Add(time.Minute)})
	_, body = get(t, srv, "/metrics")
	if v := metric(t, body, "kfklease_health_cooldown_seconds"); !strings.HasPrefix(v, "59.") {
		t.Fatalf("cooldown = %s", v)
	}
	noMetric(t, body, "kfklease_lease_holder_info") // released: nobody holds it
	_, body = get(t, srv, "/status")
	if err := json.Unmarshal([]byte(body), &st); err != nil {
		t.Fatal(err)
	}
	if st.Holding || st.CooldownIn < 59 || st.CooldownIn > 60 {
		t.Fatalf("cooldown status = %+v", st)
	}
	if metric(t, getBody(t, srv, "/metrics"), "kfklease_standby_mode") != "0" || st.Standby {
		t.Fatalf("standby mode reported while off: %+v", st)
	}

	// Standby mode.
	src.setStatus(lease.Status{Certain: true, Ready: true, AsOf: asOf, Standby: true})
	_, body = get(t, srv, "/status")
	if err := json.Unmarshal([]byte(body), &st); err != nil {
		t.Fatal(err)
	}
	if !st.Standby || st.Holding {
		t.Fatalf("standby status = %+v", st)
	}
	if v := metric(t, getBody(t, srv, "/metrics"), "kfklease_standby_mode"); v != "1" {
		t.Fatalf("kfklease_standby_mode = %s", v)
	}
	if code, _ := get(t, srv, "/nope"); code != 404 {
		t.Fatalf("unknown path: %d", code)
	}
}

// getBody returns the body of a GET.
func getBody(t *testing.T, srv *httptest.Server, path string) string {
	t.Helper()
	_, body := get(t, srv, path)
	return body
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

func TestHTTPSeveralLeases(t *testing.T) {
	a := &fakeSource{changed: make(chan struct{}, 1)}
	b := &fakeSource{changed: make(chan struct{}, 1)}
	h, err := NewHTTPLeases([]Lease{{Topic: "connect-a", Holder: "k3s-a/pod", Source: a}, {Topic: "connect-b", Holder: "k3s-a/pod", Source: b}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.Watch(ctx)
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()

	asOf := time.Now()
	b.setStatus(lease.Status{Holding: true, Epoch: 42, Deadline: time.Now().Add(8 * time.Second), Certain: true,
		Lease: lease.State{Holder: "k3s-a/pod", Epoch: 42, Expires: asOf.Add(10 * time.Second)}, AsOf: asOf})
	waitFor(t, func() bool {
		_, body := get(t, srv, "/metrics")
		return metric(t, body, `kfklease_transitions_total{to="holding",topic="connect-b"}`) == "1"
	})
	_, body := get(t, srv, "/metrics")
	for name, want := range map[string]string{
		`kfklease_holding{topic="connect-a"}`:                                           "0",
		`kfklease_epoch{topic="connect-a"}`:                                             "-1",
		`kfklease_holding{topic="connect-b"}`:                                           "1",
		`kfklease_epoch{topic="connect-b"}`:                                             "42",
		`kfklease_lease_held{topic="connect-b"}`:                                        "1",
		`kfklease_transitions_total{to="holding",topic="connect-a"}`:                    "0",
		`kfklease_lease_holder_info{holder="k3s-a/pod",site="k3s-a",topic="connect-b"}`: "1",
	} {
		if got := metric(t, body, name); got != want {
			t.Errorf("%s = %s, want %s", name, got, want)
		}
	}

	if strings.Contains(body, `kfklease_lease_holder_info{holder="k3s-a/pod",site="k3s-a",topic="connect-a"}`) {
		t.Errorf("holder info for connect-a, which nobody holds:\n%s", body)
	}

	for topic, holding := range map[string]bool{"connect-a": false, "connect-b": true} {
		code, body := get(t, srv, "/status?topic="+topic)
		var st StatusResponse
		if err := json.Unmarshal([]byte(body), &st); err != nil || code != 200 {
			t.Fatalf("status %s: %d %v %s", topic, code, err, body)
		}
		if st.Topic != topic || st.Holder != "k3s-a/pod" || st.Holding != holding {
			t.Fatalf("status %s = %+v", topic, st)
		}
	}
	if code, _ := get(t, srv, "/status"); code != http.StatusBadRequest {
		t.Fatalf("status without a topic: %d, want 400", code)
	}
	if code, _ := get(t, srv, "/status?topic=other"); code != http.StatusNotFound {
		t.Fatalf("status of another topic: %d, want 404", code)
	}
}

func TestHTTPOneLeaseTopic(t *testing.T) {
	src := &fakeSource{changed: make(chan struct{}, 1)}
	h, err := NewHTTPLeases([]Lease{{Topic: "orders", Holder: "eu/pod", Source: src}})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h.Handler())
	defer srv.Close()
	// One lease: /status needs no topic, and the metrics name it.
	for _, path := range []string{"/status", "/status?topic=orders"} {
		code, body := get(t, srv, path)
		var st StatusResponse
		if err := json.Unmarshal([]byte(body), &st); err != nil || code != 200 || st.Topic != "orders" {
			t.Fatalf("%s: %d %v %s", path, code, err, body)
		}
	}
	if code, _ := get(t, srv, "/status?topic=other"); code != http.StatusNotFound {
		t.Fatalf("status of another topic: %d, want 404", code)
	}
	_, body := get(t, srv, "/metrics")
	if metric(t, body, `kfklease_holding{topic="orders"}`) != "0" {
		t.Fatalf("metrics:\n%s", body)
	}
}
