// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package scaler

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/kfkit/kfklease/lease"
)

// StatusResponse is the body of GET /status: what the participant knows and
// believes, for dashboards and for workloads that fence by epoch.
type StatusResponse struct {
	// Topic names the lease; empty for a handler built without one.
	Topic   string `json:"topic,omitempty"`
	Holder  string `json:"holder"`
	Holding bool   `json:"holding"`
	// Epoch is the fencing token, valid while Holding.
	Epoch int64 `json:"epoch"`
	// DeadlineIn is how long the belief lasts unless renewed, in seconds;
	// 0 when not holding.
	DeadlineIn float64 `json:"deadline_in_seconds"`
	Certain    bool    `json:"certain"`
	// Lease is the state of the lease in the log, present when Certain.
	Lease *LeaseResponse `json:"lease,omitempty"`
	// AsOf is the broker time of the last record seen.
	AsOf time.Time `json:"as_of"`
	// CooldownIn is how long this participant claims nothing after the
	// health gate released the lease, in seconds; 0 otherwise.
	CooldownIn float64 `json:"health_cooldown_in_seconds"`
}

// LeaseResponse is the log's view of the lease.
type LeaseResponse struct {
	Holder  string    `json:"holder"`
	Epoch   int64     `json:"epoch"`
	Expires time.Time `json:"expires"`
}

// HTTP serves /metrics, /status and /healthz over one or more leases.
type HTTP struct {
	leases leases
	reg    *prometheus.Registry
	// labels are the variable labels of every lease metric: "topic", or
	// none for a single lease without a topic.
	labels []string
	descs  descs

	transitions *prometheus.CounterVec
	// lastHolding is per topic; the map is fixed after construction.
	lastHolding map[string]*atomic.Bool
}

// NewHTTP returns the handler for one lease. holder is this participant's
// id, reported in /status. The metrics carry no topic label.
func NewHTTP(src Source, holder string) *HTTP {
	h, err := NewHTTPLeases([]Lease{{Holder: holder, Source: src}})
	if err != nil {
		panic(err)
	}
	return h
}

// NewHTTPLeases returns the handler for several leases, at least one, with
// distinct topics. Every lease metric carries a "topic" label, unless the
// only lease has no topic. GET /status takes ?topic=, which is required
// when there is more than one lease.
func NewHTTPLeases(ls []Lease) (*HTTP, error) {
	set, err := newLeases(ls)
	if err != nil {
		return nil, err
	}
	h := &HTTP{leases: set, reg: prometheus.NewRegistry(), lastHolding: make(map[string]*atomic.Bool, len(ls))}
	if !(len(set.topics) == 1 && set.topics[0] == "") {
		h.labels = []string{"topic"}
	}
	h.descs = newDescs(h.labels)
	h.descs.holderInfo = prometheus.NewDesc("kfklease_lease_holder_info",
		"1 for the holder of the lease, as the log says as of the last record seen; no series while nobody holds it or the view is not certain. site is the holder id up to its last slash: the cluster under the chart.",
		append(slices.Clone(h.labels), "holder", "site"), nil)
	h.transitions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kfklease_transitions_total",
		Help: "Times this participant started (to=\"holding\") or stopped (to=\"standby\") holding the lease.",
	}, append(slices.Clone(h.labels), "to"))
	for _, t := range set.topics {
		h.transitions.WithLabelValues(h.values(t, "holding")...)
		h.transitions.WithLabelValues(h.values(t, "standby")...)
		b := new(atomic.Bool)
		b.Store(set.byTopic[t].Source.Status().Holding)
		h.lastHolding[t] = b
	}
	h.reg.MustRegister(h.transitions, h, collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return h, nil
}

// values returns the label values of topic's metrics, followed by more.
func (h *HTTP) values(topic string, more ...string) []string {
	if h.labels == nil {
		return more
	}
	return append([]string{topic}, more...)
}

// Watch counts transitions until ctx is done. Without it the gauges are
// still right; only kfklease_transitions_total stays at zero.
func (h *HTTP) Watch(ctx context.Context) {
	var wg sync.WaitGroup
	for _, t := range h.leases.topics {
		wg.Go(func() {
			src := h.leases.byTopic[t].Source
			for {
				select {
				case <-ctx.Done():
					return
				case <-src.Changed():
					h.note(t)
				}
			}
		})
	}
	wg.Wait()
}

func (h *HTTP) note(topic string) {
	holding := h.leases.byTopic[topic].Source.Status().Holding
	if h.lastHolding[topic].Swap(holding) == holding {
		return
	}
	to := "standby"
	if holding {
		to = "holding"
	}
	h.transitions.WithLabelValues(h.values(topic, to)...).Inc()
}

// Handler routes /metrics, /status and /healthz.
func (h *HTTP) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(h.reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /status", h.status)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	return mux
}

func (h *HTTP) status(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	l, err := h.leases.pick(q.Get("topic"), q.Has("topic"))
	if err != nil {
		code := http.StatusNotFound // a topic this handler does not serve
		if !q.Has("topic") {
			code = http.StatusBadRequest // several leases and none named
		}
		http.Error(w, err.Error(), code)
		return
	}
	s := l.Source.Status()
	h.note(l.Topic)
	resp := StatusResponse{Topic: l.Topic, Holder: l.Holder, Holding: s.Holding, Certain: s.Certain, AsOf: s.AsOf, CooldownIn: untilNow(s.CooldownUntil)}
	if s.Holding {
		resp.Epoch = s.Epoch
		resp.DeadlineIn = untilNow(s.Deadline)
	}
	if s.Certain {
		resp.Lease = &LeaseResponse{Holder: s.Lease.Holder, Epoch: s.Lease.Epoch, Expires: s.Lease.Expires}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(resp)
}

// descs are the gauges, read from the Sources at scrape time, so they are
// always current and there is nothing to keep in sync.
type descs struct {
	holding, epoch, deadline, certain, leaseHeld, leaseAge, cooldown *prometheus.Desc
	holderInfo                                                       *prometheus.Desc
}

func newDescs(labels []string) descs {
	d := func(name, help string) *prometheus.Desc { return prometheus.NewDesc(name, help, labels, nil) }
	return descs{
		holding:   d("kfklease_holding", "1 while this participant believes it holds the lease."),
		epoch:     d("kfklease_epoch", "Fencing token of the held term; -1 when not holding."),
		deadline:  d("kfklease_belief_remaining_seconds", "Seconds until the belief expires unless renewed; 0 when not holding."),
		certain:   d("kfklease_certain", "1 when the view of the log can be trusted."),
		leaseHeld: d("kfklease_lease_held", "1 when the log says someone holds the lease, as of the last record seen."),
		leaseAge:  d("kfklease_lease_as_of_seconds", "Unix time of the last record seen, on the broker's clock."),
		cooldown:  d("kfklease_health_cooldown_seconds", "Seconds until this participant may claim again after the health gate released the lease; 0 otherwise."),
	}
}

// Describe implements prometheus.Collector.
func (h *HTTP) Describe(ch chan<- *prometheus.Desc) {
	d := h.descs
	for _, x := range []*prometheus.Desc{d.holding, d.epoch, d.deadline, d.certain, d.leaseHeld, d.leaseAge, d.cooldown, d.holderInfo} {
		ch <- x
	}
}

// Collect implements prometheus.Collector.
func (h *HTTP) Collect(ch chan<- prometheus.Metric) {
	d := h.descs
	for _, t := range h.leases.topics {
		s := h.leases.byTopic[t].Source.Status()
		lv := h.values(t)
		gauge := func(desc *prometheus.Desc, v float64) {
			ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, v, lv...)
		}
		epoch, remaining := -1.0, 0.0
		if s.Holding {
			epoch = float64(s.Epoch)
			remaining = untilNow(s.Deadline)
		}
		gauge(d.holding, b2f(s.Holding))
		gauge(d.epoch, epoch)
		gauge(d.deadline, remaining)
		gauge(d.certain, b2f(s.Certain))
		held := s.Certain && s.Lease.HeldAt(s.AsOf)
		gauge(d.leaseHeld, b2f(held))
		if held {
			ch <- prometheus.MustNewConstMetric(d.holderInfo, prometheus.GaugeValue, 1, h.values(t, s.Lease.Holder, site(s.Lease.Holder))...)
		}
		gauge(d.cooldown, untilNow(s.CooldownUntil))
		if !s.AsOf.IsZero() {
			gauge(d.leaseAge, float64(s.AsOf.UnixMilli())/1000)
		}
	}
}

// site is the part of a holder id before its last slash: the chart's
// holderPrefix, the library's hostname. Empty when there is no slash.
func site(holder string) string {
	i := strings.LastIndex(holder, "/")
	if i < 0 {
		return ""
	}
	return holder[:i]
}

// untilNow is the seconds left until t, or 0 when t is zero or past.
func untilNow(t time.Time) float64 {
	if t.IsZero() {
		return 0
	}
	return max(0, time.Until(t).Seconds())
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// Source is what the HTTP handler and the scaler need; *lease.Candidate
// satisfies it.
var _ Source = (*lease.Candidate)(nil)
