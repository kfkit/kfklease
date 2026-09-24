// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package scaler

import (
	"context"
	"encoding/json"
	"net/http"
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
}

// LeaseResponse is the log's view of the lease.
type LeaseResponse struct {
	Holder  string    `json:"holder"`
	Epoch   int64     `json:"epoch"`
	Expires time.Time `json:"expires"`
}

// HTTP serves /metrics, /status and /healthz over a Source.
type HTTP struct {
	src    Source
	holder string
	reg    *prometheus.Registry

	transitions *prometheus.CounterVec
	lastHolding atomic.Bool
}

// NewHTTP returns the handler. holder is this participant's id, reported in
// /status and as a label on the metrics.
func NewHTTP(src Source, holder string) *HTTP {
	h := &HTTP{src: src, holder: holder, reg: prometheus.NewRegistry()}
	h.transitions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kfklease_transitions_total",
		Help: "Times this participant started (to=\"holding\") or stopped (to=\"standby\") holding the lease.",
	}, []string{"to"})
	h.transitions.WithLabelValues("holding")
	h.transitions.WithLabelValues("standby")
	h.reg.MustRegister(h.transitions, h, collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	h.lastHolding.Store(src.Status().Holding)
	return h
}

// Watch counts transitions until ctx is done. Without it the gauges are
// still right; only kfklease_transitions_total stays at zero.
func (h *HTTP) Watch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.src.Changed():
			h.note()
		}
	}
}

func (h *HTTP) note() {
	holding := h.src.Status().Holding
	if h.lastHolding.Swap(holding) == holding {
		return
	}
	if holding {
		h.transitions.WithLabelValues("holding").Inc()
	} else {
		h.transitions.WithLabelValues("standby").Inc()
	}
}

// Handler routes /metrics, /status and /healthz.
func (h *HTTP) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(h.reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /status", h.status)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	return mux
}

func (h *HTTP) status(w http.ResponseWriter, _ *http.Request) {
	s := h.src.Status()
	h.note()
	resp := StatusResponse{Holder: h.holder, Holding: s.Holding, Certain: s.Certain, AsOf: s.AsOf}
	if s.Holding {
		resp.Epoch = s.Epoch
		resp.DeadlineIn = max(0, time.Until(s.Deadline).Seconds())
	}
	if s.Certain {
		resp.Lease = &LeaseResponse{Holder: s.Lease.Holder, Epoch: s.Lease.Epoch, Expires: s.Lease.Expires}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(resp)
}

// The gauges are read from the Source at scrape time, so they are always
// current and there is nothing to keep in sync.
var (
	descHolding   = prometheus.NewDesc("kfklease_holding", "1 while this participant believes it holds the lease.", nil, nil)
	descEpoch     = prometheus.NewDesc("kfklease_epoch", "Fencing token of the held term; -1 when not holding.", nil, nil)
	descDeadline  = prometheus.NewDesc("kfklease_belief_remaining_seconds", "Seconds until the belief expires unless renewed; 0 when not holding.", nil, nil)
	descCertain   = prometheus.NewDesc("kfklease_certain", "1 when the view of the log can be trusted.", nil, nil)
	descLeaseHeld = prometheus.NewDesc("kfklease_lease_held", "1 when the log says someone holds the lease, as of the last record seen.", nil, nil)
	descLeaseAge  = prometheus.NewDesc("kfklease_lease_as_of_seconds", "Unix time of the last record seen, on the broker's clock.", nil, nil)
)

// Describe implements prometheus.Collector.
func (h *HTTP) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{descHolding, descEpoch, descDeadline, descCertain, descLeaseHeld, descLeaseAge} {
		ch <- d
	}
}

// Collect implements prometheus.Collector.
func (h *HTTP) Collect(ch chan<- prometheus.Metric) {
	s := h.src.Status()
	epoch, remaining := -1.0, 0.0
	if s.Holding {
		epoch = float64(s.Epoch)
		remaining = max(0, time.Until(s.Deadline).Seconds())
	}
	ch <- prometheus.MustNewConstMetric(descHolding, prometheus.GaugeValue, b2f(s.Holding))
	ch <- prometheus.MustNewConstMetric(descEpoch, prometheus.GaugeValue, epoch)
	ch <- prometheus.MustNewConstMetric(descDeadline, prometheus.GaugeValue, remaining)
	ch <- prometheus.MustNewConstMetric(descCertain, prometheus.GaugeValue, b2f(s.Certain))
	ch <- prometheus.MustNewConstMetric(descLeaseHeld, prometheus.GaugeValue, b2f(s.Certain && s.Lease.HeldAt(s.AsOf)))
	if !s.AsOf.IsZero() {
		ch <- prometheus.MustNewConstMetric(descLeaseAge, prometheus.GaugeValue, float64(s.AsOf.UnixMilli())/1000)
	}
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
