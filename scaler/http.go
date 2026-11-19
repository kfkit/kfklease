// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package scaler

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
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
	// SiteHolding is whether this participant's site holds the lease: this
	// participant, or another replica of the site as this one reads the
	// log. What KEDA is told, and what a workload behind the scaler's
	// Service fences on when the site runs several replicas.
	SiteHolding bool `json:"site_holding"`
	// SiteEpoch is the fencing token of the site's term, valid while
	// SiteHolding.
	SiteEpoch int64 `json:"site_epoch"`
	// DeadlineIn is how long the belief lasts unless renewed, in seconds;
	// 0 when not holding.
	DeadlineIn float64 `json:"deadline_in_seconds"`
	Certain    bool    `json:"certain"`
	// Ready is whether the participant has known the lease state since it
	// started; until then KEDA gets an error rather than "not holding".
	Ready bool `json:"ready"`
	// Lease is the state of the lease in the log, present when Certain.
	Lease *LeaseResponse `json:"lease,omitempty"`
	// AsOf is the broker time of the last record seen.
	AsOf time.Time `json:"as_of"`
	// CooldownIn is how long this participant claims nothing after the
	// health gate released the lease, in seconds; 0 otherwise.
	CooldownIn float64 `json:"health_cooldown_in_seconds"`
	// Standby is whether the participant is in standby mode: it follows
	// the log and never claims.
	Standby bool `json:"standby"`
	// Handover is whether this site owes the other sites a yield for the
	// term it gave up in standby: its workload is being stopped.
	Handover bool `json:"handover"`
}

// LeaseResponse is the log's view of the lease.
type LeaseResponse struct {
	Holder  string    `json:"holder"`
	Epoch   int64     `json:"epoch"`
	Expires time.Time `json:"expires"`
}

// LogLease is the lease as the log says, as of the last record read, on the
// broker's clock: a line of `kfklease-scaler status`, and what the
// kfklease_lease_* metrics say. It is who held the lease then, not a belief
// to act on now. Where LeaseResponse is the state of the log as it stands,
// with the holder of a term that ran out still in it, this one says whether
// the term was live as of AsOf, and names a holder only then.
type LogLease struct {
	// Held is whether someone holds the lease as of AsOf.
	Held bool `json:"held"`
	// Holder, Site and Epoch name the term, when Held. Site is the holder
	// id up to its last slash.
	Holder string `json:"holder,omitempty"`
	Site   string `json:"site,omitempty"`
	Epoch  int64  `json:"epoch,omitempty"`
	// Expires is when the term ends unless renewed, when Held; otherwise
	// when the latest term ended, as far as the log tells.
	Expires time.Time `json:"expires,omitzero"`
	// LastHolder is the holder of the latest term, when the log still
	// names it.
	LastHolder string    `json:"last_holder,omitempty"`
	AsOf       time.Time `json:"as_of,omitzero"`
}

// NewLogLease reads the lease from s.Lease and s.AsOf, which mean something
// only while s.Certain.
func NewLogLease(s lease.Status) LogLease {
	l := s.Lease
	out := LogLease{Held: l.HeldAt(s.AsOf), Expires: l.Expires, LastHolder: l.Last, AsOf: s.AsOf}
	if out.Held {
		out.Holder, out.Site, out.Epoch = l.Holder, lease.SiteOf(l.Holder), l.Epoch
	}
	return out
}

// HTTP serves /metrics, /status and /healthz over one or more leases.
type HTTP struct {
	leases leases
	reg    *prometheus.Registry
	// labels are the variable labels of every lease metric: "topic", or
	// none for a single lease without a topic.
	labels []string
	gauges []gauge

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
	h.gauges = newGauges(h.labels)
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
	resp := StatusResponse{Topic: l.Topic, Holder: l.Holder, Holding: s.Holding, DeadlineIn: beliefRemaining(s), Certain: s.Certain, Ready: s.Ready, AsOf: s.AsOf, CooldownIn: untilNow(s.CooldownUntil), Standby: s.Standby, Handover: s.Handover}
	if s.Holding {
		resp.Epoch = s.Epoch
	}
	if s.SiteHolding {
		resp.SiteHolding, resp.SiteEpoch = true, s.SiteEpoch
	}
	if s.Certain {
		resp.Lease = &LeaseResponse{Holder: s.Lease.Holder, Epoch: s.Lease.Epoch, Expires: s.Lease.Expires}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(resp)
}

// gauge is one gauge of a lease. Its value is read from the lease's Source
// at scrape time, so it is always current and there is nothing to keep in
// sync.
type gauge struct {
	desc  *prometheus.Desc
	value func(lease.Status) float64
	// absent, when set, reports that the gauge has no series for a status.
	absent func(lease.Status) bool
	// more, when set, returns the values of the labels the gauge has after
	// the lease's own.
	more func(lease.Status) []string
}

// newGauges is the one list of the gauges of a lease; labels are the labels
// every one of them carries.
func newGauges(labels []string) []gauge {
	g := func(name, help string, value func(lease.Status) float64) gauge {
		return gauge{desc: prometheus.NewDesc(name, help, labels, nil), value: value}
	}
	whether := func(name, help string, is func(lease.Status) bool) gauge {
		return g(name, help, func(s lease.Status) float64 { return b2f(is(s)) })
	}
	// The log's side is trusted only on a certain view.
	held := func(s lease.Status) bool { return s.Certain && NewLogLease(s).Held }

	asOf := g("kfklease_lease_as_of_seconds", "Unix time of the last record seen, on the broker's clock.",
		func(s lease.Status) float64 { return float64(s.AsOf.UnixMilli()) / 1000 })
	asOf.absent = func(s lease.Status) bool { return s.AsOf.IsZero() }

	holderInfo := gauge{
		desc: prometheus.NewDesc("kfklease_lease_holder_info",
			"1 for the holder of the lease, as the log says as of the last record seen; no series while nobody holds it or the view is not certain. site is the holder id up to its last slash: the chart's site.",
			append(slices.Clone(labels), "holder", "site"), nil),
		value:  func(lease.Status) float64 { return 1 },
		absent: func(s lease.Status) bool { return !held(s) },
		more: func(s lease.Status) []string {
			l := NewLogLease(s)
			return []string{l.Holder, l.Site}
		},
	}

	return []gauge{
		whether("kfklease_holding", "1 while this participant believes it holds the lease.",
			func(s lease.Status) bool { return s.Holding }),
		whether("kfklease_site_holding", "1 while this participant believes its site holds the lease: what KEDA is told.",
			func(s lease.Status) bool { return s.SiteHolding }),
		g("kfklease_epoch", "Fencing token of the held term; -1 when not holding.",
			func(s lease.Status) float64 {
				if !s.Holding {
					return -1
				}
				return float64(s.Epoch)
			}),
		g("kfklease_belief_remaining_seconds", "Seconds until the belief expires unless renewed; 0 when not holding.", beliefRemaining),
		whether("kfklease_certain", "1 when the view of the log can be trusted.",
			func(s lease.Status) bool { return s.Certain }),
		whether("kfklease_lease_held", "1 when the log says someone holds the lease, as of the last record seen.", held),
		holderInfo,
		asOf,
		g("kfklease_health_cooldown_seconds", "Seconds until this participant may claim again after the health gate released the lease; 0 otherwise.",
			func(s lease.Status) float64 { return untilNow(s.CooldownUntil) }),
		whether("kfklease_standby_mode", "1 while this participant is in standby mode: it follows the log and never claims.",
			func(s lease.Status) bool { return s.Standby }),
	}
}

// Describe implements prometheus.Collector.
func (h *HTTP) Describe(ch chan<- *prometheus.Desc) {
	for _, g := range h.gauges {
		ch <- g.desc
	}
}

// Collect implements prometheus.Collector.
func (h *HTTP) Collect(ch chan<- prometheus.Metric) {
	for _, t := range h.leases.topics {
		s := h.leases.byTopic[t].Source.Status()
		for _, g := range h.gauges {
			if g.absent != nil && g.absent(s) {
				continue
			}
			var more []string
			if g.more != nil {
				more = g.more(s)
			}
			ch <- prometheus.MustNewConstMetric(g.desc, prometheus.GaugeValue, g.value(s), h.values(t, more...)...)
		}
	}
}

// beliefRemaining is how long the participant's belief lasts unless
// renewed, in seconds; 0 when not holding.
func beliefRemaining(s lease.Status) float64 {
	if !s.Holding {
		return 0
	}
	return untilNow(s.Deadline)
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
