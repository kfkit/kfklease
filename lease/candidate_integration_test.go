// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"
)

func holding(ps ...*participant) []*participant {
	var out []*participant
	for _, p := range ps {
		if p.Status().Holding {
			out = append(out, p)
		}
	}
	return out
}

func TestIntegrationSingleHolder(t *testing.T) {
	topic := randomTopic(t)
	a := start(t, topic, "a")
	b := start(t, topic, "b")

	waitFor(t, itTTL, "a holder", func() bool { return len(holding(a, b)) == 1 })
	h := holding(a, b)[0]
	t.Logf("%s holds epoch %d", h.Holder(), h.Status().Epoch)

	// Renewals happen, nobody else gets in, both agree on the log.
	throughout(3*itTTL, 50*time.Millisecond, func() {
		if got := holding(a, b); len(got) != 1 || got[0] != h {
			t.Fatalf("holders changed: %d", len(got))
		}
		for _, p := range []*participant{a, b} {
			s := p.Status()
			if s.Certain && s.Lease.Holder != "" && s.Lease.Holder != h.Holder() {
				t.Fatalf("%s sees holder %q in the log", p.Holder(), s.Lease.Holder)
			}
		}
	})
	if s := h.Status(); !s.Certain || s.Lease.Holder != h.Holder() || s.Lease.Epoch != s.Epoch {
		t.Fatalf("holder status disagrees with its log view: %+v", s)
	}
}

func TestIntegrationHandoverOnStop(t *testing.T) {
	topic := randomTopic(t)
	a := start(t, topic, "a")
	waitFor(t, itTTL, "a holds", func() bool { return a.Status().Holding })
	first := a.Status().Epoch

	b := start(t, topic, "b")
	time.Sleep(itTTL / 2)
	if b.Status().Holding {
		t.Fatal("b acquired a held lease")
	}

	// A clean stop releases: b takes over well within a ttl.
	t0 := time.Now()
	a.stop(t)
	waitFor(t, itTTL, "b takes over", func() bool { return b.Status().Holding })
	took := time.Since(t0)
	t.Logf("handover after release took %v", took)
	if took > itTTL/2 {
		t.Fatalf("handover after release took %v, want under %v", took, itTTL/2)
	}
	if e := b.Status().Epoch; e <= first {
		t.Fatalf("epoch did not grow: %d after %d", e, first)
	}
}

func TestIntegrationHandoverOnCrash(t *testing.T) {
	topic := randomTopic(t)
	a := start(t, topic, "a")
	b := start(t, topic, "b")
	waitFor(t, itTTL, "a holder", func() bool { return len(holding(a, b)) == 1 })
	h, other := a, b
	if b.Status().Holding {
		h, other = b, a
	}

	// The holder vanishes without a release. The other side must wait for
	// the term to run out, and must not take over before the holder's own
	// deadline has passed.
	t0 := time.Now()
	close(h.crash)
	<-h.done
	waitFor(t, 2*itTTL, "takeover after crash", func() bool { return other.Status().Holding })
	took := time.Since(t0)
	t.Logf("takeover after crash took %v", took)
	if took < itTTL-itMargin {
		t.Fatalf("takeover after %v, before the crashed holder's deadline", took)
	}
	if took > itTTL+itTTL/2 {
		t.Fatalf("takeover after %v, expected about one ttl", took)
	}
}

func TestIntegrationLateReaderAgrees(t *testing.T) {
	topic := randomTopic(t)
	a := start(t, topic, "a")
	waitFor(t, itTTL, "a holds", func() bool { return a.Status().Holding })
	time.Sleep(itTTL) // a few renewals in the log

	b := start(t, topic, "b")
	waitFor(t, itTTL, "b is certain", func() bool { return b.Status().Certain })
	sa, sb := a.Status(), b.Status()
	if sb.Lease.Holder != a.Holder() || sb.Lease.Epoch != sa.Epoch {
		t.Fatalf("late reader sees %+v, holder has epoch %d", sb.Lease, sa.Epoch)
	}
}

func TestIntegrationEpochsFence(t *testing.T) {
	topic := randomTopic(t)
	// Three terms in a row, each holder stopping cleanly: every epoch must
	// be larger than the one before, so that a stale holder's epoch can be
	// told apart from the current holder's downstream.
	var epochs []int64
	for i := range 3 {
		p := start(t, topic, fmt.Sprintf("p%d", i))
		waitFor(t, 2*itTTL, "holder", func() bool { return p.Status().Holding })
		epochs = append(epochs, p.Status().Epoch)
		p.stop(t)
	}
	for i := 1; i < len(epochs); i++ {
		if epochs[i] <= epochs[i-1] {
			t.Fatalf("epochs not strictly increasing: %v", epochs)
		}
	}
}

func TestIntegrationHealthGate(t *testing.T) {
	topic := randomTopic(t)
	gate := HealthGate{FailAfter: time.Second, Cooldown: 3 * itTTL}
	a := startGated(t, topic, "a", gate)
	waitFor(t, itTTL, "a holds", func() bool { return a.Status().Holding })
	b := start(t, topic, "b")
	waitFor(t, itTTL, "b is certain", func() bool { return b.Status().Certain })

	// A healthy workload keeps the lease well past FailAfter.
	throughout(3*gate.FailAfter, 100*time.Millisecond, func() {
		a.ReportHealthy()
		if !a.Status().Holding {
			t.Fatal("a gave up the lease while its workload was healthy")
		}
	})

	// The workload goes quiet: a lets go after FailAfter and b takes over
	// at once, from the release, not from the term running out.
	quiet := time.Now()
	waitFor(t, gate.FailAfter+itTTL, "a releases", func() bool { return !a.Status().Holding })
	released := time.Since(quiet)
	waitFor(t, itTTL/2, "b takes over", func() bool { return b.Status().Holding })
	took := time.Since(quiet)
	t.Logf("released %v after the last healthy report, b holding after %v", released, took)
	if released < gate.FailAfter-100*time.Millisecond {
		t.Fatalf("released after %v, before FailAfter %v", released, gate.FailAfter)
	}
	if took > gate.FailAfter+itTTL-itMargin {
		t.Fatalf("b took over after %v: waited for the term instead of the release", took)
	}
	if s := a.Status(); s.CooldownUntil.IsZero() {
		t.Fatalf("a reports no cooldown: %+v", s)
	}

	// b stops and releases: the lease is free, but a stays out until its
	// cooldown is over, and claims it after that.
	b.stop(t)
	coolEnd := a.Status().CooldownUntil
	throughout(time.Until(coolEnd.Add(-100*time.Millisecond)), 50*time.Millisecond, func() {
		a.ReportHealthy()
		if a.Status().Holding {
			t.Fatalf("a claimed %v before the end of its cooldown", time.Until(coolEnd))
		}
	})
	waitFor(t, 2*itTTL, "a holds again after the cooldown", func() bool { a.ReportHealthy(); return a.Status().Holding })
}

func TestIntegrationStandby(t *testing.T) {
	topic := randomTopic(t)
	a := start(t, topic, "a")
	waitFor(t, itTTL, "a holds", func() bool { return a.Status().Holding })
	b := start(t, topic, "b")
	waitFor(t, itTTL, "b is certain", func() bool { return b.Status().Certain })

	// Standby on the holder: it stops holding at once and b takes over
	// from the release, not from the term running out.
	t0 := time.Now()
	a.SetStandby(true)
	if a.Status().Holding {
		t.Fatal("a still holding right after SetStandby")
	}
	waitFor(t, itTTL, "b takes over", func() bool { return b.Status().Holding })
	took := time.Since(t0)
	t.Logf("b holding %v after a went to standby", took)
	if took > itTTL-itMargin {
		t.Fatalf("b took over after %v: waited for the term instead of the release", took)
	}

	// b stops and releases. a, in standby, leaves the lease free.
	b.stop(t)
	throughout(2*itTTL, 50*time.Millisecond, func() {
		if a.Status().Holding {
			t.Fatal("a claimed in standby")
		}
	})
	if s := a.Status(); !s.Standby || !s.Certain || s.Lease.HeldAt(s.AsOf) {
		t.Fatalf("a in standby: %+v", s)
	}
	a.SetStandby(false)
	waitFor(t, itTTL, "a holds after standby", func() bool { return a.Status().Holding })
}

func TestIntegrationInspect(t *testing.T) {
	topic := randomTopic(t)
	a := start(t, topic, "eu/a")
	waitFor(t, itTTL, "a holds", func() bool { return a.Status().Holding })
	time.Sleep(itTTL / 2) // renewals in the log
	cfg := Config{Brokers: brokers(t), Topic: topic, TTL: itTTL, Logger: slog.New(slog.DiscardHandler)}

	ctx, cancel := context.WithTimeout(context.Background(), 3*itTTL)
	defer cancel()
	t0 := time.Now()
	s, err := Inspect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("held lease inspected in %v: %+v", time.Since(t0), s.Lease)
	if want := a.Status().Epoch; !s.Lease.HeldAt(s.AsOf) || s.Lease.Holder != "eu/a" || s.Lease.Epoch != want || s.Holding {
		t.Fatalf("inspected %+v as of %v, want eu/a holding epoch %d", s.Lease, s.AsOf, want)
	}

	// Released: free, and the last holder is still named.
	a.stop(t)
	if s, err = Inspect(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if s.Lease.HeldAt(s.AsOf) || s.Lease.Last != "eu/a" {
		t.Fatalf("after the release: %+v", s.Lease)
	}

	// The second look was at a free lease, and claimed nothing.
	if s, err = Inspect(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if s.Lease.HeldAt(s.AsOf) || s.Lease.Last != "eu/a" {
		t.Fatalf("after two looks: %+v", s.Lease)
	}

	// A topic that does not exist is an error, not a hang.
	cfg.Topic = randomTopic(t)
	short, cancel2 := context.WithTimeout(context.Background(), 2*itTTL)
	defer cancel2()
	if _, err := Inspect(short, cfg); err == nil {
		t.Fatal("inspected a missing topic")
	}
}

// Two participants of one site, as two scaler replicas of a cluster, and one
// of another site. The one of the site that does not hold believes its site
// does, with the holder's epoch, and the other site's does not; when the
// holder stops, the other of its site takes over at once, and its site
// belief carries on into its own term.
func TestIntegrationSiteReplicas(t *testing.T) {
	topic := randomTopic(t)
	delay := func(c *Config) { c.TakeoverDelay = 5 * itTTL }
	a := startWith(t, topic, "eu/a", delay)
	waitFor(t, itTTL, "a holds", func() bool { return a.Status().Holding })
	b := startWith(t, topic, "eu/b", delay)
	x := startWith(t, topic, "us/x", delay)
	// b's first site belief needs a renewal read at the end of the log.
	waitFor(t, 2*itTTL, "b believes its site holds", func() bool { return b.Status().SiteHolding })
	epoch := a.Status().Epoch
	if s := b.Status(); s.Holding || s.SiteEpoch != epoch {
		t.Fatalf("b: %+v, want the site's epoch %d and no term of its own", s, epoch)
	}
	if s := a.Status(); !s.SiteHolding || s.SiteEpoch != epoch {
		t.Fatalf("a: %+v, want its own term as the site's", s)
	}
	// Held over several renewals, not just the first.
	throughout(2*itTTL, 50*time.Millisecond, func() {
		if !b.Status().SiteHolding || x.Status().SiteHolding {
			t.Fatalf("b: %+v; x: %+v", b.Status(), x.Status())
		}
	})

	t0 := time.Now()
	a.stop(t)
	waitFor(t, itTTL, "b takes over for its site", func() bool { return b.Status().Holding })
	t.Logf("b holding %v after a stopped", time.Since(t0))
	if s := b.Status(); s.Epoch <= epoch || !s.SiteHolding || s.SiteEpoch != s.Epoch {
		t.Fatalf("b: %+v, want a new epoch above %d as the site's", s, epoch)
	}
	if x.Status().Holding || x.Status().SiteHolding {
		t.Fatalf("x took the lease of another site: %+v", x.Status())
	}
}

// The handover: a site in standby gives the term up, reports the handover,
// and once told its workload is down writes a yield; another site takes
// over from the yield, well before its takeover delay.
func TestIntegrationYield(t *testing.T) {
	topic := randomTopic(t)
	const delay = 5 * itTTL
	withDelay := func(c *Config) { c.TakeoverDelay = delay }
	a := startWith(t, topic, "eu/a", withDelay)
	waitFor(t, itTTL, "a holds", func() bool { return a.Status().Holding })
	epoch := a.Status().Epoch
	x := startWith(t, topic, "us/x", withDelay)
	waitFor(t, itTTL, "x is ready", func() bool { return x.Status().Ready })

	t0 := time.Now()
	a.SetStandby(true)
	waitFor(t, itTTL, "a reports the handover", func() bool { return a.Status().Handover })
	if s := a.Status(); s.HandoverEpoch != epoch {
		t.Fatalf("a: %+v, want a handover of epoch %d", s, epoch)
	}
	// The workload takes a while to stop; without a yield, x waits.
	time.Sleep(itTTL / 2)
	if x.Status().Holding {
		t.Fatal("x took over before the yield")
	}
	a.Yield(epoch)
	waitFor(t, delay/2, "x takes over", func() bool { return x.Status().Holding })
	took := time.Since(t0)
	t.Logf("x holding %v after a went to standby, takeover delay %v", took, delay)
	if took >= delay/2 {
		t.Fatalf("x took over after %v: waited for the delay, not the yield", took)
	}
	if s := a.Status(); s.Handover {
		t.Fatalf("a still reports a handover once another site holds: %+v", s)
	}
}
