// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"strings"
	"testing"
	"time"
)

func TestConfigDefaults(t *testing.T) {
	c, err := Config{Brokers: []string{"b:9092"}, Topic: "t", TTL: 10 * time.Second}.withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if c.Margin != 2*time.Second || c.RenewEvery != 10*time.Second/3 || c.Holder == "" || c.Logger == nil {
		t.Fatalf("defaults: %+v", c)
	}

	for _, tc := range []struct{ site, holder, wantSite, wantHolderPrefix string }{
		{"", "eu/pod", "eu", "eu/pod"},
		{"eu", "eu/pod", "eu", "eu/pod"},
		{"eu", "", "eu", "eu/"},
		{"", "solo", "solo", "solo"},
	} {
		c, err := Config{Brokers: []string{"b"}, Topic: "t", TTL: time.Second, Site: tc.site, Holder: tc.holder}.withDefaults()
		if err != nil {
			t.Fatal(err)
		}
		if c.Site != tc.wantSite || !strings.HasPrefix(c.Holder, tc.wantHolderPrefix) || SiteOf(c.Holder) != c.Site {
			t.Errorf("site %q, holder %q: got site %q, holder %q", tc.site, tc.holder, c.Site, c.Holder)
		}
	}

	bad := []Config{
		{Topic: "t", TTL: time.Second},
		{Brokers: []string{"b"}, TTL: time.Second},
		{Brokers: []string{"b"}, Topic: "t"},
		{Brokers: []string{"b"}, Topic: "t", TTL: time.Second, Partition: -1},
		{Brokers: []string{"b"}, Topic: "t", TTL: time.Second, Margin: 600 * time.Millisecond, RenewEvery: 500 * time.Millisecond},
		{Brokers: []string{"b"}, Topic: "t", TTL: time.Second, Health: HealthGate{FailAfter: time.Minute}},
		{Brokers: []string{"b"}, Topic: "t", TTL: time.Second, Health: HealthGate{FailAfter: -time.Minute, Cooldown: time.Minute}},
		{Brokers: []string{"b"}, Topic: "t", TTL: time.Second, TakeoverDelay: -time.Second},
		{Brokers: []string{"b"}, Topic: "t", TTL: time.Second, Site: "eu", Holder: "us/pod"},
		{Brokers: []string{"b"}, Topic: "t", TTL: time.Second, Site: "eu", Holder: "eu"},
	}
	for i, b := range bad {
		if _, err := b.withDefaults(); err == nil {
			t.Errorf("config %d accepted: %+v", i, b)
		}
	}
}

func TestStatusHoldingExpiresWithoutTheLoop(t *testing.T) {
	c, err := NewCandidate(Config{Brokers: []string{"b:9092"}, Topic: "t", TTL: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	c.setStatus(Status{Certain: true}, belief{time.Now().Add(50 * time.Millisecond), 3}, belief{})
	if s := c.Status(); !s.Holding || s.Epoch != 3 {
		t.Fatalf("before the deadline: %+v", s)
	}
	select {
	case <-c.Changed():
	default:
		t.Fatal("no change signal")
	}
	time.Sleep(60 * time.Millisecond)
	if s := c.Status(); s.Holding || s.Epoch != 0 || !s.Certain {
		t.Fatalf("after the deadline, with no loop running: %+v", s)
	}
}

func TestStatusCooldownExpiresWithoutTheLoop(t *testing.T) {
	c, err := NewCandidate(Config{Brokers: []string{"b:9092"}, Topic: "t", TTL: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(50 * time.Millisecond)
	c.setStatus(Status{Certain: true, CooldownUntil: until}, belief{}, belief{})
	if s := c.Status(); !s.CooldownUntil.Equal(until) || s.Holding {
		t.Fatalf("during the cooldown: %+v", s)
	}
	time.Sleep(60 * time.Millisecond)
	if s := c.Status(); !s.CooldownUntil.IsZero() {
		t.Fatalf("after the cooldown, with no loop running: %+v", s)
	}
}

func TestReportHealthyReachesTheLoop(t *testing.T) {
	c, err := NewCandidate(Config{Brokers: []string{"b:9092"}, Topic: "t", TTL: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if got, _, _ := c.loopInputs(); !got.IsZero() {
		t.Fatal("healthy before any report")
	}
	before := time.Now()
	c.ReportHealthy()
	if got, _, _ := c.loopInputs(); got.Before(before) {
		t.Fatalf("last healthy %v, before the report at %v", got, before)
	}
}

// SetStandby takes effect on Status at once, before the loop has seen it:
// a scaler must not report holding for one more poll.
func TestSetStandbyHidesHoldingAtOnce(t *testing.T) {
	c, err := NewCandidate(Config{Brokers: []string{"b:9092"}, Topic: "t", TTL: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	c.setStatus(Status{Certain: true, Ready: true}, belief{time.Now().Add(time.Minute), 3}, belief{})
	<-c.Changed()
	c.SetStandby(true)
	select {
	case <-c.Changed():
	default:
		t.Fatal("no change signal")
	}
	if s := c.Status(); s.Holding || !s.Standby {
		t.Fatalf("in standby: %+v", s)
	}
	if _, standby, _ := c.loopInputs(); !standby {
		t.Fatal("the loop does not see standby")
	}
	c.SetStandby(false)
	if s := c.Status(); s.Standby {
		t.Fatalf("standby off: %+v", s)
	}

	c, err = NewCandidate(Config{Brokers: []string{"b:9092"}, Topic: "t", TTL: 10 * time.Second, Standby: true})
	if err != nil {
		t.Fatal(err)
	}
	if s := c.Status(); !s.Standby {
		t.Fatalf("Config.Standby: %+v", s)
	}
}

// Yield reaches the loop once: a request is taken, not kept.
func TestYieldReachesTheLoopOnce(t *testing.T) {
	c, err := NewCandidate(Config{Brokers: []string{"b:9092"}, Topic: "t", TTL: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, e := c.loopInputs(); e != -1 {
		t.Fatalf("yield %d before any request", e)
	}
	if s := c.Status(); s.Handover || s.HandoverEpoch != -1 {
		t.Fatalf("handover before the loop ran: %+v", s)
	}
	c.Yield(7)
	if _, _, e := c.loopInputs(); e != 7 {
		t.Fatalf("yield %d, want 7", e)
	}
	if _, _, e := c.loopInputs(); e != -1 {
		t.Fatalf("yield %d taken twice", e)
	}
}
