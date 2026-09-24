// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
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

	bad := []Config{
		{Topic: "t", TTL: time.Second},
		{Brokers: []string{"b"}, TTL: time.Second},
		{Brokers: []string{"b"}, Topic: "t"},
		{Brokers: []string{"b"}, Topic: "t", TTL: time.Second, Partition: -1},
		{Brokers: []string{"b"}, Topic: "t", TTL: time.Second, Margin: 600 * time.Millisecond, RenewEvery: 500 * time.Millisecond},
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
	c.setStatus(Status{Certain: true}, time.Now().Add(50*time.Millisecond), 3)
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
