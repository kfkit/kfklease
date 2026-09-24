// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// These tests need a broker: KFKLEASE_BROKERS=localhost:19092 with the Kafka
// container from test/e2e, or `make integration`.

const (
	itTTL    = 2 * time.Second
	itMargin = 300 * time.Millisecond
)

func brokers(t *testing.T) []string {
	t.Helper()
	env := os.Getenv("KFKLEASE_BROKERS")
	if env == "" {
		t.Skip("KFKLEASE_BROKERS not set")
	}
	return strings.Split(env, ",")
}

func randomTopic(t *testing.T) string {
	t.Helper()
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return "kfklease-test-" + hex.EncodeToString(b[:])
}

// participant is a Candidate running in its own goroutine.
type participant struct {
	*Candidate
	cancel context.CancelFunc
	crash  chan struct{}
	done   chan struct{} // closed when Run returns
}

func start(t *testing.T, topic, holder string) *participant {
	t.Helper()
	c, err := NewCandidate(Config{
		Brokers:     brokers(t),
		Topic:       topic,
		CreateTopic: true,
		Holder:      holder,
		TTL:         itTTL,
		Margin:      itMargin,
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	if err != nil {
		t.Fatal(err)
	}
	p := &participant{Candidate: c, crash: make(chan struct{}), done: make(chan struct{})}
	c.testCrash = p.crash
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	go func() {
		defer close(p.done)
		if err := c.Run(ctx); err != nil {
			t.Errorf("%s: Run: %v", holder, err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-p.done:
		case <-time.After(2 * itTTL):
			t.Errorf("%s: Run did not return", holder)
		}
	})
	return p
}

func (p *participant) stop(t *testing.T) {
	t.Helper()
	p.cancel()
	select {
	case <-p.done:
	case <-time.After(2 * itTTL):
		t.Fatalf("%s: Run did not return", p.Holder())
	}
}

// waitFor polls the status until cond holds or the timeout passes.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

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
	deadline := time.Now().Add(3 * itTTL)
	for time.Now().Before(deadline) {
		if got := holding(a, b); len(got) != 1 || got[0] != h {
			t.Fatalf("holders changed: %d", len(got))
		}
		for _, p := range []*participant{a, b} {
			s := p.Status()
			if s.Certain && s.Lease.Holder != "" && s.Lease.Holder != h.Holder() {
				t.Fatalf("%s sees holder %q in the log", p.Holder(), s.Lease.Holder)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
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
	var mu sync.Mutex
	var epochs []int64
	seen := map[string]bool{}
	note := func(p *participant) {
		s := p.Status()
		if !s.Holding {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if k := p.Holder() + "/" + string(rune('0'+len(epochs))); !seen[k] {
			if len(epochs) == 0 || epochs[len(epochs)-1] != s.Epoch {
				epochs = append(epochs, s.Epoch)
			}
			seen[k] = true
		}
	}

	// Three terms in a row, each holder stopping cleanly.
	for i := 0; i < 3; i++ {
		p := start(t, topic, "p"+string(rune('0'+i)))
		waitFor(t, 2*itTTL, "holder", func() bool { return p.Status().Holding })
		note(p)
		p.stop(t)
	}
	for i := 1; i < len(epochs); i++ {
		if epochs[i] <= epochs[i-1] {
			t.Fatalf("epochs not increasing: %v", epochs)
		}
	}
	if len(epochs) != 3 {
		t.Fatalf("got %d epochs, want 3: %v", len(epochs), epochs)
	}
}
