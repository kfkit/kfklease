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
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// What the tests of this package share: the clock and the records of the
// unit tests, and the broker, the participants and the waits of the
// integration tests. A helper that one file uses stays in that file.

// testTTL is the TTL of the lease in the unit tests of the fold and of the
// agent.
const testTTL = 10 * time.Second

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func at(sec float64) time.Time { return t0.Add(time.Duration(sec * float64(time.Second))) }

// recordOf is a record of kind written by holder for the term epoch, 0 for
// a claim, which names none: the one place the tests build a record. A
// claim and a renew carry testTTL, a release and a yield none. Offset and
// timestamp are for whoever lands it.
func recordOf(kind Kind, holder string, epoch int64) Record {
	r := Record{Kind: kind, Holder: holder, Epoch: epoch}
	if kind == Claim || kind == Renew {
		r.TTL = testTTL
	}
	return r
}

// logged is r in the log at offset off, at broker time sec.
func logged(off int64, sec float64, r Record) Record {
	r.Offset, r.Timestamp = off, at(sec)
	return r
}

func claim(off int64, sec float64, holder string) Record {
	return logged(off, sec, recordOf(Claim, holder, 0))
}

func renew(off int64, sec float64, holder string, epoch int64) Record {
	return logged(off, sec, recordOf(Renew, holder, epoch))
}

func release(off int64, sec float64, holder string, epoch int64) Record {
	return logged(off, sec, recordOf(Release, holder, epoch))
}

// The integration tests need a broker: KFKLEASE_BROKERS=localhost:19092
// with the Kafka container from test/e2e, or `make integration`.

const (
	itTTL    = 2 * time.Second
	itMargin = 300 * time.Millisecond
)

// itTopic fits the stand's single broker.
var itTopic = TopicSettings{ReplicationFactor: 1}

// optionalBrokers returns the brokers the environment variable name lists,
// and skips the test when it is not set.
func optionalBrokers(t *testing.T, name string) []string {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		t.Skipf("%s not set", name)
	}
	return strings.Split(v, ",")
}

// brokers is the stand's plaintext listener.
func brokers(t *testing.T) []string {
	t.Helper()
	return optionalBrokers(t, "KFKLEASE_BROKERS")
}

func randomTopic(t *testing.T) string {
	t.Helper()
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return "kfklease-test-" + hex.EncodeToString(b[:])
}

// adminClient is a client on the plaintext listener, and its admin client,
// closed when the test ends.
func adminClient(t *testing.T) (*kgo.Client, *kadm.Client) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers(t)...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl, kadm.NewClient(cl)
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
	return startGated(t, topic, holder, HealthGate{})
}

func startGated(t *testing.T, topic, holder string, gate HealthGate) *participant {
	t.Helper()
	return startWith(t, topic, holder, func(c *Config) { c.Health = gate })
}

// startWith starts a participant on the plaintext listener whose config opt
// adjusts.
func startWith(t *testing.T, topic, holder string, opt func(*Config)) *participant {
	t.Helper()
	cfg := Config{
		Brokers:       brokers(t),
		Topic:         topic,
		CreateTopic:   true,
		TopicSettings: itTopic,
		Holder:        holder,
		TTL:           itTTL,
		Margin:        itMargin,
		Logger:        slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	opt(&cfg)
	return startConfig(t, cfg)
}

// startConfig starts a participant with cfg as it is. A Run that returns an error
// fails the test, and so does one that has not returned two TTLs after the
// test ended.
func startConfig(t *testing.T, cfg Config) *participant {
	t.Helper()
	c, err := NewCandidate(cfg)
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
			t.Errorf("%s: Run: %v", c.Holder(), err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-p.done:
		case <-time.After(2 * itTTL):
			t.Errorf("%s: Run did not return", c.Holder())
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

// throughout runs check every step until d has passed: for what has to hold,
// or must not happen, all that time. check fails the test itself.
func throughout(d, step time.Duration, check func()) {
	for until := time.Now().Add(d); time.Now().Before(until); time.Sleep(step) {
		check()
	}
}
