// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// stall is a network that stops carrying what a client writes and later
// carries it after all, the connections surviving: what a renew in flight
// meets when its holder is cut off for a few seconds.
type stall struct {
	mu   sync.Mutex
	heal chan struct{} // nil while the network works
}

func (s *stall) set(on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case on && s.heal == nil:
		s.heal = make(chan struct{})
	case !on && s.heal != nil:
		close(s.heal)
		s.heal = nil
	}
}

func (s *stall) dialer() kgo.Opt {
	d := &net.Dialer{Timeout: 10 * time.Second}
	return kgo.Dialer(func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := d.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return stalledConn{conn, s}, nil
	})
}

type stalledConn struct {
	net.Conn
	s *stall
}

func (c stalledConn) Write(b []byte) (int, error) {
	c.s.mu.Lock()
	heal := c.s.heal
	c.s.mu.Unlock()
	if heal != nil {
		<-heal
	}
	return c.Conn.Write(b)
}

// The client's write deadline would cut the stall short.
func (c stalledConn) SetWriteDeadline(time.Time) error { return nil }
func (c stalledConn) SetDeadline(t time.Time) error    { return c.Conn.SetReadDeadline(t) }

// A holder cut off from the brokers with a renew in flight is stopped while
// it still believes. The release of the stop waits for the renew (rule 2)
// and is given up once the belief would have run out; when the network
// comes back, no release of the stopped holder lands behind the next
// holder's claim. Before this was so, the renew and the release landed
// together, both too late, and a reader that joined the log right there
// took the pair for an anchor.
func TestIntegrationStopWithARenewInFlight(t *testing.T) {
	const ttl = 6 * time.Second // a belief of 4.8 s, a renewal every 2 s
	topic := randomTopic(t)
	net := &stall{}
	lease := func(c *Config) { c.TTL, c.Margin = ttl, 0 }
	a := startWith(t, topic, "s/a", func(c *Config) {
		lease(c)
		c.ClientOpts = []kgo.Opt{net.dialer()}
	})
	waitFor(t, 5*ttl, "a holding", func() bool { return a.Status().Holding })
	b := startWith(t, topic, "s/b", lease)

	deadline := a.Status().Deadline
	waitFor(t, 2*ttl, "a renewal", func() bool { return a.Status().Deadline.After(deadline) })
	net.set(true)
	time.Sleep(ttl/3 + 300*time.Millisecond) // a's next renew is written into the stall
	stillBelieved := a.Status().Holding
	a.cancel()
	waitFor(t, 3*ttl, "b holding", func() bool { return b.Status().Holding })
	claimed := b.Status().Epoch
	net.set(false)
	select {
	case <-a.done:
	case <-time.After(3 * ttl):
		t.Fatal("a did not stop")
	}
	time.Sleep(time.Second)

	for _, r := range readAll(t, topic) {
		if r.Offset > claimed && r.Holder == a.Holder() && r.Kind == Release {
			t.Fatalf("b claimed at offset %d; a's release of epoch %d landed behind it, at offset %d", claimed, r.Epoch, r.Offset)
		}
	}
	if !b.Status().Holding {
		t.Fatal("b lost the lease")
	}
	if !stillBelieved {
		t.Skip("a had stopped believing before the stop: the run proved nothing")
	}
}

// readAll returns every record of the lease log, with offsets and broker
// timestamps.
func readAll(t *testing.T, topic string) []Record {
	t.Helper()
	cl, adm := adminClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	end, _ := ends.Lookup(topic, 0)
	cl.AddConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}})
	var out []Record
	for int64(len(out)) < end.Offset {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("read %d of %d records of %s", len(out), end.Offset, topic)
		}
		fetches.EachRecord(func(kr *kgo.Record) {
			r, err := DecodeValue(kr.Value)
			if err != nil {
				t.Fatal(err)
			}
			r.Offset, r.Timestamp = kr.Offset, kr.Timestamp
			out = append(out, r)
		})
	}
	return out
}

// The topic is deleted and created again under two running participants.
// Their views, their offsets and the epochs they know belong to a log that
// is gone: both stop with ErrLogReplaced rather than stay up and useless.
func TestIntegrationTopicRecreated(t *testing.T) {
	topic := randomTopic(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ps := []replaceable{runReplaceable(ctx, t, topic, "s/a", 0), runReplaceable(ctx, t, topic, "s/b", 0)}
	waitFor(t, 10*time.Second, "a holder", func() bool { return ps[0].c.Status().Holding != ps[1].c.Status().Holding })
	time.Sleep(3 * itTTL) // some offsets to have passed
	recreateTopic(ctx, t, topic)
	for _, p := range ps {
		p.wantReplaced(t)
	}
}

// The same with two sites and a takeover delay, and then new participants
// on the recreated, empty topic, the one of the site that did not hold
// first. That site's workload may start only once the old holder's has had
// the delay to stop: an empty log is a term of an unknown holder.
func TestIntegrationTopicRecreatedTakeoverDelay(t *testing.T) {
	topic := randomTopic(t)
	const delay = 3 * itTTL
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := runReplaceable(ctx, t, topic, "a/x", delay)
	// A new topic is empty too: the first claim waits the delay.
	waitFor(t, delay+2*itTTL, "a holds", func() bool { return a.c.Status().Holding })
	b := runReplaceable(ctx, t, topic, "b/x", delay)
	waitFor(t, itTTL, "b is ready", func() bool { return b.c.Status().Ready })
	time.Sleep(itTTL)
	if !a.c.Status().Holding || b.c.Status().Holding {
		t.Fatalf("a: %+v; b: %+v; want a holding", a.c.Status(), b.c.Status())
	}
	recreateTopic(ctx, t, topic)
	a.wantReplaced(t)
	b.wantReplaced(t)

	b = runReplaceable(ctx, t, topic, "b/y", delay)
	bStarted := time.Now()
	waitFor(t, itTTL, "the new b is ready", func() bool { return b.c.Status().Ready })
	a = runReplaceable(ctx, t, topic, "a/y", delay)
	// b's view became certain after bStarted.
	throughout(time.Until(bStarted.Add(delay)), 20*time.Millisecond, func() {
		if s := b.c.Status(); s.Holding {
			t.Fatalf("b holds epoch %d %v after it started on the recreated topic, within the takeover delay %v", s.Epoch, time.Since(bStarted), delay)
		}
	})
	waitFor(t, 2*itTTL, "a holder", func() bool { return a.c.Status().Holding || b.c.Status().Holding })
	throughout(2*itTTL, 20*time.Millisecond, func() {
		if a.c.Status().Holding == b.c.Status().Holding {
			t.Fatalf("a: %+v; b: %+v; want exactly one holding", a.c.Status(), b.c.Status())
		}
	})
	t.Logf("after the recreation, b took %v to hold: %v, a: %v", time.Since(bStarted), b.c.Status().Holding, a.c.Status().Holding)
}

// replaceable is a Candidate running in its own goroutine whose Run may end
// with an error: the participant of a topic about to be recreated.
type replaceable struct {
	c   *Candidate
	err chan error
}

func runReplaceable(ctx context.Context, t *testing.T, topic, holder string, delay time.Duration) replaceable {
	t.Helper()
	c, err := NewCandidate(Config{
		Brokers: brokers(t), Topic: topic, CreateTopic: true, TopicSettings: itTopic,
		Holder: holder, TTL: itTTL, Margin: itMargin, TakeoverDelay: delay,
	})
	if err != nil {
		t.Fatal(err)
	}
	r := replaceable{c, make(chan error, 1)}
	go func() { r.err <- c.Run(ctx) }()
	return r
}

// wantReplaced fails the test unless Run returns ErrLogReplaced soon.
func (r replaceable) wantReplaced(t *testing.T) {
	t.Helper()
	select {
	case err := <-r.err:
		if !errors.Is(err, ErrLogReplaced) {
			t.Errorf("%s: Run returned %v, want ErrLogReplaced", r.c.Holder(), err)
		}
	case <-time.After(30 * itTTL):
		s := r.c.Status()
		t.Errorf("%s still runs a minute after the topic was recreated: holding=%v certain=%v ready=%v", r.c.Holder(), s.Holding, s.Certain, s.Ready)
	}
}

// recreateTopic deletes the topic and creates it again.
func recreateTopic(ctx context.Context, t *testing.T, topic string) {
	t.Helper()
	cl, adm := adminClient(t)
	if _, err := adm.DeleteTopics(ctx, topic); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 30*time.Second, "the topic again", func() bool {
		_, err := EnsureTopic(ctx, cl, topic, itTTL, itTopic)
		return err == nil
	})
}
