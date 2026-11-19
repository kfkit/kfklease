// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Config describes a lease and this participant.
type Config struct {
	// Brokers are the bootstrap addresses.
	Brokers []string
	// Topic holds the lease. One lease per partition; see EnsureTopic.
	Topic string
	// Partition of the lease within Topic. Zero is fine for a one-partition
	// topic.
	Partition int32
	// CreateTopic makes Run create Topic with EnsureTopic if it is missing,
	// and check it and log what falls short of TopicSettings if it exists.
	CreateTopic bool
	// TopicSettings are the replication and durability settings of a topic
	// CreateTopic creates. The zero value is three replicas, two in sync.
	TopicSettings TopicSettings

	// Site is where this participant runs, a cluster say: the lease belongs
	// to a site rather than to one process. A participant claims at once
	// when the term that ended last was its own site's, and waits
	// TakeoverDelay after the end of another site's term. Empty means the
	// site of Holder, see SiteOf.
	Site string
	// Holder identifies this participant in the log as "<site>/<name>". It
	// must be unique per process incarnation: a restarted process must not
	// inherit the term of its previous life. Empty means
	// "<site>/<hostname>-<random>", or "<hostname>/<random>" without a Site.
	Holder string
	// TakeoverDelay is how long a participant waits, after a term of
	// another site ended by expiry or release, before it claims: the time
	// that site's workload gets to stop. Zero claims as soon as the lease
	// is free.
	TakeoverDelay time.Duration

	// TTL is how long a term lasts without renewal. It is a property of the
	// lease and must be the same for every participant.
	TTL time.Duration
	// Margin is subtracted from every deadline the holder sets for itself.
	// It has to cover twice the clock skew between brokers, the drift of
	// this client's clock over one TTL, and the latency of a renewal; see
	// docs/protocol.md. Zero means TTL / 5.
	Margin time.Duration
	// RenewEvery is how often a holder renews. Zero means TTL / 3.
	RenewEvery time.Duration

	// Standby starts the participant in standby: it follows the log and
	// never claims. See Candidate.SetStandby.
	Standby bool

	// Health is an optional gate on the workload's health, fed by
	// Candidate.ReportHealthy. The zero value turns it off.
	Health HealthGate

	// Auth is how the client authenticates to the brokers: TLS, mTLS, SASL.
	// The zero value is plaintext.
	Auth Auth

	// Logger receives protocol events. Nil means slog.Default.
	Logger *slog.Logger
	// ClientOpts are passed to the Kafka client after the options the
	// protocol and Auth set: dial timeouts, other SASL mechanisms and the
	// like.
	ClientOpts []kgo.Opt
}

// HealthGate makes a holder give the lease up when its workload is not
// healthy. A holder whose workload has not been reported healthy for
// FailAfter, counted from the later of the start of its term and the last
// healthy report, releases the lease and claims nothing for Cooldown.
type HealthGate struct {
	// FailAfter is how long a holder's workload may go without a healthy
	// report. It is also the time a workload has to come up in a new term.
	// Zero turns the gate off.
	FailAfter time.Duration
	// Cooldown is how long the participant stays out after such a release,
	// so that the lease does not come straight back to a workload that
	// cannot run here. Required with FailAfter.
	Cooldown time.Duration
}

func (c Config) withDefaults() (_ Config, err error) {
	if len(c.Brokers) == 0 {
		return c, errors.New("lease: no brokers")
	}
	if c.Topic == "" {
		return c, errors.New("lease: no topic")
	}
	if c.Partition < 0 {
		return c, errors.New("lease: negative partition")
	}
	if c.TTL <= 0 {
		return c, errors.New("lease: ttl must be positive")
	}
	if c.Margin < 0 || c.RenewEvery < 0 {
		return c, errors.New("lease: negative margin or renew interval")
	}
	if c.Margin == 0 {
		c.Margin = c.TTL / 5
	}
	if c.RenewEvery == 0 {
		c.RenewEvery = c.TTL / 3
	}
	if c.Health.FailAfter < 0 || c.Health.Cooldown < 0 {
		return c, errors.New("lease: negative health fail-after or cooldown")
	}
	if c.Health.FailAfter > 0 && c.Health.Cooldown == 0 {
		return c, errors.New("lease: a health gate needs a cooldown")
	}
	if c.Margin+c.RenewEvery >= c.TTL {
		return c, fmt.Errorf("lease: margin %v + renew interval %v must stay below ttl %v", c.Margin, c.RenewEvery, c.TTL)
	}
	if c.TopicSettings, err = c.TopicSettings.withDefaults(); err != nil {
		return c, err
	}
	if c.TakeoverDelay < 0 {
		return c, errors.New("lease: negative takeover delay")
	}
	if c.Holder == "" {
		host, _ := os.Hostname()
		var b [4]byte
		if _, err := rand.Read(b[:]); err != nil {
			return c, fmt.Errorf("lease: random holder id: %w", err)
		}
		c.Holder = host + "/" + hex.EncodeToString(b[:])
		if c.Site != "" {
			c.Holder = c.Site + "/" + host + "-" + hex.EncodeToString(b[:])
		}
	}
	switch site := SiteOf(c.Holder); {
	case c.Site == "":
		c.Site = site
	case c.Site != site || c.Holder == c.Site:
		return c, fmt.Errorf("lease: holder %q is not in site %q: holder ids are <site>/<name>", c.Holder, c.Site)
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c, nil
}

// consumeFromStart is the partition to follow, from its first retained
// record.
func (c Config) consumeFromStart() map[string]map[int32]kgo.Offset {
	return map[string]map[int32]kgo.Offset{c.Topic: {c.Partition: kgo.NewOffset().AtStart()}}
}

// pollEvery is how long the loop waits for records before looking at its
// timers again.
func (c Config) pollEvery() time.Duration { return c.RenewEvery / 4 }

// Status is what a participant currently knows and believes.
type Status struct {
	// Holding is this participant's belief that it holds the lease. It is the
	// only field a workload may act on.
	Holding bool
	// Epoch is the fencing token of the held term, valid while Holding.
	Epoch int64
	// Deadline is when the belief expires unless renewed, on the local
	// monotonic clock; zero unless Holding.
	Deadline time.Time

	// SiteHolding is the belief that this participant's site holds the
	// lease: it holds it itself, or it reads from the log, fresh, that
	// another participant of its site does. That other participant's term
	// counts from the moment its record was read here, for the TTL less the
	// margin. It needs a TakeoverDelay, which keeps other sites away for
	// that long after the term ended, and it is never set in standby. What
	// a site's workload acts on when the site runs several participants.
	SiteHolding bool
	// SiteEpoch is the fencing token of the site's term, valid while
	// SiteHolding: Epoch when Holding.
	SiteEpoch int64
	// SiteDeadline is when the site belief expires unless the log shows a
	// renewal, on the local monotonic clock; zero unless SiteHolding.
	SiteDeadline time.Time

	// Certain is whether Lease can be trusted; see View.
	Certain bool
	// Lease is the state of the lease in the log, as of AsOf on the broker's
	// clock. It may be held by someone else, or by this participant past
	// its own deadline.
	Lease State
	AsOf  time.Time

	// CooldownUntil is when this participant may claim again after the
	// health gate released the lease, on the local monotonic clock; zero
	// unless it is waiting.
	CooldownUntil time.Time

	// Ready is whether this participant has known the state of the lease
	// since it started: false until its view is first certain with the log
	// read up to its end. Before that, not Holding means "do not know"
	// rather than "another holds it".
	Ready bool

	// Standby is whether this participant is in standby: it follows the
	// log and never claims.
	Standby bool

	// Handover is set in standby while this participant's site owes the
	// other sites a yield: the term that ended last was the site's, no
	// yield for it is in the log, and the takeover delay after its end has
	// not passed. Whoever drives the site's workload stops it now and,
	// once nothing of it runs any more, calls Candidate.Yield with
	// HandoverEpoch; another site then takes over without waiting for the
	// rest of the delay.
	Handover bool
	// HandoverEpoch is the term to yield, valid while Handover; -1
	// otherwise.
	HandoverEpoch int64
}

// Candidate takes part in a lease: it follows the log, claims the lease when
// it is free, renews it while it believes it is the holder and gives it up
// when told to stop.
//
// A Candidate is safe for concurrent use by the goroutine running it and
// any number of goroutines reading its status.
type Candidate struct {
	cfg Config
	log *slog.Logger

	mu      sync.Mutex
	status  Status // without Holding and Epoch, which depend on the clock
	changed chan struct{}
	// The beliefs: its own term and its site's, each until a deadline
	// (monotonic). Status checks the deadlines itself, so that a Candidate
	// whose loop is stalled (a frozen process waking up) never reports a
	// belief it has outlived.
	own, site belief

	// healthyAt is the last ReportHealthy, for the loop to pick up.
	healthyAt time.Time
	// standby is what SetStandby asked for, for the loop to pick up.
	standby bool
	// yieldEpoch is what Yield asked for, for the loop to pick up; -1 for
	// nothing.
	yieldEpoch int64

	// testCrash makes Run return at once, without a release, when closed.
	testCrash <-chan struct{}
}

// NewCandidate validates cfg and returns a Candidate that does nothing until
// Run is called.
func NewCandidate(cfg Config) (*Candidate, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	return &Candidate{
		cfg:        cfg,
		log:        cfg.Logger.With("lease", cfg.Topic, "holder", cfg.Holder),
		changed:    make(chan struct{}, 1),
		standby:    cfg.Standby,
		yieldEpoch: -1,
		status:     Status{HandoverEpoch: -1},
	}, nil
}

// Holder returns this participant's identity in the log.
func (c *Candidate) Holder() string { return c.cfg.Holder }

// Status returns the current status. Holding is evaluated against the clock
// at the time of the call.
func (c *Candidate) Status() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.status
	s.Standby = c.standby
	now := time.Now()
	switch {
	case c.standby:
		// Neither belief; the loop drops them as soon as it sees standby.
	case now.Before(c.own.deadline):
		s.Holding = true
		s.Epoch, s.Deadline = c.own.epoch, c.own.deadline
		s.SiteHolding = true
		s.SiteEpoch, s.SiteDeadline = s.Epoch, s.Deadline
	case now.Before(c.site.deadline):
		s.SiteHolding = true
		s.SiteEpoch, s.SiteDeadline = c.site.epoch, c.site.deadline
	}
	if !now.Before(s.CooldownUntil) {
		s.CooldownUntil = time.Time{}
	}
	if !c.standby {
		// A handover is for standby only; the loop drops it as soon as it
		// sees standby off.
		s.Handover, s.HandoverEpoch = false, -1
	}
	return s
}

// ReportHealthy tells the health gate that the workload was seen healthy
// just now. Without Config.Health it does nothing.
func (c *Candidate) ReportHealthy() {
	now := time.Now()
	c.mu.Lock()
	c.healthyAt = now
	c.mu.Unlock()
}

// SetStandby switches standby on or off. In standby the participant follows
// the log and never claims. A term it holds ends at once: Status stops
// reporting Holding when SetStandby returns, and the release is written
// right after. Another site then takes the lease over after its takeover
// delay, which is the time this site's workload has to stop. Off, the
// participant claims again by the usual rules.
func (c *Candidate) SetStandby(on bool) {
	c.mu.Lock()
	changed := c.standby != on
	c.standby = on
	c.mu.Unlock()
	if changed {
		c.signal()
	}
}

// Yield writes a yield for epoch, the HandoverEpoch of Status, if Status
// still reports that handover when the loop picks it up: it tells the other
// sites that this site's workload has stopped after that term, so they may
// take over at once rather than after their takeover delay. Call it only
// once nothing of the workload runs any more, terminating pods included;
// a yield is a promise the protocol cannot check.
func (c *Candidate) Yield(epoch int64) {
	c.mu.Lock()
	c.yieldEpoch = epoch
	c.mu.Unlock()
	c.signal()
}

// loopInputs are what other goroutines asked for, for the loop to pick up;
// a yield request is taken.
func (c *Candidate) loopInputs() (healthyAt time.Time, standby bool, yieldEpoch int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	yieldEpoch, c.yieldEpoch = c.yieldEpoch, -1
	return c.healthyAt, c.standby, yieldEpoch
}

// Changed is signalled whenever Status changes. Signals coalesce: a receiver
// that lags sees one signal for several changes, never none.
func (c *Candidate) Changed() <-chan struct{} { return c.changed }

func (c *Candidate) setStatus(s Status, own, site belief) {
	c.mu.Lock()
	same := c.status == s && c.own == own && c.site == site
	c.status, c.own, c.site = s, own, site
	c.mu.Unlock()
	if !same {
		c.signal()
	}
}

func (c *Candidate) signal() {
	select {
	case c.changed <- struct{}{}:
	default:
	}
}

// Run takes part in the lease until ctx is done. If the candidate holds the
// lease at that point it stops believing at once and releases the term
// before it returns: after the record it has in flight, if any, was read
// back, and not at all once the belief would have run out anyway (see
// stop in agent.go). Run returns nil after a clean stop and an error when
// it cannot go on: the topic is missing or misconfigured, the broker
// rejects the client, or the log is no longer the one it read
// (ErrLogReplaced: the topic was deleted and created again).
//
// Losing the connection to Kafka is not an error: the candidate keeps
// trying, stops believing it holds the lease when its deadline passes, and
// picks up where it was once the log is readable again.
func (c *Candidate) Run(ctx context.Context) error {
	cfg := c.cfg
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID("kfklease"),
		kgo.ConsumePartitions(cfg.consumeFromStart()),
		kgo.FetchMaxWait(cfg.pollEvery()),
		kgo.KeepRetryableFetchErrors(),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.MaxProduceRequestsInflightPerBroker(1),
		// A record that did not land within a TTL is useless: a claim is
		// long stale and a renew would be a late renew.
		kgo.RecordDeliveryTimeout(cfg.TTL),
	}
	authOpts, err := cfg.Auth.clientOpts()
	if err != nil {
		return err
	}
	opts = append(opts, authOpts...)
	opts = append(opts, cfg.ClientOpts...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("lease: kafka client: %w", err)
	}
	defer cl.Close()

	if cfg.CreateTopic {
		warnings, err := EnsureTopic(ctx, cl, cfg.Topic, cfg.TTL, cfg.TopicSettings)
		if err != nil {
			return err
		}
		for _, w := range warnings {
			c.log.Warn("existing lease topic falls short", "problem", w)
		}
	}

	r := &run{c: c, cfg: cfg, log: c.log, cl: cl, results: make(chan produceResult, 8)}
	if err := r.bootstrap(ctx); err != nil {
		return err
	}
	err = r.loop(ctx)
	if errors.Is(err, errCrash) {
		return nil
	}
	return err
}

var errCrash = errors.New("lease: crash requested")

type produceResult struct {
	offset int64
	err    error
}

// run is the Kafka side of one Run call: it feeds the agent and produces
// what the agent decides.
type run struct {
	c       *Candidate
	cfg     Config
	log     *slog.Logger
	cl      *kgo.Client
	results chan produceResult
	a       *agent
}

func (r *run) bootstrap(ctx context.Context) error {
	start, end, err := r.partitionBounds(ctx)
	if err != nil {
		return err
	}
	now := time.Now()
	r.a = newAgent(r.cfg, r.log, start.Offset, end.Offset, now)
	r.log.Info("joined", "start_offset", start.Offset, "end_offset", end.Offset, "certain", r.a.view.Certain())
	r.publish(now)
	return nil
}

// partitionBounds returns the partition's start and end offsets. A topic
// that was just created has no leader for a moment, so it retries for a
// while before giving up.
func (r *run) partitionBounds(ctx context.Context) (start, end kadm.ListedOffset, err error) {
	adm := kadm.NewClient(r.cl)
	deadline := time.Now().Add(max(r.cfg.TTL, 10*time.Second))
	for {
		start, end, err = listBounds(ctx, adm, r.cfg.Topic, r.cfg.Partition)
		if err == nil || time.Now().After(deadline) {
			return start, end, err
		}
		r.log.Debug("partition not ready", "err", err)
		r.cl.PurgeTopicsFromClient(r.cfg.Topic)
		r.cl.AddConsumePartitions(r.cfg.consumeFromStart())
		select {
		case <-ctx.Done():
			return start, end, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func listBounds(ctx context.Context, adm *kadm.Client, topic string, partition int32) (start, end kadm.ListedOffset, err error) {
	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return start, end, fmt.Errorf("lease: list start offsets: %w", err)
	}
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return start, end, fmt.Errorf("lease: list end offsets: %w", err)
	}
	start, ok := starts.Lookup(topic, partition)
	if !ok {
		return start, end, fmt.Errorf("lease: topic %q partition %d not found", topic, partition)
	}
	end, _ = ends.Lookup(topic, partition)
	if err := errors.Join(start.Err, end.Err); err != nil {
		return start, end, fmt.Errorf("lease: topic %q partition %d: %w", topic, partition, err)
	}
	return start, end, nil
}

func (r *run) loop(ctx context.Context) error {
	// A record outlives ctx: a stop does not take back the one in flight,
	// it waits for it and writes the release behind it.
	live := context.WithoutCancel(ctx)
	// stopBy is zero until ctx is done; from then on the loop only winds
	// the agent down, for a TTL at most.
	var stopBy time.Time
	released := int64(-1) // the epoch of the release the stop wrote
	for {
		base := ctx
		if !stopBy.IsZero() {
			base = live
		}
		pollCtx, cancel := context.WithTimeout(base, r.cfg.pollEvery())
		fetches := r.cl.PollFetches(pollCtx)
		cancel()
		if fetches.IsClientClosed() {
			return errors.New("lease: kafka client closed")
		}
		if err := r.consume(fetches); err != nil {
			return err
		}

	drain:
		for {
			select {
			case res := <-r.results:
				if errors.Is(res.err, kerr.UnknownTopicID) {
					return fmt.Errorf("%w: topic %q: %w", ErrLogReplaced, r.cfg.Topic, res.err)
				}
				if err := r.a.acked(time.Now(), res.offset, res.err); err != nil {
					return fmt.Errorf("topic %q: %w", r.cfg.Topic, err)
				}
			default:
				break drain
			}
		}

		select {
		case <-r.c.testCrash:
			return errCrash
		default:
		}

		healthyAt, standby, yieldEpoch := r.c.loopInputs()
		if !healthyAt.IsZero() {
			r.a.healthy(healthyAt)
		}
		now := time.Now()
		if stopBy.IsZero() && ctx.Err() != nil {
			stopBy = now.Add(r.cfg.TTL)
			r.a.stop(now)
		}
		r.a.setStandby(now, standby)
		if yieldEpoch >= 0 {
			r.a.requestYield(now, yieldEpoch)
		}
		if rec, ok := r.a.tick(now); ok {
			if rec.Kind == Release && !stopBy.IsZero() {
				released = rec.Epoch
			}
			r.produce(live, rec)
		}
		r.publish(now)
		if stopBy.IsZero() {
			continue
		}
		switch done := r.a.stopDone(now); {
		case done && released >= 0:
			r.log.Info("released", "epoch", released)
			return nil
		case done:
			return nil
		case !now.Before(stopBy):
			r.log.Warn("stopped with a record still in flight")
			return nil
		}
	}
}

func (r *run) consume(fetches kgo.Fetches) error {
	now := time.Now()
	var err error
	fetches.EachError(func(_ string, _ int32, ferr error) {
		switch {
		case errors.Is(ferr, context.DeadlineExceeded) || errors.Is(ferr, context.Canceled):
		case errors.Is(ferr, kerr.UnknownTopicID):
			err = fmt.Errorf("%w: topic %q: %w", ErrLogReplaced, r.cfg.Topic, ferr)
		default:
			r.a.fetchFailed(ferr)
		}
	})
	if err != nil {
		return err
	}

	fetches.EachPartition(func(fp kgo.FetchTopicPartition) {
		if fp.Err != nil || fp.Topic != r.cfg.Topic || fp.Partition != r.cfg.Partition {
			return
		}
		r.a.fetched(now, fp.HighWatermark)
		for _, kr := range fp.Records {
			if err != nil {
				return
			}
			if kr.Attrs.TimestampType() != 1 {
				err = fmt.Errorf("lease: topic %q does not use LogAppendTime", r.cfg.Topic)
				return
			}
			if err = r.a.record(now, kr.Offset, kr.Timestamp, kr.Value); err != nil {
				err = fmt.Errorf("topic %q: %w", r.cfg.Topic, err)
			}
		}
	})
	if err == nil {
		r.publish(now)
	}
	return err
}

func (r *run) produce(ctx context.Context, rec Record) {
	kr, err := r.kafkaRecord(rec)
	if err != nil {
		r.log.Error("encode record", "err", err)
		return
	}
	r.cl.Produce(ctx, kr, func(kr *kgo.Record, err error) {
		r.results <- produceResult{offset: kr.Offset, err: err}
	})
}

// recordKey is the same for every record: one lease per partition, and
// compaction keeps the latest record.
const recordKey = "lease"

func (r *run) kafkaRecord(rec Record) (*kgo.Record, error) {
	value, err := EncodeValue(rec)
	if err != nil {
		return nil, err
	}
	return &kgo.Record{Topic: r.cfg.Topic, Partition: r.cfg.Partition, Key: []byte(recordKey), Value: value}, nil
}

func (r *run) publish(now time.Time) {
	r.c.setStatus(r.a.status(now))
}
