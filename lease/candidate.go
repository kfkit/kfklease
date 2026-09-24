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
	// CreateTopic makes Run create Topic with EnsureTopic if it is missing.
	CreateTopic bool

	// Holder identifies this participant in the log. It must be unique per
	// process incarnation: a restarted process must not inherit the term of
	// its previous life. Empty means "<hostname>/<random>".
	Holder string

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

func (c Config) withDefaults() (Config, error) {
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
	if c.Margin+c.RenewEvery >= c.TTL {
		return c, fmt.Errorf("lease: margin %v + renew interval %v must stay below ttl %v", c.Margin, c.RenewEvery, c.TTL)
	}
	if c.Holder == "" {
		host, _ := os.Hostname()
		var b [4]byte
		if _, err := rand.Read(b[:]); err != nil {
			return c, fmt.Errorf("lease: random holder id: %w", err)
		}
		c.Holder = host + "/" + hex.EncodeToString(b[:])
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

	// Certain is whether Lease can be trusted; see View.
	Certain bool
	// Lease is the state of the lease in the log, as of AsOf on the broker's
	// clock. It may be held by someone else, or by this participant past
	// its own deadline.
	Lease State
	AsOf  time.Time
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
	// The belief: holder of epoch until deadline (monotonic). Status checks
	// the deadline itself, so that a Candidate whose loop is stalled (a
	// frozen process waking up) never reports a belief it has outlived.
	deadline time.Time
	epoch    int64

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
		cfg:     cfg,
		log:     cfg.Logger.With("lease", cfg.Topic, "holder", cfg.Holder),
		changed: make(chan struct{}, 1),
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
	if !c.deadline.IsZero() && time.Now().Before(c.deadline) {
		s.Holding = true
		s.Epoch = c.epoch
	}
	return s
}

// Changed is signalled whenever Status changes. Signals coalesce: a receiver
// that lags sees one signal for several changes, never none.
func (c *Candidate) Changed() <-chan struct{} { return c.changed }

func (c *Candidate) setStatus(s Status, deadline time.Time, epoch int64) {
	c.mu.Lock()
	same := c.status == s && c.deadline.Equal(deadline) && c.epoch == epoch
	c.status, c.deadline, c.epoch = s, deadline, epoch
	c.mu.Unlock()
	if same {
		return
	}
	select {
	case c.changed <- struct{}{}:
	default:
	}
}

// Run takes part in the lease until ctx is done. If the candidate holds the
// lease at that point it releases it first. Run returns nil after a clean
// stop and an error when it cannot go on: the topic is missing or
// misconfigured, or the broker rejects the client.
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
		if err := EnsureTopic(ctx, cl, cfg.Topic, cfg.TTL); err != nil {
			return err
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
	r.shutdown()
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
	for {
		pollCtx, cancel := context.WithTimeout(ctx, r.cfg.pollEvery())
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
				r.a.acked(time.Now(), res.offset, res.err)
			default:
				break drain
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-r.c.testCrash:
			return errCrash
		default:
		}

		now := time.Now()
		if rec, ok := r.a.tick(now); ok {
			r.produce(ctx, rec)
		}
		r.publish(now)
	}
}

func (r *run) consume(fetches kgo.Fetches) error {
	now := time.Now()
	fetches.EachError(func(_ string, _ int32, err error) {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return
		}
		r.a.fetchFailed(err)
	})

	var err error
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
			err = r.a.record(now, kr.Offset, kr.Timestamp, kr.Value)
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

// shutdown gives the lease up if the participant believes it holds it.
func (r *run) shutdown() {
	now := time.Now()
	rec, ok := r.a.release(now)
	r.publish(now)
	if !ok {
		return
	}
	kr, err := r.kafkaRecord(rec)
	if err != nil {
		r.log.Error("encode record", "err", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.TTL)
	defer cancel()
	if err := r.cl.ProduceSync(ctx, kr).FirstErr(); err != nil {
		r.log.Warn("release not written", "epoch", rec.Epoch, "err", err)
		return
	}
	r.log.Info("released", "epoch", rec.Epoch)
}

func (r *run) publish(now time.Time) {
	r.c.setStatus(r.a.status(now))
}
