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
	// It has to cover the clock skew between brokers and the difference
	// between the client's monotonic clock and the broker's over one TTL.
	// Zero means TTL / 5.
	Margin time.Duration
	// RenewEvery is how often a holder renews. Zero means TTL / 3.
	RenewEvery time.Duration

	// Logger receives protocol events. Nil means slog.Default.
	Logger *slog.Logger
	// ClientOpts are passed to the Kafka client after the options the
	// protocol sets itself: TLS, SASL, dial timeouts and the like.
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

type inflight struct {
	kind   Kind
	sentAt time.Time
	// offset is -1 until the broker acknowledged the record.
	offset int64
}

// run is the state of one Run call.
type run struct {
	c       *Candidate
	cfg     Config
	log     *slog.Logger
	cl      *kgo.Client
	results chan produceResult

	view *View
	// nextOffset is the offset the next record must have; anything higher
	// is a hole.
	nextOffset int64
	// highWater is the broker's end offset as last reported.
	highWater int64
	// ownOutcomes remembers verdicts on this participant's records read
	// back before the broker's acknowledgement told us their offset.
	ownOutcomes map[int64]Outcome

	// Belief: this participant holds epoch until deadline (monotonic).
	epoch    int64
	deadline time.Time

	inflight    *inflight
	lastSend    time.Time
	nextClaimAt time.Time

	// Silence bookkeeping, all on the monotonic clock.
	connected    bool
	fetchedOnce  bool
	caughtUpAt   time.Time
	lastRecordAt time.Time
	// lastRecordAt is paired with the broker time of that record, to
	// estimate the broker's clock between records.
	lastRecordBroker time.Time
}

func (r *run) bootstrap(ctx context.Context) error {
	start, end, err := r.partitionBounds(ctx)
	if err != nil {
		return err
	}

	r.view = NewView(r.cfg.TTL)
	if start.Offset == 0 {
		// Nothing was ever deleted, so the reader sees everything. Holes
		// left by compaction are caught record by record.
		r.view = NewViewFromStart(r.cfg.TTL)
	}
	r.nextOffset = start.Offset
	r.highWater = end.Offset
	r.ownOutcomes = make(map[int64]Outcome)
	r.epoch = -1
	now := time.Now()
	r.connected = true
	r.caughtUpAt = now
	r.log.Info("joined", "start_offset", start.Offset, "end_offset", end.Offset, "certain", r.view.Certain())
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
				r.acked(res)
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

		r.step(ctx)
	}
}

func (r *run) consume(fetches kgo.Fetches) error {
	fetches.EachError(func(_ string, _ int32, err error) {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return
		}
		if r.connected {
			// A topic created a moment ago has no leader yet, which is
			// not worth a warning.
			level := slog.LevelWarn
			if !r.fetchedOnce {
				level = slog.LevelDebug
			}
			r.log.Log(context.Background(), level, "fetch failed", "err", err)
		}
		r.connected = false
	})

	var err error
	fetches.EachPartition(func(fp kgo.FetchTopicPartition) {
		if fp.Err != nil || fp.Topic != r.cfg.Topic || fp.Partition != r.cfg.Partition {
			return
		}
		if !r.connected {
			r.connected = true
			r.caughtUpAt = time.Now()
			if r.fetchedOnce {
				r.log.Info("fetching again")
			}
		}
		r.fetchedOnce = true
		if fp.HighWatermark > r.highWater {
			r.highWater = fp.HighWatermark
		}
		for _, rec := range fp.Records {
			if err == nil {
				err = r.consumeRecord(rec)
			}
		}
	})
	return err
}

func (r *run) consumeRecord(kr *kgo.Record) error {
	if kr.Offset < r.nextOffset {
		return nil
	}
	if kr.Attrs.TimestampType() != 1 {
		return fmt.Errorf("lease: topic %q does not use LogAppendTime", r.cfg.Topic)
	}
	now := time.Now()
	if kr.Offset > r.nextOffset {
		r.log.Warn("hole in the log", "expected", r.nextOffset, "got", kr.Offset)
		r.view.Gap()
		r.caughtUpAt = now
	}
	r.nextOffset = kr.Offset + 1
	r.lastRecordAt = now
	r.lastRecordBroker = kr.Timestamp

	rec, err := DecodeValue(kr.Value)
	if err != nil {
		r.log.Warn("skipping record", "offset", kr.Offset, "err", err)
		return nil
	}
	rec.Offset = kr.Offset
	rec.Timestamp = kr.Timestamp
	out, err := r.view.Apply(rec)
	if err != nil {
		return err
	}
	r.log.Debug("record", "offset", rec.Offset, "kind", rec.Kind, "by", rec.Holder, "epoch", rec.Epoch, "outcome", out)

	if rec.Holder == r.cfg.Holder {
		r.ownRecord(rec.Offset, rec.Kind, out)
	}
	if r.believing(now) && r.view.Certain() {
		s := r.view.State()
		if s.Holder != r.cfg.Holder || s.Epoch != r.epoch || !s.HeldAt(r.view.Clock()) {
			r.log.Warn("term ended in the log", "epoch", r.epoch, "log_holder", s.Holder, "log_epoch", s.Epoch)
			r.dropBelief()
		}
	}
	r.publish(now)
	return nil
}

// ownRecord handles a record written by this participant. Only the record
// in flight can change the belief, and only once its offset is known from
// the broker's acknowledgement; anything else is a record we gave up on.
func (r *run) ownRecord(offset int64, kind Kind, out Outcome) {
	in := r.inflight
	if in == nil || in.kind != kind {
		return
	}
	if in.offset == -1 {
		r.ownOutcomes[offset] = out
		return
	}
	if in.offset == offset {
		r.settle(out)
	}
}

func (r *run) acked(res produceResult) {
	in := r.inflight
	if in == nil {
		return
	}
	if res.err != nil {
		r.log.Warn("produce failed", "kind", in.kind, "err", res.err)
		r.inflight = nil
		if in.kind == Claim {
			r.nextClaimAt = time.Now().Add(r.cfg.RenewEvery)
		}
		return
	}
	in.offset = res.offset
	if out, ok := r.ownOutcomes[res.offset]; ok {
		r.settle(out)
	}
}

// settle applies the fold's verdict on the record in flight to the belief.
func (r *run) settle(out Outcome) {
	in := r.inflight
	r.inflight = nil
	clear(r.ownOutcomes)
	now := time.Now()
	switch {
	case out == Pending:
		// The view lost certainty while the record was in flight. The
		// record may well be valid, but the belief needs proof.
		r.log.Warn("own record unjudged", "kind", in.kind, "offset", in.offset)
	case in.kind == Claim && out == Accepted:
		r.epoch = in.offset
		r.extendBelief(in.sentAt)
		r.log.Info("acquired", "epoch", r.epoch)
	case in.kind == Claim:
		s := r.view.State()
		wait := min(max(s.Expires.Sub(r.brokerNow(now)), r.cfg.pollEvery()), r.cfg.TTL)
		r.nextClaimAt = now.Add(wait)
		r.log.Debug("claim rejected", "log_holder", s.Holder, "log_epoch", s.Epoch, "retry_in", wait)
	case in.kind == Renew && out == Accepted:
		if r.believing(now) {
			r.extendBelief(in.sentAt)
		}
	case in.kind == Renew:
		r.log.Warn("renew rejected", "epoch", r.epoch)
		r.dropBelief()
	}
	r.publish(now)
}

func (r *run) believing(now time.Time) bool { return now.Before(r.deadline) }

// extendBelief sets the deadline from the send time of an accepted record:
// the record landed after it was sent, so the term in the log outlives the
// belief by at least the margin.
func (r *run) extendBelief(sentAt time.Time) {
	r.deadline = sentAt.Add(r.cfg.TTL - r.cfg.Margin)
}

func (r *run) dropBelief() {
	if r.deadline.IsZero() {
		return
	}
	r.deadline = time.Time{}
	r.log.Info("no longer holder", "epoch", r.epoch)
}

// brokerNow estimates the broker's clock from the last record seen.
func (r *run) brokerNow(now time.Time) time.Time {
	if r.lastRecordAt.IsZero() {
		return r.view.Clock()
	}
	return r.lastRecordBroker.Add(now.Sub(r.lastRecordAt))
}

// step measures silence and decides whether to write something.
func (r *run) step(ctx context.Context) {
	now := time.Now()
	if !r.believing(now) && !r.deadline.IsZero() {
		r.log.Warn("deadline passed without renewal", "epoch", r.epoch)
		r.dropBelief()
		r.publish(now)
	}

	if r.connected && r.nextOffset >= r.highWater && !r.view.Certain() {
		since := r.caughtUpAt
		if r.lastRecordAt.After(since) {
			since = r.lastRecordAt
		}
		if now.Sub(since) >= r.cfg.TTL+r.cfg.Margin && r.view.Quiet(r.cfg.TTL) {
			r.log.Info("lease is free: nothing written for a ttl")
			r.publish(now)
		}
	}

	if r.inflight != nil || !r.view.Certain() {
		return
	}
	switch {
	case r.believing(now):
		if now.Sub(r.lastSend) >= r.cfg.RenewEvery {
			r.send(ctx, Renew)
		}
	case now.After(r.nextClaimAt) && !r.view.State().HeldAt(r.brokerNow(now)):
		r.send(ctx, Claim)
	}
}

func (r *run) send(ctx context.Context, kind Kind) {
	kr, err := r.record(kind, r.epoch)
	if err != nil {
		r.log.Error("encode record", "err", err)
		return
	}
	now := time.Now()
	r.inflight = &inflight{kind: kind, sentAt: now, offset: -1}
	r.lastSend = now
	r.cl.Produce(ctx, kr, func(kr *kgo.Record, err error) {
		r.results <- produceResult{offset: kr.Offset, err: err}
	})
}

// recordKey is the same for every record: one lease per partition, and
// compaction keeps the latest record.
const recordKey = "lease"

// record builds the Kafka record for a lease record by this participant.
func (r *run) record(kind Kind, epoch int64) (*kgo.Record, error) {
	rec := Record{Kind: kind, Holder: r.cfg.Holder, TTL: r.cfg.TTL}
	if kind != Claim {
		rec.Epoch = epoch
	}
	value, err := EncodeValue(rec)
	if err != nil {
		return nil, err
	}
	return &kgo.Record{Topic: r.cfg.Topic, Partition: r.cfg.Partition, Key: []byte(recordKey), Value: value}, nil
}

// shutdown gives the lease up if the participant believes it holds it. The
// belief goes first, the record second: a release that never lands only
// costs the others a wait of one TTL.
func (r *run) shutdown() {
	now := time.Now()
	if !r.believing(now) {
		r.publish(now)
		return
	}
	epoch := r.epoch
	r.dropBelief()
	r.publish(now)

	kr, err := r.record(Release, epoch)
	if err != nil {
		r.log.Error("encode record", "err", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.TTL)
	defer cancel()
	if err := r.cl.ProduceSync(ctx, kr).FirstErr(); err != nil {
		r.log.Warn("release not written", "epoch", epoch, "err", err)
		return
	}
	r.log.Info("released", "epoch", epoch)
}

func (r *run) publish(now time.Time) {
	s := Status{Certain: r.view.Certain(), AsOf: r.view.Clock()}
	if s.Certain {
		s.Lease = r.view.State()
	}
	deadline := r.deadline
	if !r.believing(now) {
		deadline = time.Time{}
	}
	r.c.setStatus(s, deadline, r.epoch)
}
