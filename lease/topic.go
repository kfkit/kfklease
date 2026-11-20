// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// The topic configuration the protocol depends on, and the configuration a
// lease log is created with. See docs/protocol.md.
const (
	timestampTypeKey      = "message.timestamp.type"
	timestampTypeValue    = "LogAppendTime"
	cleanupPolicyKey      = "cleanup.policy"
	cleanupPolicyValue    = "compact"
	minCompactionLagKey   = "min.compaction.lag.ms"
	minCompactionLagFloor = time.Hour
	segmentMsKey          = "segment.ms"
	minInsyncKey          = "min.insync.replicas"
	uncleanElectionKey    = "unclean.leader.election.enable"
	flushMessagesKey      = "flush.messages"

	// DefaultReplicationFactor is the replication factor of a lease topic
	// when TopicSettings leave it unset.
	DefaultReplicationFactor = 3
)

// TopicSettings are what EnsureTopic creates a lease topic with, beyond what
// the protocol needs, and what it expects of an existing one. The zero value
// is the default: three replicas, two of them in sync.
type TopicSettings struct {
	// ReplicationFactor of a new topic. Zero means DefaultReplicationFactor.
	// A cluster with fewer brokers fails the creation; nothing lowers it.
	ReplicationFactor int16
	// MinInsyncReplicas of a new topic: how many replicas an acknowledged
	// record is on. Zero means 2 when ReplicationFactor is 3 or more,
	// otherwise 1.
	MinInsyncReplicas int
	// FlushEveryRecord sets flush.messages=1 on a new topic: every record is
	// fsynced before it is acknowledged, which protects the tail of the log
	// when all replicas go down at once, at the cost of renewal latency.
	// Off in the zero value; the chart and the binary turn it on by default.
	FlushEveryRecord bool
}

func (s TopicSettings) withDefaults() (TopicSettings, error) {
	if s.ReplicationFactor < 0 || s.MinInsyncReplicas < 0 {
		return s, errors.New("lease: negative replicationFactor or minInsyncReplicas")
	}
	if s.ReplicationFactor == 0 {
		s.ReplicationFactor = DefaultReplicationFactor
	}
	if s.MinInsyncReplicas == 0 {
		s.MinInsyncReplicas = 1
		if s.ReplicationFactor >= 3 {
			s.MinInsyncReplicas = 2
		}
	}
	if s.MinInsyncReplicas > int(s.ReplicationFactor) {
		return s, fmt.Errorf("lease: minInsyncReplicas %d is more than replicationFactor %d", s.MinInsyncReplicas, s.ReplicationFactor)
	}
	return s, nil
}

// createConfigs is the configuration of a new lease topic. The active
// segment is never compacted, so it rolls as often as compaction may look
// at it: otherwise the log keeps a week of renewals (the broker's default
// segment.ms) and every new participant reads all of them.
func (s TopicSettings) createConfigs(ttl time.Duration) map[string]*string {
	lag := strconv.FormatInt(max(100*ttl, minCompactionLagFloor).Milliseconds(), 10)
	configs := map[string]*string{
		timestampTypeKey:    ptr(timestampTypeValue),
		cleanupPolicyKey:    ptr(cleanupPolicyValue),
		minCompactionLagKey: ptr(lag),
		segmentMsKey:        ptr(lag),
		minInsyncKey:        ptr(strconv.Itoa(s.MinInsyncReplicas)),
		// The epoch is the offset of the accepted claim: after an unclean
		// election the same offset can hold a different record.
		uncleanElectionKey: ptr("false"),
	}
	if s.FlushEveryRecord {
		configs[flushMessagesKey] = ptr("1")
	}
	return configs
}

// checkExisting looks at an existing topic: its configuration, how many
// partitions it has and the fewest replicas any of them has. It returns an
// error for what the protocol cannot do without and a warning for what
// makes the lease less durable than s asks for or easy to misconfigure. It
// never changes the topic.
func (s TopicSettings) checkExisting(topic string, configs map[string]string, layout partitionLayout) (warnings []string, err error) {
	if v := configs[timestampTypeKey]; v != timestampTypeValue {
		if v == "" {
			return nil, fmt.Errorf("lease: topic %q: %s not reported by the broker", topic, timestampTypeKey)
		}
		return nil, fmt.Errorf("lease: topic %q has %s=%s, need %s", topic, timestampTypeKey, v, timestampTypeValue)
	}
	warn := func(format string, args ...any) {
		warnings = append(warnings, fmt.Sprintf("topic %q: ", topic)+fmt.Sprintf(format, args...))
	}
	if layout.partitions > 1 {
		// A lease lives in one partition. Participants that name different
		// partitions of one topic take part in different leases, and each
		// gets a holder of its own.
		warn("%d partitions: a lease uses one of them, and every participant of the lease must name the same one (the scaler uses partition 0)", layout.partitions)
	}
	if layout.fewestReplicas < int(s.ReplicationFactor) {
		warn("%d replicas, fewer than replicationFactor %d", layout.fewestReplicas, s.ReplicationFactor)
	}
	if n, err := strconv.Atoi(configs[minInsyncKey]); err != nil || n < s.MinInsyncReplicas {
		warn("%s=%s, below the %d expected", minInsyncKey, configs[minInsyncKey], s.MinInsyncReplicas)
	}
	if configs[uncleanElectionKey] == "true" {
		warn("%s=true: an unclean election can put a different record at an epoch's offset", uncleanElectionKey)
	}
	if !strings.Contains(configs[cleanupPolicyKey], cleanupPolicyValue) {
		warn("%s=%s, without %s: the log grows or loses the current term", cleanupPolicyKey, configs[cleanupPolicyKey], cleanupPolicyValue)
	}
	return warnings, nil
}

// EnsureTopic creates the lease topic if it does not exist: one partition,
// LogAppendTime timestamps, compaction that leaves the recent past alone,
// and the replication s asks for. An existing topic is never altered: it is
// checked, an error returned for what the protocol cannot do without
// (LogAppendTime), and a warning for each setting that makes it less
// durable than s asks for.
func EnsureTopic(ctx context.Context, cl *kgo.Client, topic string, ttl time.Duration, s TopicSettings) (warnings []string, err error) {
	if ttl <= 0 {
		return nil, errors.New("lease: ttl must be positive")
	}
	if s, err = s.withDefaults(); err != nil {
		return nil, err
	}
	adm := kadm.NewClient(cl)

	// CreateTopic reports the broker's verdict as err.
	_, err = adm.CreateTopic(ctx, 1, s.ReplicationFactor, s.createConfigs(ttl), topic)
	switch {
	case err == nil:
		return nil, nil
	case errors.Is(err, kerr.InvalidReplicationFactor):
		n := "unknown"
		if bs, berr := adm.ListBrokers(ctx); berr == nil {
			n = strconv.Itoa(len(bs))
		}
		return nil, fmt.Errorf("lease: create topic %q: replicationFactor %d, but the cluster has %s brokers; lower replicationFactor: %w", topic, s.ReplicationFactor, n, err)
	case !errors.Is(err, kerr.TopicAlreadyExists):
		return nil, fmt.Errorf("lease: create topic %q: %w", topic, err)
	}

	// A topic another participant created a moment ago may not be known to
	// every broker yet.
	var (
		configs map[string]string
		layout  partitionLayout
	)
	err = untilReady(ctx, topicReadyWait, func() (err error) {
		configs, layout, err = inspectTopic(ctx, adm, topic)
		return err
	}, func(err error) bool { return errors.Is(err, kerr.UnknownTopicOrPartition) })
	if err != nil {
		return nil, err
	}
	return s.checkExisting(topic, configs, layout)
}

// A topic created a moment ago has no leader for a few hundred milliseconds
// and may not be known to every broker yet: whoever asks about it retries
// for a while before giving up.
const (
	topicReadyWait  = 10 * time.Second
	topicReadyRetry = 200 * time.Millisecond
)

// untilReady calls try every topicReadyRetry until it succeeds, and gives up
// with its error once wait has passed, or with ctx's when ctx is done
// first. retry is asked about each failure there is still time to try again
// after: false makes the error final, and before it answers it may prepare
// the next try.
func untilReady(ctx context.Context, wait time.Duration, try func() error, retry func(error) bool) error {
	deadline := time.Now().Add(wait)
	for {
		err := try()
		if err == nil || time.Now().After(deadline) || !retry(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(topicReadyRetry):
		}
	}
}

// partitionLayout is what checkExisting needs to know about a topic's
// partitions.
type partitionLayout struct {
	partitions     int
	fewestReplicas int // the fewest replicas any partition has
}

// inspectTopic returns an existing topic's configuration and its partition
// layout.
func inspectTopic(ctx context.Context, adm *kadm.Client, topic string) (map[string]string, partitionLayout, error) {
	configs, err := describeTopicConfigs(ctx, adm, topic)
	if err != nil {
		return nil, partitionLayout{}, err
	}
	layout, err := describePartitions(ctx, adm, topic)
	return configs, layout, err
}

func describeTopicConfigs(ctx context.Context, adm *kadm.Client, topic string) (map[string]string, error) {
	rcs, err := adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("lease: describe topic %q: %w", topic, err)
	}
	rc, err := rcs.On(topic, nil)
	if err == nil {
		err = rc.Err
	}
	if err != nil {
		return nil, fmt.Errorf("lease: describe topic %q: %w", topic, err)
	}
	configs := make(map[string]string, len(rc.Configs))
	for _, c := range rc.Configs {
		configs[c.Key] = c.MaybeValue()
	}
	return configs, nil
}

func describePartitions(ctx context.Context, adm *kadm.Client, topic string) (partitionLayout, error) {
	tds, err := adm.ListTopics(ctx, topic)
	if err != nil {
		return partitionLayout{}, fmt.Errorf("lease: describe topic %q: %w", topic, err)
	}
	td, ok := tds[topic]
	switch {
	case !ok || len(td.Partitions) == 0 && td.Err == nil:
		return partitionLayout{}, fmt.Errorf("lease: describe topic %q: no partitions reported", topic)
	case td.Err != nil:
		return partitionLayout{}, fmt.Errorf("lease: describe topic %q: %w", topic, td.Err)
	}
	layout := partitionLayout{partitions: len(td.Partitions), fewestReplicas: -1}
	for _, p := range td.Partitions {
		if layout.fewestReplicas < 0 || len(p.Replicas) < layout.fewestReplicas {
			layout.fewestReplicas = len(p.Replicas)
		}
	}
	return layout, nil
}

func ptr(s string) *string { return &s }
