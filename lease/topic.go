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

// checkExisting looks at an existing topic: its configuration and the
// fewest replicas any of its partitions has. It returns an error for what
// the protocol cannot do without and a warning for what makes the lease
// less durable than s asks for. It never changes the topic.
func (s TopicSettings) checkExisting(topic string, configs map[string]string, replicas int) (warnings []string, err error) {
	if v := configs[timestampTypeKey]; v != timestampTypeValue {
		if v == "" {
			return nil, fmt.Errorf("lease: topic %q: %s not reported by the broker", topic, timestampTypeKey)
		}
		return nil, fmt.Errorf("lease: topic %q has %s=%s, need %s", topic, timestampTypeKey, v, timestampTypeValue)
	}
	warn := func(format string, args ...any) {
		warnings = append(warnings, fmt.Sprintf("topic %q: ", topic)+fmt.Sprintf(format, args...))
	}
	if replicas < int(s.ReplicationFactor) {
		warn("%d replicas, fewer than replicationFactor %d", replicas, s.ReplicationFactor)
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
	deadline := time.Now().Add(10 * time.Second)
	for {
		configs, replicas, err := inspectTopic(ctx, adm, topic)
		switch {
		case err == nil:
			return s.checkExisting(topic, configs, replicas)
		case !errors.Is(err, kerr.UnknownTopicOrPartition) || time.Now().After(deadline):
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// inspectTopic returns an existing topic's configuration and the fewest
// replicas any of its partitions has.
func inspectTopic(ctx context.Context, adm *kadm.Client, topic string) (map[string]string, int, error) {
	configs, err := describeTopicConfigs(ctx, adm, topic)
	if err != nil {
		return nil, 0, err
	}
	replicas, err := fewestReplicas(ctx, adm, topic)
	return configs, replicas, err
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

func fewestReplicas(ctx context.Context, adm *kadm.Client, topic string) (int, error) {
	tds, err := adm.ListTopics(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("lease: describe topic %q: %w", topic, err)
	}
	td, ok := tds[topic]
	switch {
	case !ok || len(td.Partitions) == 0 && td.Err == nil:
		return 0, fmt.Errorf("lease: describe topic %q: no partitions reported", topic)
	case td.Err != nil:
		return 0, fmt.Errorf("lease: describe topic %q: %w", topic, td.Err)
	}
	fewest := -1
	for _, p := range td.Partitions {
		if fewest < 0 || len(p.Replicas) < fewest {
			fewest = len(p.Replicas)
		}
	}
	return fewest, nil
}

func ptr(s string) *string { return &s }
