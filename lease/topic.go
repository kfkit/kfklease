// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// The topic configuration the protocol depends on. See docs/protocol.md.
const (
	timestampTypeKey      = "message.timestamp.type"
	timestampTypeValue    = "LogAppendTime"
	cleanupPolicyKey      = "cleanup.policy"
	cleanupPolicyValue    = "compact"
	minCompactionLagKey   = "min.compaction.lag.ms"
	minCompactionLagFloor = time.Hour
)

// EnsureTopic creates the lease topic if it does not exist: one partition,
// the cluster's default replication factor, LogAppendTime timestamps and
// compaction that leaves the recent past alone. An existing topic is checked
// for LogAppendTime, the one setting the protocol cannot do without.
func EnsureTopic(ctx context.Context, cl *kgo.Client, topic string, ttl time.Duration) error {
	if ttl <= 0 {
		return errors.New("lease: ttl must be positive")
	}
	adm := kadm.NewClient(cl)

	lag := max(100*ttl, minCompactionLagFloor)
	configs := map[string]*string{
		timestampTypeKey:    ptr(timestampTypeValue),
		cleanupPolicyKey:    ptr(cleanupPolicyValue),
		minCompactionLagKey: ptr(strconv.FormatInt(lag.Milliseconds(), 10)),
	}
	// CreateTopic reports the broker's verdict as err.
	_, err := adm.CreateTopic(ctx, 1, -1, configs, topic)
	switch {
	case err == nil:
		return nil
	case !errors.Is(err, kerr.TopicAlreadyExists):
		return fmt.Errorf("lease: create topic %q: %w", topic, err)
	}

	rcs, err := adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return fmt.Errorf("lease: describe topic %q: %w", topic, err)
	}
	rc, err := rcs.On(topic, nil)
	if err != nil {
		return fmt.Errorf("lease: describe topic %q: %w", topic, err)
	}
	if rc.Err != nil {
		return fmt.Errorf("lease: describe topic %q: %w", topic, rc.Err)
	}
	for _, c := range rc.Configs {
		if c.Key != timestampTypeKey {
			continue
		}
		if c.Value == nil || *c.Value != timestampTypeValue {
			return fmt.Errorf("lease: topic %q has %s=%s, need %s", topic, timestampTypeKey, strValue(c.Value), timestampTypeValue)
		}
		return nil
	}
	return fmt.Errorf("lease: topic %q: %s not reported by the broker", topic, timestampTypeKey)
}

func ptr(s string) *string { return &s }

func strValue(p *string) string {
	if p == nil {
		return "<unset>"
	}
	return *p
}
