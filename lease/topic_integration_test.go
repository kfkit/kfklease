// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// A new topic gets the settings of a lease log, each set on the topic
// itself rather than inherited from the broker.
func TestIntegrationTopicCreated(t *testing.T) {
	cl, adm := adminClient(t)
	ctx := context.Background()
	topic := randomTopic(t)
	warnings, err := EnsureTopic(ctx, cl, topic, 10*time.Second, TopicSettings{ReplicationFactor: 1, FlushEveryRecord: true})
	if err != nil || warnings != nil {
		t.Fatalf("EnsureTopic: %v, warnings %q", err, warnings)
	}
	rcs, err := adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	rc, err := rcs.On(topic, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"message.timestamp.type":         "LogAppendTime",
		"cleanup.policy":                 "compact",
		"min.compaction.lag.ms":          "3600000",
		"segment.ms":                     "3600000",
		"min.insync.replicas":            "1",
		"unclean.leader.election.enable": "false",
		"flush.messages":                 "1",
	}
	for _, c := range rc.Configs {
		w, ok := want[c.Key]
		if !ok {
			continue
		}
		delete(want, c.Key)
		if c.MaybeValue() != w || c.Source.String() != "DYNAMIC_TOPIC_CONFIG" {
			t.Errorf("%s = %s from %s, want %s on the topic", c.Key, c.MaybeValue(), c.Source, w)
		}
	}
	if len(want) > 0 {
		t.Errorf("not reported: %v", want)
	}
	// Run checks it again at start: as created, nothing to report.
	if warnings, err := EnsureTopic(ctx, cl, topic, 10*time.Second, TopicSettings{ReplicationFactor: 1}); err != nil || warnings != nil {
		t.Fatalf("second EnsureTopic: %v, warnings %q", err, warnings)
	}
}

// The default asks for three replicas; the stand has one broker. The topic
// is not created with fewer, and the error names the setting.
func TestIntegrationTopicMoreReplicasThanBrokers(t *testing.T) {
	cl, adm := adminClient(t)
	ctx := context.Background()
	topic := randomTopic(t)
	_, err := EnsureTopic(ctx, cl, topic, 10*time.Second, TopicSettings{})
	if err == nil || !strings.Contains(err.Error(), "replicationFactor 3, but the cluster has 1 brokers") {
		t.Fatalf("err = %v, want one naming replicationFactor", err)
	}
	t.Log(err)
	tds, err := adm.ListTopics(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if tds.Has(topic) {
		t.Fatalf("topic %s was created anyway", topic)
	}
}

// An existing topic made with the broker's defaults, and more than one
// partition, is reported, not altered.
func TestIntegrationTopicExistingReported(t *testing.T) {
	cl, adm := adminClient(t)
	ctx := context.Background()
	topic := randomTopic(t)
	if _, err := adm.CreateTopic(ctx, 3, 1, map[string]*string{"message.timestamp.type": ptr("LogAppendTime")}, topic); err != nil {
		t.Fatal(err)
	}
	warnings, err := EnsureTopic(ctx, cl, topic, 10*time.Second, TopicSettings{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("warnings: %q", warnings)
	for i, w := range []string{"3 partitions", "1 replicas, fewer than replicationFactor 3", "min.insync.replicas=1, below the 2 expected", "cleanup.policy=delete"} {
		if i >= len(warnings) || !strings.Contains(warnings[i], w) {
			t.Errorf("warning %d: want %q in %q", i, w, warnings)
		}
	}
	rcs, err := adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	rc, _ := rcs.On(topic, nil)
	for _, c := range rc.Configs {
		if c.Key == "cleanup.policy" && c.MaybeValue() != "delete" {
			t.Errorf("cleanup.policy altered to %s", c.MaybeValue())
		}
	}
}

// TestIntegrationTopicFlushLatency measures what flush.messages=1 costs a
// renewal: the time from produce to acknowledgement with acks=all, with and
// without it. It reports; it asserts nothing about the numbers.
func TestIntegrationTopicFlushLatency(t *testing.T) {
	const n = 200
	measure := func(flush bool) []time.Duration {
		cl, _ := adminClient(t)
		ctx := context.Background()
		topic := randomTopic(t)
		if _, err := EnsureTopic(ctx, cl, topic, 10*time.Second, TopicSettings{ReplicationFactor: 1, FlushEveryRecord: flush}); err != nil {
			t.Fatal(err)
		}
		pr, err := kgo.NewClient(kgo.SeedBrokers(brokers(t)...), kgo.RequiredAcks(kgo.AllISRAcks()),
			kgo.MaxProduceRequestsInflightPerBroker(1), kgo.DefaultProduceTopic(topic))
		if err != nil {
			t.Fatal(err)
		}
		defer pr.Close()
		rec := func() *kgo.Record { return &kgo.Record{Key: []byte(recordKey), Value: make([]byte, 64)} }
		// The first produce waits for the new partition's leader.
		deadline := time.Now().Add(10 * time.Second)
		for err := pr.ProduceSync(ctx, rec()).FirstErr(); err != nil; err = pr.ProduceSync(ctx, rec()).FirstErr() {
			if time.Now().After(deadline) {
				t.Fatal(err)
			}
			time.Sleep(100 * time.Millisecond)
		}
		ds := make([]time.Duration, n)
		for i := range ds {
			start := time.Now()
			if err := pr.ProduceSync(ctx, rec()).FirstErr(); err != nil {
				t.Fatal(err)
			}
			ds[i] = time.Since(start)
		}
		slices.Sort(ds)
		return ds
	}
	pct := func(ds []time.Duration, p int) time.Duration { return ds[(len(ds)-1)*p/100] }
	for _, flush := range []bool{false, true} {
		ds := measure(flush)
		t.Logf("flush.messages=1 %-5v: %d acks=all produces, p50 %v, p90 %v, p99 %v, max %v",
			flush, n, pct(ds, 50), pct(ds, 90), pct(ds, 99), ds[len(ds)-1])
	}
}
