// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"maps"
	"strings"
	"testing"
	"time"
)

func TestTopicSettingsDefaults(t *testing.T) {
	for _, tc := range []struct {
		name    string
		in      TopicSettings
		rf      int16
		isr     int
		wantErr string
	}{
		{name: "zero", in: TopicSettings{}, rf: 3, isr: 2},
		{name: "five replicas", in: TopicSettings{ReplicationFactor: 5}, rf: 5, isr: 2},
		{name: "two replicas", in: TopicSettings{ReplicationFactor: 2}, rf: 2, isr: 1},
		{name: "one replica, the stand", in: TopicSettings{ReplicationFactor: 1}, rf: 1, isr: 1},
		{name: "explicit isr", in: TopicSettings{ReplicationFactor: 5, MinInsyncReplicas: 3}, rf: 5, isr: 3},
		{name: "isr above rf", in: TopicSettings{ReplicationFactor: 2, MinInsyncReplicas: 3}, wantErr: "minInsyncReplicas 3 is more than replicationFactor 2"},
		{name: "negative", in: TopicSettings{ReplicationFactor: -1}, wantErr: "negative"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.in.withDefaults()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.ReplicationFactor != tc.rf || got.MinInsyncReplicas != tc.isr {
				t.Fatalf("got rf %d isr %d, want %d %d", got.ReplicationFactor, got.MinInsyncReplicas, tc.rf, tc.isr)
			}
		})
	}
}

func TestTopicCreateConfigs(t *testing.T) {
	deref := func(m map[string]*string) map[string]string {
		out := make(map[string]string, len(m))
		for k, v := range m {
			out[k] = *v
		}
		return out
	}
	base := map[string]string{
		"message.timestamp.type":         "LogAppendTime",
		"cleanup.policy":                 "compact",
		"min.compaction.lag.ms":          "3600000",
		"segment.ms":                     "3600000",
		"min.insync.replicas":            "2",
		"unclean.leader.election.enable": "false",
	}
	for _, tc := range []struct {
		name string
		s    TopicSettings
		ttl  time.Duration
		want map[string]string
	}{
		{name: "defaults", ttl: 10 * time.Second, want: base},
		{name: "long ttl moves lag and segment together", ttl: time.Minute, want: with(base, map[string]string{
			"min.compaction.lag.ms": "6000000", "segment.ms": "6000000",
		})},
		{name: "one replica", s: TopicSettings{ReplicationFactor: 1}, ttl: 10 * time.Second, want: with(base, map[string]string{
			"min.insync.replicas": "1",
		})},
		{name: "flush every record", s: TopicSettings{FlushEveryRecord: true}, ttl: 10 * time.Second, want: with(base, map[string]string{
			"flush.messages": "1",
		})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := tc.s.withDefaults()
			if err != nil {
				t.Fatal(err)
			}
			if got := deref(s.createConfigs(tc.ttl)); !maps.Equal(got, tc.want) {
				t.Fatalf("got %v\nwant %v", got, tc.want)
			}
		})
	}
}

func with(base, more map[string]string) map[string]string {
	out := maps.Clone(base)
	maps.Copy(out, more)
	return out
}

func TestTopicCheckExisting(t *testing.T) {
	good := map[string]string{
		"message.timestamp.type":         "LogAppendTime",
		"cleanup.policy":                 "compact",
		"min.insync.replicas":            "2",
		"unclean.leader.election.enable": "false",
	}
	for _, tc := range []struct {
		name     string
		configs  map[string]string
		replicas int
		wantErr  string
		warnings []string // substrings, one per warning, in order
	}{
		{name: "as created", configs: good, replicas: 3},
		{name: "more than asked", configs: with(good, map[string]string{"min.insync.replicas": "3"}), replicas: 5},
		{name: "compact,delete is compact", configs: with(good, map[string]string{"cleanup.policy": "compact,delete"}), replicas: 3},
		{name: "CreateTime", configs: with(good, map[string]string{"message.timestamp.type": "CreateTime"}), replicas: 3, wantErr: "message.timestamp.type=CreateTime, need LogAppendTime"},
		{name: "timestamp type not reported", configs: map[string]string{}, replicas: 3, wantErr: "not reported"},
		{name: "broker defaults", configs: map[string]string{
			"message.timestamp.type": "LogAppendTime", "cleanup.policy": "delete", "min.insync.replicas": "1", "unclean.leader.election.enable": "true",
		}, replicas: 1, warnings: []string{
			"1 replicas, fewer than replicationFactor 3",
			"min.insync.replicas=1, below the 2 expected",
			"unclean.leader.election.enable=true",
			"cleanup.policy=delete, without compact",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := TopicSettings{}.withDefaults()
			warnings, err := s.checkExisting("t", tc.configs, tc.replicas)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(warnings) != len(tc.warnings) {
				t.Fatalf("warnings %q, want %d", warnings, len(tc.warnings))
			}
			for i, w := range tc.warnings {
				if !strings.Contains(warnings[i], w) {
					t.Errorf("warning %d = %q, want %q", i, warnings[i], w)
				}
			}
		})
	}
}
