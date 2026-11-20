// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func hb(t *testing.T, at time.Duration, cluster string, epoch int64) *kgo.Record {
	t.Helper()
	v, err := json.Marshal(Heartbeat{Cluster: cluster, Epoch: epoch})
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Timestamp: time.Unix(0, 0).Add(at), Value: v}
}

func TestSummarizeFencesAndMeasures(t *testing.T) {
	ms := time.Millisecond
	// a holds epoch 3 and keeps writing 300 ms into b's epoch 7: those two
	// beats are stale. Then b stops and c takes over after a 900 ms gap.
	records := []*kgo.Record{
		hb(t, 0, "a", 3), hb(t, 200*ms, "a", 3), hb(t, 400*ms, "a", 3),
		hb(t, 500*ms, "b", 7),
		hb(t, 600*ms, "a", 3), hb(t, 800*ms, "a", 3), // stale
		hb(t, 700*ms, "b", 7), hb(t, 900*ms, "b", 7),
		hb(t, 1800*ms, "c", 12), hb(t, 2000*ms, "c", 12),
		{Timestamp: time.Unix(0, 0).Add(2100 * ms), Value: []byte("not json")},
	}
	rep := summarize(records, time.Time{})
	if rep.Records != 11 || rep.Stale != 2 || len(rep.Epochs) != 3 {
		t.Fatalf("records=%d stale=%d epochs=%d", rep.Records, rep.Stale, len(rep.Epochs))
	}
	if rep.Epochs[0].Epoch != 3 || rep.Epochs[0].Cluster != "a" || rep.Epochs[0].Count != 5 {
		t.Fatalf("epoch 3: %+v", rep.Epochs[0])
	}
	if rep.Overlap != 300*ms {
		t.Fatalf("overlap = %v, want 300ms", rep.Overlap)
	}
	if rep.Gap != 900*ms {
		t.Fatalf("gap = %v, want 900ms", rep.Gap)
	}
}

// On a lease topic deleted and created again the epochs start from 0: the
// spans are measured in the order they came, whatever their epochs.
func TestSummarizeAcrossARecreatedTopic(t *testing.T) {
	ms := time.Millisecond
	cases := []struct {
		name         string
		records      []*kgo.Record
		overlap, gap time.Duration
	}{
		{"one after the other", []*kgo.Record{
			hb(t, 0, "a", 40), hb(t, 1000*ms, "a", 40),
			hb(t, 1500*ms, "b", 0), hb(t, 2000*ms, "b", 0),
		}, 0, 500 * ms},
		{"both at once", []*kgo.Record{
			hb(t, 0, "a", 40), hb(t, 800*ms, "b", 0), hb(t, 1000*ms, "a", 40), hb(t, 2000*ms, "b", 0),
		}, 200 * ms, 0},
		{"an old epoch inside a later pair", []*kgo.Record{
			hb(t, 0, "a", 40), hb(t, 3000*ms, "a", 40),
			hb(t, 1000*ms, "b", 0), hb(t, 1200*ms, "b", 0),
			hb(t, 1500*ms, "b", 2), hb(t, 2000*ms, "b", 2),
		}, 500 * ms, 0},
	}
	for _, tc := range cases {
		rep := summarize(tc.records, time.Time{})
		if rep.Overlap != tc.overlap || rep.Gap != tc.gap {
			t.Errorf("%s: overlap %v, gap %v; want %v, %v", tc.name, rep.Overlap, rep.Gap, tc.overlap, tc.gap)
		}
	}
}

func TestSummarizeEmpty(t *testing.T) {
	rep := summarize(nil, time.Time{})
	if rep.Records != 0 || rep.Overlap != 0 || rep.Gap != 0 || len(rep.Epochs) != 0 {
		t.Fatalf("%+v", rep)
	}
}
