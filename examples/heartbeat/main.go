// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

// Command kfklease-heartbeat is an example workload that fences by epoch,
// and the tool that checks it did.
//
// beat: the workload. Before every write it asks the local scaler for its
// status and writes a heartbeat only while the scaler believes it holds the
// lease, tagging the record with the lease epoch. A pod that outlives its
// cluster's term (KEDA has not scaled it down yet, or the cluster came back
// from a crash) sees holding=false and writes nothing.
//
// report: the downstream side. It reads the heartbeats and applies the
// fencing rule a real consumer would: a record whose epoch is below the
// highest epoch seen so far is stale and rejected. It then reports, by the
// broker's clock, how long two epochs overlapped and how long nobody wrote.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kfkit/kfklease/scaler"
)

// Heartbeat is one record on the topic.
type Heartbeat struct {
	Cluster string `json:"cluster"`
	Holder  string `json:"holder"`
	Epoch   int64  `json:"epoch"`
	Seq     int64  `json:"seq"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: kfklease-heartbeat beat|report [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "beat":
		err = beat(os.Args[2:])
	case "report":
		err = report(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		slog.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func env(name, def string) string {
	if v, ok := os.LookupEnv(name); ok {
		return v
	}
	return def
}

func beat(args []string) error {
	fs := flag.NewFlagSet("beat", flag.ExitOnError)
	brokers := fs.String("brokers", env("HEARTBEAT_BROKERS", ""), "bootstrap addresses (HEARTBEAT_BROKERS)")
	topic := fs.String("topic", env("HEARTBEAT_TOPIC", "kfklease-heartbeats"), "heartbeat topic (HEARTBEAT_TOPIC)")
	statusURL := fs.String("status-url", env("KFKLEASE_STATUS_URL", "http://kfklease:9091/status"), "the scaler's /status (KFKLEASE_STATUS_URL)")
	cluster := fs.String("cluster", env("CLUSTER", ""), "name of this cluster, the record key (CLUSTER)")
	every := fs.Duration("every", 200*time.Millisecond, "heartbeat interval")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *brokers == "" || *cluster == "" {
		return errors.New("brokers and cluster are required")
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(*brokers, ",")...), kgo.DefaultProduceTopic(*topic),
		kgo.RecordDeliveryTimeout(5*time.Second), kgo.AllowAutoTopicCreation())
	if err != nil {
		return err
	}
	defer cl.Close()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := ensureTopic(ctx, cl, *topic); err != nil {
		return err
	}

	log.Info("beating", "cluster", *cluster, "topic", *topic, "status", *statusURL)
	client := &http.Client{Timeout: *every}
	var seq int64
	ticker := time.NewTicker(*every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		st, err := status(ctx, client, *statusURL)
		if err != nil {
			log.Warn("status", "err", err)
			continue
		}
		// The fence: no belief, no write. The epoch travels with the record
		// so the consumer can fence too.
		if !st.Holding {
			continue
		}
		seq++
		value, err := json.Marshal(Heartbeat{Cluster: *cluster, Holder: st.Holder, Epoch: st.Epoch, Seq: seq})
		if err != nil {
			return err
		}
		cl.Produce(ctx, &kgo.Record{Key: []byte(*cluster), Value: value}, func(_ *kgo.Record, err error) {
			if err != nil {
				log.Warn("heartbeat not written", "err", err)
			}
		})
	}
}

func status(ctx context.Context, client *http.Client, url string) (scaler.StatusResponse, error) {
	var st scaler.StatusResponse
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return st, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return st, fmt.Errorf("status: %s", resp.Status)
	}
	return st, json.NewDecoder(resp.Body).Decode(&st)
}

// ensureTopic creates the heartbeat topic with broker timestamps, which is
// what the report measures with.
func ensureTopic(ctx context.Context, cl *kgo.Client, topic string) error {
	lat := "LogAppendTime"
	_, err := kadm.NewClient(cl).CreateTopic(ctx, 1, -1, map[string]*string{"message.timestamp.type": &lat}, topic)
	if err != nil && !strings.Contains(err.Error(), "TOPIC_ALREADY_EXISTS") {
		return fmt.Errorf("create topic %q: %w", topic, err)
	}
	return nil
}

// Report is what report prints.
type Report struct {
	Since   time.Time `json:"since"`
	Records int       `json:"records"`
	// Stale are records rejected by the fence: epoch below one already seen.
	Stale int `json:"stale"`
	// Epochs in order of first appearance, with their spans by the broker's
	// clock.
	Epochs []EpochSpan `json:"epochs"`
	// Overlap is the longest time two epochs both wrote, by the broker's
	// clock; the fence rejects the older epoch's records in that window.
	Overlap time.Duration `json:"overlap"`
	// Gap is the longest silence between one epoch's last heartbeat and
	// the next epoch's first.
	Gap time.Duration `json:"gap"`
}

// EpochSpan is one holder's tenure as seen on the topic.
type EpochSpan struct {
	Epoch   int64     `json:"epoch"`
	Cluster string    `json:"cluster"`
	First   time.Time `json:"first"`
	Last    time.Time `json:"last"`
	Count   int       `json:"count"`
}

func report(args []string) error {
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	brokers := fs.String("brokers", env("HEARTBEAT_BROKERS", ""), "bootstrap addresses (HEARTBEAT_BROKERS)")
	topic := fs.String("topic", env("HEARTBEAT_TOPIC", "kfklease-heartbeats"), "heartbeat topic (HEARTBEAT_TOPIC)")
	since := fs.String("since", "", "only heartbeats at or after this RFC 3339 time, by the broker's clock")
	settle := fs.Duration("settle", 2*time.Second, "how long the end of the topic must stay quiet before the report is final")
	asJSON := fs.Bool("json", false, "print the report as JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *brokers == "" {
		return errors.New("brokers are required")
	}
	var from time.Time
	if *since != "" {
		var err error
		if from, err = time.Parse(time.RFC3339Nano, *since); err != nil {
			return fmt.Errorf("since: %w", err)
		}
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ConsumeTopics(*topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchMaxWait(200*time.Millisecond))
	if err != nil {
		return err
	}
	defer cl.Close()

	var records []*kgo.Record
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	quietSince := time.Now()
	for time.Since(quietSince) < *settle {
		pollCtx, pollCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		fetches := cl.PollFetches(pollCtx)
		pollCancel()
		if err := fetches.Err0(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if fetches.NumRecords() > 0 {
			quietSince = time.Now()
			fetches.EachRecord(func(r *kgo.Record) {
				if !from.IsZero() && r.Timestamp.Before(from) {
					return
				}
				records = append(records, r)
			})
		}
	}
	rep := summarize(records, from)
	if *asJSON {
		return json.NewEncoder(os.Stdout).Encode(rep)
	}
	fmt.Printf("heartbeats since %s: %d records, %d epochs, %d stale (fenced)\n", from.Format(time.RFC3339), rep.Records, len(rep.Epochs), rep.Stale)
	for _, e := range rep.Epochs {
		fmt.Printf("  epoch %d on %s: %d beats, %s .. %s\n", e.Epoch, e.Cluster, e.Count, e.First.Format("15:04:05.000"), e.Last.Format("15:04:05.000"))
	}
	fmt.Printf("  overlap between epochs: %s, gap between epochs: %s\n", rep.Overlap.Round(time.Millisecond), rep.Gap.Round(time.Millisecond))
	return nil
}

// summarize applies the fence and measures the spans. Records come in
// offset order, which is broker-timestamp order on one partition.
func summarize(records []*kgo.Record, from time.Time) Report {
	rep := Report{Since: from, Records: len(records)}
	spans := map[int64]*EpochSpan{}
	var maxEpoch int64 = -1
	for _, r := range records {
		var hb Heartbeat
		if err := json.Unmarshal(r.Value, &hb); err != nil {
			continue
		}
		if hb.Epoch < maxEpoch {
			rep.Stale++
		}
		maxEpoch = max(maxEpoch, hb.Epoch)
		sp, ok := spans[hb.Epoch]
		if !ok {
			sp = &EpochSpan{Epoch: hb.Epoch, Cluster: hb.Cluster, First: r.Timestamp}
			spans[hb.Epoch] = sp
		}
		sp.Last = r.Timestamp
		sp.Count++
	}
	for _, sp := range spans {
		rep.Epochs = append(rep.Epochs, *sp)
	}
	sort.Slice(rep.Epochs, func(i, j int) bool { return rep.Epochs[i].Epoch < rep.Epochs[j].Epoch })
	for i := 1; i < len(rep.Epochs); i++ {
		prev, cur := rep.Epochs[i-1], rep.Epochs[i]
		if d := prev.Last.Sub(cur.First); d > 0 {
			rep.Overlap = max(rep.Overlap, d)
		} else {
			rep.Gap = max(rep.Gap, -d)
		}
	}
	return rep
}
