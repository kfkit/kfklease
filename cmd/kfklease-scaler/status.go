// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/kfkit/kfklease/lease"
	"github.com/kfkit/kfklease/scaler"
)

// leaseStatus is one line of `kfklease-scaler status`: the lease as the log
// says, as of the last record read, on the broker's clock.
type leaseStatus struct {
	Topic string `json:"topic"`
	scaler.LogLease
}

func newLeaseStatus(topic string, s lease.Status) leaseStatus {
	return leaseStatus{Topic: topic, LogLease: scaler.NewLogLease(s)}
}

// status prints the state of every lease, one JSON object per line in the
// order of -topic, read from the log without taking part in the lease.
func status(args []string) error {
	fs := flag.NewFlagSet("kfklease-scaler status", flag.ExitOnError)
	c := commonFlags(fs)
	wait := fs.Duration("wait", 0, "give up on a lease whose state is not known by then; 0 for 3 × ttl. A lease nobody wrote to for a while takes a ttl of silence to prove free")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: kfklease-scaler status [flags]\n\nPrints the state of each lease, one JSON object per line, read from the log\nwithout taking part in it. Writes nothing.\n\n")
		fs.PrintDefaults()
	}
	_ = fs.Parse(args)
	log, topics, err := c.setup()
	if err != nil {
		return err
	}
	if *wait <= 0 {
		*wait = 3 * *c.ttl
	}
	ctx, cancel := context.WithTimeout(context.Background(), *wait)
	defer cancel()

	base := c.config(log)
	results := make([]leaseStatus, len(topics))
	errs := make([]error, len(topics))
	var wg sync.WaitGroup
	for i, t := range topics {
		wg.Go(func() {
			cfg := base
			cfg.Topic = t
			s, err := lease.Inspect(ctx, cfg)
			if err != nil {
				errs[i] = fmt.Errorf("topic %s: %w", t, err)
				return
			}
			results[i] = newLeaseStatus(t, s)
		})
	}
	wg.Wait()
	return errors.Join(printStatus(os.Stdout, results, errs), errors.Join(errs...))
}

// printStatus writes the leases whose state is known, one JSON object per
// line.
func printStatus(w io.Writer, results []leaseStatus, errs []error) error {
	enc := json.NewEncoder(w)
	for i, r := range results {
		if errs[i] != nil {
			continue
		}
		if err := enc.Encode(r); err != nil {
			return err
		}
	}
	return nil
}
