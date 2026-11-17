// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package scaler

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

// Probe checks the workload over HTTP while this participant holds the
// lease and reports every success to the lease's health gate
// (lease.Config.Health). The gate, not the probe, decides when a run of
// failures is long enough to give the lease up.
type Probe struct {
	// URL is fetched with GET; a status from 200 to 399 is healthy, as for
	// a Kubernetes httpGet probe.
	URL string
	// Interval is the time between probes; Timeout bounds each one.
	Interval time.Duration
	Timeout  time.Duration
	// Log receives a line when the probe starts and stops failing.
	Log *slog.Logger
}

// Run probes until ctx is done, calling healthy on every success. It probes
// only while src holds the lease: elsewhere the workload is scaled to zero
// and a failure means nothing.
func (p Probe) Run(ctx context.Context, src Source, healthy func()) {
	cl := &http.Client{Timeout: p.Timeout}
	ticker := time.NewTicker(p.Interval)
	defer ticker.Stop()
	failing := false
	for {
		if src.Status().Holding {
			err := p.check(ctx, cl)
			switch {
			case err == nil:
				healthy()
				if failing {
					p.Log.Info("workload probe passing", "url", p.URL)
				}
			case ctx.Err() != nil:
				return
			case !failing:
				p.Log.Warn("workload probe failing", "url", p.URL, "err", err)
			}
			failing = err != nil
		} else {
			failing = false
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (p Probe) check(ctx context.Context, cl *http.Client) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.URL, nil)
	if err != nil {
		return err
	}
	resp, err := cl.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}
