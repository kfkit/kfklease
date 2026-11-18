// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"errors"
	"fmt"
)

// Inspect reads the lease from the log without taking part in it and
// returns its state once known, in the Lease and AsOf fields of the
// returned Status. It runs a participant in standby, which never writes,
// until its view is certain: at once on a log read from offset 0 or with a
// term proven by its own renewals, after a TTL of silence at the end of
// the log otherwise. CreateTopic is ignored. Give ctx a deadline: Inspect
// returns ctx's error if the state is not known by then.
func Inspect(ctx context.Context, cfg Config) (Status, error) {
	cfg.Standby = true
	cfg.CreateTopic = false
	c, err := NewCandidate(cfg)
	if err != nil {
		return Status{}, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(runCtx) }()
	for {
		if s := c.Status(); s.Ready {
			cancel()
			<-done
			return s, nil
		}
		select {
		case <-c.Changed():
		case err := <-done:
			if err == nil {
				err = errors.New("lease: stopped before the lease state was known")
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				err = fmt.Errorf("lease: state of %q not known: %w", cfg.Topic, ctxErr)
			}
			return Status{}, err
		}
	}
}
