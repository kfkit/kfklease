// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/kfkit/kfklease/lease"
)

func TestStatusLines(t *testing.T) {
	asOf := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		s    lease.Status
		want string
	}{
		{"held",
			lease.Status{AsOf: asOf, Lease: lease.State{Holder: "eu/pod-1", Epoch: 42, Expires: asOf.Add(8 * time.Second), Last: "eu/pod-1"}},
			`{"topic":"t","held":true,"holder":"eu/pod-1","site":"eu","epoch":42,"expires":"2026-11-18T12:00:08Z","last_holder":"eu/pod-1","as_of":"2026-11-18T12:00:00Z"}`},
		{"released",
			lease.Status{AsOf: asOf, Lease: lease.State{Epoch: 42, Expires: asOf.Add(-time.Second), Last: "eu/pod-1"}},
			`{"topic":"t","held":false,"expires":"2026-11-18T11:59:59Z","last_holder":"eu/pod-1","as_of":"2026-11-18T12:00:00Z"}`},
		{"ran out before the last record",
			lease.Status{AsOf: asOf, Lease: lease.State{Holder: "us/pod-2", Epoch: 7, Expires: asOf.Add(-time.Second), Last: "us/pod-2"}},
			`{"topic":"t","held":false,"expires":"2026-11-18T11:59:59Z","last_holder":"us/pod-2","as_of":"2026-11-18T12:00:00Z"}`},
		{"never held",
			lease.Status{Lease: lease.State{Epoch: -1}},
			`{"topic":"t","held":false}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := printStatus(&buf, []leaseStatus{newLeaseStatus("t", tc.s)}, []error{nil}); err != nil {
				t.Fatal(err)
			}
			if got := buf.String(); got != tc.want+"\n" {
				t.Fatalf("got  %s\nwant %s", got, tc.want)
			}
		})
	}

	// A lease whose state is not known is left out; the others are printed.
	var buf bytes.Buffer
	rs := []leaseStatus{{Topic: "a"}, {}, {Topic: "c"}}
	if err := printStatus(&buf, rs, []error{nil, errors.New("not known"), nil}); err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); got != "{\"topic\":\"a\",\"held\":false}\n{\"topic\":\"c\",\"held\":false}\n" {
		t.Fatalf("got %q", got)
	}
}
