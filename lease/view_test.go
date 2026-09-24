// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"testing"
	"time"
)

const testTTL = 10 * time.Second

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func at(sec float64) time.Time { return t0.Add(time.Duration(sec * float64(time.Second))) }

func claim(off int64, sec float64, holder string) Record {
	return Record{Offset: off, Timestamp: at(sec), Kind: Claim, Holder: holder, TTL: testTTL}
}

func renew(off int64, sec float64, holder string, epoch int64) Record {
	return Record{Offset: off, Timestamp: at(sec), Kind: Renew, Holder: holder, Epoch: epoch, TTL: testTTL}
}

func release(off int64, sec float64, holder string, epoch int64) Record {
	return Record{Offset: off, Timestamp: at(sec), Kind: Release, Holder: holder, Epoch: epoch}
}

type step struct {
	rec  Record
	want Outcome
}

func apply(t *testing.T, v *View, steps []step) {
	t.Helper()
	for i, s := range steps {
		got, err := v.Apply(s.rec)
		if err != nil {
			t.Fatalf("step %d (%v@%d): %v", i, s.rec.Kind, s.rec.Offset, err)
		}
		if got != s.want {
			t.Fatalf("step %d (%v@%d by %s): got %v, want %v", i, s.rec.Kind, s.rec.Offset, s.rec.Holder, got, s.want)
		}
	}
}

func wantState(t *testing.T, v *View, holder string, epoch int64, expiresSec float64) {
	t.Helper()
	if !v.Certain() {
		t.Fatalf("view is not certain")
	}
	s := v.State()
	if s.Holder != holder || s.Epoch != epoch || !s.Expires.Equal(at(expiresSec)) {
		t.Fatalf("state = {%q %d %v}, want {%q %d %v}", s.Holder, s.Epoch, s.Expires.Sub(t0), holder, epoch, at(expiresSec).Sub(t0))
	}
}

func TestFoldFromStart(t *testing.T) {
	tests := []struct {
		name    string
		steps   []step
		holder  string
		epoch   int64
		expires float64
	}{
		{
			name:   "first claim wins, second is rejected",
			steps:  []step{{claim(0, 0, "a"), Accepted}, {claim(1, 0.1, "b"), Rejected}},
			holder: "a", epoch: 0, expires: 10,
		},
		{
			name:   "renew extends from the renew's timestamp",
			steps:  []step{{claim(0, 0, "a"), Accepted}, {renew(1, 4, "a", 0), Accepted}},
			holder: "a", epoch: 0, expires: 14,
		},
		{
			name:   "claim at the exact expiry is granted",
			steps:  []step{{claim(0, 0, "a"), Accepted}, {claim(1, 10, "b"), Accepted}},
			holder: "b", epoch: 1, expires: 20,
		},
		{
			name:   "claim just before expiry is rejected",
			steps:  []step{{claim(0, 0, "a"), Accepted}, {claim(1, 9.999, "b"), Rejected}},
			holder: "a", epoch: 0, expires: 10,
		},
		{
			name: "late renew cannot revive a term",
			steps: []step{
				{claim(0, 0, "a"), Accepted},
				{renew(1, 10, "a", 0), Rejected},
				{claim(2, 11, "b"), Accepted},
			},
			holder: "b", epoch: 2, expires: 21,
		},
		{
			name: "stale holder cannot renew into someone else's term",
			steps: []step{
				{claim(0, 0, "a"), Accepted},
				{claim(1, 12, "b"), Accepted},
				{renew(2, 13, "a", 0), Rejected},
				{release(3, 13.5, "a", 0), Rejected},
			},
			holder: "b", epoch: 1, expires: 22,
		},
		{
			name: "renew with a wrong epoch is rejected",
			steps: []step{
				{claim(0, 0, "a"), Accepted},
				{renew(1, 1, "a", 7), Rejected},
			},
			holder: "a", epoch: 0, expires: 10,
		},
		{
			name: "holder cannot claim over its own live term",
			steps: []step{
				{claim(0, 0, "a"), Accepted},
				{claim(1, 1, "a"), Rejected},
			},
			holder: "a", epoch: 0, expires: 10,
		},
		{
			name: "release frees the lease at once and keeps epochs growing",
			steps: []step{
				{claim(0, 0, "a"), Accepted},
				{release(1, 2, "a", 0), Accepted},
				{claim(2, 2.5, "b"), Accepted},
			},
			holder: "b", epoch: 2, expires: 12.5,
		},
		{
			name: "ttl above the lease ttl is rejected",
			steps: []step{
				{Record{Offset: 0, Timestamp: at(0), Kind: Claim, Holder: "a", TTL: testTTL + 1}, Rejected},
				{Record{Offset: 1, Timestamp: at(1), Kind: Claim, Holder: "a", TTL: 0}, Rejected},
				{claim(2, 2, "b"), Accepted},
			},
			holder: "b", epoch: 2, expires: 12,
		},
		{
			name: "broker clock going backwards is clamped",
			steps: []step{
				{claim(0, 5, "a"), Accepted},
				// A new partition leader with a slower clock stamps this one.
				{renew(1, 3, "a", 0), Accepted},
			},
			holder: "a", epoch: 0, expires: 15,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewViewFromStart(testTTL)
			apply(t, v, tt.steps)
			wantState(t, v, tt.holder, tt.epoch, tt.expires)
		})
	}
}

func TestApplyContract(t *testing.T) {
	v := NewViewFromStart(testTTL)
	apply(t, v, []step{{claim(5, 0, "a"), Accepted}})
	if _, err := v.Apply(renew(5, 1, "a", 5)); err == nil {
		t.Fatalf("repeated offset: want error")
	}
	if _, err := v.Apply(Record{Offset: 6, Timestamp: at(1), Holder: "a"}); err == nil {
		t.Fatalf("unknown kind: want error")
	}
	wantState(t, v, "a", 5, 10)
}

func TestUncertainAnchorsOnRenewPair(t *testing.T) {
	v := NewView(testTTL)
	apply(t, v, []step{
		// History before offset 40 is gone. Nothing here can be judged yet:
		// the claim by b may well have been rejected.
		{claim(40, 100, "b"), Pending},
		{renew(41, 101, "a", 17), Pending},
		// The second renew of term a/17 proves the first one valid.
		{renew(42, 104, "a", 17), Accepted},
	})
	wantState(t, v, "a", 17, 114)
}

func TestUncertainAnchorsOnClaim(t *testing.T) {
	v := NewView(testTTL)
	apply(t, v, []step{
		{claim(40, 100, "a"), Pending},
		{claim(41, 100.5, "b"), Pending},
		{renew(42, 103, "a", 40), Accepted},
	})
	wantState(t, v, "a", 40, 113)
}

func TestUncertainReplaysRecordsAfterAnchor(t *testing.T) {
	v := NewView(testTTL)
	apply(t, v, []step{
		{renew(40, 100, "a", 17), Pending},
		// c claims too early. Without it the silence between 100 and 111
		// would settle the view on its own.
		{claim(41, 105, "c"), Pending},
		// a's term runs out at 110, b takes over at 111.
		{claim(42, 111, "b"), Pending},
		// a's delayed renew arrives: it anchors the view at offset 40, the
		// replay rejects c, grants b, and the renew itself is rejected.
		{renew(43, 112, "a", 17), Rejected},
	})
	wantState(t, v, "b", 42, 121)
}

func TestUncertainSilenceBetweenRecords(t *testing.T) {
	v := NewView(testTTL)
	apply(t, v, []step{
		{renew(40, 100, "a", 17), Pending},
		{claim(41, 110, "b"), Accepted},
	})
	wantState(t, v, "b", 41, 120)
}

func TestUncertainSingleRecordsDoNotSettle(t *testing.T) {
	v := NewView(testTTL)
	apply(t, v, []step{
		{claim(40, 100, "a"), Pending},
		{claim(41, 105, "b"), Pending},
		{claim(42, 112, "c"), Pending},
		{release(43, 113, "a", 3), Pending},
	})
	if v.Certain() {
		t.Fatalf("view became certain without proof")
	}
}

func TestReleaseIsNotAnAnchor(t *testing.T) {
	v := NewView(testTTL)
	apply(t, v, []step{
		{release(40, 100, "a", 17), Pending},
		{renew(41, 101, "a", 17), Pending},
	})
	if v.Certain() {
		t.Fatalf("view anchored on a release")
	}
}

func TestQuiet(t *testing.T) {
	v := NewView(testTTL)
	apply(t, v, []step{{renew(40, 100, "a", 17), Pending}})
	if v.Quiet(testTTL - 1) {
		t.Fatalf("quiet shorter than ttl settled the view")
	}
	if !v.Quiet(testTTL) {
		t.Fatalf("quiet of a full ttl did not settle the view")
	}
	if !v.Certain() || v.State().Holder != "" {
		t.Fatalf("after quiet: certain=%v holder=%q", v.Certain(), v.State().Holder)
	}
	apply(t, v, []step{{claim(41, 100.5, "b"), Accepted}})
	wantState(t, v, "b", 41, 110.5)

	// A certain view keeps its epoch through a quiet period.
	v.Quiet(testTTL)
	if s := v.State(); s.Holder != "" || s.Epoch != 41 {
		t.Fatalf("after second quiet: %+v", s)
	}
}

func TestGapResetsView(t *testing.T) {
	v := NewViewFromStart(testTTL)
	apply(t, v, []step{{claim(0, 0, "a"), Accepted}})
	v.Gap()
	if v.Certain() {
		t.Fatalf("view is certain after a gap")
	}
	apply(t, v, []step{
		// Silence must not be measured across the gap: records were skipped.
		{claim(90, 50, "b"), Pending},
		{renew(91, 53, "b", 90), Accepted},
	})
	wantState(t, v, "b", 90, 63)
}

func TestValueRoundTrip(t *testing.T) {
	for _, r := range []Record{
		{Kind: Claim, Holder: "k3s-a/9f2c", TTL: testTTL},
		{Kind: Renew, Holder: "k3s-a/9f2c", Epoch: 42, TTL: testTTL},
		{Kind: Release, Holder: "k3s-a/9f2c", Epoch: 42},
	} {
		b, err := EncodeValue(r)
		if err != nil {
			t.Fatalf("encode %v: %v", r.Kind, err)
		}
		got, err := DecodeValue(b)
		if err != nil {
			t.Fatalf("decode %s: %v", b, err)
		}
		if got != r {
			t.Fatalf("round trip: got %+v, want %+v (wire %s)", got, r, b)
		}
	}
	for _, bad := range []string{`{`, `{"v":2,"kind":"claim","holder":"a"}`, `{"v":1,"kind":"steal","holder":"a"}`, `{"v":1,"kind":"claim"}`} {
		if _, err := DecodeValue([]byte(bad)); err == nil {
			t.Fatalf("decode %s: want error", bad)
		}
	}
}
