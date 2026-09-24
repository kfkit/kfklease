// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"encoding/json"
	"fmt"
	"time"
)

// Kind is the type of a lease record.
type Kind uint8

const (
	// Claim asks to start a new term. It is granted only if the lease is not
	// held at the record's timestamp. The offset of a granted claim becomes
	// the epoch of the term.
	Claim Kind = iota + 1
	// Renew extends the term named by Epoch. It is valid only while that term
	// is still live and held by the same holder.
	Renew
	// Release ends the term named by Epoch early.
	Release
)

func (k Kind) String() string {
	switch k {
	case Claim:
		return "claim"
	case Renew:
		return "renew"
	case Release:
		return "release"
	}
	return fmt.Sprintf("kind(%d)", uint8(k))
}

// Record is one entry of a lease log: a message on the lease topic together
// with the metadata assigned by the broker.
type Record struct {
	// Offset is the partition offset of the message.
	Offset int64
	// Timestamp is the broker's LogAppendTime. Client clocks never enter the
	// protocol.
	Timestamp time.Time
	Kind      Kind
	// Holder identifies the writer. It must be unique per process
	// incarnation, not per host.
	Holder string
	// Epoch names the term a Renew or Release refers to. Ignored for Claim.
	Epoch int64
	// TTL is how long the term stays live after this record.
	TTL time.Duration
}

// term returns the identity of the term the record belongs to or, for a
// claim, would start.
func (r Record) term() (string, int64) {
	if r.Kind == Claim {
		return r.Holder, r.Offset
	}
	return r.Holder, r.Epoch
}

const wireVersion = 1

type wireRecord struct {
	Version int    `json:"v"`
	Kind    string `json:"kind"`
	Holder  string `json:"holder"`
	Epoch   int64  `json:"epoch,omitempty"`
	TTLMs   int64  `json:"ttl_ms,omitempty"`
}

// EncodeValue returns the message value for r. Offset and Timestamp are not
// part of the value: the broker assigns them.
func EncodeValue(r Record) ([]byte, error) {
	switch r.Kind {
	case Claim, Renew, Release:
	default:
		return nil, fmt.Errorf("lease: cannot encode %v", r.Kind)
	}
	if r.Holder == "" {
		return nil, fmt.Errorf("lease: empty holder")
	}
	w := wireRecord{
		Version: wireVersion,
		Kind:    r.Kind.String(),
		Holder:  r.Holder,
		TTLMs:   r.TTL.Milliseconds(),
	}
	if r.Kind != Claim {
		w.Epoch = r.Epoch
	}
	return json.Marshal(w)
}

// DecodeValue parses a message value. The caller fills in Offset and
// Timestamp from the message metadata.
func DecodeValue(b []byte) (Record, error) {
	var w wireRecord
	if err := json.Unmarshal(b, &w); err != nil {
		return Record{}, fmt.Errorf("lease: decode record: %w", err)
	}
	if w.Version != wireVersion {
		return Record{}, fmt.Errorf("lease: unsupported record version %d", w.Version)
	}
	r := Record{
		Holder: w.Holder,
		Epoch:  w.Epoch,
		TTL:    time.Duration(w.TTLMs) * time.Millisecond,
	}
	switch w.Kind {
	case "claim":
		r.Kind = Claim
		r.Epoch = 0
	case "renew":
		r.Kind = Renew
	case "release":
		r.Kind = Release
	default:
		return Record{}, fmt.Errorf("lease: unknown record kind %q", w.Kind)
	}
	if r.Holder == "" {
		return Record{}, fmt.Errorf("lease: empty holder")
	}
	return r, nil
}
