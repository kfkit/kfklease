// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
	// Yield says that the workload of the writer's site has stopped after
	// the term named by Epoch, which that site held and which has ended.
	// It leaves the lease state alone: the fold rejects it. Writers of
	// other sites read it as the end of their takeover delay; see
	// agent.yieldFrom.
	Yield
)

// kinds is the one table of record kinds: the name of each, on the wire and
// in the logs, and which of the fields its value carries, as in the Records
// table of docs/protocol.md. A kind that is not in it is not a kind: it is
// neither encoded nor applied.
var kinds = map[Kind]struct {
	name       string
	epoch, ttl bool
}{
	Claim:   {name: "claim", ttl: true},
	Renew:   {name: "renew", epoch: true, ttl: true},
	Release: {name: "release", epoch: true, ttl: true},
	Yield:   {name: "yield", epoch: true},
}

func (k Kind) String() string {
	if spec, ok := kinds[k]; ok {
		return spec.name
	}
	return fmt.Sprintf("kind(%d)", uint8(k))
}

// kindNamed returns the kind with the given name on the wire.
func kindNamed(name string) (Kind, bool) {
	for k, spec := range kinds {
		if spec.name == name {
			return k, true
		}
	}
	return 0, false
}

// errEmptyHolder is what a record without a writer is refused with, written
// or read.
var errEmptyHolder = errors.New("lease: empty holder")

// Record is one entry of a lease log: a message on the lease topic together
// with the metadata assigned by the broker.
type Record struct {
	// Offset is the partition offset of the message.
	Offset int64
	// Timestamp is the broker's LogAppendTime. Client clocks never enter the
	// protocol.
	Timestamp time.Time
	Kind      Kind
	// Holder identifies the writer. It must be unique among the
	// participants alive at one time; it may repeat across restarts of a
	// process. See Config.Holder.
	Holder string
	// Epoch names the term a Renew, Release or Yield refers to. Ignored for
	// Claim.
	Epoch int64
	// TTL is how long the term stays live after a Claim or Renew. A
	// Release carries it and the fold does not read it; a Yield does not
	// carry it.
	TTL time.Duration
}

// SiteOf returns the site of a holder id "<site>/<name>": everything before
// the last slash. A holder id without a slash is a site of its own.
func SiteOf(holder string) string {
	if i := strings.LastIndex(holder, "/"); i >= 0 {
		return holder[:i]
	}
	return holder
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

// wireRecord is the message value. Which kind carries which field is the
// Records table of docs/protocol.md, restated in kinds: EncodeValue writes
// by it and DecodeValue reads by it.
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
	spec, ok := kinds[r.Kind]
	if !ok {
		return nil, fmt.Errorf("lease: cannot encode %v", r.Kind)
	}
	if r.Holder == "" {
		return nil, errEmptyHolder
	}
	w := wireRecord{
		Version: wireVersion,
		Kind:    spec.name,
		Holder:  r.Holder,
	}
	if spec.ttl {
		w.TTLMs = r.TTL.Milliseconds()
	}
	if spec.epoch {
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
	kind, ok := kindNamed(w.Kind)
	if !ok {
		return Record{}, fmt.Errorf("lease: unknown record kind %q", w.Kind)
	}
	if w.Holder == "" {
		return Record{}, errEmptyHolder
	}
	// A field its kind does not carry is not read, whatever the value says.
	r := Record{Kind: kind, Holder: w.Holder}
	spec := kinds[kind]
	if spec.epoch {
		r.Epoch = w.Epoch
	}
	if spec.ttl {
		r.TTL = time.Duration(w.TTLMs) * time.Millisecond
	}
	return r, nil
}
