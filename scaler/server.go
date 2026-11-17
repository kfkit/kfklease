// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

// Package scaler exposes leases to KEDA as an external scaler: the metric is
// 1 while this participant holds the lease and 0 otherwise, so a ScaledObject
// with idleReplicaCount 0 and minReplicaCount = maxReplicaCount = N runs N
// replicas where the lease is held and none elsewhere. One scaler can serve
// several leases; a ScaledObject picks its lease by the "topic" metadata of
// its trigger.
package scaler

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kfkit/kfklease/internal/externalscaler"
	"github.com/kfkit/kfklease/lease"
)

// MetricName is the metric KEDA sees.
const MetricName = "kfklease-holder"

// Source is what the server needs from a lease participant. *lease.Candidate
// satisfies it.
type Source interface {
	Status() lease.Status
	Changed() <-chan struct{}
}

// Lease is one lease a scaler serves.
type Lease struct {
	// Topic names the lease, as the "topic" metadata of a ScaledObject's
	// trigger does. It may be empty only when it is the only lease: then
	// the metadata is not checked.
	Topic string
	// Holder is this participant's id in the lease, reported by the HTTP
	// handler.
	Holder string
	// Source is the participant.
	Source Source
}

// leases is the set a server or HTTP handler serves, by topic.
type leases struct {
	byTopic map[string]Lease
	// topics are the topics in order, for listings and messages.
	topics []string
}

func newLeases(ls []Lease) (leases, error) {
	if len(ls) == 0 {
		return leases{}, errors.New("scaler: no leases")
	}
	set := leases{byTopic: make(map[string]Lease, len(ls))}
	for _, l := range ls {
		if l.Source == nil {
			return leases{}, fmt.Errorf("scaler: lease %q has no source", l.Topic)
		}
		if l.Topic == "" && len(ls) > 1 {
			return leases{}, errors.New("scaler: a lease without a topic must be the only one")
		}
		if _, dup := set.byTopic[l.Topic]; dup {
			return leases{}, fmt.Errorf("scaler: topic %q given twice", l.Topic)
		}
		set.byTopic[l.Topic] = l
		set.topics = append(set.topics, l.Topic)
	}
	slices.Sort(set.topics)
	return set, nil
}

// pick returns the lease a request names by topic; named is false when the
// request names none. With one lease, no topic and a lease without a topic
// both mean that lease.
func (ls leases) pick(topic string, named bool) (Lease, error) {
	if len(ls.topics) == 1 {
		l := ls.byTopic[ls.topics[0]]
		if !named || l.Topic == "" || l.Topic == topic {
			return l, nil
		}
		return Lease{}, fmt.Errorf("this scaler serves topic %q, not %q", l.Topic, topic)
	}
	if !named {
		return Lease{}, fmt.Errorf("this scaler serves several leases (%s): name one by its topic", strings.Join(ls.topics, ", "))
	}
	if l, ok := ls.byTopic[topic]; ok {
		return l, nil
	}
	return Lease{}, fmt.Errorf("this scaler serves topics %s, not %q", strings.Join(ls.topics, ", "), topic)
}

// Server implements the KEDA external scaler protocol over one or more
// leases.
type Server struct {
	externalscaler.UnimplementedExternalScalerServer

	leases leases
	// heartbeat is how often StreamIsActive repeats the current value when
	// nothing changes, so that a dead stream is noticed.
	heartbeat time.Duration
}

// New returns a server over one lease. topic may be empty, and then the
// "topic" metadata of a ScaledObject is not checked; when set, metadata
// naming another topic is refused, which catches a ScaledObject pointed at
// the wrong scaler. heartbeat must be positive.
func New(src Source, topic string, heartbeat time.Duration) *Server {
	s, err := NewLeases([]Lease{{Topic: topic, Source: src}}, heartbeat)
	if err != nil {
		panic(err)
	}
	return s
}

// NewLeases returns a server over several leases, at least one, with
// distinct topics. A ScaledObject names its lease by the "topic" metadata of
// its trigger; with more than one lease that metadata is required, and a
// topic the server does not serve is refused. heartbeat must be positive.
func NewLeases(ls []Lease, heartbeat time.Duration) (*Server, error) {
	if heartbeat <= 0 {
		return nil, errors.New("scaler: heartbeat must be positive")
	}
	set, err := newLeases(ls)
	if err != nil {
		return nil, err
	}
	return &Server{leases: set, heartbeat: heartbeat}, nil
}

// Register adds the server to a gRPC registrar.
func (s *Server) Register(r grpc.ServiceRegistrar) {
	externalscaler.RegisterExternalScalerServer(r, s)
}

// source returns the participant of the lease the ScaledObject names.
func (s *Server) source(ref *externalscaler.ScaledObjectRef) (Source, error) {
	if ref == nil {
		return nil, status.Error(codes.InvalidArgument, "missing scaledObjectRef")
	}
	topic, named := ref.GetScalerMetadata()["topic"]
	l, err := s.leases.pick(topic, named)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return l.Source, nil
}

func holding(src Source) bool { return src.Status().Holding }

// IsActive reports whether this participant holds the lease.
func (s *Server) IsActive(_ context.Context, ref *externalscaler.ScaledObjectRef) (*externalscaler.IsActiveResponse, error) {
	src, err := s.source(ref)
	if err != nil {
		return nil, err
	}
	return &externalscaler.IsActiveResponse{Result: holding(src)}, nil
}

// StreamIsActive pushes the current value at once, then again on every
// change and at least once per heartbeat.
func (s *Server) StreamIsActive(ref *externalscaler.ScaledObjectRef, stream externalscaler.ExternalScaler_StreamIsActiveServer) error {
	src, err := s.source(ref)
	if err != nil {
		return err
	}
	ctx := stream.Context()
	ticker := time.NewTicker(s.heartbeat)
	defer ticker.Stop()
	last := holding(src)
	if err := stream.Send(&externalscaler.IsActiveResponse{Result: last}); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-src.Changed():
			cur := holding(src)
			if cur == last {
				continue
			}
			last = cur
		case <-ticker.C:
			last = holding(src)
		}
		if err := stream.Send(&externalscaler.IsActiveResponse{Result: last}); err != nil {
			return err
		}
	}
}

// GetMetricSpec declares one metric with target 1: active while holding; the
// replica count comes from the ScaledObject's minReplicaCount.
func (s *Server) GetMetricSpec(_ context.Context, ref *externalscaler.ScaledObjectRef) (*externalscaler.GetMetricSpecResponse, error) {
	if _, err := s.source(ref); err != nil {
		return nil, err
	}
	return &externalscaler.GetMetricSpecResponse{
		MetricSpecs: []*externalscaler.MetricSpec{{MetricName: MetricName, TargetSize: 1}},
	}, nil
}

// GetMetrics returns 1 while holding and 0 otherwise.
func (s *Server) GetMetrics(_ context.Context, req *externalscaler.GetMetricsRequest) (*externalscaler.GetMetricsResponse, error) {
	src, err := s.source(req.GetScaledObjectRef())
	if err != nil {
		return nil, err
	}
	var v int64
	if holding(src) {
		v = 1
	}
	return &externalscaler.GetMetricsResponse{
		MetricValues: []*externalscaler.MetricValue{{MetricName: MetricName, MetricValue: v}},
	}, nil
}
