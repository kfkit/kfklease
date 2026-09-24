// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

// Package scaler exposes a lease to KEDA as an external scaler: the metric is
// 1 while this participant holds the lease and 0 otherwise, so a ScaledObject
// with maxReplicaCount 1 follows the lease.
package scaler

import (
	"context"
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

// Server implements the KEDA external scaler protocol over a Source.
type Server struct {
	externalscaler.UnimplementedExternalScalerServer

	src Source
	// topic, when set, must match the "topic" metadata of the ScaledObject
	// if that metadata is present. It catches a ScaledObject pointed at the
	// wrong scaler.
	topic string
	// heartbeat is how often StreamIsActive repeats the current value when
	// nothing changes, so that a dead stream is noticed.
	heartbeat time.Duration
}

// New returns a server over src. topic may be empty; heartbeat must be
// positive.
func New(src Source, topic string, heartbeat time.Duration) *Server {
	if heartbeat <= 0 {
		panic("scaler: heartbeat must be positive")
	}
	return &Server{src: src, topic: topic, heartbeat: heartbeat}
}

// Register adds the server to a gRPC registrar.
func (s *Server) Register(r grpc.ServiceRegistrar) {
	externalscaler.RegisterExternalScalerServer(r, s)
}

func (s *Server) check(ref *externalscaler.ScaledObjectRef) error {
	if ref == nil {
		return status.Error(codes.InvalidArgument, "missing scaledObjectRef")
	}
	if want, ok := ref.GetScalerMetadata()["topic"]; ok && s.topic != "" && want != s.topic {
		return status.Errorf(codes.InvalidArgument, "this scaler serves topic %q, not %q", s.topic, want)
	}
	return nil
}

func (s *Server) holding() bool { return s.src.Status().Holding }

// metric is the metric value KEDA compares with the target of 1.
func (s *Server) metric() int64 {
	if s.holding() {
		return 1
	}
	return 0
}

// IsActive reports whether this participant holds the lease.
func (s *Server) IsActive(_ context.Context, ref *externalscaler.ScaledObjectRef) (*externalscaler.IsActiveResponse, error) {
	if err := s.check(ref); err != nil {
		return nil, err
	}
	return &externalscaler.IsActiveResponse{Result: s.holding()}, nil
}

// StreamIsActive pushes the current value at once, then again on every
// change and at least once per heartbeat.
func (s *Server) StreamIsActive(ref *externalscaler.ScaledObjectRef, stream externalscaler.ExternalScaler_StreamIsActiveServer) error {
	if err := s.check(ref); err != nil {
		return err
	}
	ctx := stream.Context()
	ticker := time.NewTicker(s.heartbeat)
	defer ticker.Stop()
	last := s.holding()
	if err := stream.Send(&externalscaler.IsActiveResponse{Result: last}); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.src.Changed():
			cur := s.holding()
			if cur == last {
				continue
			}
			last = cur
		case <-ticker.C:
			last = s.holding()
		}
		if err := stream.Send(&externalscaler.IsActiveResponse{Result: last}); err != nil {
			return err
		}
	}
}

// GetMetricSpec declares one metric with target 1: one replica while holding.
func (s *Server) GetMetricSpec(_ context.Context, ref *externalscaler.ScaledObjectRef) (*externalscaler.GetMetricSpecResponse, error) {
	if err := s.check(ref); err != nil {
		return nil, err
	}
	return &externalscaler.GetMetricSpecResponse{
		MetricSpecs: []*externalscaler.MetricSpec{{MetricName: MetricName, TargetSize: 1}},
	}, nil
}

// GetMetrics returns 1 while holding and 0 otherwise.
func (s *Server) GetMetrics(_ context.Context, req *externalscaler.GetMetricsRequest) (*externalscaler.GetMetricsResponse, error) {
	if err := s.check(req.GetScaledObjectRef()); err != nil {
		return nil, err
	}
	return &externalscaler.GetMetricsResponse{
		MetricValues: []*externalscaler.MetricValue{{MetricName: MetricName, MetricValue: s.metric()}},
	}, nil
}
