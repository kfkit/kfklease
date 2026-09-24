// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

// Command kfklease-scaler takes part in one lease and serves it to KEDA as an
// external scaler. Run one per cluster; point a ScaledObject with
// maxReplicaCount 1 at it, and the workload runs where the lease is held.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/kfkit/kfklease/lease"
	"github.com/kfkit/kfklease/scaler"
)

func main() {
	if err := run(); err != nil {
		slog.Error("exiting", "err", err)
		os.Exit(1)
	}
}

// env returns the value of an environment variable or a default; flags
// override it.
func env(name, def string) string {
	if v, ok := os.LookupEnv(name); ok {
		return v
	}
	return def
}

func envDuration(name string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(name)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
		os.Exit(2)
	}
	return d
}

func run() error {
	var (
		brokers     = flag.String("brokers", env("KFKLEASE_BROKERS", ""), "comma-separated bootstrap addresses (KFKLEASE_BROKERS)")
		topic       = flag.String("topic", env("KFKLEASE_TOPIC", ""), "lease topic (KFKLEASE_TOPIC)")
		holder      = flag.String("holder", env("KFKLEASE_HOLDER", ""), "holder id, unique per process; empty for hostname/random (KFKLEASE_HOLDER)")
		ttl         = flag.Duration("ttl", envDuration("KFKLEASE_TTL", 10*time.Second), "lease ttl, the same for all participants (KFKLEASE_TTL)")
		margin      = flag.Duration("margin", envDuration("KFKLEASE_MARGIN", 0), "safety margin subtracted from the holder's deadline; 0 for ttl/5 (KFKLEASE_MARGIN)")
		createTopic = flag.Bool("create-topic", env("KFKLEASE_CREATE_TOPIC", "true") == "true", "create the topic if missing (KFKLEASE_CREATE_TOPIC)")
		listen      = flag.String("listen", env("KFKLEASE_LISTEN", ":9090"), "gRPC listen address (KFKLEASE_LISTEN)")
		logLevel    = flag.String("log-level", env("KFKLEASE_LOG_LEVEL", "info"), "debug, info, warn or error (KFKLEASE_LOG_LEVEL)")
	)
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		return fmt.Errorf("log level: %w", err)
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)

	if *brokers == "" || *topic == "" {
		return errors.New("brokers and topic are required")
	}
	cand, err := lease.NewCandidate(lease.Config{
		Brokers:     strings.Split(*brokers, ","),
		Topic:       *topic,
		CreateTopic: *createTopic,
		Holder:      *holder,
		TTL:         *ttl,
		Margin:      *margin,
		Logger:      log,
	})
	if err != nil {
		return err
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	gs := grpc.NewServer()
	scaler.New(cand, *topic, *ttl).Register(gs)
	hs := health.NewServer()
	grpc_health_v1.RegisterHealthServer(gs, hs)
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() { serveErr <- gs.Serve(lis) }()
	log.Info("serving", "addr", lis.Addr().String(), "topic", *topic, "holder", cand.Holder(), "ttl", *ttl)

	runErr := make(chan error, 1)
	go func() { runErr <- cand.Run(ctx) }()

	select {
	case err := <-runErr:
		gs.Stop()
		if err == nil {
			return errors.New("candidate stopped")
		}
		return err
	case err := <-serveErr:
		stop()
		<-runErr
		return fmt.Errorf("grpc: %w", err)
	case <-ctx.Done():
		log.Info("shutting down")
		hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		err := <-runErr // releases the lease if held
		gs.GracefulStop()
		return err
	}
}
