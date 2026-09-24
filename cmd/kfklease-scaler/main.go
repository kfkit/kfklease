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

		tlsEnabled  = flag.Bool("tls", env("KFKLEASE_TLS", "false") == "true", "connect with TLS (KFKLEASE_TLS)")
		tlsCA       = flag.String("tls-ca-file", env("KFKLEASE_TLS_CA_FILE", ""), "PEM file with the CA that signed the brokers' certificates, re-read on every connection (KFKLEASE_TLS_CA_FILE; or the PEM itself in KFKLEASE_TLS_CA)")
		tlsCert     = flag.String("tls-cert-file", env("KFKLEASE_TLS_CERT_FILE", ""), "PEM client certificate for mTLS, re-read on every connection (KFKLEASE_TLS_CERT_FILE; or the PEM itself in KFKLEASE_TLS_CERT)")
		tlsKey      = flag.String("tls-key-file", env("KFKLEASE_TLS_KEY_FILE", ""), "PEM client key for mTLS, re-read on every connection (KFKLEASE_TLS_KEY_FILE; or the PEM itself in KFKLEASE_TLS_KEY)")
		tlsServer   = flag.String("tls-server-name", env("KFKLEASE_TLS_SERVER_NAME", ""), "name to verify the brokers' certificates against (KFKLEASE_TLS_SERVER_NAME)")
		tlsInsecure = flag.Bool("tls-insecure-skip-verify", env("KFKLEASE_TLS_INSECURE_SKIP_VERIFY", "false") == "true", "skip certificate verification; test stands only (KFKLEASE_TLS_INSECURE_SKIP_VERIFY)")
		saslMech    = flag.String("sasl-mechanism", env("KFKLEASE_SASL_MECHANISM", ""), "plain, scram-sha-256, scram-sha-512 or oauthbearer (KFKLEASE_SASL_MECHANISM)")
		saslUser    = flag.String("sasl-username", env("KFKLEASE_SASL_USERNAME", ""), "SASL username (KFKLEASE_SASL_USERNAME)")
		saslUserF   = flag.String("sasl-username-file", env("KFKLEASE_SASL_USERNAME_FILE", ""), "file with the SASL username, for secrets that hold both (KFKLEASE_SASL_USERNAME_FILE)")
		saslPass    = flag.String("sasl-password", env("KFKLEASE_SASL_PASSWORD", ""), "SASL password; prefer the file (KFKLEASE_SASL_PASSWORD)")
		saslPassF   = flag.String("sasl-password-file", env("KFKLEASE_SASL_PASSWORD_FILE", ""), "file with the SASL password, re-read on every authentication (KFKLEASE_SASL_PASSWORD_FILE)")
		saslToken   = flag.String("sasl-token", env("KFKLEASE_SASL_TOKEN", ""), "OAUTHBEARER token; prefer the file (KFKLEASE_SASL_TOKEN)")
		saslTokenF  = flag.String("sasl-token-file", env("KFKLEASE_SASL_TOKEN_FILE", ""), "file with the OAUTHBEARER token, re-read on every authentication (KFKLEASE_SASL_TOKEN_FILE)")
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
		Auth: lease.Auth{
			TLS: lease.TLS{
				Enabled: *tlsEnabled,
				CA:      os.Getenv("KFKLEASE_TLS_CA"), CAFile: *tlsCA,
				Cert: os.Getenv("KFKLEASE_TLS_CERT"), CertFile: *tlsCert,
				Key: os.Getenv("KFKLEASE_TLS_KEY"), KeyFile: *tlsKey,
				ServerName: *tlsServer, InsecureSkipVerify: *tlsInsecure,
			},
			SASL: lease.SASL{
				Mechanism: *saslMech, Username: *saslUser, UsernameFile: *saslUserF,
				Password: *saslPass, PasswordFile: *saslPassF,
				Token: *saslToken, TokenFile: *saslTokenF,
			},
		},
		Logger: log,
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
