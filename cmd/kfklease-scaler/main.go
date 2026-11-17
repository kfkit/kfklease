// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

// Command kfklease-scaler takes part in one or more leases and serves them to
// KEDA as an external scaler. Run one per cluster; point a ScaledObject with
// idleReplicaCount 0 and minReplicaCount = maxReplicaCount at it, naming its
// lease by the topic in the trigger metadata, and the workload runs where
// the lease is held.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
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

func envInt(name string, def int) int {
	v, ok := os.LookupEnv(name)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
		os.Exit(2)
	}
	return n
}

func run() error {
	var (
		brokers     = flag.String("brokers", env("KFKLEASE_BROKERS", ""), "comma-separated bootstrap addresses (KFKLEASE_BROKERS)")
		topic       = flag.String("topic", env("KFKLEASE_TOPIC", ""), "lease topic; comma-separated for several leases, one per topic (KFKLEASE_TOPIC)")
		holder      = flag.String("holder", env("KFKLEASE_HOLDER", ""), "holder id, unique per process; empty for hostname/random (KFKLEASE_HOLDER)")
		ttl         = flag.Duration("ttl", envDuration("KFKLEASE_TTL", 10*time.Second), "lease ttl, the same for all participants (KFKLEASE_TTL)")
		margin      = flag.Duration("margin", envDuration("KFKLEASE_MARGIN", 0), "safety margin subtracted from the holder's deadline; 0 for ttl/5 (KFKLEASE_MARGIN)")
		createTopic = flag.Bool("create-topic", env("KFKLEASE_CREATE_TOPIC", "true") == "true", "create the topic if missing, check it if it exists (KFKLEASE_CREATE_TOPIC)")
		replicas    = flag.Int("replication-factor", envInt("KFKLEASE_REPLICATION_FACTOR", lease.DefaultReplicationFactor), "replication factor of a topic created by -create-topic; never lowered to fit the cluster (KFKLEASE_REPLICATION_FACTOR)")
		minISR      = flag.Int("min-insync-replicas", envInt("KFKLEASE_MIN_INSYNC_REPLICAS", 0), "min.insync.replicas of a created topic; 0 for 2 with three or more replicas, otherwise 1 (KFKLEASE_MIN_INSYNC_REPLICAS)")
		flushEvery  = flag.Bool("flush-every-record", env("KFKLEASE_FLUSH_EVERY_RECORD", "false") == "true", "create the topic with flush.messages=1: fsync every record before it is acknowledged (KFKLEASE_FLUSH_EVERY_RECORD)")
		listen      = flag.String("listen", env("KFKLEASE_LISTEN", ":9090"), "gRPC listen address (KFKLEASE_LISTEN)")
		httpListen  = flag.String("http-listen", env("KFKLEASE_HTTP_LISTEN", ":9091"), "HTTP listen address for /metrics, /status and /healthz; empty to disable (KFKLEASE_HTTP_LISTEN)")
		logLevel    = flag.String("log-level", env("KFKLEASE_LOG_LEVEL", "info"), "debug, info, warn or error (KFKLEASE_LOG_LEVEL)")

		healthURL       = flag.String("health-url", env("KFKLEASE_HEALTH_URL", ""), "workload URL probed while holding; empty turns the health gate off. With several topics, comma-separated in the same order, an empty entry for a lease without a gate (KFKLEASE_HEALTH_URL)")
		healthInterval  = flag.Duration("health-interval", envDuration("KFKLEASE_HEALTH_INTERVAL", 10*time.Second), "time between workload probes (KFKLEASE_HEALTH_INTERVAL)")
		healthTimeout   = flag.Duration("health-timeout", envDuration("KFKLEASE_HEALTH_TIMEOUT", 5*time.Second), "timeout of one workload probe (KFKLEASE_HEALTH_TIMEOUT)")
		healthFailAfter = flag.Duration("health-fail-after", envDuration("KFKLEASE_HEALTH_FAIL_AFTER", 5*time.Minute), "release the lease when the workload has not passed a probe for this long, counted from the start of the term (KFKLEASE_HEALTH_FAIL_AFTER)")
		healthCooldown  = flag.Duration("health-cooldown", envDuration("KFKLEASE_HEALTH_COOLDOWN", 10*time.Minute), "claim nothing for this long after such a release (KFKLEASE_HEALTH_COOLDOWN)")

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
	if *replicas < 1 || *replicas > math.MaxInt16 || *minISR < 0 {
		return fmt.Errorf("replication factor %d or min insync replicas %d out of range", *replicas, *minISR)
	}
	topics := strings.Split(*topic, ",")
	urls := make([]string, len(topics))
	if *healthURL != "" {
		urls = strings.Split(*healthURL, ",")
		if len(urls) != len(topics) {
			return fmt.Errorf("health url: %d entries for %d topics; give one per topic, empty for none", len(urls), len(topics))
		}
		if *healthInterval <= 0 || *healthTimeout <= 0 || *healthFailAfter <= 0 || *healthCooldown <= 0 {
			return errors.New("health interval, timeout, fail-after and cooldown must be positive")
		}
		if *healthFailAfter < 2**healthInterval {
			return fmt.Errorf("health fail-after %v must be at least two probe intervals (%v)", *healthFailAfter, *healthInterval)
		}
	}
	base := lease.Config{
		Brokers:     strings.Split(*brokers, ","),
		CreateTopic: *createTopic,
		TopicSettings: lease.TopicSettings{
			ReplicationFactor: int16(*replicas),
			MinInsyncReplicas: *minISR,
			FlushEveryRecord:  *flushEvery,
		},
		Holder: *holder,
		TTL:    *ttl,
		Margin: *margin,
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
	}
	// One candidate per lease, each with its own client: the leases are
	// independent, and so is their traffic.
	cands := make([]*lease.Candidate, len(topics))
	served := make([]scaler.Lease, len(topics))
	for i, t := range topics {
		if t == "" {
			return fmt.Errorf("topic %q: empty entry", *topic)
		}
		cfg := base
		cfg.Topic = t
		if urls[i] != "" {
			cfg.Health = lease.HealthGate{FailAfter: *healthFailAfter, Cooldown: *healthCooldown}
		}
		cand, err := lease.NewCandidate(cfg)
		if err != nil {
			return fmt.Errorf("topic %s: %w", t, err)
		}
		cands[i] = cand
		served[i] = scaler.Lease{Topic: t, Holder: cand.Holder(), Source: cand}
	}
	srv, err := scaler.NewLeases(served, *ttl)
	if err != nil {
		return err
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	gs := grpc.NewServer()
	srv.Register(gs)
	hs := health.NewServer()
	grpc_health_v1.RegisterHealthServer(gs, hs)
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() { serveErr <- gs.Serve(lis) }()
	for _, l := range served {
		log.Info("serving", "addr", lis.Addr().String(), "topic", l.Topic, "holder", l.Holder, "ttl", *ttl)
	}

	var hs2 *http.Server
	if *httpListen != "" {
		h, err := scaler.NewHTTPLeases(served)
		if err != nil {
			return err
		}
		go h.Watch(ctx)
		hs2 = &http.Server{Addr: *httpListen, Handler: h.Handler(), ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := hs2.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				serveErr <- fmt.Errorf("http: %w", err)
			}
		}()
		log.Info("serving http", "addr", *httpListen)
	}
	defer func() {
		if hs2 != nil {
			_ = hs2.Close()
		}
	}()

	for i, u := range urls {
		if u == "" {
			continue
		}
		p := scaler.Probe{URL: u, Interval: *healthInterval, Timeout: *healthTimeout, Log: log.With("lease", topics[i])}
		go p.Run(ctx, cands[i], cands[i].ReportHealthy)
		log.Info("health gate on", "topic", topics[i], "url", u, "fail_after", *healthFailAfter, "cooldown", *healthCooldown)
	}

	// The candidates run together: when one stops, all stop, and each
	// releases its lease if it holds it.
	runCtx, stopAll := context.WithCancel(ctx)
	defer stopAll()
	runErr := make(chan error, len(cands))
	for _, c := range cands {
		go func() { runErr <- c.Run(runCtx) }()
	}
	// waitAll stops every candidate, waits for the rest after first and
	// joins their errors.
	waitAll := func(first error) error {
		stopAll()
		errs := []error{first}
		for range len(cands) - 1 {
			errs = append(errs, <-runErr)
		}
		return errors.Join(errs...)
	}

	select {
	case err := <-runErr:
		gs.Stop()
		if err == nil {
			err = errors.New("candidate stopped")
		}
		return waitAll(err)
	case err := <-serveErr:
		stop()
		return errors.Join(fmt.Errorf("grpc: %w", err), waitAll(<-runErr))
	case <-ctx.Done():
		log.Info("shutting down")
		hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		err := waitAll(<-runErr) // releases the leases that are held
		gs.GracefulStop()
		return err
	}
}
