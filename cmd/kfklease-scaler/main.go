// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

// Command kfklease-scaler takes part in one or more leases and serves them to
// KEDA as an external scaler. Run one per cluster; point a ScaledObject with
// idleReplicaCount 0 and minReplicaCount = maxReplicaCount at it, naming its
// lease by the topic in the trigger metadata, and the workload runs where
// the lease is held.
//
//	kfklease-scaler [flags]          serve the leases to KEDA
//	kfklease-scaler status [flags]   print the state of the leases and exit
//
// Every flag falls back to a KFKLEASE_* environment variable, so in a
// scaler's pod `kfklease-scaler status` reads the leases that pod serves.
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
	"slices"
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
	args := os.Args[1:]
	cmd := run
	if len(args) > 0 && args[0] == "status" {
		cmd, args = status, args[1:]
	}
	if err := cmd(args); err != nil {
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

// common are the flags of every command: the leases and how to reach them.
type common struct {
	brokers, topic, logLevel *string
	ttl                      *time.Duration

	tlsEnabled, tlsInsecure                                                   *bool
	tlsCA, tlsCert, tlsKey, tlsServer                                         *string
	saslMech, saslUser, saslUserF, saslPass, saslPassF, saslToken, saslTokenF *string
}

func commonFlags(fs *flag.FlagSet) *common {
	return &common{
		brokers:  fs.String("brokers", env("KFKLEASE_BROKERS", ""), "comma-separated bootstrap addresses (KFKLEASE_BROKERS)"),
		topic:    fs.String("topic", env("KFKLEASE_TOPIC", ""), "lease topic; comma-separated for several leases, one per topic (KFKLEASE_TOPIC)"),
		ttl:      fs.Duration("ttl", envDuration("KFKLEASE_TTL", 10*time.Second), "lease ttl, the same for all participants (KFKLEASE_TTL)"),
		logLevel: fs.String("log-level", env("KFKLEASE_LOG_LEVEL", "info"), "debug, info, warn or error (KFKLEASE_LOG_LEVEL)"),

		tlsEnabled:  fs.Bool("tls", env("KFKLEASE_TLS", "false") == "true", "connect with TLS (KFKLEASE_TLS)"),
		tlsCA:       fs.String("tls-ca-file", env("KFKLEASE_TLS_CA_FILE", ""), "PEM file with the CA that signed the brokers' certificates, re-read on every connection (KFKLEASE_TLS_CA_FILE; or the PEM itself in KFKLEASE_TLS_CA)"),
		tlsCert:     fs.String("tls-cert-file", env("KFKLEASE_TLS_CERT_FILE", ""), "PEM client certificate for mTLS, re-read on every connection (KFKLEASE_TLS_CERT_FILE; or the PEM itself in KFKLEASE_TLS_CERT)"),
		tlsKey:      fs.String("tls-key-file", env("KFKLEASE_TLS_KEY_FILE", ""), "PEM client key for mTLS, re-read on every connection (KFKLEASE_TLS_KEY_FILE; or the PEM itself in KFKLEASE_TLS_KEY)"),
		tlsServer:   fs.String("tls-server-name", env("KFKLEASE_TLS_SERVER_NAME", ""), "name to verify the brokers' certificates against (KFKLEASE_TLS_SERVER_NAME)"),
		tlsInsecure: fs.Bool("tls-insecure-skip-verify", env("KFKLEASE_TLS_INSECURE_SKIP_VERIFY", "false") == "true", "skip certificate verification; test stands only (KFKLEASE_TLS_INSECURE_SKIP_VERIFY)"),
		saslMech:    fs.String("sasl-mechanism", env("KFKLEASE_SASL_MECHANISM", ""), "plain, scram-sha-256, scram-sha-512 or oauthbearer (KFKLEASE_SASL_MECHANISM)"),
		saslUser:    fs.String("sasl-username", env("KFKLEASE_SASL_USERNAME", ""), "SASL username (KFKLEASE_SASL_USERNAME)"),
		saslUserF:   fs.String("sasl-username-file", env("KFKLEASE_SASL_USERNAME_FILE", ""), "file with the SASL username, for secrets that hold both (KFKLEASE_SASL_USERNAME_FILE)"),
		saslPass:    fs.String("sasl-password", env("KFKLEASE_SASL_PASSWORD", ""), "SASL password; prefer the file (KFKLEASE_SASL_PASSWORD)"),
		saslPassF:   fs.String("sasl-password-file", env("KFKLEASE_SASL_PASSWORD_FILE", ""), "file with the SASL password, re-read on every authentication (KFKLEASE_SASL_PASSWORD_FILE)"),
		saslToken:   fs.String("sasl-token", env("KFKLEASE_SASL_TOKEN", ""), "OAUTHBEARER token; prefer the file (KFKLEASE_SASL_TOKEN)"),
		saslTokenF:  fs.String("sasl-token-file", env("KFKLEASE_SASL_TOKEN_FILE", ""), "file with the OAUTHBEARER token, re-read on every authentication (KFKLEASE_SASL_TOKEN_FILE)"),
	}
}

// setup makes a JSON logger the default, checks the required flags and
// returns the logger and the lease topics.
func (c *common) setup() (*slog.Logger, []string, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(*c.logLevel)); err != nil {
		return nil, nil, fmt.Errorf("log level: %w", err)
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)
	if *c.brokers == "" || *c.topic == "" {
		return nil, nil, errors.New("brokers and topic are required")
	}
	topics := strings.Split(*c.topic, ",")
	if slices.Contains(topics, "") {
		return nil, nil, fmt.Errorf("topic %q: empty entry", *c.topic)
	}
	return log, topics, nil
}

// config is the lease configuration the common flags describe, without a
// topic.
func (c *common) config(log *slog.Logger) lease.Config {
	return lease.Config{
		Brokers: strings.Split(*c.brokers, ","),
		TTL:     *c.ttl,
		Auth: lease.Auth{
			TLS: lease.TLS{
				Enabled: *c.tlsEnabled,
				CA:      os.Getenv("KFKLEASE_TLS_CA"), CAFile: *c.tlsCA,
				Cert: os.Getenv("KFKLEASE_TLS_CERT"), CertFile: *c.tlsCert,
				Key: os.Getenv("KFKLEASE_TLS_KEY"), KeyFile: *c.tlsKey,
				ServerName: *c.tlsServer, InsecureSkipVerify: *c.tlsInsecure,
			},
			SASL: lease.SASL{
				Mechanism: *c.saslMech, Username: *c.saslUser, UsernameFile: *c.saslUserF,
				Password: *c.saslPass, PasswordFile: *c.saslPassF,
				Token: *c.saslToken, TokenFile: *c.saslTokenF,
			},
		},
		Logger: log,
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("kfklease-scaler", flag.ExitOnError)
	c := commonFlags(fs)
	var (
		site        = fs.String("site", env("KFKLEASE_SITE", ""), "where this scaler runs, a cluster: the lease belongs to a site; empty for the holder id up to its last slash (KFKLEASE_SITE)")
		holder      = fs.String("holder", env("KFKLEASE_HOLDER", ""), "holder id <site>/<name>, unique per process; empty for <site>/<hostname>-<random> (KFKLEASE_HOLDER)")
		takeover    = fs.Duration("takeover-delay", envDuration("KFKLEASE_TAKEOVER_DELAY", 0), "wait this long after another site's term ended before claiming, so that its workload has stopped; 0 claims at once (KFKLEASE_TAKEOVER_DELAY)")
		standby     = fs.Bool("standby", env("KFKLEASE_STANDBY", "false") == "true", "standby mode: follow the leases and never claim them, to take this site out for maintenance or move the workload away (KFKLEASE_STANDBY)")
		margin      = fs.Duration("margin", envDuration("KFKLEASE_MARGIN", 0), "safety margin subtracted from the holder's deadline; 0 for ttl/5 (KFKLEASE_MARGIN)")
		createTopic = fs.Bool("create-topic", env("KFKLEASE_CREATE_TOPIC", "true") == "true", "create the topic if missing, check it if it exists (KFKLEASE_CREATE_TOPIC)")
		replicas    = fs.Int("replication-factor", envInt("KFKLEASE_REPLICATION_FACTOR", lease.DefaultReplicationFactor), "replication factor of a topic created by -create-topic; never lowered to fit the cluster (KFKLEASE_REPLICATION_FACTOR)")
		minISR      = fs.Int("min-insync-replicas", envInt("KFKLEASE_MIN_INSYNC_REPLICAS", 0), "min.insync.replicas of a created topic; 0 for 2 with three or more replicas, otherwise 1 (KFKLEASE_MIN_INSYNC_REPLICAS)")
		flushEvery  = fs.Bool("flush-every-record", env("KFKLEASE_FLUSH_EVERY_RECORD", "false") == "true", "create the topic with flush.messages=1: fsync every record before it is acknowledged (KFKLEASE_FLUSH_EVERY_RECORD)")
		listen      = fs.String("listen", env("KFKLEASE_LISTEN", ":9090"), "gRPC listen address (KFKLEASE_LISTEN)")
		httpListen  = fs.String("http-listen", env("KFKLEASE_HTTP_LISTEN", ":9091"), "HTTP listen address for /metrics, /status and /healthz; empty to disable (KFKLEASE_HTTP_LISTEN)")

		healthURL       = fs.String("health-url", env("KFKLEASE_HEALTH_URL", ""), "workload URL probed while holding; empty turns the health gate off. With several topics, comma-separated in the same order, an empty entry for a lease without a gate (KFKLEASE_HEALTH_URL)")
		healthInterval  = fs.Duration("health-interval", envDuration("KFKLEASE_HEALTH_INTERVAL", 10*time.Second), "time between workload probes (KFKLEASE_HEALTH_INTERVAL)")
		healthTimeout   = fs.Duration("health-timeout", envDuration("KFKLEASE_HEALTH_TIMEOUT", 5*time.Second), "timeout of one workload probe (KFKLEASE_HEALTH_TIMEOUT)")
		healthFailAfter = fs.Duration("health-fail-after", envDuration("KFKLEASE_HEALTH_FAIL_AFTER", 5*time.Minute), "release the lease when the workload has not passed a probe for this long, counted from the start of the term (KFKLEASE_HEALTH_FAIL_AFTER)")
		healthCooldown  = fs.Duration("health-cooldown", envDuration("KFKLEASE_HEALTH_COOLDOWN", 10*time.Minute), "claim nothing for this long after such a release (KFKLEASE_HEALTH_COOLDOWN)")

		scaledObject = fs.String("scaled-object", env("KFKLEASE_SCALED_OBJECT", ""), "ScaledObject of the lease's workload in this pod's namespace, for the handover: in standby, pause it when the site's term ends and tell the other sites once its pods are gone, so they take over without the takeover delay. Needs a service account that may get and patch it, get its target's scale and list pods; empty turns the handover off. With several topics, comma-separated in the same order, an empty entry for none (KFKLEASE_SCALED_OBJECT)")
	)
	_ = fs.Parse(args)

	log, topics, err := c.setup()
	if err != nil {
		return err
	}
	if *replicas < 1 || *replicas > math.MaxInt16 || *minISR < 0 {
		return fmt.Errorf("replication factor %d or min insync replicas %d out of range", *replicas, *minISR)
	}
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
	scaledObjects := make([]string, len(topics))
	if *scaledObject != "" {
		scaledObjects = strings.Split(*scaledObject, ",")
		if len(scaledObjects) != len(topics) {
			return fmt.Errorf("scaled object: %d entries for %d topics; give one per topic, empty for none", len(scaledObjects), len(topics))
		}
	}
	var kube *scaler.Kube
	if slices.ContainsFunc(scaledObjects, func(so string) bool { return so != "" }) {
		if kube, err = scaler.InClusterKube(); err != nil {
			return fmt.Errorf("handover: %w", err)
		}
	}
	base := c.config(log)
	base.CreateTopic = *createTopic
	base.TopicSettings = lease.TopicSettings{
		ReplicationFactor: int16(*replicas),
		MinInsyncReplicas: *minISR,
		FlushEveryRecord:  *flushEvery,
	}
	base.Site, base.Holder, base.TakeoverDelay = *site, *holder, *takeover
	base.Margin, base.Standby = *margin, *standby
	// One candidate per lease, each with its own client: the leases are
	// independent, and so is their traffic.
	cands := make([]*lease.Candidate, len(topics))
	served := make([]scaler.Lease, len(topics))
	for i, t := range topics {
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
	srv, err := scaler.NewLeases(served, *c.ttl)
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
	hs.SetServingStatus(readyService, grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() { serveErr <- gs.Serve(lis) }()
	for _, l := range served {
		log.Info("serving", "addr", lis.Addr().String(), "topic", l.Topic, "holder", l.Holder, "site", lease.SiteOf(l.Holder), "ttl", *c.ttl, "takeover_delay", *takeover, "standby", *standby)
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
	for i, so := range scaledObjects {
		if so == "" {
			continue
		}
		h := scaler.Handover{Source: cands[i], Workload: kube, ScaledObject: so, Site: lease.SiteOf(cands[i].Holder()),
			Interval: handoverInterval, Log: log.With("lease", topics[i])}
		go h.Run(runCtx)
		log.Info("handover on", "topic", topics[i], "scaled_object", so, "namespace", kube.Namespace)
	}
	runErr := make(chan error, len(cands))
	for _, c := range cands {
		go func() { runErr <- c.Run(runCtx) }()
	}
	go func() {
		srcs := make([]scaler.Source, len(cands))
		for i, c := range cands {
			srcs[i] = c
		}
		if waitReady(runCtx, srcs, 200*time.Millisecond) {
			hs.SetServingStatus(readyService, grpc_health_v1.HealthCheckResponse_SERVING)
			log.Info("ready: the state of every lease is known")
		}
	}()
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
		// Stop answering KEDA first: its streams to this pod break, and it
		// asks another replica of the site, which still sees the term, and
		// takes it over once the release lands. Then release.
		hs.Shutdown() // every service NOT_SERVING, for good
		gs.Stop()
		return waitAll(<-runErr) // releases the leases that are held
	}
}

// handoverInterval is how often a handover looks at its lease and, while
// the workload stops, counts its pods.
const handoverInterval = time.Second

// readyService is the gRPC health service the chart's readiness probe asks
// for: SERVING once the scaler knows the state of every lease it serves.
// Until then it would answer KEDA with an error, so a rolling update waits
// for it and the Service sends KEDA elsewhere. The liveness probe asks for
// the server as a whole ("") and does not wait.
const readyService = "ready"

// waitReady polls srcs every interval until all of them are Ready and
// reports true, or false when ctx is done first.
func waitReady(ctx context.Context, srcs []scaler.Source, every time.Duration) bool {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if !slices.ContainsFunc(srcs, func(s scaler.Source) bool { return !s.Status().Ready }) {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
		}
	}
}
