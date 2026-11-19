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

// lookupEnv returns the parsed value of an environment variable, or def when
// the variable is unset. A value that does not parse is an error that names
// the variable: nothing falls back to the default in silence.
func lookupEnv[T any](name string, def T, parse func(string) (T, error)) (T, error) {
	v, ok := os.LookupEnv(name)
	if !ok {
		return def, nil
	}
	x, err := parse(v)
	if err != nil {
		return def, fmt.Errorf("%s: %w", name, err)
	}
	return x, nil
}

// envParse is the default of a flag: lookupEnv, and a value that does not
// parse ends the process with status 2, as a bad flag does. Booleans go
// through strconv.ParseBool, so a variable accepts what its flag accepts.
func envParse[T any](name string, def T, parse func(string) (T, error)) T {
	x, err := lookupEnv(name, def, parse)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	return x
}

// text parses a string variable: any value is taken as it is.
func text(s string) (string, error) { return s, nil }

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
		brokers:  fs.String("brokers", envParse("KFKLEASE_BROKERS", "", text), "comma-separated bootstrap addresses (KFKLEASE_BROKERS)"),
		topic:    fs.String("topic", envParse("KFKLEASE_TOPIC", "", text), "lease topic; comma-separated for several leases, one per topic (KFKLEASE_TOPIC)"),
		ttl:      fs.Duration("ttl", envParse("KFKLEASE_TTL", 10*time.Second, time.ParseDuration), "lease ttl, the same for all participants (KFKLEASE_TTL)"),
		logLevel: fs.String("log-level", envParse("KFKLEASE_LOG_LEVEL", "info", text), "debug, info, warn or error (KFKLEASE_LOG_LEVEL)"),

		tlsEnabled:  fs.Bool("tls", envParse("KFKLEASE_TLS", false, strconv.ParseBool), "connect with TLS (KFKLEASE_TLS)"),
		tlsCA:       fs.String("tls-ca-file", envParse("KFKLEASE_TLS_CA_FILE", "", text), "PEM file with the CA that signed the brokers' certificates, re-read on every connection (KFKLEASE_TLS_CA_FILE; or the PEM itself in KFKLEASE_TLS_CA)"),
		tlsCert:     fs.String("tls-cert-file", envParse("KFKLEASE_TLS_CERT_FILE", "", text), "PEM client certificate for mTLS, re-read on every connection (KFKLEASE_TLS_CERT_FILE; or the PEM itself in KFKLEASE_TLS_CERT)"),
		tlsKey:      fs.String("tls-key-file", envParse("KFKLEASE_TLS_KEY_FILE", "", text), "PEM client key for mTLS, re-read on every connection (KFKLEASE_TLS_KEY_FILE; or the PEM itself in KFKLEASE_TLS_KEY)"),
		tlsServer:   fs.String("tls-server-name", envParse("KFKLEASE_TLS_SERVER_NAME", "", text), "name to verify the brokers' certificates against (KFKLEASE_TLS_SERVER_NAME)"),
		tlsInsecure: fs.Bool("tls-insecure-skip-verify", envParse("KFKLEASE_TLS_INSECURE_SKIP_VERIFY", false, strconv.ParseBool), "skip certificate verification; test stands only (KFKLEASE_TLS_INSECURE_SKIP_VERIFY)"),
		saslMech:    fs.String("sasl-mechanism", envParse("KFKLEASE_SASL_MECHANISM", "", text), "plain, scram-sha-256, scram-sha-512 or oauthbearer (KFKLEASE_SASL_MECHANISM)"),
		saslUser:    fs.String("sasl-username", envParse("KFKLEASE_SASL_USERNAME", "", text), "SASL username (KFKLEASE_SASL_USERNAME)"),
		saslUserF:   fs.String("sasl-username-file", envParse("KFKLEASE_SASL_USERNAME_FILE", "", text), "file with the SASL username, for secrets that hold both (KFKLEASE_SASL_USERNAME_FILE)"),
		saslPass:    fs.String("sasl-password", envParse("KFKLEASE_SASL_PASSWORD", "", text), "SASL password; prefer the file (KFKLEASE_SASL_PASSWORD)"),
		saslPassF:   fs.String("sasl-password-file", envParse("KFKLEASE_SASL_PASSWORD_FILE", "", text), "file with the SASL password, re-read on every authentication (KFKLEASE_SASL_PASSWORD_FILE)"),
		saslToken:   fs.String("sasl-token", envParse("KFKLEASE_SASL_TOKEN", "", text), "OAUTHBEARER token; prefer the file (KFKLEASE_SASL_TOKEN)"),
		saslTokenF:  fs.String("sasl-token-file", envParse("KFKLEASE_SASL_TOKEN_FILE", "", text), "file with the OAUTHBEARER token, re-read on every authentication (KFKLEASE_SASL_TOKEN_FILE)"),
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

// perTopic splits the comma-separated value of a flag that takes one entry
// per topic, in the order of -topic, an empty entry for a lease without.
// An empty value is no entry for any of them; anything else has to have as
// many entries as there are topics. what names the flag in the error.
func perTopic(what, value string, topics []string) ([]string, error) {
	if value == "" {
		return make([]string, len(topics)), nil
	}
	entries := strings.Split(value, ",")
	if len(entries) != len(topics) {
		return nil, fmt.Errorf("%s: %d entries for %d topics; give one per topic, empty for none", what, len(entries), len(topics))
	}
	return entries, nil
}

func run(args []string) error {
	fs := flag.NewFlagSet("kfklease-scaler", flag.ExitOnError)
	c := commonFlags(fs)
	var (
		site        = fs.String("site", envParse("KFKLEASE_SITE", "", text), "where this scaler runs, a cluster: the lease belongs to a site; empty for the holder id up to its last slash (KFKLEASE_SITE)")
		holder      = fs.String("holder", envParse("KFKLEASE_HOLDER", "", text), "holder id <site>/<name>, unique among the participants running at one time; empty for <site>/<hostname>-<random> (KFKLEASE_HOLDER)")
		takeover    = fs.Duration("takeover-delay", envParse("KFKLEASE_TAKEOVER_DELAY", 0, time.ParseDuration), "wait this long after another site's term ended before claiming, so that its workload has stopped; 0 claims at once (KFKLEASE_TAKEOVER_DELAY)")
		standby     = fs.Bool("standby", envParse("KFKLEASE_STANDBY", false, strconv.ParseBool), "standby mode: follow the leases and never claim them, to take this site out for maintenance or move the workload away (KFKLEASE_STANDBY)")
		margin      = fs.Duration("margin", envParse("KFKLEASE_MARGIN", 0, time.ParseDuration), "safety margin subtracted from the holder's deadline; 0 for ttl/5 (KFKLEASE_MARGIN)")
		createTopic = fs.Bool("create-topic", envParse("KFKLEASE_CREATE_TOPIC", true, strconv.ParseBool), "create the topic if missing, check it if it exists (KFKLEASE_CREATE_TOPIC)")
		replicas    = fs.Int("replication-factor", envParse("KFKLEASE_REPLICATION_FACTOR", lease.DefaultReplicationFactor, strconv.Atoi), "replication factor of a topic created by -create-topic; never lowered to fit the cluster (KFKLEASE_REPLICATION_FACTOR)")
		minISR      = fs.Int("min-insync-replicas", envParse("KFKLEASE_MIN_INSYNC_REPLICAS", 0, strconv.Atoi), "min.insync.replicas of a created topic; 0 for 2 with three or more replicas, otherwise 1 (KFKLEASE_MIN_INSYNC_REPLICAS)")
		flushEvery  = fs.Bool("flush-every-record", envParse("KFKLEASE_FLUSH_EVERY_RECORD", true, strconv.ParseBool), "create the topic with flush.messages=1: fsync every record before it is acknowledged; false leaves flush.messages to the broker (KFKLEASE_FLUSH_EVERY_RECORD)")
		listen      = fs.String("listen", envParse("KFKLEASE_LISTEN", ":9090", text), "gRPC listen address (KFKLEASE_LISTEN)")
		httpListen  = fs.String("http-listen", envParse("KFKLEASE_HTTP_LISTEN", ":9091", text), "HTTP listen address for /metrics, /status and /healthz; empty to disable (KFKLEASE_HTTP_LISTEN)")

		healthURL       = fs.String("health-url", envParse("KFKLEASE_HEALTH_URL", "", text), "workload URL probed while holding; empty turns the health gate off. With several topics, comma-separated in the same order, an empty entry for a lease without a gate (KFKLEASE_HEALTH_URL)")
		healthInterval  = fs.Duration("health-interval", envParse("KFKLEASE_HEALTH_INTERVAL", 10*time.Second, time.ParseDuration), "time between workload probes (KFKLEASE_HEALTH_INTERVAL)")
		healthTimeout   = fs.Duration("health-timeout", envParse("KFKLEASE_HEALTH_TIMEOUT", 5*time.Second, time.ParseDuration), "timeout of one workload probe (KFKLEASE_HEALTH_TIMEOUT)")
		healthFailAfter = fs.Duration("health-fail-after", envParse("KFKLEASE_HEALTH_FAIL_AFTER", 5*time.Minute, time.ParseDuration), "release the lease when the workload has not passed a probe for this long, counted from the start of the term (KFKLEASE_HEALTH_FAIL_AFTER)")
		healthCooldown  = fs.Duration("health-cooldown", envParse("KFKLEASE_HEALTH_COOLDOWN", 10*time.Minute, time.ParseDuration), "claim nothing for this long after such a release (KFKLEASE_HEALTH_COOLDOWN)")

		scaledObject = fs.String("scaled-object", envParse("KFKLEASE_SCALED_OBJECT", "", text), "ScaledObject of the lease's workload in this pod's namespace, for the handover: in standby, pause it when the site's term ends and tell the other sites once its pods are gone, so they take over without the takeover delay. Needs a service account that may get and patch it, get its target's scale and list pods; empty turns the handover off. With several topics, comma-separated in the same order, an empty entry for none (KFKLEASE_SCALED_OBJECT)")
	)
	_ = fs.Parse(args)

	log, topics, err := c.setup()
	if err != nil {
		return err
	}
	if *replicas < 1 || *replicas > math.MaxInt16 || *minISR < 0 {
		return fmt.Errorf("replication factor %d or min insync replicas %d out of range", *replicas, *minISR)
	}
	urls, err := perTopic("health url", *healthURL, topics)
	if err != nil {
		return err
	}
	if *healthURL != "" {
		if *healthInterval <= 0 || *healthTimeout <= 0 || *healthFailAfter <= 0 || *healthCooldown <= 0 {
			return errors.New("health interval, timeout, fail-after and cooldown must be positive")
		}
		if *healthFailAfter < 2**healthInterval {
			return fmt.Errorf("health fail-after %v must be at least two probe intervals (%v)", *healthFailAfter, *healthInterval)
		}
	}
	scaledObjects, err := perTopic("scaled object", *scaledObject, topics)
	if err != nil {
		return err
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
