// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// The stand's broker has a SASL_PLAINTEXT listener (PLAIN user "kfklease",
// SCRAM users created here) and an SSL listener that requires a client
// certificate. KFKLEASE_SASL_BROKERS, KFKLEASE_MTLS_BROKERS and
// KFKLEASE_CERTS_DIR point at them; `make integration` sets all three.

const (
	saslUser     = "kfklease"
	saslPassword = "kfklease-secret"
)

func optionalBrokers(t *testing.T, name string) []string {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		t.Skipf("%s not set", name)
	}
	return strings.Split(v, ",")
}

// upsertScram gives saslUser a SCRAM credential through the plaintext
// admin listener.
func upsertScram(t *testing.T, mechanism kadm.ScramMechanism) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers(t)...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := kadm.NewClient(cl).AlterUserSCRAMs(ctx, nil, []kadm.UpsertSCRAM{{
		User: saslUser, Mechanism: mechanism, Iterations: 4096, Password: saslPassword,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.Error(); err != nil {
		t.Fatal(err)
	}
}

// acquiresWith runs one candidate with the given brokers and auth and
// expects it to hold the lease within a ttl.
func acquiresWith(t *testing.T, bs []string, auth Auth) {
	t.Helper()
	c, err := NewCandidate(Config{Brokers: bs, Topic: randomTopic(t), CreateTopic: true, TTL: itTTL, Margin: itMargin, Auth: auth})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
	}()
	waitFor(t, 2*itTTL, "holding", func() bool { return c.Status().Holding })
}

// failsWith expects Run to give up with an error rather than hang when the
// credentials are wrong.
func failsWith(t *testing.T, bs []string, auth Auth) {
	t.Helper()
	c, err := NewCandidate(Config{Brokers: bs, Topic: randomTopic(t), CreateTopic: true, TTL: itTTL, Auth: auth})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	err = c.Run(ctx)
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run = %v, want an authentication error", err)
	}
	if c.Status().Holding {
		t.Fatal("holding with bad credentials")
	}
}

func TestIntegrationSASLPlain(t *testing.T) {
	bs := optionalBrokers(t, "KFKLEASE_SASL_BROKERS")
	pw := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(pw, []byte(saslPassword+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	acquiresWith(t, bs, Auth{SASL: SASL{Mechanism: MechanismPlain, Username: saslUser, PasswordFile: pw}})
	failsWith(t, bs, Auth{SASL: SASL{Mechanism: MechanismPlain, Username: saslUser, Password: "wrong"}})
}

func TestIntegrationSASLScram(t *testing.T) {
	bs := optionalBrokers(t, "KFKLEASE_SASL_BROKERS")
	upsertScram(t, kadm.ScramSha256)
	upsertScram(t, kadm.ScramSha512)
	acquiresWith(t, bs, Auth{SASL: SASL{Mechanism: MechanismScramSha256, Username: saslUser, Password: saslPassword}})
	acquiresWith(t, bs, Auth{SASL: SASL{Mechanism: MechanismScramSha512, Username: saslUser, Password: saslPassword}})
	failsWith(t, bs, Auth{SASL: SASL{Mechanism: MechanismScramSha512, Username: saslUser, Password: "wrong"}})
}

func TestIntegrationMTLS(t *testing.T) {
	bs := optionalBrokers(t, "KFKLEASE_MTLS_BROKERS")
	dir := os.Getenv("KFKLEASE_CERTS_DIR")
	if dir == "" {
		t.Skip("KFKLEASE_CERTS_DIR not set")
	}
	ca, cert, key := filepath.Join(dir, "ca.crt"), filepath.Join(dir, "client.crt"), filepath.Join(dir, "client.key")
	acquiresWith(t, bs, Auth{TLS: TLS{Enabled: true, CAFile: ca, CertFile: cert, KeyFile: key}})
	// Without the client certificate the broker rejects the handshake.
	failsWith(t, bs, Auth{TLS: TLS{Enabled: true, CAFile: ca}})
	// Without the CA the client rejects the broker.
	failsWith(t, bs, Auth{TLS: TLS{Enabled: true, CertFile: cert, KeyFile: key}})
}
