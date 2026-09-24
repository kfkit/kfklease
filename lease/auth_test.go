// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// selfSigned writes a certificate and key and returns their paths.
func selfSigned(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		IsCA:         true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certFile = writeFile(t, "cert.pem", string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})))
	keyFile = writeFile(t, "key.pem", string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})))
	return certFile, keyFile
}

func TestAuthZeroValueIsPlaintext(t *testing.T) {
	opts, err := Auth{}.clientOpts()
	if err != nil || len(opts) != 0 {
		t.Fatalf("opts=%d err=%v", len(opts), err)
	}
}

func TestTLSConfig(t *testing.T) {
	cert, key := selfSigned(t)

	cfg, err := TLS{Enabled: true}.config()
	if err != nil || cfg.RootCAs != nil || len(cfg.Certificates) != 0 || cfg.InsecureSkipVerify {
		t.Fatalf("plain tls: %+v %v", cfg, err)
	}

	cfg, err = TLS{Enabled: true, CAFile: cert, CertFile: cert, KeyFile: key, ServerName: "broker"}.config()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RootCAs == nil || len(cfg.Certificates) != 1 || cfg.ServerName != "broker" {
		t.Fatalf("mtls: RootCAs=%v certs=%d server=%q", cfg.RootCAs != nil, len(cfg.Certificates), cfg.ServerName)
	}

	bad := map[string]TLS{
		"missing ca":       {Enabled: true, CAFile: filepath.Join(t.TempDir(), "none.pem")},
		"ca without certs": {Enabled: true, CAFile: writeFile(t, "empty.pem", "not a pem")},
		"cert without key": {Enabled: true, CertFile: cert},
		"key without cert": {Enabled: true, KeyFile: key},
		"mismatched pair":  {Enabled: true, CertFile: cert, KeyFile: writeFile(t, "other.pem", "garbage")},
	}
	for name, tc := range bad {
		if _, err := tc.config(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := (Auth{TLS: TLS{CAFile: cert}}).clientOpts(); err == nil {
		t.Error("ca file without tls enabled: accepted")
	}
}

func TestSASLMechanisms(t *testing.T) {
	ctx := context.Background()
	pwFile := writeFile(t, "password", "s3cret\n")

	m, err := SASL{Mechanism: "PLAIN", Username: "u", PasswordFile: pwFile}.mechanism()
	if err != nil {
		t.Fatal(err)
	}
	if m.Name() != "PLAIN" {
		t.Fatalf("name = %s", m.Name())
	}
	// The file is read at authentication time, so a rotated password is
	// picked up without a restart; the trailing newline is dropped.
	if err := os.WriteFile(pwFile, []byte("r0tated\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, _, err = m.Authenticate(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	// plain's first client message is "\x00user\x00pass".
	_, first, err := m.Authenticate(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if got := string(first); got != "\x00u\x00r0tated" {
		t.Fatalf("plain first message = %q", got)
	}

	for _, mech := range []string{MechanismScramSha256, MechanismScramSha512} {
		m, err := SASL{Mechanism: mech, Username: "u", Password: "p"}.mechanism()
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.ToLower(m.Name()); got != mech {
			t.Fatalf("name = %s, want %s", got, mech)
		}
	}
	m, err = SASL{Mechanism: MechanismOAuthBearer, TokenFile: writeFile(t, "token", "tok")}.mechanism()
	if err != nil || m.Name() != "OAUTHBEARER" {
		t.Fatalf("oauth: %v %v", m, err)
	}
	m, err = SASL{}.mechanism()
	if err != nil || m != nil {
		t.Fatalf("no sasl: %v %v", m, err)
	}

	bad := map[string]SASL{
		"unknown mechanism":          {Mechanism: "gssapi", Username: "u", Password: "p"},
		"plain without username":     {Mechanism: MechanismPlain, Password: "p"},
		"plain without password":     {Mechanism: MechanismPlain, Username: "u"},
		"password inline and file":   {Mechanism: MechanismPlain, Username: "u", Password: "p", PasswordFile: pwFile},
		"missing password file":      {Mechanism: MechanismScramSha256, Username: "u", PasswordFile: "/nonexistent"},
		"oauth without token":        {Mechanism: MechanismOAuthBearer},
		"credentials without a mech": {Username: "u", Password: "p"},
	}
	for name, tc := range bad {
		if _, err := tc.mechanism(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// The mechanisms must be the franz-go ones, not look-alikes.
var (
	_ = plain.Plain
	_ = scram.Sha256
)
