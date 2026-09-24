// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
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

// certAuthority is a CA that can sign leaf certificates, for a TLS server
// in a test.
type certAuthority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  string
}

func newCA(t *testing.T, name string) *certAuthority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, IsCA: true, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &certAuthority{cert: cert, key: key, pem: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))}
}

// leaf issues a certificate for 127.0.0.1 and returns it with its key, PEM.
func (ca *certAuthority) leaf(t *testing.T, name string, client bool) (certPEM, keyPEM string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	usage := x509.ExtKeyUsageServerAuth
	if client {
		usage = x509.ExtKeyUsageClientAuth
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
}

// tlsServer accepts connections that present a client certificate signed
// by ca and returns its address.
func tlsServer(t *testing.T, ca *certAuthority) string {
	t.Helper()
	certPEM, keyPEM := ca.leaf(t, "server", false)
	pair, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	lis, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{pair}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool, MinVersion: tls.VersionTLS12,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lis.Close() })
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			go func() {
				// Drive the handshake so the client sees its result.
				_ = c.(*tls.Conn).Handshake()
				_ = c.Close()
			}()
		}
	}()
	return lis.Addr().String()
}

func dialOK(t *testing.T, cfg TLS, addr string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := cfg.dial(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := c.(*tls.Conn).HandshakeContext(ctx); err != nil {
		return err
	}
	// With TLS 1.3 the client's handshake completes before the server has
	// judged the client certificate; a rejection arrives as an alert on the
	// first read, a clean close as EOF.
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func TestTLSFilesAreReadOnEveryConnection(t *testing.T) {
	ca, other := newCA(t, "ca"), newCA(t, "other")
	addr := tlsServer(t, ca)
	clientCert, clientKey := ca.leaf(t, "client", true)
	otherCert, otherKey := other.leaf(t, "client", true)

	caFile := writeFile(t, "ca.crt", ca.pem)
	certFile := writeFile(t, "tls.crt", clientCert)
	keyFile := writeFile(t, "tls.key", clientKey)
	cfg := TLS{Enabled: true, CAFile: caFile, CertFile: certFile, KeyFile: keyFile}
	if err := dialOK(t, cfg, addr); err != nil {
		t.Fatalf("mtls: %v", err)
	}

	// The client certificate is rotated to one the server does not trust:
	// the next connection must fail, which proves the files were re-read.
	if err := os.WriteFile(certFile, []byte(otherCert), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, []byte(otherKey), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := dialOK(t, cfg, addr); err == nil {
		t.Fatal("rotated client certificate was not picked up")
	}
	// And back.
	if err := os.WriteFile(certFile, []byte(clientCert), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, []byte(clientKey), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := dialOK(t, cfg, addr); err != nil {
		t.Fatalf("after rotating back: %v", err)
	}
	// A rotated CA file is picked up the same way.
	if err := os.WriteFile(caFile, []byte(other.pem), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := dialOK(t, cfg, addr); err == nil {
		t.Fatal("rotated ca was not picked up")
	}
}

func TestTLSInlinePEM(t *testing.T) {
	ca := newCA(t, "ca")
	addr := tlsServer(t, ca)
	clientCert, clientKey := ca.leaf(t, "client", true)

	// Everything from environment-style values, no files.
	if err := dialOK(t, TLS{Enabled: true, CA: ca.pem, Cert: clientCert, Key: clientKey}, addr); err != nil {
		t.Fatalf("inline mtls: %v", err)
	}
	// Without the client certificate the server refuses; without the CA the
	// client refuses.
	if err := dialOK(t, TLS{Enabled: true, CA: ca.pem}, addr); err == nil {
		t.Fatal("no client certificate: accepted")
	}
	if err := dialOK(t, TLS{Enabled: true, Cert: clientCert, Key: clientKey}, addr); err == nil {
		t.Fatal("no ca: accepted")
	}
	// Inline and file for the same item is a configuration error.
	if _, err := (TLS{Enabled: true, CA: ca.pem, CAFile: writeFile(t, "ca.crt", ca.pem)}).config(); err == nil {
		t.Fatal("ca inline and as a file: accepted")
	}
	if _, err := (TLS{Enabled: true, CA: "not pem"}).config(); err == nil {
		t.Fatal("garbage ca: accepted")
	}
}

func TestSASLUsernameFromFile(t *testing.T) {
	userFile := writeFile(t, "username", "alice\n")
	m, err := SASL{Mechanism: MechanismPlain, UsernameFile: userFile, Password: "p"}.mechanism()
	if err != nil {
		t.Fatal(err)
	}
	_, first, err := m.Authenticate(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if got := string(first); got != "\x00alice\x00p" {
		t.Fatalf("plain first message = %q", got)
	}
	if _, err := (SASL{Mechanism: MechanismPlain, Username: "u", UsernameFile: userFile, Password: "p"}).mechanism(); err == nil {
		t.Fatal("username inline and as a file: accepted")
	}
}
