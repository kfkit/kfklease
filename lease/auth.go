// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Auth is how the client authenticates to the brokers. The zero value is
// plaintext without authentication. Anything franz-go supports beyond this
// (AWS MSK IAM, custom mechanisms) goes through Config.ClientOpts.
type Auth struct {
	TLS  TLS
	SASL SASL
}

// TLS configures the connection. Enabled alone means TLS with the system
// roots; a CA adds a private CA; a client certificate and key add mTLS.
// Each of the three comes either inline as PEM (from an environment
// variable, say) or as a file. Files are re-read on every new connection,
// so a certificate renewed in a mounted Secret is picked up on the next
// reconnect without a restart; inline values are fixed for the life of the
// process.
type TLS struct {
	Enabled  bool
	CA       string // PEM
	CAFile   string
	Cert     string // PEM
	CertFile string
	Key      string // PEM
	KeyFile  string
	// ServerName overrides the name the broker's certificate is checked
	// against, for brokers advertised by IP.
	ServerName string
	// InsecureSkipVerify disables certificate verification. It is for
	// test stands, not for anything that matters.
	InsecureSkipVerify bool
}

// SASL configures the mechanism. Secrets can come from files, which are
// re-read on every authentication so that a rotated secret takes effect on
// the next reconnect without a restart.
type SASL struct {
	// Mechanism is one of "plain", "scram-sha-256", "scram-sha-512" or
	// "oauthbearer"; empty means no SASL.
	Mechanism string
	// Username or UsernameFile, for plain and scram.
	Username     string
	UsernameFile string
	// Password or PasswordFile, for plain and scram.
	Password     string
	PasswordFile string
	// Token or TokenFile, for oauthbearer.
	Token     string
	TokenFile string
}

// Mechanisms the SASL config understands, in the spelling Mechanism takes.
const (
	MechanismPlain       = "plain"
	MechanismScramSha256 = "scram-sha-256"
	MechanismScramSha512 = "scram-sha-512"
	MechanismOAuthBearer = "oauthbearer"
)

// clientOpts turns the auth config into client options. Everything is
// validated here, so that a bad file or PEM fails at startup; the secrets
// themselves are then read again on every connection or authentication.
func (a Auth) clientOpts() ([]kgo.Opt, error) {
	var opts []kgo.Opt
	if a.TLS.Enabled {
		if _, err := a.TLS.config(); err != nil {
			return nil, err
		}
		opts = append(opts, kgo.Dialer(a.TLS.dial))
	} else if a.TLS.given() {
		return nil, errors.New("lease: tls material given but tls is not enabled")
	}
	mech, err := a.SASL.mechanism()
	if err != nil {
		return nil, err
	}
	if mech != nil {
		opts = append(opts, kgo.SASL(mech))
	}
	return opts, nil
}

func (t TLS) given() bool {
	return t.CA != "" || t.CAFile != "" || t.Cert != "" || t.CertFile != "" || t.Key != "" || t.KeyFile != ""
}

// dial opens a TLS connection with a config built from the current
// contents of the files.
func (t TLS) dial(ctx context.Context, network, host string) (net.Conn, error) {
	cfg, err := t.config()
	if err != nil {
		return nil, err
	}
	d := tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}, Config: cfg}
	return d.DialContext(ctx, network, host)
}

// config reads the PEM material, inline or from files, into a tls.Config.
func (t TLS) config() (*tls.Config, error) {
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         t.ServerName,
		InsecureSkipVerify: t.InsecureSkipVerify, //nolint:gosec // opt-in, documented as unsafe
	}
	ca, err := pemSource(t.CA, t.CAFile, "ca")
	if err != nil {
		return nil, fmt.Errorf("lease: tls: %w", err)
	}
	if ca != nil {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ca) {
			return nil, errors.New("lease: tls ca: no certificates in the PEM")
		}
		cfg.RootCAs = pool
	}
	cert, err := pemSource(t.Cert, t.CertFile, "client certificate")
	if err != nil {
		return nil, fmt.Errorf("lease: tls: %w", err)
	}
	key, err := pemSource(t.Key, t.KeyFile, "client key")
	if err != nil {
		return nil, fmt.Errorf("lease: tls: %w", err)
	}
	switch {
	case cert != nil && key != nil:
		pair, err := tls.X509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("lease: tls client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	case cert != nil || key != nil:
		return nil, errors.New("lease: tls client certificate needs both cert and key")
	}
	return cfg, nil
}

// pemSource returns PEM given inline or as a file, or nil when neither.
func pemSource(value, file, what string) ([]byte, error) {
	switch {
	case value != "" && file != "":
		return nil, fmt.Errorf("%s given both inline and as a file", what)
	case value != "":
		return []byte(value), nil
	case file != "":
		b, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", what, err)
		}
		return b, nil
	default:
		return nil, nil
	}
}

func (s SASL) mechanism() (sasl.Mechanism, error) {
	mech := strings.ToLower(s.Mechanism)
	if mech == "" {
		if s.Username != "" || s.UsernameFile != "" || s.Password != "" || s.PasswordFile != "" || s.Token != "" || s.TokenFile != "" {
			return nil, errors.New("lease: sasl credentials given but no mechanism")
		}
		return nil, nil
	}
	switch mech {
	case MechanismPlain, MechanismScramSha256, MechanismScramSha512:
		username, err := secretSource(s.Username, s.UsernameFile, "username")
		if err != nil {
			return nil, fmt.Errorf("lease: sasl %s: %w", mech, err)
		}
		password, err := secretSource(s.Password, s.PasswordFile, "password")
		if err != nil {
			return nil, fmt.Errorf("lease: sasl %s: %w", mech, err)
		}
		credentials := func() (user, pass string, err error) {
			if user, err = username(); err != nil {
				return "", "", err
			}
			pass, err = password()
			return user, pass, err
		}
		switch mech {
		case MechanismPlain:
			return plain.Plain(func(context.Context) (plain.Auth, error) {
				u, p, err := credentials()
				return plain.Auth{User: u, Pass: p}, err
			}), nil
		case MechanismScramSha256:
			return scram.Sha256(scramAuth(credentials)), nil
		default:
			return scram.Sha512(scramAuth(credentials)), nil
		}
	case MechanismOAuthBearer:
		token, err := secretSource(s.Token, s.TokenFile, "token")
		if err != nil {
			return nil, fmt.Errorf("lease: sasl %s: %w", mech, err)
		}
		return oauth.Oauth(func(context.Context) (oauth.Auth, error) {
			t, err := token()
			return oauth.Auth{Token: t}, err
		}), nil
	default:
		return nil, fmt.Errorf("lease: unknown sasl mechanism %q", s.Mechanism)
	}
}

func scramAuth(credentials func() (string, string, error)) func(context.Context) (scram.Auth, error) {
	return func(context.Context) (scram.Auth, error) {
		u, p, err := credentials()
		return scram.Auth{User: u, Pass: p}, err
	}
}

// secretSource returns a getter for a secret given inline or as a file.
// The file is read on every call and trailing whitespace is dropped, since
// mounted secrets often end with a newline.
func secretSource(value, file, what string) (func() (string, error), error) {
	switch {
	case value != "" && file != "":
		return nil, fmt.Errorf("%s given both inline and as a file", what)
	case value != "":
		return func() (string, error) { return value, nil }, nil
	case file != "":
		if _, err := os.Stat(file); err != nil {
			return nil, fmt.Errorf("%s file: %w", what, err)
		}
		return func() (string, error) {
			b, err := os.ReadFile(file)
			if err != nil {
				return "", fmt.Errorf("lease: %s file: %w", what, err)
			}
			return strings.TrimRight(string(b), "\r\n"), nil
		}, nil
	default:
		return nil, fmt.Errorf("no %s", what)
	}
}
