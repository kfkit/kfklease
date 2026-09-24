// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"

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
// roots; CAFile adds a private CA; CertFile and KeyFile add a client
// certificate (mTLS).
type TLS struct {
	Enabled  bool
	CAFile   string
	CertFile string
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
	Username  string
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

// clientOpts turns the auth config into client options. Files named in the
// TLS config are read once, here; SASL secret files are read on every
// authentication.
func (a Auth) clientOpts() ([]kgo.Opt, error) {
	var opts []kgo.Opt
	if a.TLS.Enabled {
		cfg, err := a.TLS.config()
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(cfg))
	} else if a.TLS.CAFile != "" || a.TLS.CertFile != "" || a.TLS.KeyFile != "" {
		return nil, errors.New("lease: tls files given but tls is not enabled")
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

func (t TLS) config() (*tls.Config, error) {
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         t.ServerName,
		InsecureSkipVerify: t.InsecureSkipVerify, //nolint:gosec // opt-in, documented as unsafe
	}
	if t.CAFile != "" {
		pem, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("lease: tls ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("lease: tls ca: no certificates in %s", t.CAFile)
		}
		cfg.RootCAs = pool
	}
	switch {
	case t.CertFile != "" && t.KeyFile != "":
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("lease: tls client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	case t.CertFile != "" || t.KeyFile != "":
		return nil, errors.New("lease: tls client certificate needs both cert and key")
	}
	return cfg, nil
}

func (s SASL) mechanism() (sasl.Mechanism, error) {
	mech := strings.ToLower(s.Mechanism)
	if mech == "" {
		if s.Username != "" || s.Password != "" || s.PasswordFile != "" || s.Token != "" || s.TokenFile != "" {
			return nil, errors.New("lease: sasl credentials given but no mechanism")
		}
		return nil, nil
	}
	switch mech {
	case MechanismPlain, MechanismScramSha256, MechanismScramSha512:
		if s.Username == "" {
			return nil, fmt.Errorf("lease: sasl %s needs a username", mech)
		}
		password, err := secretSource(s.Password, s.PasswordFile, "password")
		if err != nil {
			return nil, fmt.Errorf("lease: sasl %s: %w", mech, err)
		}
		switch mech {
		case MechanismPlain:
			return plain.Plain(func(context.Context) (plain.Auth, error) {
				p, err := password()
				return plain.Auth{User: s.Username, Pass: p}, err
			}), nil
		case MechanismScramSha256:
			return scram.Sha256(s.scramAuth(password)), nil
		default:
			return scram.Sha512(s.scramAuth(password)), nil
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

func (s SASL) scramAuth(password func() (string, error)) func(context.Context) (scram.Auth, error) {
	return func(context.Context) (scram.Auth, error) {
		p, err := password()
		return scram.Auth{User: s.Username, Pass: p}, err
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
