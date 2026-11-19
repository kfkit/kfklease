// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// envTestName is set and unset by the tests of the environment helpers; no
// flag reads it.
const envTestName = "KFKLEASE_TEST_VALUE"

// look is lookupEnv of envTestName with the type erased, so that one table
// holds every kind of variable.
func look[T any](def T, parse func(string) (T, error)) func() (any, error) {
	return func() (any, error) { return lookupEnv(envTestName, def, parse) }
}

// errAny stands for an error the parser does not export.
var errAny = errors.New("any error")

func TestLookupEnv(t *testing.T) {
	set := func(v string) *string { return &v }
	tests := []struct {
		name  string
		value *string // nil: the variable is unset
		look  func() (any, error)
		want  any
		err   error // what a value that does not parse fails with
	}{
		// A boolean variable takes what its flag takes, and nothing else.
		{"bool true", set("true"), look(false, strconv.ParseBool), true, nil},
		{"bool TRUE", set("TRUE"), look(false, strconv.ParseBool), true, nil},
		{"bool 1", set("1"), look(false, strconv.ParseBool), true, nil},
		{"bool false", set("false"), look(true, strconv.ParseBool), false, nil},
		{"bool 0", set("0"), look(true, strconv.ParseBool), false, nil},
		{"bool unset, default true", nil, look(true, strconv.ParseBool), true, nil},
		{"bool unset, default false", nil, look(false, strconv.ParseBool), false, nil},
		{"bool yes", set("yes"), look(false, strconv.ParseBool), nil, strconv.ErrSyntax},
		{"bool empty", set(""), look(true, strconv.ParseBool), nil, strconv.ErrSyntax},

		{"duration", set("1m30s"), look(time.Second, time.ParseDuration), 90 * time.Second, nil},
		{"duration unset", nil, look(time.Second, time.ParseDuration), time.Second, nil},
		{"duration without a unit", set("10"), look(time.Second, time.ParseDuration), nil, errAny},

		{"int", set("3"), look(1, strconv.Atoi), 3, nil},
		{"int unset", nil, look(1, strconv.Atoi), 1, nil},
		{"int in words", set("three"), look(1, strconv.Atoi), nil, strconv.ErrSyntax},

		// A string is taken as it is: set and empty is not the default.
		{"string", set("a,b"), look("def", text), "a,b", nil},
		{"string empty", set(""), look("def", text), "", nil},
		{"string unset", nil, look("def", text), "def", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(envTestName, "") // restores the variable after the test
			if tt.value == nil {
				if err := os.Unsetenv(envTestName); err != nil {
					t.Fatal(err)
				}
			} else {
				t.Setenv(envTestName, *tt.value)
			}
			got, err := tt.look()
			if tt.err == nil {
				if err != nil || got != tt.want {
					t.Fatalf("got %v, %v; want %v", got, err, tt.want)
				}
				return
			}
			if err == nil {
				t.Fatalf("got %v and no error for a value that does not parse", got)
			}
			if tt.err != errAny && !errors.Is(err, tt.err) {
				t.Fatalf("err = %v, want %v", err, tt.err)
			}
			// The message names the variable first: NAME: <error>.
			if !strings.HasPrefix(err.Error(), envTestName+": ") {
				t.Fatalf("err = %q, want it to start with the variable's name", err)
			}
		})
	}
}

// Every boolean variable of the binary is strict: a value its flag would not
// take stops the binary with status 2 and one line that names the variable,
// before anything else happens. The test runs itself as that binary, with
// no other KFKLEASE_* variable set: run then gets no further than the
// missing brokers, status 1, with nothing dialled and nothing listened on.
func TestBooleanVariablesAreStrict(t *testing.T) {
	const child = "KFKLEASE_TEST_RUN_CHILD"
	if os.Getenv(child) != "" {
		fmt.Fprintln(os.Stderr, run(nil))
		os.Exit(1)
	}
	env := slices.DeleteFunc(os.Environ(), func(kv string) bool { return strings.HasPrefix(kv, "KFKLEASE_") })
	env = append(env, child+"=1")
	start := func(t *testing.T, extra ...string) (int, string) {
		t.Helper()
		cmd := exec.Command(os.Args[0], "-test.run=^TestBooleanVariablesAreStrict$")
		cmd.Env = append(slices.Clone(env), extra...)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		var exit *exec.ExitError
		if err := cmd.Run(); err != nil && !errors.As(err, &exit) {
			t.Fatal(err)
		}
		return cmd.ProcessState.ExitCode(), stderr.String()
	}
	for _, name := range []string{
		"KFKLEASE_TLS", "KFKLEASE_TLS_INSECURE_SKIP_VERIFY", "KFKLEASE_STANDBY",
		"KFKLEASE_CREATE_TOPIC", "KFKLEASE_FLUSH_EVERY_RECORD",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			code, out := start(t, name+"=yes")
			want := name + `: strconv.ParseBool: parsing "yes": invalid syntax` + "\n"
			if code != 2 || out != want {
				t.Fatalf("exit %d, stderr %q; want 2 and %q", code, out, want)
			}
			// What the flag takes gets past the flags.
			if code, out := start(t, name+"=TRUE"); code != 1 || !strings.Contains(out, "brokers and topic are required") {
				t.Fatalf("exit %d, stderr %q for TRUE; want 1 and the missing brokers", code, out)
			}
		})
	}
}
