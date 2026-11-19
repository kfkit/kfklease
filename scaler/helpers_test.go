// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package scaler

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kfkit/kfklease/lease"
)

// What the tests of this package share: the wait, the lease participant
// they stand a scaler on and the Kubernetes API they stand a handover on.
// A helper that one file uses stays in that file.

// eventually polls until cond holds, for two seconds.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// fakeSource is a lease participant that reports what the test set.
type fakeSource struct {
	mu      sync.Mutex
	st      lease.Status
	changed chan struct{}
	// unknown makes the source a participant that has not read the log
	// yet; otherwise Status reports Ready.
	unknown bool
}

// newFakeSource is a participant that is ready and holds nothing.
func newFakeSource() *fakeSource {
	return &fakeSource{changed: make(chan struct{}, 1)}
}

func (f *fakeSource) Status() lease.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.st
	s.Ready = s.Ready || !f.unknown
	return s
}

func (f *fakeSource) setUnknown(unknown bool) {
	f.mu.Lock()
	f.unknown = unknown
	f.mu.Unlock()
	select {
	case f.changed <- struct{}{}:
	default:
	}
}

func (f *fakeSource) Changed() <-chan struct{} { return f.changed }

// set makes the source the holder, or no participant of its site.
func (f *fakeSource) set(holding bool) {
	f.setStatus(lease.Status{Holding: holding, Epoch: 7, SiteHolding: holding, SiteEpoch: 7})
}

// setSibling makes another participant of the source's site the holder,
// or nobody.
func (f *fakeSource) setSibling(holding bool) {
	f.setStatus(lease.Status{SiteHolding: holding, SiteEpoch: 9})
}

func (f *fakeSource) setStatus(s lease.Status) {
	f.mu.Lock()
	f.st = s
	f.mu.Unlock()
	select {
	case f.changed <- struct{}{}:
	default:
	}
}

// fakeAPI is the part of the Kubernetes API the handover uses, for one
// ScaledObject "conn" in namespace "ns".
type fakeAPI struct {
	mu      sync.Mutex
	token   string
	gvkr    string // the scaleTargetGVKR status, as JSON; empty for none
	scaleAt string // the path of the target's scale subresource
	sel     string
	// replicas is the scale's spec.replicas; pods are the pods the selector
	// matches, all of them terminating if terminating is set.
	replicas    int
	pods        int
	terminating bool
	patches     []string
	tokens      []string
}

func (f *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tokens = append(f.tokens, r.Header.Get("Authorization"))
	if r.Header.Get("Authorization") != "Bearer "+f.token {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"kind":"Status","message":"Unauthorized"}`)
		return
	}
	const so = "/apis/keda.sh/v1alpha1/namespaces/ns/scaledobjects/conn"
	switch {
	case r.Method == http.MethodPatch && r.URL.Path == so:
		if ct := r.Header.Get("Content-Type"); ct != "application/merge-patch+json" {
			http.Error(w, "content type "+ct, http.StatusUnsupportedMediaType)
			return
		}
		b, _ := io.ReadAll(r.Body)
		f.patches = append(f.patches, string(b))
		fmt.Fprint(w, `{}`)
	case r.Method == http.MethodGet && r.URL.Path == so:
		status := "{}"
		if f.gvkr != "" {
			status = `{"scaleTargetGVKR":` + f.gvkr + `}`
		}
		fmt.Fprintf(w, `{"spec":{"scaleTargetRef":{"name":"conn"}},"status":%s}`, status)
	case r.Method == http.MethodGet && r.URL.Path == f.scaleAt:
		// As the API server writes a Scale: spec.replicas is left out at
		// zero.
		spec := "{}"
		if f.replicas != 0 {
			spec = fmt.Sprintf(`{"replicas":%d}`, f.replicas)
		}
		fmt.Fprintf(w, `{"kind":"Scale","spec":%s,"status":{"replicas":%d,"selector":%q}}`, spec, f.pods, f.sel)
	case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/ns/pods" && r.URL.Query().Get("labelSelector") == f.sel:
		pod := `{"metadata":{"name":"p"}}`
		if f.terminating {
			pod = `{"metadata":{"name":"p","deletionTimestamp":"2026-11-19T10:00:00Z"}}`
		}
		items := slices.Repeat([]string{pod}, f.pods)
		fmt.Fprintf(w, `{"items":[%s]}`, strings.Join(items, ","))
	default:
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprintf(w, `{"kind":"Status","message":"%s %s is forbidden"}`, r.Method, r.URL.String())
	}
}

// setScale changes the workload while a handover looks at it.
func (f *fakeAPI) setScale(replicas, pods int) {
	f.mu.Lock()
	f.replicas, f.pods = replicas, pods
	f.mu.Unlock()
}

// requests is how many requests the API has answered.
func (f *fakeAPI) requests() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.tokens)
}

func newFakeKube(t *testing.T) (*Kube, *fakeAPI) {
	t.Helper()
	api := &fakeAPI{
		token:   "one",
		gvkr:    `{"group":"apps","version":"v1","kind":"Deployment","resource":"deployments"}`,
		scaleAt: "/apis/apps/v1/namespaces/ns/deployments/conn/scale",
		sel:     "app=conn,tier in (a,b)",
		// A workload that runs.
		replicas: 3,
		pods:     3,
	}
	srv := httptest.NewServer(api)
	t.Cleanup(srv.Close)
	tok := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tok, []byte("one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return &Kube{Host: srv.URL, Namespace: "ns", TokenFile: tok, Client: srv.Client()}, api
}
