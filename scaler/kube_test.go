// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package scaler

import (
	"context"
	"encoding/json"
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
)

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

func TestKubePauseAndUnpause(t *testing.T) {
	k, api := newFakeKube(t)
	ctx := context.Background()
	if err := k.Pause(ctx, "conn"); err != nil {
		t.Fatal(err)
	}
	if err := k.Unpause(ctx, "conn"); err != nil {
		t.Fatal(err)
	}
	want := []map[string]any{
		{"metadata": map[string]any{"annotations": map[string]any{PausedReplicasAnnotation: "0"}}},
		{"metadata": map[string]any{"annotations": map[string]any{PausedReplicasAnnotation: nil}}},
	}
	if len(api.patches) != len(want) {
		t.Fatalf("patches = %q", api.patches)
	}
	for i, p := range api.patches {
		var got map[string]any
		if err := json.Unmarshal([]byte(p), &got); err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(got) != fmt.Sprint(want[i]) {
			t.Fatalf("patch %d = %s, want %v", i, p, want[i])
		}
	}
}

// The workload is down only when its target is scaled to zero and no pod is
// left. Either alone is not that: replicas asked for with no pod is a
// rollout, a pod restart or a KEDA that has not acted on the pause yet, and
// the pod comes back; a pod with no replicas asked for is still running.
func TestKubeRemaining(t *testing.T) {
	k, api := newFakeKube(t)
	ctx := context.Background()
	for _, tt := range []struct {
		name           string
		replicas, pods int
		terminating    bool
		want           int
	}{
		{"running", 3, 3, false, 3},
		{"replicas asked for, no pod", 1, 0, false, 1},
		{"scaled to zero, a pod terminating", 0, 1, true, 1},
		{"scaled to zero, a pod running", 0, 1, false, 1},
		{"scaled down, more pods terminating", 1, 3, true, 3},
		{"scaled to zero, no pod: down", 0, 0, false, 0},
	} {
		api.replicas, api.pods, api.terminating = tt.replicas, tt.pods, tt.terminating
		if n, err := k.Remaining(ctx, "conn"); err != nil || n != tt.want {
			t.Errorf("%s: remaining = %d, %v; want %d", tt.name, n, err, tt.want)
		}
	}

	// A target in the core group, a custom resource elsewhere: the paths
	// follow KEDA's status.
	api.gvkr = `{"group":"","version":"v1","kind":"ReplicationController","resource":"replicationcontrollers"}`
	api.scaleAt = "/api/v1/namespaces/ns/replicationcontrollers/conn/scale"
	if _, err := k.Remaining(ctx, "conn"); err != nil {
		t.Fatal(err)
	}
	api.gvkr = `{"group":"kafka.strimzi.io","version":"v1beta2","kind":"KafkaConnect","resource":"kafkaconnects"}`
	api.scaleAt = "/apis/kafka.strimzi.io/v1beta2/namespaces/ns/kafkaconnects/conn/scale"
	if _, err := k.Remaining(ctx, "conn"); err != nil {
		t.Fatal(err)
	}

	// No count without a target KEDA has resolved, nor without a selector:
	// an empty one would count every pod of the namespace.
	api.gvkr = ""
	if _, err := k.Remaining(ctx, "conn"); err == nil || !strings.Contains(err.Error(), "no scale target") {
		t.Fatalf("err = %v, want no scale target", err)
	}
	api.gvkr = `{"group":"apps","version":"v1","kind":"Deployment","resource":"deployments"}`
	api.scaleAt = "/apis/apps/v1/namespaces/ns/deployments/conn/scale"
	api.sel = ""
	if _, err := k.Remaining(ctx, "conn"); err == nil || !strings.Contains(err.Error(), "no selector") {
		t.Fatalf("err = %v, want no selector", err)
	}
}

// The token is read for every request, and an API error says what the
// server said.
func TestKubeTokenAndErrors(t *testing.T) {
	k, api := newFakeKube(t)
	ctx := context.Background()
	if err := k.Pause(ctx, "conn"); err != nil {
		t.Fatal(err)
	}
	api.token = "two"
	err := k.Pause(ctx, "conn")
	if err == nil || !strings.Contains(err.Error(), "Unauthorized") {
		t.Fatalf("err = %v, want Unauthorized", err)
	}
	if err := os.WriteFile(k.TokenFile, []byte("two"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := k.Pause(ctx, "conn"); err != nil {
		t.Fatal(err)
	}
	if err := k.Pause(ctx, "other"); err == nil || !strings.Contains(err.Error(), "forbidden") {
		t.Fatalf("err = %v, want forbidden", err)
	}
	if got := api.tokens[len(api.tokens)-2]; got != "Bearer two" {
		t.Fatalf("token sent = %q", got)
	}
}
