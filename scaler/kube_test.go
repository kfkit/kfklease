// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package scaler

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

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
