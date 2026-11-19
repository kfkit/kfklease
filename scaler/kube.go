// Copyright 2026 Ivan Abramov
// SPDX-License-Identifier: Apache-2.0

package scaler

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// PausedReplicasAnnotation makes KEDA scale a ScaledObject's target to the
// given count at once and stop scaling it until the annotation is removed.
const PausedReplicasAnnotation = "autoscaling.keda.sh/paused-replicas"

// Kube is the scaler's access to the Kubernetes API for the handover: the
// ScaledObjects of its namespace, their targets' scale subresource, and the
// pods. It is a few REST calls, made with the pod's service account; the
// chart's Role grants exactly these.
type Kube struct {
	// Host is the API server's base URL, https://host:port.
	Host      string
	Namespace string
	// TokenFile holds the service account token, re-read on every request:
	// projected tokens rotate.
	TokenFile string
	Client    *http.Client
}

const serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// InClusterKube returns the client of a pod's own service account and
// namespace.
func InClusterKube() (*Kube, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, errors.New("kube: not in a cluster: KUBERNETES_SERVICE_HOST or KUBERNETES_SERVICE_PORT unset")
	}
	ns, err := os.ReadFile(serviceAccountDir + "/namespace")
	if err != nil {
		return nil, fmt.Errorf("kube: namespace: %w", err)
	}
	ca, err := os.ReadFile(serviceAccountDir + "/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("kube: ca: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("kube: ca.crt holds no certificate")
	}
	return &Kube{
		Host:      "https://" + net.JoinHostPort(host, port),
		Namespace: strings.TrimSpace(string(ns)),
		TokenFile: serviceAccountDir + "/token",
		Client: &http.Client{
			Timeout:   10 * time.Second,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}},
		},
	}, nil
}

// Pause sets the paused-replicas annotation of ScaledObject so to "0".
func (k *Kube) Pause(ctx context.Context, so string) error {
	return k.annotate(ctx, so, "0")
}

// Unpause removes the paused-replicas annotation of ScaledObject so; it
// does nothing if there is none.
func (k *Kube) Unpause(ctx context.Context, so string) error {
	return k.annotate(ctx, so, nil)
}

func (k *Kube) annotate(ctx context.Context, so string, value any) error {
	patch := map[string]any{"metadata": map[string]any{"annotations": map[string]any{PausedReplicasAnnotation: value}}}
	body, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	return k.do(ctx, http.MethodPatch, k.scaledObjectPath(so), body, nil)
}

func (k *Kube) scaledObjectPath(so string) string {
	return fmt.Sprintf("/apis/keda.sh/v1alpha1/namespaces/%s/scaledobjects/%s", url.PathEscape(k.Namespace), url.PathEscape(so))
}

// Remaining is how much of ScaledObject so's target is left: the larger of
// the replicas its scale subresource asks for (spec.replicas) and the count
// of the pods that subresource selects, terminating ones included. It is 0
// only when the target is scaled to zero and no pod is left. No pod alone
// is not that: while replicas are asked for, the target's controller
// creates one, as in the middle of a rollout or a pod restart, or before
// KEDA has acted on a pause. The target's resource comes from the status
// KEDA writes on the ScaledObject.
func (k *Kube) Remaining(ctx context.Context, so string) (int, error) {
	var obj struct {
		Spec struct {
			ScaleTargetRef struct {
				Name string `json:"name"`
			} `json:"scaleTargetRef"`
		} `json:"spec"`
		Status struct {
			GVKR *struct {
				Group    string `json:"group"`
				Version  string `json:"version"`
				Resource string `json:"resource"`
			} `json:"scaleTargetGVKR"`
		} `json:"status"`
	}
	if err := k.do(ctx, http.MethodGet, k.scaledObjectPath(so), nil, &obj); err != nil {
		return 0, err
	}
	gvkr, name := obj.Status.GVKR, obj.Spec.ScaleTargetRef.Name
	if gvkr == nil || gvkr.Resource == "" || name == "" {
		return 0, fmt.Errorf("kube: scaledobject %s: no scale target in its status yet", so)
	}
	base := "/apis/" + gvkr.Group + "/" + gvkr.Version
	if gvkr.Group == "" {
		base = "/api/" + gvkr.Version
	}
	var scale struct {
		Spec struct {
			// Left out by the API server when it is zero.
			Replicas int `json:"replicas"`
		} `json:"spec"`
		Status struct {
			Selector string `json:"selector"`
		} `json:"status"`
	}
	scalePath := fmt.Sprintf("%s/namespaces/%s/%s/%s/scale", base, url.PathEscape(k.Namespace), gvkr.Resource, url.PathEscape(name))
	if err := k.do(ctx, http.MethodGet, scalePath, nil, &scale); err != nil {
		return 0, err
	}
	if scale.Status.Selector == "" {
		// An empty selector would select every pod of the namespace.
		return 0, fmt.Errorf("kube: %s %s: the scale subresource has no selector", gvkr.Resource, name)
	}
	var pods struct {
		Items []json.RawMessage `json:"items"`
	}
	podsPath := fmt.Sprintf("/api/v1/namespaces/%s/pods?labelSelector=%s", url.PathEscape(k.Namespace), url.QueryEscape(scale.Status.Selector))
	if err := k.do(ctx, http.MethodGet, podsPath, nil, &pods); err != nil {
		return 0, err
	}
	return max(scale.Spec.Replicas, len(pods.Items)), nil
}

// do sends one request; a body is a JSON merge patch.
func (k *Kube) do(ctx context.Context, method, path string, body []byte, out any) error {
	token, err := os.ReadFile(k.TokenFile)
	if err != nil {
		return fmt.Errorf("kube: token: %w", err)
	}
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, k.Host+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/merge-patch+json")
	}
	resp, err := k.Client.Do(req)
	if err != nil {
		return fmt.Errorf("kube: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return fmt.Errorf("kube: %s %s: %w", method, path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		var st struct {
			Message string `json:"message"`
		}
		_ = json.Unmarshal(b, &st)
		return fmt.Errorf("kube: %s %s: %s: %s", method, path, resp.Status, st.Message)
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("kube: %s %s: %w", method, path, err)
	}
	return nil
}
