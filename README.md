# kfklease

Distributed leases and leader election on top of Apache Kafka®, with a
[KEDA](https://keda.sh) metric for lease-driven failover.

> Status: v0.2: the protocol, the client, the KEDA scaler and broker
> authentication work and are tested on a two-cluster stand, but nothing has
> run in production yet.

## Why

Some services must run in exactly one place at a time: a single cloud, a single
region, a single cluster. When that place goes away, something has to decide
who takes over, and the workload has to start there.

`kfklease` uses a Kafka cluster that already spans those places as the
coordination layer:

- a **lease** is held by one owner at a time and has to be renewed;
- when the owner stops renewing, another candidate acquires the lease;
- the current lease state is exposed as a **metric for KEDA**, so a standby
  deployment scales from zero when it becomes the owner and back to zero when
  it loses the lease.

Failover through KEDA is the first use case, not the only one: the lease is a
general building block for leader election between Kafka clients.

## Design

The lease is a deterministic fold over a Kafka log: writing a record decides
nothing, and every reader reaches the same verdict on it. The rules, and why
they hold up under delays, freezes and a compacted log, are in
[docs/protocol.md](docs/protocol.md). The state machine lives in
[`lease/`](lease) and has no dependencies; `lease.Candidate` wraps it in a
Kafka client built on [franz-go](https://github.com/twmb/franz-go).

## Usage

```go
c, err := lease.NewCandidate(lease.Config{
	Brokers:     []string{"kafka:9092"},
	Topic:       "kfklease-demo",
	CreateTopic: true,
	TTL:         10 * time.Second,
})
if err != nil {
	log.Fatal(err)
}
go c.Run(ctx) // blocks until ctx is done; releases the lease on the way out

for range c.Changed() {
	if s := c.Status(); s.Holding {
		// Act as the holder. s.Epoch is the fencing token: pass it downstream
		// and let downstream reject anything with a smaller epoch.
	}
}
```

### KEDA

`kfklease-scaler` runs one participant per cluster and serves it to KEDA as an
[external scaler](https://keda.sh/docs/latest/scalers/external-push/). A
ScaledObject with `maxReplicaCount: 1` then runs the workload where the lease
is held and nowhere else:

```yaml
triggers:
  - type: external-push
    metadata:
      scalerAddress: kfklease-scaler.kfklease.svc:9090
```

The chart installs the scaler, its service and that ScaledObject; one
release per cluster. The scaler address in the ScaledObject follows the
release name and namespace, so the same command works in every cluster with
a different `holderPrefix`:

```bash
helm install kfklease oci://ghcr.io/kfkit/charts/kfklease --version 0.2.1 \
  -n kfklease --create-namespace \
  --set brokers=kafka:9092 --set topic=my-lease --set holderPrefix=eu-west \
  --set scaledObject.target=my-singleton
```

The chart pulls `ghcr.io/kfkit/kfklease-scaler` at the same version:
linux/amd64 and linux/arm64, signed with cosign. The digest and the verify
command are in the [release notes](https://github.com/kfkit/kfklease/releases).
The chart's values are documented in
[charts/kfklease/values.yaml](charts/kfklease/values.yaml).

### Authentication

Brokers that need authentication are configured under `auth`. The chart
holds no secret itself; it points at Kubernetes Secrets, whoever creates
them (cert-manager, Strimzi, External Secrets, `kubectl`):

- **TLS / mTLS**: certificates are mounted from the Secret as files and
  re-read on every connection, so a renewed certificate is picked up on the
  next reconnect without a restart. The defaults fit the `kubernetes.io/tls`
  layout (`ca.crt`, `tls.crt`, `tls.key`); a Strimzi `KafkaUser` uses
  `user.crt` and `user.key`.
- **SASL** (PLAIN, SCRAM-SHA-256/512, OAUTHBEARER): the password or token,
  and optionally the username, come in as environment variables straight
  from the Secret. A pod reads its environment once, so pair it with a
  restart on change, for example stakater/reloader through `podAnnotations`.

```bash
  --set auth.tls.enabled=true --set auth.tls.secretName=kafka-client-tls --set auth.tls.clientCert=true \
  --set auth.sasl.mechanism=scram-sha-512 --set auth.sasl.secretName=kfklease-sasl --set auth.sasl.usernameKey=username
```

The same settings are flags and `KFKLEASE_TLS_*` / `KFKLEASE_SASL_*`
variables on the binary, and `lease.Config.Auth` in the library; anything
else franz-go supports (AWS MSK IAM, custom mechanisms) goes through
`lease.Config.ClientOpts`.

### Metrics and status

The scaler serves HTTP on `:9091` (`service.httpPort`):

- `/metrics`, Prometheus: `kfklease_holding`, `kfklease_epoch`,
  `kfklease_belief_remaining_seconds`, `kfklease_certain`,
  `kfklease_lease_held`, `kfklease_lease_as_of_seconds` and
  `kfklease_transitions_total{to="holding"|"standby"}`. The pod carries
  `prometheus.io/*` annotations; `metrics.serviceMonitor.enabled` adds a
  ServiceMonitor for the Prometheus Operator.
- `/status`, JSON: the same, plus the log's view of the lease. A workload
  that fences by epoch reads its epoch here.
- `/healthz`.

The binary is configured by flags or environment: `KFKLEASE_BROKERS`,
`KFKLEASE_TOPIC`, `KFKLEASE_TTL`, `KFKLEASE_HOLDER` (unique per process; the
chart uses the pod name). The two-cluster stand the chart is tested on is in
[test/e2e](test/e2e).

KEDA gives failover, not mutual exclusion: the old pod is still terminating
while the new one starts, a `cooldownPeriod` above zero stretches that, and a
cluster coming back from a crash restarts its stale pod until its own KEDA
is up again and scales it down. Workloads that must not overlap check the
epoch downstream.

`Status().Holding` is a belief with a deadline, not a fact: a holder that
cannot reach Kafka stops believing after one TTL minus a margin, and the next
holder is elected only after the term runs out in the log, so the two never
overlap as long as the margin covers clock skew.

## Development

```bash
make test          # unit tests, no broker needed
make integration   # starts the Kafka container from test/e2e and runs the client tests
make image         # builds the scaler image
make generate      # regenerates the KEDA gRPC stubs from proto/ (buf via go run)
```

The end-to-end stand with two Kubernetes clusters is described in
[test/e2e](test/e2e).

## Roadmap

- [x] Lease protocol on a compacted topic: acquire, renew, release, fencing
- [x] Go library
- [x] KEDA external scaler
- [x] Container image
- [x] Helm chart
- [x] Failure-mode tests: crash, partition, freeze, broker restart
- [x] Failure-mode tests: clock skew and drift, in the simulation
- [x] Image and chart on GHCR, cosign-signed

## License

Copyright 2026 Ivan Abramov.

Licensed under the [Apache License, Version 2.0](LICENSE).

Apache Kafka and Kafka are either registered trademarks or trademarks of The
Apache Software Foundation in the United States and other countries. This
project is not affiliated with, endorsed by, or sponsored by The Apache
Software Foundation.
