# kfklease

Distributed leases and leader election on top of Apache Kafka®, with a
[KEDA](https://keda.sh) metric for lease-driven failover.

> Status: v0.3: the protocol, the client, the KEDA scaler, broker
> authentication and metrics work and are tested on a two-cluster stand
> down to the workload's own writes, but nothing has run in production yet.

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

The operator's guide, from a Kafka cluster to a workload that fails over,
is [docs/install.md](docs/install.md). In short:

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

`CreateTopic` creates a missing topic as a lease log: compacted,
`LogAppendTime`, three replicas with `min.insync.replicas=2` and unclean
leader election off (`Config.TopicSettings`; the chart's
`replicationFactor`, `minInsyncReplicas` and `flushEveryRecord`). A Kafka
cluster with fewer than three brokers needs `ReplicationFactor` set to its
size. An existing topic is checked and never altered; see
[The lease topic](docs/install.md#the-lease-topic).

### KEDA

`kfklease-scaler` runs one participant per cluster and serves it to KEDA as an
[external scaler](https://keda.sh/docs/latest/scalers/external-push/). A
ScaledObject with `idleReplicaCount: 0` and `minReplicaCount` equal to
`maxReplicaCount` then runs the workload where the lease is held and nowhere
else, at one replica or at N:

```yaml
triggers:
  - type: external-push
    metadata:
      scalerAddress: kfklease-scaler.kfklease.svc:9090
```

The chart installs the scaler, its service and that ScaledObject; one
release per cluster. One scaler can serve several leases, one per topic,
each with its own workload (`extraLeases`); a ScaledObject names its lease
with `topic` in the trigger metadata. The scaler address in the ScaledObject follows the
release name and namespace, so the same command works in every cluster with
a different `site`:

```bash
helm install kfklease oci://ghcr.io/kfkit/charts/kfklease --version 0.3.0 \
  -n kfklease --create-namespace \
  --set brokers=kafka:9092 --set topic=my-lease --set site=eu-west \
  --set scaledObject.targetRef.name=my-singleton
```

The lease belongs to a site, not to a scaler pod. A restarted scaler (an
upgrade, a node drain, an OOM kill) claims for its site at once and the
workload stays where it is: KEDA keeps it through `cooldownPeriod` (60 s)
while the new pod reads the log, and another site waits `takeoverDelay`
(by default `pollingInterval + cooldownPeriod + takeoverMargin`, 95 s)
after the end of a foreign term before it claims, so the old site's
workload is down by then. A Kafka outage ends with the lease where it was.
Each cluster runs two scaler pods by default (`replicaCount`): one holds
the term, every one answers KEDA for the site, and losing one, or an
upgrade to an image that does not pull, moves nothing.

Upgrading from 0.3.0: `holderPrefix` is now `site`, which is required
(`--set site=eu-west`); `scaledObject.target` is now
`scaledObject.targetRef.name` (`--set scaledObject.targetRef.name=my-singleton`);
the old values fail the render. `cooldownPeriod` is now 60 s and a
failover waits `takeoverDelay`. Two scaler pods run per cluster
(`replicaCount: 2`, `RollingUpdate`, a PodDisruptionBudget), and KEDA is
told whether the site holds the lease; a workload that reads `/status`
fences on `site_holding` and `site_epoch`. See [CHANGELOG.md](CHANGELOG.md).

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
  `kfklease_lease_held`, `kfklease_lease_as_of_seconds`,
  `kfklease_lease_holder_info{holder,site}` (who holds it, as the log says),
  `kfklease_health_cooldown_seconds`, `kfklease_standby_mode` and
  `kfklease_transitions_total{to="holding"|"standby"}`. The pod carries
  `prometheus.io/*` annotations; `metrics.serviceMonitor.enabled` adds a
  ServiceMonitor for the Prometheus Operator.
- `/status`, JSON: the same, plus the log's view of the lease and `ready`
  (false until a new scaler has read enough of the log to know who holds
  the lease; until then KEDA gets an error, not "not holding"). A workload
  that fences by epoch reads its epoch here.
- `/healthz`.

The binary is configured by flags or environment: `KFKLEASE_BROKERS`,
`KFKLEASE_TOPIC`, `KFKLEASE_TTL`, `KFKLEASE_SITE`, `KFKLEASE_HOLDER`
(`<site>/<name>`, unique per process; the chart uses the pod name),
`KFKLEASE_TAKEOVER_DELAY`, `KFKLEASE_STANDBY`. The two-cluster stand the chart is tested on is in
[test/e2e](test/e2e).

KEDA gives failover, not mutual exclusion: the old pod may still be
terminating when the new one starts if `takeoverDelay` does not cover its
stop, and a
cluster coming back from a crash restarts its stale pod until its own KEDA
is up again and scales it down. Workloads that must not overlap check the
epoch downstream. If the scaler alone is gone, KEDA's `fallback`, which
the chart renders with `replicas: 0`, scales the workload to zero instead
of leaving it running; see
[docs/install.md](docs/install.md#when-the-scaler-is-gone).

The lease follows the scaler, not the workload. To fail over when the
workload cannot run where the lease is held, set `healthGate.url` (the
Kafka Connect REST API, say): a holder whose workload has not passed a
probe for `healthGate.failAfter` releases the lease and sits out
`healthGate.cooldown`; see
[docs/install.md](docs/install.md#when-the-workload-cannot-run-here).

To take a cluster out, or move the workload on purpose, set `standby: true`
on its release: its scaler never claims, a holder releases, and another
cluster takes over. With the handover (`handover.enabled`, on by default)
the scaler pauses its ScaledObject at zero, waits until the workload's pods
are gone and writes a `yield` to the lease topic, and the other cluster
takes over about a TTL after the release instead of after its takeover
delay. For that the scaler pods run under a ServiceAccount of the release
with a namespaced Role (get and patch on the release's ScaledObjects, get
on their targets' `scale`, list pods); pull secrets go in
`imagePullSecrets`, not on the `default` ServiceAccount. See
[docs/install.md](docs/install.md#the-handover). `kfklease-scaler status`
prints who holds each lease, read from the topic, from anywhere that
reaches Kafka (`kubectl exec deploy/kfklease -- /app status` in a scaler's
pod); see
[docs/install.md](docs/install.md#standby-and-the-status-command).

### Fencing by epoch

[examples/heartbeat](examples/heartbeat) is a workload that does it: before
every write it asks the scaler's `/status` and writes only while
`site_holding` is true, tagging the record with `site_epoch`; its `report` mode is the
consumer side, which rejects a record whose epoch is below one it has
already seen. The e2e stand runs it as the workload and, after every
scenario, checks by the broker's clock that no two epochs ever wrote at
once and that nothing stale got through.

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
- [x] N replicas of any scalable target; fail closed when the scaler is gone
- [x] Optional health gate: fail over when the workload cannot run where the lease is held
- [x] Several leases, one per topic, served by one scaler
- [x] The lease belongs to a site: a scaler restart or a Kafka outage leaves the workload in place
- [x] Standby mode and a `status` command
- [x] Scaler replicas per site
- [x] A handover that skips the takeover delay
- [x] Failure-mode tests: clock skew and drift, in the simulation
- [x] Example workload fencing by epoch; workload-level exclusion check on the stand
- [x] Image and chart on GHCR, cosign-signed

## License

Copyright 2026 Ivan Abramov.

Licensed under the [Apache License, Version 2.0](LICENSE).

Apache Kafka and Kafka are either registered trademarks or trademarks of The
Apache Software Foundation in the United States and other countries. This
project is not affiliated with, endorsed by, or sponsored by The Apache
Software Foundation.
