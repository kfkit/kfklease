# Installing and using kfklease

This is the operator's guide: from a Kafka cluster and two Kubernetes
clusters to a workload that runs in exactly one of them and fails over. For
what the lease is and why it is safe, read [protocol.md](protocol.md).

## What you get

One `kfklease-scaler` per cluster takes part in a lease held on a Kafka
topic and serves the result to KEDA as an external scaler. A ScaledObject
keeps your workload at one replica in the cluster that holds the lease and
at zero everywhere else. When that cluster dies, freezes or loses Kafka, its
term runs out, another cluster takes the lease, and KEDA starts the
workload there.

```
cluster A                              cluster B
┌────────────────────────┐             ┌────────────────────────┐
│ kfklease-scaler ──────┐│             │┌────── kfklease-scaler │
│   holds the lease     ││   Kafka     ││   standby             │
│ KEDA ──► workload ×1  ││◄──topic──►││   KEDA ──► workload ×0 │
└────────────────────────┘             └────────────────────────┘
```

Failover, not mutual exclusion: the old pod is still terminating while the
new one starts, and a cluster coming back from a crash restarts its stale
pod until its own KEDA scales it down. A workload whose correctness needs a
single writer fences by epoch; see [Fencing](#fencing-by-epoch).

## Prerequisites

- A Kafka cluster (2.7 or later for `LogAppendTime` on a compacted topic;
  any KRaft release is fine) reachable from every cluster that takes part.
  It is the arbiter, so it must outlive any one of them: put it where the
  clusters are not.
- [KEDA](https://keda.sh) 2.x in every cluster.
- Helm 3.8 or later (OCI registries).
- A topic, or the right to create one: `createTopic: true` (the default)
  creates it with the settings the protocol needs.

## Install

One Helm release per cluster, the same lease everywhere, a different
`holderPrefix` in each cluster:

```bash
helm install kfklease oci://ghcr.io/kfkit/charts/kfklease --version 0.3.0 \
  -n kfklease --create-namespace \
  --set brokers=kafka-1:9092,kafka-2:9092 \
  --set topic=orders-singleton \
  --set holderPrefix=eu-west \
  --set scaledObject.targetRef.name=orders-worker
```

| Value | Meaning |
|---|---|
| `brokers` | bootstrap addresses, comma-separated |
| `topic` | the lease. One lease per topic; a workload that needs its own lease needs its own topic and its own release |
| `holderPrefix` | names the cluster in holder ids (`eu-west/<pod>`), in logs and on the topic. Defaults to the release name |
| `ttl` | how long a term lasts without renewal, `10s` by default. The same for every participant of a lease |
| `margin` | how much earlier than the term's end the holder stops believing, `ttl/5` by default; see [Choosing TTL and margin](#choosing-ttl-and-margin) |
| `scaledObject.targetRef.name` | the workload to scale, in the release namespace |
| `scaledObject.targetRef.kind`, `.apiVersion` | its kind, `Deployment` and `apps/v1` by default: anything with a scale subresource, a StatefulSet or Strimzi's `KafkaConnect`, say |
| `scaledObject.replicas` | replicas while this cluster holds the lease, 1 by default; zero otherwise |
| `scaledObject.fallback.enabled` | `true` by default: when KEDA cannot reach the scaler, scale the workload to zero instead of leaving it running; see [When the scaler is gone](#when-the-scaler-is-gone) |
| `scaledObject.enabled` | `false` to write your own ScaledObject; see below |

Upgrading from 0.3.0: `scaledObject.target` is renamed to
`scaledObject.targetRef.name`; a release that still sets the old value
fails to render with a message that names the new one.

The workload itself is yours, in the same namespace. Its
replica count belongs to KEDA from now on, so leave `replicas` out or set
it to 0. Keep `terminationGracePeriodSeconds` short: every second of it is
a second the old and the new holder may overlap.

Check:

```bash
kubectl -n kfklease get scaledobject orders-worker   # ACTIVE True in one cluster, False in the others
kubectl -n kfklease logs deploy/kfklease             # "acquired" in one cluster
curl http://kfklease.kfklease.svc:9091/status        # from inside the cluster
```

The scaler pods of the standby clusters log `joined` and then nothing much
until a takeover. That is correct.

### Your own ScaledObject

With `scaledObject.enabled=false`, point a ScaledObject at the scaler
yourself. The trigger is `external-push`; `topic` in the metadata is
optional and only checked against the scaler's topic, which catches a
ScaledObject aimed at the wrong release:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: orders-worker
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: orders-worker
  idleReplicaCount: 0
  minReplicaCount: 1   # N while holding
  maxReplicaCount: 1   # the same N
  pollingInterval: 5
  cooldownPeriod: 0
  fallback:            # a scaler error scales to idleReplicaCount
    failureThreshold: 3
    replicas: 0
  triggers:
    - type: external-push
      metadata:
        scalerAddress: kfklease.kfklease.svc:9090
        topic: orders-singleton
```

The replica counts are the point. The scaler reports 1 of a target of 1
while it holds and 0 otherwise; KEDA scales the target to
`idleReplicaCount` while the metric is 0 and hands it to the HPA, which
keeps it between `minReplicaCount` and `maxReplicaCount`, while it is 1.
With both at N, that is N replicas in the holding cluster and none
elsewhere. `cooldownPeriod: 0` because every second of cooldown is a second
of overlap after a handover. `fallback` with `replicas: 0` because without
it a scaler KEDA cannot reach leaves the workload running; see below.

## Authentication

Everything the scaler needs to authenticate comes from Kubernetes Secrets
that already exist: cert-manager, Strimzi, External Secrets, or `kubectl
create secret`. The chart never holds a secret itself.

### TLS and mTLS

Certificates are mounted from a Secret as files. The scaler re-reads them
on every connection, so a renewed certificate takes effect on the next
reconnect without a restart.

```bash
  --set brokers=kafka-1:9093 \
  --set auth.tls.enabled=true \
  --set auth.tls.secretName=kafka-client-tls \
  --set auth.tls.clientCert=true          # mTLS; omit for TLS with a CA only
```

The defaults fit the `kubernetes.io/tls` layout cert-manager produces:
`ca.crt`, `tls.crt`, `tls.key`. A Strimzi `KafkaUser` with TLS
authentication produces `ca.crt`, `user.crt`, `user.key`:

```bash
  --set auth.tls.certKey=user.crt --set auth.tls.keyKey=user.key
```

For brokers with public certificates, `auth.tls.enabled=true` alone uses
the system roots. `auth.tls.serverName` overrides the name the brokers'
certificates are checked against when they are advertised by IP.

### SASL

Credentials go in as environment variables straight from the Secret. A pod
reads its environment once, so a rotated Secret needs a pod restart: with
[stakater/reloader](https://github.com/stakater/Reloader) installed, one
annotation does it.

```bash
  --set auth.sasl.mechanism=scram-sha-512 \
  --set auth.sasl.secretName=orders-kfklease-user \
  --set auth.sasl.username=orders-kfklease \
  --set podAnnotations."reloader\.stakater\.com/auto"=true
```

Mechanisms: `plain`, `scram-sha-256`, `scram-sha-512`, `oauthbearer`. The
Secret's keys default to `password` (what a Strimzi `KafkaUser` with SCRAM
produces) and `token` for OAUTHBEARER; when the Secret holds the username
too, `auth.sasl.usernameKey=username` reads it from there. SASL over TLS is
the two sections together.

The broker's ACLs for the scaler's user: `Read`, `Write` and `Describe` on
the lease topic, `Create` on the topic if `createTopic` is on, and
`DescribeConfigs` on the topic to verify an existing one.

## Fencing by epoch

Every term of the lease has an epoch, the offset of the claim that started
it. It grows with every handover and is what a workload uses to keep a
stale instance from writing.

The scaler exposes it at `GET /status` on port 9091:

```json
{
  "holder": "eu-west/kfklease-7d9c4-x2k8p",
  "holding": true,
  "epoch": 1842,
  "deadline_in_seconds": 7.3,
  "certain": true,
  "lease": {"holder": "eu-west/kfklease-7d9c4-x2k8p", "epoch": 1842, "expires": "2026-09-24T10:15:32Z"},
  "as_of": "2026-09-24T10:15:22Z"
}
```

The pattern, in [examples/heartbeat](../examples/heartbeat):

1. Before a write, ask `/status`. No `holding`, no write.
2. Send the epoch with the write.
3. On the receiving side, reject anything with an epoch below the highest
   one seen. That is where exclusion actually happens: an instance that
   outlived its term can still send, but nothing it sends is accepted.

`holding` is a belief with a deadline (`deadline_in_seconds`), so a workload
that cannot check before every write should check at least once per margin.

## Metrics

`GET /metrics` on the same port, Prometheus format. The pod carries
`prometheus.io/scrape`, `port` and `path` annotations;
`metrics.serviceMonitor.enabled=true` adds a ServiceMonitor for the
Prometheus Operator.

| Metric | Meaning |
|---|---|
| `kfklease_holding` | 1 while this participant believes it holds the lease |
| `kfklease_epoch` | the held term's epoch; -1 when not holding |
| `kfklease_belief_remaining_seconds` | seconds until the belief expires unless renewed |
| `kfklease_certain` | 1 when the scaler's view of the log can be trusted; 0 right after a start on a compacted topic, until it settles |
| `kfklease_lease_held` | 1 when the log says someone holds the lease |
| `kfklease_lease_as_of_seconds` | broker time of the last record seen |
| `kfklease_transitions_total{to}` | handovers into (`holding`) and out of (`standby`) the lease |

Alerts worth having: `sum(kfklease_holding) != 1` across the clusters of one
lease for longer than a TTL (nobody, or more than one, holds it),
`kfklease_certain == 0` for longer than two TTLs (the scaler cannot settle
its view: check its connection to Kafka), and a rising
`kfklease_transitions_total` (flapping).

## When the scaler is gone

The scaler is one pod per cluster. If it is gone while the workload's pods
live (its node is lost, it is Pending, its image does not pull), it neither
renews the lease nor tells KEDA to stop: the term runs out, another cluster
takes the lease and starts the workload. Without a fallback, KEDA marks
the ScaledObject not ready and leaves the replicas alone, and both clusters
run the workload until the scaler is back.

The chart therefore renders KEDA's `fallback` with `replicas: 0`. By the
KEDA v2.17 source (`pkg/scaling/executor/scale_scaledobjects.go`), a scale
loop that gets an error from the scaler and no active answer then scales
the target to `idleReplicaCount`, once `cooldownPeriod` has passed since
the last active answer. The bound on the old cluster stopping on its own
is the time until KEDA's next call fails, at most `pollingInterval`, plus
`cooldownPeriod`, plus the workload's `terminationGracePeriodSeconds`.
Compare it with how soon another cluster can claim: one TTL after the
scaler's last renewal, which may be up to TTL/3 older than the moment it
went, so at the earliest about 6.7 s with the defaults, plus that cluster's
own KEDA reaction and pod start. With `pollingInterval: 5`, `cooldownPeriod:
0` and a short grace period the old pods are usually gone first; a lower
`pollingInterval` or a longer TTL widens the gap. The pod-level window is
not a guarantee either way: fence by epoch. The `scaler-down` scenario on
the stand checks the bound.

The cost: any error KEDA gets from the scaler scales the workload to zero,
a scaler restart included. Today a restarting scaler reports "not holding"
until it holds again, so a restart stops the workload in any case.

Two cases the fallback does not cover. A scaler that hangs rather than
fails (a frozen process, a node whose packets vanish) keeps KEDA's call
waiting instead of failing it; the pod's liveness probe restarts a hung
scaler, and a lost node takes the scaler's endpoint away once Kubernetes
marks the node not ready. And KEDA itself down in the old cluster stops
nothing; that is the revived-cluster case in [Fencing by
epoch](#fencing-by-epoch).

## Choosing TTL and margin

- **TTL** is the failover time: after the holder dies, the standby takes
  over about one TTL later, plus KEDA's reaction and the pod's start. It is
  also how often the holder writes to the topic (every TTL/3). 10 s is a
  reasonable default; 5 s if failover time matters more than a few extra
  records per minute; longer if the clusters see the broker over a flaky
  link and should not fail over on every blip.
- **Margin** is safety. The holder stops believing this much before its
  term ends in the log, to cover clocks that disagree. It must be at least
  twice the clock skew between brokers plus the drift of the holder's own
  clock over one TTL plus a renewal's round trip. The default of TTL/5 (2 s
  at 10 s) covers brokers within a second of each other. A holder that can
  still read the log also drops its belief the moment it sees another
  holder's claim; the margin is for the holder that cannot.

The TTL is a property of the lease: every participant must use the same
value, and the fold rejects records that ask for more.

## Operations

**Upgrading the chart.** `helm upgrade` with the same values; the
Deployment's strategy is Recreate, so the old pod releases the lease as it
stops and the new one joins. If this cluster held the lease, the release
lets another cluster take it at once, and the workload moves. Upgrade the
standby clusters first if that matters.

**Rotating credentials.** TLS files: nothing to do, the next connection
uses the new files. SASL environment: restart the pod (reloader does it).

**Kafka unavailable.** Nobody can renew: the holder stops believing within
a TTL and KEDA scales the workload to zero everywhere. When Kafka is back,
the first cluster to claim gets the lease. A restart of the broker shorter
than TTL minus margin passes unnoticed: a renewal gets through in time.

**Changing the TTL.** It is one value for all participants. Stop all
scalers, change the value everywhere, start them. Records with the old TTL
in the topic are harmless: the fold reads each record's own TTL, and only
rejects ones asking for more than the configured value.

**Deleting the topic.** Don't, while anyone runs: the epoch is the offset,
and a recreated topic starts from zero. Stop all scalers first.

**Several workloads.** One lease per topic and one release per lease. The
scaler serves any number of ScaledObjects that point at it, but they all
follow the same lease.

## Troubleshooting

| Symptom | Look at |
|---|---|
| ScaledObject shows `READY False`, KEDA logs `connection refused` to the scaler | the scaler pod is not up, or `scalerAddress` names the wrong Service or namespace. With the fallback the workload is at zero in this cluster until the scaler answers |
| Scaler logs `topic ... does not use LogAppendTime` | the topic exists with `message.timestamp.type=CreateTime`. Set it to `LogAppendTime`, or let `createTopic` create a new one |
| Scaler logs `partition not ready` a few times at start | a freshly created topic; it settles within seconds |
| `kfklease_certain` stays 0 | the scaler is not reading the log: check `fetch failed` in its logs, the brokers, the auth |
| Every cluster is standby, `kfklease_lease_held` is 1 | someone else holds the lease: a scaler you forgot, or a previous pod still releasing. Wait one TTL |
| `acquired` and `no longer holder` alternate | renewals are not landing in time: the broker is slow or the link is flaky. Raise the TTL, or look at the broker |
| Two clusters run the workload for a few seconds after a handover | expected at the pod level (termination, `cooldownPeriod`, a revived cluster's stale pod). Fence by epoch |

Set `logLevel: debug` to see every record the scaler folds.

## Without Kubernetes

The binary alone is a lease participant with a gRPC and an HTTP endpoint;
KEDA is only its first consumer. `kfklease-scaler -h` lists every flag with
its `KFKLEASE_*` variable. The Go library is smaller still:

```go
c, err := lease.NewCandidate(lease.Config{
	Brokers: []string{"kafka:9092"}, Topic: "orders-singleton", CreateTopic: true,
	TTL: 10 * time.Second,
	Auth: lease.Auth{SASL: lease.SASL{Mechanism: lease.MechanismScramSha512, Username: "u", PasswordFile: "/run/secrets/password"}},
})
go c.Run(ctx)
for range c.Changed() {
	s := c.Status()
	// s.Holding, s.Epoch, s.Deadline
}
```

`Run` releases the lease when `ctx` is cancelled. See the package
documentation for `Config` and `Status`.
