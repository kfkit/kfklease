# Installing and using kfklease

This is the operator's guide: from a Kafka cluster and two Kubernetes
clusters to a workload that runs in exactly one of them and fails over. For
what the lease is and why it is safe, read [protocol.md](protocol.md).

## What you get

One `kfklease-scaler` per cluster takes part in a lease held on a Kafka
topic and serves the result to KEDA as an external scaler. A ScaledObject
keeps your workload at one replica in the cluster that holds the lease and
at zero everywhere else. When that cluster dies, freezes or loses Kafka, its
term runs out, another cluster takes the lease after the takeover delay,
and KEDA starts the workload there. The lease belongs to the cluster, its
site, not to the scaler pod: a scaler restart, or a Kafka outage, leaves the
workload where it is; see [Sites and the takeover
delay](#sites-and-the-takeover-delay).

```
cluster A                              cluster B
┌────────────────────────┐             ┌────────────────────────┐
│ kfklease-scaler ──────┐│             │┌────── kfklease-scaler │
│   holds the lease     ││   Kafka     ││   standby             │
│ KEDA ──► workload ×1  ││◄──topic──►││   KEDA ──► workload ×0 │
└────────────────────────┘             └────────────────────────┘
```

Failover, not mutual exclusion: the old pod may still be terminating when
the new one starts if the takeover delay does not cover its stop, and a
cluster coming back from a crash restarts its stale
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
  creates it with the settings the protocol needs and three replicas, so
  the Kafka cluster needs three brokers; see [The lease topic](#the-lease-topic).

## Install

One Helm release per cluster, the same lease everywhere, a different
`site` in each cluster:

```bash
helm install kfklease oci://ghcr.io/kfkit/charts/kfklease --version 0.3.0 \
  -n kfklease --create-namespace \
  --set brokers=kafka-1:9092,kafka-2:9092 \
  --set topic=orders-singleton \
  --set site=eu-west \
  --set scaledObject.targetRef.name=orders-worker
```

| Value | Meaning |
|---|---|
| `brokers` | bootstrap addresses, comma-separated |
| `topic` | the lease. One lease per topic; more leases, each with its own workload, go in `extraLeases` (see [Several leases, one scaler](#several-leases-one-scaler)) |
| `site` | required: names the cluster. The lease belongs to it; holder ids are `<site>/<pod>`, in logs and on the topic |
| `takeoverDelay` | seconds a cluster waits after another site's term ended before it claims; `0` (the default) derives `pollingInterval + cooldownPeriod + takeoverMargin` of the slowest ScaledObject, 95 s with the defaults, and a value below that fails the render |
| `takeoverMargin` | seconds the old site's workload gets to stop once its KEDA scales it to zero, `30` by default: cover its `terminationGracePeriodSeconds` |
| `ttl` | how long a term lasts without renewal, `10s` by default. The same for every participant of a lease |
| `margin` | how much earlier than the term's end the holder stops believing, `ttl/5` by default; see [Choosing TTL and margin](#choosing-ttl-and-margin) |
| `replicationFactor` | replicas of the topic `createTopic` creates, `3` by default. A cluster with fewer brokers fails the creation; see [The lease topic](#the-lease-topic) |
| `scaledObject.targetRef.name` | the workload to scale, in the release namespace |
| `scaledObject.targetRef.kind`, `.apiVersion` | its kind, `Deployment` and `apps/v1` by default: anything with a scale subresource, a StatefulSet or Strimzi's `KafkaConnect`, say |
| `scaledObject.replicas` | replicas while this cluster holds the lease, 1 by default; zero otherwise |
| `scaledObject.cooldownPeriod` | how long KEDA keeps the workload after the scaler stopped saying "holding" or could not be asked, `60` by default: longer than a scaler restart |
| `scaledObject.fallback.enabled` | `true` by default: when KEDA cannot reach the scaler, scale the workload to zero instead of leaving it running; see [When the scaler is gone](#when-the-scaler-is-gone) |
| `scaledObject.enabled` | `false` to write your own ScaledObject; see below |
| `imagePullSecrets`, `podLabels`, `priorityClassName` | for the scaler pod, empty by default |
| `podSecurityContext`, `securityContext` | the pod's and the container's, empty by default. The image runs as nonroot and writes nothing, so the restricted Pod Security Standard fits; `values.yaml` has an example |

Upgrading from 0.3.0: `holderPrefix` is replaced by `site`, which is
required, and `scaledObject.target` is renamed to
`scaledObject.targetRef.name`; a release that still sets an old value
fails to render with a message that names the new one. `cooldownPeriod`
is 60 s where it was 0, and a failover to another cluster waits
`takeoverDelay` (95 s by default) after the old term ends. A new topic gets
three replicas where it used to get the broker's
`default.replication.factor`: on a Kafka cluster with fewer than three
brokers, set `replicationFactor` to the number of brokers.

The workload itself is yours, in the same namespace. Its
replica count belongs to KEDA from now on, so leave `replicas` out or set
it to 0. Keep its `terminationGracePeriodSeconds` within `takeoverMargin`:
a workload still stopping when the other site starts its own overlaps
with it.

Check:

```bash
kubectl -n kfklease get scaledobject orders-worker   # ACTIVE True in one cluster, False in the others
kubectl -n kfklease logs deploy/kfklease             # "acquired" in one cluster
curl http://kfklease.kfklease.svc:9091/status        # from inside the cluster
```

The scaler pods of the standby clusters log `joined` and then nothing much
until a takeover. That is correct.

### Sites and the takeover delay

Each cluster is a site, and the lease belongs to the site that holds it.
Two rules, both on the writer's side of the protocol
([docs/protocol.md](protocol.md#sites)):

- A scaler claims at once when the term that ended last was its own
  site's: a restarted scaler pod, after its predecessor released or its
  term ran out, and every cluster after a Kafka outage that ended its own
  term.
- A scaler of another site waits `takeoverDelay` after that term ended, by
  expiry or by release, before it claims.

What keeps the workload in place through a scaler restart is KEDA's
`cooldownPeriod`: the old pod says "not holding" as it releases, the new
pod answers KEDA with an error until it has read the log (`ready` in
`/status`), and KEDA keeps the replicas for `cooldownPeriod` after the last
"holding". The new pod claims within seconds, so the workload's pods are
the same ones afterwards. What keeps two sites from running the workload
at once on a failover is the delay: the old site's KEDA scales its
workload down within `pollingInterval + cooldownPeriod`, and the workload
stops within `takeoverMargin`, all before the other site may claim. The
order that has to hold:

```
scaler restart  <  cooldownPeriod  <  takeoverDelay  (>= pollingInterval + cooldownPeriod + takeoverMargin)
```

The chart derives the delay from the slowest ScaledObject of the release
and refuses a smaller one. The price is the failover time: TTL plus
`takeoverDelay` plus the pod start, about 1 min 50 s with the defaults.
Lower `cooldownPeriod` for a faster failover if your scaler restarts are
fast; the delay follows.

### The lease topic

With `createTopic: true` the scaler creates a missing topic with settings
chosen for a lease log, not the broker's defaults:

| Setting | Value | Why |
|---|---|---|
| partitions | 1 | one lease per topic |
| `message.timestamp.type` | `LogAppendTime` | the broker's clock is the only one the protocol uses |
| `cleanup.policy` | `compact` | the latest record is all a participant needs |
| `min.compaction.lag.ms` | `max(100 × ttl, 1h)` | the recent past stays complete |
| `segment.ms` | the same | the active segment is never compacted; with the default of seven days every new participant reads a week of renewals |
| replication factor | `replicationFactor`, 3 | with one replica, losing a broker loses the lease |
| `min.insync.replicas` | `minInsyncReplicas`; by default 2 with three or more replicas, otherwise 1 | the scaler writes with `acks=all`: an acknowledged record is on that many brokers |
| `unclean.leader.election.enable` | `false`, set on the topic | the epoch is the offset of the accepted claim; after an unclean election the same offset can hold a different record |
| `flush.messages` | `1` with `flushEveryRecord: true`; unset by default | every record is fsynced before it is acknowledged, which protects the tail of the log when all replicas go down at once |

A cluster with fewer brokers than `replicationFactor` fails the creation
with an error that names `replicationFactor`; nothing lowers it on its
own. `flushEveryRecord` goes against the Kafka documentation, which relies
on replication instead; at one record every few seconds it costs latency,
not throughput: on the one-broker stand a produce with `acks=all` took
0.9–1.3 ms at the median without it and 1.5–1.9 ms with it.

An existing topic is never altered. At start the scaler checks it: no
`LogAppendTime` stops it, as before; fewer replicas than
`replicationFactor`, `min.insync.replicas` below the value above,
`unclean.leader.election.enable=true` or a `cleanup.policy` without
`compact` are logged as warnings (`existing lease topic falls short`).
Fix them with `kafka-configs.sh --alter` or a partition reassignment; the
checks need `DescribeConfigs` and `Describe` on the topic. Change
`min.insync.replicas` on a topic with Eligible Leader Replicas only if you
mean it: it clears the topic's ELR state.

### Several leases, one scaler

Workloads that fail over independently, three Kafka Connect clusters and a
MirrorMaker 2, say, need a lease each, but not a scaler each. List the
others under `extraLeases`; the scaler takes part in every lease, one per
topic, and the chart writes one ScaledObject per lease:

```yaml
topic: connect-orders
scaledObject:
  targetRef:
    apiVersion: kafka.strimzi.io/v1beta2
    kind: KafkaConnect
    name: orders
  replicas: 3
extraLeases:
  - topic: connect-billing
    scaledObject:
      targetRef:
        apiVersion: kafka.strimzi.io/v1beta2
        kind: KafkaConnect
        name: billing
      replicas: 5
    healthGate:
      url: http://billing-connect-api:8083/
  - topic: mm2-eu-us
    scaledObject:
      targetRef:
        apiVersion: kafka.strimzi.io/v1beta2
        kind: KafkaMirrorMaker2
        name: eu-us
```

Each entry's `scaledObject` is merged over the top-level one, so
`pollingInterval`, `cooldownPeriod`, `fallback`, `replicas` and the target's
`kind` and `apiVersion` carry over unless the entry sets them; the target's
`name` is the entry's own and required. `ttl`, `margin`, `site`,
`takeoverDelay`, `auth` and the [health gate](#when-the-workload-cannot-run-here) timings are
the same for every lease; `healthGate.url` is per lease, and a lease without
one has no gate. Topics must differ, and so must the targets.

The leases are independent: each has its own terms, epochs and holder, and
one cluster may hold some of them while another holds the rest. They share
the scaler pod, though, so they share its fate: when it is gone for longer
than `cooldownPeriod`, every lease it serves runs out, and with `fallback`
every workload it scales goes to zero. A restart within `cooldownPeriod`
keeps them all. Workloads that must not move together
belong in separate releases.

Without the chart, give the binary the topics comma-separated
(`-topic connect-orders,connect-billing` or `KFKLEASE_TOPIC`) and, for the
health gate, the URLs in the same order with an empty entry for a lease
without a gate (`-health-url ,http://billing-connect-api:8083/`).

### Your own ScaledObject

With `scaledObject.enabled=false`, point a ScaledObject at the scaler
yourself. The trigger is `external-push`; `topic` in the metadata
names the lease. With one lease it is optional and only checked against the
scaler's topic, which catches a ScaledObject aimed at the wrong release;
with several it is required, and a topic the scaler does not serve is an
error:

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
  cooldownPeriod: 60   # longer than a scaler restart
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
elsewhere. `cooldownPeriod` longer than a scaler restart, so that a
restart leaves the workload alone; the scaler's `takeoverDelay`
(`KFKLEASE_TAKEOVER_DELAY` without the chart) must then exceed
`pollingInterval + cooldownPeriod` plus the workload's stop, see [Sites and
the takeover delay](#sites-and-the-takeover-delay). `fallback` with
`replicas: 0` because without it a scaler KEDA cannot reach leaves the
workload running; see below.

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

It also carries the lease's `topic`. A scaler with
[several leases](#several-leases-one-scaler) needs to be told which:
`GET /status?topic=connect-billing`; without `topic` it answers 400 and
names the topics it serves, and an unknown one is a 404.

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

Every metric of a lease carries a `topic` label, so the leases of a scaler
serving [several](#several-leases-one-scaler) stay apart.

| Metric | Meaning |
|---|---|
| `kfklease_holding` | 1 while this participant believes it holds the lease |
| `kfklease_epoch` | the held term's epoch; -1 when not holding |
| `kfklease_belief_remaining_seconds` | seconds until the belief expires unless renewed |
| `kfklease_certain` | 1 when the scaler's view of the log can be trusted; 0 right after a start on a compacted topic, until it settles |
| `kfklease_lease_held` | 1 when the log says someone holds the lease |
| `kfklease_lease_as_of_seconds` | broker time of the last record seen |
| `kfklease_lease_holder_info{holder,site}` | 1 for the holder the log names, on every participant, the standbys too; no series while nobody holds the lease or the view is not certain. `site` is the holder id up to its last slash, the chart's `site` |
| `kfklease_transitions_total{to}` | handovers into (`holding`) and out of (`standby`) the lease |
| `kfklease_health_cooldown_seconds` | seconds until this participant may claim again after the [health gate](#when-the-workload-cannot-run-here) gave the lease up; 0 otherwise |

Alerts worth having: `sum by (topic) (kfklease_holding) != 1` across the
clusters for longer than a TTL (nobody, or more than one, holds it),
`kfklease_certain == 0` for longer than two TTLs (the scaler cannot settle
its view: check its connection to Kafka), and a rising
`kfklease_transitions_total` (flapping). Where the workload runs:
`max by (topic, site) (kfklease_lease_holder_info)`.

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
Another cluster claims one TTL after the scaler's last renewal at the
earliest, plus `takeoverDelay`, which the chart keeps above
`pollingInterval + cooldownPeriod + takeoverMargin`: the old pods are gone
first as long as the workload stops within `takeoverMargin`. The pod-level
window is not a guarantee: fence by epoch. The `scaler-down` scenario on
the stand checks the bound.

The cost: an error KEDA gets from the scaler for longer than
`cooldownPeriod` scales the workload to zero. A scaler restart is shorter:
the new pod answers with an error until it has read the log, then claims
for its site and answers "holding".

Two cases the fallback does not cover. A scaler that hangs rather than
fails (a frozen process, a node whose packets vanish) keeps KEDA's call
waiting instead of failing it; the pod's liveness probe restarts a hung
scaler, and a lost node takes the scaler's endpoint away once Kubernetes
marks the node not ready. And KEDA itself down in the old cluster stops
nothing; that is the revived-cluster case in [Fencing by
epoch](#fencing-by-epoch).

## When the workload cannot run here

The lease follows the scaler and its link to Kafka, not the workload. If
the workload cannot run in the cluster that holds the lease (no nodes, an
image that does not pull, a crash loop, its database unreachable from
there), nothing fails over by itself. The health gate makes it:

```yaml
healthGate:
  url: http://my-connect-connect-api:8083/health
  interval: 10s     # time between probes
  timeout: 5s       # per probe
  failAfter: 5m
  cooldown: 10m
```

While this cluster holds the lease, the scaler GETs `url`; a status from
200 to 399 is healthy, as for a Kubernetes `httpGet` probe. When no probe
has passed for `failAfter`, counted from the later of the start of the term
and the last passing probe, the scaler stops believing it holds the lease,
writes a release, and claims nothing for `cooldown`. KEDA scales the
workload here to zero after `cooldownPeriod`, and another cluster takes the
lease `takeoverDelay` after the release, without waiting for the term to
run out, by which time this cluster's workload has stopped. Clusters that
do not hold the lease do not probe; their workload is at zero.

- **`failAfter`** is also the time a workload gets to come up after a
  failover, since it counts from the start of the term: a Kafka Connect
  cluster that needs three minutes to start its workers and rebalance needs
  more than three minutes here, or the lease moves on before it ever ran.
  The binary refuses a `failAfter` below two probe intervals.
- **`cooldown`** keeps the lease from coming straight back to a cluster
  whose workload just failed. A cluster in cooldown still follows the log;
  if no one else can take the lease, nobody holds it until the cooldown is
  over. With one participant the gate therefore stops the workload for the
  cooldown and then tries again.
- If every cluster's workload is broken, the lease goes round: each holder
  keeps it for `failAfter`, then sits out `cooldown`. Make `cooldown` longer
  than `failAfter` times the number of other clusters if a round trip
  should leave a pause.
- The probe only feeds the gate; the gate decides. A probe that hangs counts
  as failing after `timeout`.

`kfklease_health_cooldown_seconds` and `health_cooldown_in_seconds` in
`/status` show a cluster that sits out; the scaler logs `workload probe
failing` at the first failure and `workload unhealthy, releasing` when the
gate fires. Without `url` (the default) there is no gate.

## Choosing TTL and margin

- **TTL** is part of the failover time: after the holder dies, the standby
  takes over about one TTL plus `takeoverDelay` later, plus KEDA's reaction
  and the pod's start. It is
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
stops and the new one claims it for the site at once. The workload stays,
as long as the new pod is up within `cooldownPeriod`; another cluster would
wait `takeoverDelay`. The same holds for a node drain, an OOM kill or a
restart by reloader.

**Rotating credentials.** TLS files: nothing to do, the next connection
uses the new files. SASL environment: restart the pod (reloader does it).

**Kafka unavailable.** Nobody can renew: the holder stops believing within
a TTL, and KEDA scales the workload to zero once `cooldownPeriod` has
passed. When Kafka is back, every cluster sees the same expired term: the
site that held it claims at once, the others wait `takeoverDelay`, so the
lease comes back where it was, and so does the workload, which never
stopped if the outage was shorter than `cooldownPeriod`. A restart of the
broker shorter than TTL minus margin passes unnoticed: a renewal gets
through in time.

**Changing the TTL.** It is one value for all participants. Stop all
scalers, change the value everywhere, start them. Records with the old TTL
in the topic are harmless: the fold reads each record's own TTL, and only
rejects ones asking for more than the configured value.

**Deleting the topic.** Don't, while anyone runs: the epoch is the offset,
and a recreated topic starts from zero. Stop all scalers first.

**Several workloads.** One lease per topic. Workloads that move together
share a lease: any number of ScaledObjects may name the same topic.
Workloads that move independently get a lease each, in one release with
[`extraLeases`](#several-leases-one-scaler) or in a release each; separate
releases also keep a scaler restart from touching the other leases.

## Troubleshooting

| Symptom | Look at |
|---|---|
| ScaledObject shows `READY False`, KEDA logs `connection refused` to the scaler | the scaler pod is not up, or `scalerAddress` names the wrong Service or namespace. With the fallback the workload is at zero in this cluster until the scaler answers |
| Scaler logs `topic ... does not use LogAppendTime` | the topic exists with `message.timestamp.type=CreateTime`. Set it to `LogAppendTime`, or let `createTopic` create a new one |
| Scaler logs `partition not ready` a few times at start | a freshly created topic; it settles within seconds |
| Scaler exits with `replicationFactor 3, but the cluster has 1 brokers` | the Kafka cluster has fewer brokers than the topic asks for. Set `replicationFactor` to the number of brokers |
| Scaler logs `existing lease topic falls short` | the topic was made with other settings; see [The lease topic](#the-lease-topic) |
| `kfklease_certain` stays 0 | the scaler is not reading the log: check `fetch failed` in its logs, the brokers, the auth |
| Every cluster is standby, `kfklease_lease_held` is 1 | someone else holds the lease: a scaler you forgot, or a previous pod still releasing. Wait one TTL |
| `acquired` and `no longer holder` alternate | renewals are not landing in time: the broker is slow or the link is flaky. Raise the TTL, or look at the broker |
| Two clusters run the workload for a few seconds after a handover | the workload takes longer to stop than `takeoverMargin`, or a revived cluster runs its stale pod until its KEDA is back. Raise `takeoverMargin`; fence by epoch |
| A failover takes about two minutes | TTL plus `takeoverDelay`, which is `pollingInterval + cooldownPeriod + takeoverMargin`: the price of a scaler restart that keeps the workload. Lower `cooldownPeriod` if scaler restarts are fast |
| KEDA logs `the lease state is not known yet` | a scaler that just started and has not read the log yet; it settles within seconds. If it does not, look at its connection to Kafka |

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
