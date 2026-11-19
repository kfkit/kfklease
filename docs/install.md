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
helm install kfklease oci://ghcr.io/kfkit/charts/kfklease --version 0.4.0 \
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
| `replicaCount` | scaler pods per cluster, `2` by default. One holds the term, every one answers KEDA for the site; see [Scaler replicas](#scaler-replicas) |
| `podDisruptionBudget.enabled`, `.minAvailable` | `true` and `1` by default: a drain never takes the last scaler pod of the cluster. Rendered only with more than one replica |
| `standby` | `false` by default; `true` keeps this cluster out of every lease of the release: it follows them and never claims. See [Standby and the status command](#standby-and-the-status-command) |
| `handover.enabled` | `true` by default: when the cluster gives a lease up in standby, its scaler pauses the ScaledObject at zero and tells the other clusters once the workload's pods are gone, so they take over without waiting for `takeoverDelay`. Renders a namespaced Role; see [The handover](#the-handover) |
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
| `affinity` | empty by default, which spreads the scaler pods over nodes with a preferred anti-affinity; anything set replaces that |

Upgrading from 0.3.0: `holderPrefix` is replaced by `site`, which is
required, and `scaledObject.target` is renamed to
`scaledObject.targetRef.name`; a release that still sets an old value
fails to render with a message that names the new one. `cooldownPeriod`
is 60 s where it was 0, and a failover to another cluster waits
`takeoverDelay` (95 s by default) after the old term ends. A new topic gets
three replicas where it used to get the broker's
`default.replication.factor`: on a Kafka cluster with fewer than three
brokers, set `replicationFactor` to the number of brokers. The scaler runs
two pods per cluster where it ran one, rolled one at a time
(`RollingUpdate`) where it was recreated, with a PodDisruptionBudget;
`replicaCount: 1` keeps one pod, which a drain or an upgrade then takes
away before its successor is up.

The workload itself is yours, in the same namespace. Its
replica count belongs to KEDA from now on, so leave `replicas` out or set
it to 0. Keep its `terminationGracePeriodSeconds` within `takeoverMargin`:
a workload still stopping when the other site starts its own overlaps
with it.

Check:

```bash
kubectl -n kfklease get scaledobject orders-worker   # ACTIVE True in one cluster, False in the others
kubectl -n kfklease logs -l app.kubernetes.io/name=kfklease --prefix  # "acquired" in one pod of one cluster
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

### Scaler replicas

The chart runs `replicaCount` scaler pods per cluster, two by default, and
every one of them takes part in the lease for the site. One holds the term
and renews it. The others read its renewals from the log and answer KEDA
"my site holds" while the term is valid, counted from the moment they read
the renewal, for the TTL less the margin; a pod cut off from Kafka sees no
more renewals and stops saying so when the term runs out. When the holder
goes (its pod deleted, its node lost, a drain), it releases if it can, and
another pod of the site claims at once, the term being its site's. KEDA,
behind the scaler's Service, gets the same answer from every ready pod, so
losing one neither blinds KEDA nor moves the workload. The rules are in
[docs/protocol.md](protocol.md#several-participants-per-site).

- A pod is ready once it knows the state of every lease it serves; until
  then it would answer KEDA with an error, so the Service sends KEDA
  elsewhere.
- An upgrade starts a new pod before it stops an old one and waits for it
  to be ready. An upgrade to an image that does not pull stops there, with
  the old pods still answering.
- A PodDisruptionBudget with `minAvailable: 1` keeps a drain from taking
  every scaler pod of the cluster at once, and the pods prefer different
  nodes.
- A stopping pod stops answering KEDA before it releases, so KEDA has asked
  another pod by the time the term moves.
- The health gate probes from every pod while the site holds the lease;
  when the workload cannot run, none of them takes the term over for it.

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
| `flush.messages` | `1` with `flushEveryRecord: true`, the default; unset with `false` | every record is fsynced before it is acknowledged, which protects the tail of the log when all replicas go down at once |

A cluster with fewer brokers than `replicationFactor` fails the creation
with an error that names `replicationFactor`; nothing lowers it on its
own. `flushEveryRecord` is on by default and goes against the Kafka
documentation, which relies on replication instead; at one record every
few seconds it costs latency, not throughput: on the one-broker stand a produce with `acks=all` took
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
  "topic": "orders-singleton",
  "holder": "eu-west/kfklease-7d9c4-x2k8p",
  "holding": true,
  "epoch": 1842,
  "site_holding": true,
  "site_epoch": 1842,
  "deadline_in_seconds": 7.3,
  "certain": true,
  "ready": true,
  "lease": {"holder": "eu-west/kfklease-7d9c4-x2k8p", "epoch": 1842, "expires": "2026-09-24T10:15:32Z"},
  "as_of": "2026-09-24T10:15:22Z",
  "health_cooldown_in_seconds": 0,
  "standby": false,
  "handover": false
}
```

These are all the fields; a test keeps the sample and the scaler's answer
in step. `lease` is what the log says and is there while `certain` is
true. A scaler with
[several leases](#several-leases-one-scaler) needs to be told which:
`GET /status?topic=connect-billing`; without `topic` it answers 400 and
names the topics it serves, and an unknown one is a 404.

The pattern, in [examples/heartbeat](../examples/heartbeat):

1. Before a write, ask `/status`. No `site_holding`, no write. The
   Service sends the request to any scaler pod of the cluster, and
   `holding` is true on one of them only; `site_holding` is true on every
   one while the site holds the lease.
2. Send the epoch with the write: `site_epoch`.
3. On the receiving side, reject anything with an epoch below the highest
   one seen. That is where exclusion actually happens: an instance that
   outlived its term can still send, but nothing it sends is accepted.

`site_holding` is a belief with a deadline, so a workload that cannot check
before every write should check at least once per margin.

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
| `kfklease_site_holding` | 1 while this participant believes its site holds the lease: what it tells KEDA. 1 on every scaler pod of the holding cluster |
| `kfklease_epoch` | the held term's epoch; -1 when not holding |
| `kfklease_belief_remaining_seconds` | seconds until the belief expires unless renewed |
| `kfklease_certain` | 1 when the scaler's view of the log can be trusted; 0 right after a start on a compacted topic, until it settles |
| `kfklease_lease_held` | 1 when the log says someone holds the lease |
| `kfklease_lease_as_of_seconds` | broker time of the last record seen |
| `kfklease_lease_holder_info{holder,site}` | 1 for the holder the log names, on every participant, the standbys too; no series while nobody holds the lease or the view is not certain. `site` is the holder id up to its last slash, the chart's `site` |
| `kfklease_transitions_total{to}` | handovers into (`holding`) and out of (`standby`) the lease |
| `kfklease_health_cooldown_seconds` | seconds until this participant may claim again after the [health gate](#when-the-workload-cannot-run-here) gave the lease up; 0 otherwise |
| `kfklease_standby_mode` | 1 while this participant is in [standby mode](#standby-and-the-status-command): it never claims |

Alerts worth having: `sum by (topic) (kfklease_holding) != 1` across the
clusters for longer than a TTL (nobody, or more than one, holds it),
`kfklease_site_holding` that differs between the pods of one cluster for
longer than a TTL (one of them is cut off from Kafka),
`kfklease_certain == 0` for longer than two TTLs (the scaler cannot settle
its view: check its connection to Kafka), and a rising
`kfklease_transitions_total` (flapping). Where the workload runs:
`max by (topic, site) (kfklease_lease_holder_info)`.

## When the scaler is gone

With [several scaler pods](#scaler-replicas) per cluster, losing one is
nothing: another answers KEDA and takes the term over. This section is about
losing all of them, or the only one with `replicaCount: 1`. If the scaler
is gone while the workload's pods
live (its node is lost, it is Pending, its image does not pull), it neither
renews the lease nor tells KEDA to stop: the term runs out, another cluster
takes the lease and starts the workload. Without a fallback, KEDA marks
the ScaledObject not ready and leaves the replicas alone, and both clusters
run the workload until the scaler is back.

The chart therefore renders KEDA's `fallback` with `replicas: 0`. By
KEDA's source (`pkg/scaling/executor/scale_scaledobjects.go`, read at the
version the stand pins in
[test/e2e/manifests/keda.yaml](../test/e2e/manifests/keda.yaml)), a scale
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
  link and should not fail over on every blip, or if a term has to outlast
  the hard death of a broker ([below](#when-the-partition-leader-dies)).
- **Margin** is safety. The holder stops believing this much before its
  term ends in the log, to cover clocks that disagree. It must be at least
  twice the clock skew between brokers plus the drift of the holder's own
  clock over one TTL plus a renewal's round trip. The default of TTL/5 (2 s
  at 10 s) covers brokers within a second of each other. A holder that can
  still read the log also drops its belief the moment it sees another
  holder's claim; the margin is for the holder that cannot.

The TTL is a property of the lease: every participant must use the same
value, and the fold rejects records that ask for more.

### When the partition leader dies

The lease topic has one partition, and one broker leads it. A broker that
is stopped gracefully hands the leadership over first, and the lease does
not notice. A broker that dies hard (a kill, a lost node, a power cut)
leaves the partition without a leader until the controller declares the
broker dead, after `broker.session.timeout.ms`, 9 s by default, and then
elects another. No renewal lands in between.

A holder renews every TTL/3 and believes for TTL minus the margin after
the send of its last accepted renewal. Right before a renewal the belief
has `ttl − margin − ttl/3` left, 4.7 s with the defaults, and right after
one `ttl − margin`, 8 s. Both are shorter than 9 s. So at the default TTL
a hard failure of the partition leader always costs the term: the holder
stops believing, nobody holds the lease for a few seconds, and the next
claim starts a new term with a new epoch. Safety holds: there are never
two holders.

Under the chart the workload stays where it is: the term that ended was
the site's own, so the site claims again within seconds, and KEDA's
`cooldownPeriod` covers the gap. What changes is the epoch. A workload
that [fences by epoch](#fencing-by-epoch) sees a few seconds without
`site_holding` and then a new `site_epoch`; a program that uses
`lease.Candidate` directly sees a few seconds without a holder and a new
epoch, maybe on another participant.

Measured with three brokers (Apache Kafka 4.3.1, KRaft, default settings),
a lease topic with three replicas and `min.insync.replicas=2`, three
`lease.Candidate`s, TTL 10 s, margin 2 s, statuses sampled every 5 ms:

| Event | What the lease did |
|---|---|
| `SIGKILL` of the partition leader | the holder logged `deadline passed without renewal` 6.5 s later; nobody held for 3.9 s; another participant took a new epoch |
| graceful stop of the partition leader | nothing: the holder kept its term |
| two of three brokers killed (no ISR quorum, no controller quorum) | the holder stopped believing after 7.7 s; nobody held for the 37.5 s that were left of the outage; held again 13.1 s after the brokers were started |

No two holders at any moment of the run.

To keep a term through a hard failure of the partition leader, the belief
left right before a renewal has to outlast the session timeout and the
election:

```
ttl − margin − ttl/3  >  broker.session.timeout.ms + election
```

There are two ways to get there:

- **A longer TTL.** With the default margin of TTL/5 the left side is 7/15
  of the TTL, so the brokers' default of 9 s takes a TTL of about 20 s, and
  more the longer the election takes. The cost is the failover time: when
  the holder is really gone, every takeover waits out a TTL first, 20 s
  where it was 10 s.
- **A shorter `broker.session.timeout.ms` on the brokers**, below the
  4.7 s the default TTL leaves, less the election. The cost falls on the
  whole Kafka cluster, not on the lease: a broker that pauses for longer
  than the timeout (a long GC, a slow disk) is declared dead and loses its
  leaderships, on every topic. And the brokers may not be yours to
  configure.

With neither, a hard failure of the partition leader costs what the table
shows: a few seconds without a holder and a new epoch.

## Operations

**Upgrading the chart.** `helm upgrade` with the same values. The pods are
replaced one at a time, each new one ready before an old one stops, and a
holder that stops releases the lease to another pod of its site at once
(see [Scaler replicas](#scaler-replicas)). The workload stays; another
cluster would wait `takeoverDelay`. The same holds for a node drain, an OOM
kill or a restart by reloader. With `replicaCount: 1` the one pod is
replaced, and the workload stays as long as the new pod is up within
`cooldownPeriod`.

**Rotating credentials.** TLS files: nothing to do, the next connection
uses the new files. SASL environment: restart the pod (reloader does it).

**Kafka unavailable.** Nobody can renew: the holder stops believing within
a TTL, and KEDA scales the workload to zero once `cooldownPeriod` has
passed. When Kafka is back, every cluster sees the same expired term: the
site that held it claims at once, the others wait `takeoverDelay`, so the
lease comes back where it was, and so does the workload, which never
stopped if the outage was shorter than `cooldownPeriod`. A restart of the
broker shorter than TTL minus margin passes unnoticed: a renewal gets
through in time. The hard death of the partition leader is never that
short with the brokers' defaults; see [When the partition leader
dies](#when-the-partition-leader-dies).

**Changing the TTL.** It is one value for all participants. Stop all
scalers, change the value everywhere, start them. Records with the old TTL
in the topic are harmless: the fold reads each record's own TTL, and only
rejects ones asking for more than the configured value.

**Deleting the topic.** Don't, while anyone runs: the epoch is the offset,
and a recreated topic starts from zero. Stop all scalers first.

**Taking a cluster out, moving the workload on purpose.** Set `standby`
on that cluster's release; see [Standby and the status
command](#standby-and-the-status-command).

**Several workloads.** One lease per topic. Workloads that move together
share a lease: any number of ScaledObjects may name the same topic.
Workloads that move independently get a lease each, in one release with
[`extraLeases`](#several-leases-one-scaler) or in a release each; separate
releases also keep a scaler restart from touching the other leases.

### Standby and the status command

`standby: true` takes a cluster out of its leases: its scaler follows the
log, answers KEDA "not holding" and never claims, so the cluster's workload
stays at zero. Use it for maintenance, or to move the workload to another
cluster on purpose:

```bash
helm upgrade kfklease oci://ghcr.io/kfkit/charts/kfklease -n kfklease --version <installed> --reuse-values --set standby=true
```

On the holding cluster this restarts the scaler, whose pod releases the
lease on its way out. With the handover (the default, see below) the
workload stops at once and another cluster takes over as soon as its pods
are gone: a TTL or so after the release, plus the pod start. Without it,
the cluster's KEDA scales the workload to zero after `cooldownPeriod`, and
another cluster claims `takeoverDelay` after the release, so the old pods
are gone before the new ones start: about `takeoverDelay` plus the pod
start, 1 min 40 s or so with the defaults. With no other cluster out of
standby, nobody holds the lease.

`standby: false` lets the cluster claim again by the usual rules. Nothing
moves back on its own: while another cluster holds the lease, it stays
there. To move it back, put that cluster in standby.

The library has the same switch at run time: `Candidate.SetStandby`, or
`Config.Standby` to start in it. A participant in standby that holds the
lease stops believing at once and writes a release.

#### The handover

The takeover delay is there because nothing tells the other clusters when
the old workload is gone. In standby, the scaler knows: once its site's
term has ended, a scaler pod of the cluster

1. sets KEDA's `autoscaling.keda.sh/paused-replicas: "0"` on the
   ScaledObject, so KEDA scales the workload to zero now rather than after
   `cooldownPeriod`;
2. looks at the target's scale subresource once a second and waits for
   two things: KEDA has applied the pause, so the scale asks for zero
   replicas, and no pod matches its selector, terminating ones included.
   No pod alone is not enough: in the middle of a rollout or a pod restart
   there is none for a moment, and while replicas are asked for the
   target's controller creates the next one;
3. once both hold, writes a `yield` record for the term to the lease
   topic.

Another cluster reads the yield as the end of its takeover delay, but
claims no earlier than a TTL after the old term's last renewal (plus half a
renewal interval): until then a scaler replica of the old cluster that
missed the release could still answer KEDA "my site holds". The handover
gives up when `takeoverDelay` has passed since the term ended; by then the
other cluster claims anyway. A handover that does not finish (a KEDA that
does not apply the pause, a pod that never terminates, an API error) writes
no yield, and the other cluster waits the full delay, as without the
handover.

The pause is lifted when the cluster holds the lease again, and once
another cluster holds it, so a scaler stopped half-way through a handover
does not leave the workload paused: the next scaler pod picks the handover
up where it was. It is never lifted while the cluster's own term is the
latest one in the log: the yield promised the others that the workload
stays down.

The chart gives the scaler a ServiceAccount and a Role in the release
namespace, nothing cluster-wide:

| Resource | Verbs | Why |
|---|---|---|
| `scaledobjects.keda.sh`, by name: the release's own | `get`, `patch` | pause and lift; read the target from KEDA's `status.scaleTargetGVKR` |
| `<target>/scale` in the target's group, by name | `get` | the replicas asked of the target and its pod selector |
| `pods` | `list` | whether any pod of the target is left |

The target's resource is its kind in lower case with an `s`
(`deployments`, `statefulsets`, `kafkaconnects`, `kafkamirrormaker2s`).
`handover.enabled: false` renders none of it, and standby moves the
workload after `takeoverDelay`. Outside the chart: `-scaled-object` (or
`KFKLEASE_SCALED_OBJECT`), one ScaledObject per topic, and the same access
for the pod's service account. With your own ScaledObject
(`scaledObject.enabled: false`) the chart renders no handover; the access
and `KFKLEASE_SCALED_OBJECT` are then yours to add.

`/status` reports `"handover": true` while a scaler waits for the workload
to go. The library side is `Status.Handover` and `Candidate.Yield`: a
yield is a promise the protocol cannot check, so call it only once nothing
of the workload runs.

**Who holds the lease**, from anywhere that reaches Kafka, without taking
part in it:

```bash
kubectl -n kfklease exec deploy/kfklease -- /app status
```

```json
{"topic":"orders-singleton","held":true,"holder":"eu-west/kfklease-7d9c4-x2k8p","site":"eu-west","epoch":1842,"expires":"2026-09-24T10:15:32Z","last_holder":"eu-west/kfklease-7d9c4-x2k8p","as_of":"2026-09-24T10:15:22Z"}
```

In the scaler's pod it reads the brokers, topics, TTL and credentials from
the pod's `KFKLEASE_*` environment; elsewhere give them as flags
(`kfklease-scaler status -h`). One line per lease, in the order of the
topics; timestamps are the broker's. A lease nobody has written to for a
while takes a TTL of silence to prove free, so `status` may wait that long;
it gives up after `-wait` (3 × TTL by default) and exits non-zero. It never
writes to the topic.

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
| The epoch changed and nothing failed over: `deadline passed without renewal`, then `acquired` with a new epoch in the same cluster | a broker died hard, the leader of the lease's partition: at the default TTL the term does not outlast the brokers' session timeout. See [When the partition leader dies](#when-the-partition-leader-dies) |
| Two clusters run the workload for a few seconds after a handover | the workload takes longer to stop than `takeoverMargin`, or a revived cluster runs its stale pod until its KEDA is back. Raise `takeoverMargin`; fence by epoch |
| A failover takes about two minutes | TTL plus `takeoverDelay`, which is `pollingInterval + cooldownPeriod + takeoverMargin`: the price of a scaler restart that keeps the workload. Lower `cooldownPeriod` if scaler restarts are fast |
| Standby moves the workload only after `takeoverDelay` | the handover did not finish: the scaler logs `handover: cannot pause`, `cannot count the workload's pods` (RBAC, a target kind whose plural is not kind + `s`, a ScaledObject KEDA has not resolved yet) or keeps logging nothing while a pod of the workload hangs in `Terminating`. See [The handover](#the-handover) |
| A ScaledObject keeps `autoscaling.keda.sh/paused-replicas: "0"` | a handover that never finished, with the cluster's own term still the latest. It goes when the cluster or another one holds the lease; remove it by hand only when no other cluster could have taken over on the promise of a yield |
| Scaler exits with `the log was replaced or truncated` | the lease topic was deleted and created again, or the partition lost acknowledged records, under running scalers. They restart and read the log as it is; epochs start over, so a workload that fences by epoch must not trust an older one |
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
