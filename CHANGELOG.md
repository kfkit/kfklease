# Changelog

All notable changes to this project are documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Breaking

- Chart: `scaledObject.target` is renamed to `scaledObject.targetRef.name`.
  A release that still sets `scaledObject.target` fails to render with
  "scaledObject.target was renamed to scaledObject.targetRef.name". Set
  `scaledObject.targetRef.name` instead.
- Chart: the ScaledObject now has KEDA's `fallback` with `replicas: 0`
  (`scaledObject.fallback.enabled`, on by default). When KEDA cannot reach
  the scaler it scales the workload to zero once `cooldownPeriod` has
  passed, where it used to leave the replicas as they were. A cluster whose
  scaler is gone no longer keeps running the workload while another cluster
  takes the lease. To keep the old behaviour, set
  `scaledObject.fallback.enabled=false`.
- A topic created by `createTopic` (chart), `-create-topic` (binary) or
  `Config.CreateTopic` (library) gets three replicas, where it used to get
  the broker's `default.replication.factor`. On a Kafka cluster with fewer
  than three brokers the scaler now exits with "replicationFactor 3, but
  the cluster has N brokers" instead of creating the topic. Set
  `replicationFactor` (chart), `-replication-factor` /
  `KFKLEASE_REPLICATION_FACTOR` (binary) or
  `Config.TopicSettings.ReplicationFactor` (library) to the number of
  brokers. Existing topics are not touched.
- Library: `lease.EnsureTopic` takes a `lease.TopicSettings` and returns the
  warnings about an existing topic:
  `EnsureTopic(ctx, cl, topic, ttl, settings) ([]string, error)` where it
  was `EnsureTopic(ctx, cl, topic, ttl) error`. Pass
  `lease.TopicSettings{}` for the defaults and log or ignore the warnings.
- Chart: `holderPrefix` is replaced by `site`, which is required. Holder
  ids are `<site>/<pod name>`, where they were `<holderPrefix or release
  name>/<pod name>`. A release that sets `holderPrefix` fails to render
  with "holderPrefix was replaced by site", and one without `site` with
  "site is required". Set `--set site=<the cluster's name>`, the value you
  had in `holderPrefix`.
- Chart: `scaledObject.cooldownPeriod` is 60 by default, where it was 0,
  so that a scaler restart leaves the workload running. KEDA now keeps a
  workload for a minute after its cluster lost the lease. Set it back to 0
  only together with a scaler that never restarts while holding.
- Chart: another cluster takes the lease over `takeoverDelay` after the
  previous holder's term ended, by expiry or release, where it used to
  claim at once: 95 s with the defaults (`pollingInterval` 5 +
  `cooldownPeriod` 60 + `takeoverMargin` 30), so a failover now takes about
  TTL + 95 s plus the pod start, and so does a release by the health gate.
  A cluster of the same site still claims at once. An explicit
  `takeoverDelay` below `pollingInterval + cooldownPeriod + takeoverMargin`
  fails to render; lower `cooldownPeriod` or `takeoverMargin` for a faster
  failover.
- Scaler: a scaler that has not yet read enough of the log to know who
  holds the lease answers KEDA's `IsActive` and `GetMetrics` with an
  `Unavailable` error, and pushes nothing on `StreamIsActive`, where it
  used to answer "not holding". With the chart's fallback, KEDA scales the
  workload to zero only after `cooldownPeriod` of such errors; without a
  fallback it keeps the replicas. A ScaledObject of your own should have a
  `cooldownPeriod` longer than a scaler restart.

### Added

- Chart: `scaledObject.replicas` runs N replicas of the target in the
  cluster that holds the lease and none elsewhere (`idleReplicaCount: 0`,
  `minReplicaCount = maxReplicaCount = N`).
- Chart: `scaledObject.targetRef.apiVersion` and `.kind` scale any target
  with a scale subresource, a StatefulSet or Strimzi's `KafkaConnect`, not
  only a Deployment.
- Stand: the `replicas` scenario, three workload pods where the lease is
  held, failed over by a partition.
- Stand: the `scaler-down` scenario: the holder's scaler loses its Service
  endpoint and Kafka and is killed without a release, and its workload must
  stop on its own.
- Docs: "When the scaler is gone" in docs/install.md, with the bound on how
  soon the old cluster stops on its own.
- Health gate, off by default: with `healthGate.url` (chart) or
  `-health-url` / `KFKLEASE_HEALTH_URL` (binary), a holder whose workload
  has not passed an HTTP probe for `failAfter` releases the lease and claims
  nothing for `cooldown`. Library: `lease.Config.Health` and
  `Candidate.ReportHealthy`; `scaler.Probe`.
- Metric `kfklease_health_cooldown_seconds`, field
  `health_cooldown_in_seconds` in `/status`, and `Status.CooldownUntil` in
  the library.
- Several leases, one per topic, served by one scaler: chart value
  `extraLeases`, one ScaledObject per lease; binary `-topic a,b,c`
  (`KFKLEASE_TOPIC`) with `-health-url` taking one URL per topic. A
  ScaledObject names its lease with `topic` in the trigger metadata, which
  is required when the scaler serves more than one. `GET /status?topic=`
  picks the lease, and `/status` carries a `topic` field. Library:
  `scaler.Lease`, `scaler.NewLeases`, `scaler.NewHTTPLeases`.

- The lease topic is created with settings chosen for a lease log:
  `min.insync.replicas` 2 with three or more replicas (`minInsyncReplicas`,
  `-min-insync-replicas`), `unclean.leader.election.enable=false`, and
  `segment.ms` equal to `min.compaction.lag.ms`, so that a new participant
  does not read a week of renewals. Optional `flush.messages=1`
  (`flushEveryRecord`, `-flush-every-record`), off by default.
- Metric `kfklease_lease_holder_info{holder,site}`: 1 for the holder the
  log names, reported by every participant, so any cluster's scaler says
  where the lease is. `site` is the holder id up to its last slash (the
  chart's `site`).
- Chart: `imagePullSecrets`, `podLabels`, `priorityClassName`,
  `podSecurityContext` and `securityContext` (the container's), all empty
  by default.
- An existing lease topic is checked at start: fewer replicas than asked
  for, `min.insync.replicas` below the expected value,
  `unclean.leader.election.enable=true` or a `cleanup.policy` without
  `compact` are logged as warnings. It is never altered.

- The lease belongs to a site: binary `-site` / `KFKLEASE_SITE` and
  `-takeover-delay` / `KFKLEASE_TAKEOVER_DELAY` (0, claim at once, by
  default), library `lease.Config.Site` and `lease.Config.TakeoverDelay`,
  `lease.SiteOf`. A candidate claims at once after a term of its own site
  and waits the takeover delay after a term of another site; a claim that
  lands within the delay anyway is released at once. A scaler restart and
  a Kafka outage leave the workload where it was.
- `lease.State.Last`, the holder of the latest term after it ended;
  `lease.Status.Ready` and `ready` in `/status`: whether the participant
  has known the lease state since it started.
- Stand: the `restart` scenario deletes the holder's scaler pod and checks
  that the workload's pods stay the same and the other cluster never
  starts; the `broker` scenario now ends with the lease on the site that
  held it before the outage.

### Changed

- Metric `kfklease_lease_holder_info`: the `site` of a holder id without a
  slash is the holder id itself, where it was empty.

- The scaler's lease metrics carry a `topic` label
  (`kfklease_holding{topic="orders"}`). Queries that select or sum without
  naming labels work as before; an alert on `sum(kfklease_holding)` over
  several leases should become `sum by (topic) (kfklease_holding)`.

### Fixed

- Stand: the `broker` scenario brings Kafka back without reissuing the
  stand's CA, so an mTLS participant can reconnect after the outage.
