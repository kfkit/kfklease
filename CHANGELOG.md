# Changelog

All notable changes to this project are documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- An existing lease topic with more than one partition is logged as a
  warning at start. A lease lives in one partition (the scaler uses 0);
  participants of one lease that name different partitions each get a
  holder of their own.

### Fixed

- With a takeover delay, a scaler that starts on an empty lease topic no
  longer claims at once: a topic deleted and created again looks the same
  as a new one, and the cluster that held the lease on the old topic may
  still run its workload. Every scaler waits `takeoverDelay` from its
  start, whatever its site, as after a log whose history is gone. The
  first start of a new install waits the delay once.

## [0.5.0] - 2026-11-19

### Breaking

- Boolean `KFKLEASE_*` variables (`KFKLEASE_STANDBY`, `KFKLEASE_TLS`,
  `KFKLEASE_TLS_INSECURE_SKIP_VERIFY`, `KFKLEASE_CREATE_TOPIC`,
  `KFKLEASE_FLUSH_EVERY_RECORD`) accept what their flags accept (`1`,
  `TRUE`, `t`, ...). Anything else, an empty value included, stops the
  scaler with status 2; it used to be read as false without a word, so
  `KFKLEASE_STANDBY=1` left a cluster claiming. The chart renders `true`
  or `false` and is not affected; a manifest of your own with another
  value is.
- Library: `scaler.Workload.Pods` is `Remaining`, and is 0 only when the
  target is scaled to zero and no pod is left. An implementation of
  `Workload` other than `scaler.Kube` has to follow.
- Library: `lease.Candidate.Run` returns `lease.ErrLogReplaced` when the
  lease topic was deleted and created again; it used to keep running.

### Fixed

- A stop (`Run`'s context cancelled, a pod terminated) no longer writes a
  release behind a record that is still in flight. A holder cut off from
  the brokers with a renew on its way and stopped while it still believed
  could leave `renew, release` in the log after its term had run out and
  another participant had claimed; a participant that joined the log right
  there took the pair for an anchor, saw the lease as free and held it
  together with the real holder. The same pair could follow a release by
  the health gate or by standby. A release now waits for the record in
  flight and is written only before the deadline of the belief it ends,
  for a term the log shows as live.
- A participant claims only when it has read the log up to its end. It
  used to claim before its first fetch came back, and, for its own site or
  without a takeover delay, while still replaying history.
- Another site no longer claims before the natural end of a released term
  (plus two poll intervals), whatever the takeover delay: with a delay
  shorter than the TTL, a participant that missed the release still
  answered "my site holds" when the other site had the lease. The chart's
  delay (95 s by default) was long enough; a `TakeoverDelay` below the TTL
  was not.
- A silence between two records proves the lease free only when it is the
  margin longer than a TTL: with partition leaders whose clocks differ, a
  late reader could call a held lease free.
- A participant whose lease topic was deleted and created again, or whose
  partition lost acknowledged records, stops with `lease.ErrLogReplaced`
  (the scaler exits and restarts). It used to stay up, ready and holding
  nothing, for good.
- The handover writes its yield only when KEDA has scaled the target to
  zero and no pod is left. "No pod right now" was enough before, which is
  also what a rollout or a pod restart looks like for a moment.
- Docs: the `scalerAddress` in the README is the one the chart renders
  (`kfklease.kfklease.svc:9090`); the `/status` sample in docs/install.md
  shows every field, and a test keeps it so; the Records table in
  docs/protocol.md matches the wire format (`v`, and `ttl_ms` on a
  release). The scenarios are listed in test/e2e/README.md and the metrics
  in docs/install.md, and nowhere else.

### Changed

- `make lint` runs `shellcheck -x` over the stand's scripts. In CI a
  missing shellcheck fails it; elsewhere the scripts are skipped with a
  message.
- `make chart-test` runs the chart's tests and `make e2e` the stand from
  nothing to the last scenario; CI calls make for every job, so what it
  runs is what runs locally.
- Docs and the library's comments: a holder id has to be unique among the
  participants alive at one time, where they asked for one per process
  incarnation. It may repeat when a process restarts, as the chart's
  `<site>/<pod name>` does. The behaviour is the same.

### Added

- `View.AllowSkew`: how far apart two partition leaders' clocks may be.
  Participants pass their margin.
- Library: `lease.HandoverDriver`, the decisions of the handover without
  the I/O (when to pause, when to ask for the yield, when to lift the
  pause), which `scaler.Handover` now carries out; `lease.State.LastSite`;
  `scaler.LogLease`, the lease as the log says, shared by
  `kfklease-scaler status` and the metrics.
- Docs: "When the partition leader dies" in docs/install.md: what a hard
  failure of the lease partition's leader costs at the default TTL (the
  term and its epoch, not the workload), measured, and the two ways to
  ride through it.

## [0.4.0] - 2026-11-18

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
- Chart: two scaler pods per cluster by default (`replicaCount: 2`), where
  there was one, updated with `RollingUpdate` (one new pod at a time, none
  stopped before its successor is ready) where they were recreated, with a
  PodDisruptionBudget of `minAvailable: 1` and a preferred anti-affinity
  that spreads them over nodes. Every pod takes part in the lease for the
  site and answers KEDA for it. A cluster now runs twice the scaler pods,
  a drain waits for the second pod rather than evicting the only one, and
  an empty `affinity` is no longer empty in the pod. To keep one pod, set
  `replicaCount: 1`; the PodDisruptionBudget is then not rendered.
- Scaler: KEDA is told whether the scaler's *site* holds the lease
  (`lease.Status.SiteHolding`), where it was told whether this scaler pod
  holds it. With one scaler per site the answers are the same. A workload
  that fences by epoch behind the scaler's Service, as the heartbeat
  example does, must read `site_holding` and `site_epoch` from `/status`
  rather than `holding` and `epoch`, which are true on one pod of the site
  only. The health probe likewise runs on every pod while the site holds.
- Chart: with `handover.enabled` (on by default) the scaler pods run under
  a ServiceAccount of their own, named after the release, where they ran
  under the namespace's `default` ServiceAccount; the release also creates
  a Role and a RoleBinding. Image pull secrets attached to the `default`
  ServiceAccount no longer reach the scaler pods, so the image may fail to
  pull, and installing needs the right to create Roles in the namespace.
  List the pull secrets in `imagePullSecrets`, or set
  `handover.enabled=false` to keep the `default` ServiceAccount (standby
  then waits for `takeoverDelay`, as before).

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
  does not read a week of renewals, and `flush.messages=1`
  (`flushEveryRecord`, `-flush-every-record`), on by default; see Changed.
- The handover: a cluster put in standby stops its workload at once and
  tells the others when it is gone, so another cluster takes over about a
  TTL after the release rather than after `takeoverDelay`. The scaler sets
  KEDA's `autoscaling.keda.sh/paused-replicas: "0"` on the ScaledObject,
  waits until the target has no pods left, terminating ones included, and
  writes a new record kind, `yield`, to the lease topic; it lifts the pause
  when its cluster holds the lease again or another one does. Chart:
  `handover.enabled`, on by default, with a namespaced Role (get and patch
  on the release's ScaledObjects, get on their targets' `scale`
  subresource, list pods). Binary: `-scaled-object` /
  `KFKLEASE_SCALED_OBJECT`, one per topic. `/status` has `handover`.
  Library: `Status.Handover`, `Status.HandoverEpoch`, `Candidate.Yield`,
  `lease.Yield`, `scaler.Handover`, `scaler.Kube`. Scalers of earlier
  versions log "skipping record" for a yield and wait the full delay.
- Stand: the `standby` scenario expects the takeover well within the
  takeover delay, and the new `handover-crash` scenario kills the scalers
  between the pause and the yield.
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

- Standby mode: the participant follows the log and never claims; a holder
  stops believing at once and writes a release. Chart value `standby`,
  binary `-standby` / `KFKLEASE_STANDBY`, library `lease.Config.Standby`
  and `Candidate.SetStandby`, `lease.Status.Standby`, `standby` in
  `/status` and the metric `kfklease_standby_mode`. For maintenance, or to
  move the workload to another cluster on purpose.
- `kfklease-scaler status` prints who holds each lease, one JSON line per
  topic, read from the topic without taking part in the lease; in a
  scaler's pod it takes everything from the pod's environment. Library:
  `lease.Inspect`.
- Stand: the `standby` scenario puts the holding cluster in standby, checks
  that the workload moves without overlap and that `status` names the new
  site, and that nothing moves back once standby is off.

- Scaler replicas per site: every participant of a site that does not hold
  the lease believes its site does while it reads the holder's renewals
  fresh from the log, for the term less the margin, and needs a takeover
  delay to do so. Library: `lease.Status.SiteHolding`, `SiteEpoch`,
  `SiteDeadline`; `site_holding` and `site_epoch` in `/status`; the metric
  `kfklease_site_holding`. Chart: `replicaCount`, `podDisruptionBudget`.
- The scaler's readiness: a gRPC health service `ready` that serves once
  the scaler knows the state of every lease, which the chart's readiness
  probe asks for. A stopping scaler stops answering KEDA before it
  releases.
- Stand: the `holder-pod` scenario deletes the scaler pod that holds the
  lease, and the `bad-image` scenario upgrades a cluster's scaler to an
  image that does not pull; in both the workload keeps its pods and the
  other cluster never starts. `deploy.sh` takes `IMAGE_TAG` and `WAIT`.

### Changed

- A lease topic created by the chart or the binary gets `flush.messages=1`
  by default (`flushEveryRecord: true`, `-flush-every-record=true`): every
  record is fsynced before it is acknowledged. A renewal takes about half a
  millisecond longer at the median. Existing topics are not altered. To
  leave `flush.messages` to the broker, set `flushEveryRecord: false`
  (chart) or `-flush-every-record=false` / `KFKLEASE_FLUSH_EVERY_RECORD=false`
  (binary). The library's `TopicSettings.FlushEveryRecord` stays off unless
  set.

- The health gate counts per site: a participant whose site holds the
  lease and whose workload has not passed a probe for `failAfter` claims
  nothing for `cooldown`, holder or not, and a term that follows another of
  the same site within a TTL continues the time the gate counts from. A
  release by the gate therefore moves the lease to another site rather than
  to another scaler pod of the same one.

- Metric `kfklease_lease_holder_info`: the `site` of a holder id without a
  slash is the holder id itself, where it was empty.

- The scaler's lease metrics carry a `topic` label
  (`kfklease_holding{topic="orders"}`). Queries that select or sum without
  naming labels work as before; an alert on `sum(kfklease_holding)` over
  several leases should become `sum by (topic) (kfklease_holding)`.

### Fixed

- A participant that starts on a log read from offset 0, which a compacted
  topic usually is, reported `Ready` before it had read a record, so a
  scaler that had just started could answer KEDA "not holding" for a
  moment instead of an error. `Ready` now also waits until the log has
  been read to its end.

- Stand: the `broker` scenario brings Kafka back without reissuing the
  stand's CA, so an mTLS participant can reconnect after the outage.
