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

### Fixed

- Stand: the `broker` scenario brings Kafka back without reissuing the
  stand's CA, so an mTLS participant can reconnect after the outage.
