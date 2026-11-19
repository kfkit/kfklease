---
name: kfklease
description: Adopt and operate kfklease, a lease on Apache Kafka with a KEDA external scaler that keeps a workload running in one Kubernetes cluster at a time. Use when installing its Helm chart, choosing the TTL, the takeover delay or the lease topic's settings, pointing a ScaledObject at the scaler, fencing a workload by epoch, moving a workload to another cluster on purpose (standby, handover), or working out why a failover was slow, did not happen, or ran the workload twice.
---

# Adopting kfklease

kfklease keeps a workload at N replicas in the one cluster that holds a lease and at zero in the
others. The lease is a compacted topic of Apache Kafka; a `kfklease-scaler` in each cluster takes
part in it and tells KEDA whether its cluster holds it.

The operator's guide is the source of truth; this skill is the order to do things in and the
mistakes to avoid. Read the section a step names before acting on it, and quote it to the user
rather than this file when the two differ:

- Install and operate: https://github.com/kfkit/kfklease/blob/main/docs/install.md
- Why it is safe: https://github.com/kfkit/kfklease/blob/main/docs/protocol.md
- What changed between releases: https://github.com/kfkit/kfklease/blob/main/CHANGELOG.md
- Chart values, with comments: https://github.com/kfkit/kfklease/blob/main/charts/kfklease/values.yaml

## Say this first

It is **failover, not mutual exclusion**. An old pod may still be terminating when the new one
starts, and a cluster that comes back from a crash restarts its stale pod until its own KEDA
scales it down. If the workload must never write from two places, it has to fence by epoch
(below). Ask the user which it is before anything else; do not promise exactly-once.

## Before installing, find out

1. **Where the brokers are.** They are the arbiter, so they must be reachable from every cluster
   and must outlive any one of them. Brokers inside one of the clusters defeat the purpose.
2. **How many brokers.** The chart creates the lease topic with three replicas. With fewer
   brokers the scaler exits with `replicationFactor 3, but the cluster has N brokers`: set
   `replicationFactor` (and read "The lease topic" for what that costs).
3. **KEDA 2.x in every cluster**, Helm 3.8 or later.
4. **A name for each cluster**: `site`. It is required, it must differ between clusters, and it
   ends up in holder ids on the topic.
5. **The workload**: its kind and name (anything with a scale subresource), how many replicas it
   runs while its cluster holds the lease, and how long it takes to stop.
6. **How the brokers authenticate clients**: TLS, mTLS or SASL. Secrets never go into values:
   certificates come as files from a mounted Secret, SASL credentials as environment variables
   from a Secret. See "Authentication".

## Install

One release per cluster, the same `topic` and `ttl` everywhere, a different `site` in each. Take
the command from "Install" in the guide and the current version from the releases page. The four
values every install needs: `brokers`, `topic`, `site`, `scaledObject.targetRef.name`.

The workload lives in the release's namespace. Its replica count belongs to KEDA from now on:
leave `replicas` out of its manifest or set it to 0, or the two will fight.

Then check, in every cluster, with the three commands under "Check" in the guide: the
ScaledObject is `ACTIVE True` in exactly one cluster, one scaler pod of that cluster logged
`acquired`, and `/status` answers. The scalers of the other clusters log `joined` and then
nothing much. That is correct.

## The numbers, and what they trade

- **`ttl`** (10 s). A failover to another cluster takes about `ttl` + `takeoverDelay` + KEDA's
  reaction + the pod start. The same value for every participant of a lease; changing it means
  stopping all scalers ("Changing the TTL").
- **`takeoverDelay`**. Leave it at 0: the chart derives `pollingInterval + cooldownPeriod +
  takeoverMargin` (95 s with the defaults) and refuses a smaller value. It is the time the old
  cluster's workload gets to stop before another cluster starts one. Shorten the failover by
  lowering `scaledObject.cooldownPeriod`, but keep it longer than a scaler restart, or a restart
  moves the workload.
- **`takeoverMargin`** (30 s) must cover the workload's `terminationGracePeriodSeconds`. A
  workload still stopping when the other cluster starts its own overlaps with it.
- **A hard failure of the partition leader** costs the holder its term at the default TTL: the
  epoch changes, the workload stays. Read "When the partition leader dies" before promising
  stable epochs, and before raising the TTL to avoid it.
- **`replicaCount`** (2). Leave it: one pod holds the term, both answer KEDA, and losing one
  moves nothing.
- **`scaledObject.fallback.enabled`** (true). Leave it: when KEDA cannot reach the scaler, the
  workload goes to zero instead of running on while another cluster takes the lease.

## Fencing by epoch

For a workload that must not write from two places ("Fencing by epoch" in the guide,
`examples/heartbeat` in the repository):

1. Before a write, `GET /status` on the scaler's Service (port 9091). No `site_holding`, no write.
   Use `site_holding` and `site_epoch`, not `holding` and `epoch`: the Service answers from any
   scaler pod, and only one of them holds the term.
2. Send `site_epoch` with the write.
3. The receiver rejects anything with an epoch below the highest it has seen. That is where
   exclusion happens.

`site_holding` is a belief with a deadline. A workload that cannot ask before every write asks at
least once per margin (`ttl/5`).

## Moving the workload on purpose

Set `standby=true` on the release of the cluster to leave ("Standby and the status command"). With
the handover (on by default) its scaler pauses the ScaledObject, waits until the target is scaled
to zero and no pod is left, and tells the others; another cluster takes over in about a TTL
instead of after `takeoverDelay`. Nothing moves back when standby is switched off: to move it
back, put the other cluster in standby.

`kfklease-scaler status`, run in a scaler pod, prints who holds each lease.

## Do not

- **Do not delete and recreate the lease topic** while scalers run. They notice, exit and
  restart, and the epochs start over from zero, so a receiver that fences by epoch must be told
  to forget the old ones. Stop all scalers first.
- **Do not alter the topic's settings** to "tidy up": `LogAppendTime` is what the protocol
  measures time with, and the scaler refuses a topic without it.
- **Do not set a boolean `KFKLEASE_*` variable to anything but `true` or `false`** in a manifest
  of your own (`yes`, an empty value): the scaler exits. The chart renders them correctly.
- **Do not run one scaler for two clusters, or give two clusters one `site`.** The site is what
  the takeover delay protects.
- **Do not put the brokers in one of the clusters** that compete for the lease.
- **Do not remove a `autoscaling.keda.sh/paused-replicas` annotation by hand** during a handover
  unless no other cluster could have taken over on the promise of it; see the troubleshooting
  table.
- **Do not read a quiet standby scaler as broken.** It follows the log and says nothing.

## When something is off

Go to "Troubleshooting" in the guide with the symptom; the table maps each one to what to look
at. The ones people hit first:

- A failover takes about two minutes: that is `ttl` + `takeoverDelay`, by design. See "The
  numbers" above.
- `acquired` and `no longer holder` alternate: renewals do not land in time. Look at the brokers
  and the link before touching the TTL.
- The workload ran in two clusters for a few seconds after a handover: it takes longer to stop
  than `takeoverMargin`, or a revived cluster ran its stale pod. Raise the margin; fence by epoch.
- KEDA logs `the lease state is not known yet`: a scaler that just started and has not read the
  log; it settles within seconds.

Alerts worth having are listed under "Metrics": the first one is
`sum by (topic) (kfklease_holding) != 1` across the clusters for longer than a TTL.
