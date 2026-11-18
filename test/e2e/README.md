# End-to-end stand

Two independent single-node Kubernetes clusters and one Kafka broker, all from
a single compose file. The clusters compete for a lease; Kafka is the arbiter.

```
kafka   apache/kafka, KRaft, 172.30.0.10        (host: localhost)
          :9092  plaintext                       (host :19092)
          :9094  SASL_PLAINTEXT, PLAIN + SCRAM   (host :19094)
          :9095  mTLS, client certificate required (host :19095)
k3s-a   k3s + KEDA,          172.30.0.11        (API: https://127.0.0.1:6443)
k3s-b   k3s + KEDA,          172.30.0.12        (API: https://127.0.0.1:6444)
```

The `certs` service writes a CA, the broker certificate, a client
certificate and the broker's `jaas.conf` (PLAIN user `kfklease`) to
`.out/certs` on every start. The integration tests use all three listeners;
on the stand, `deploy.sh` connects k3s-a over mTLS from a mounted Secret
and k3s-b over SASL PLAIN with username and password from a Secret as
environment variables, so the two clusters share the lease across both
kinds of authentication the chart offers.

Kafka runs as a plain container outside both clusters on purpose: killing,
pausing or partitioning a cluster must never take the arbiter down with it.
The lease protocol only sees a bootstrap address, so how the broker is managed
does not matter here.

## Usage

Requires Docker with cgroup v2, `docker compose`, `kubectl` and `envsubst`.
About 2 GB of memory for the whole stand.

```bash
./smoke.sh                     # up + readiness checks
./build-image.sh               # scaler image, imported into both clusters
./deploy.sh k3s-a; ./deploy.sh k3s-b   # the chart plus the test workload
./scenarios.sh                 # failover scenarios, see below
docker compose down -v         # tear down, drop cluster state
```

`deploy.sh` renders `charts/kfklease` (helm, or the `alpine/helm` image when
helm is not installed) with site `<cluster>`, the chart's two scaler pods, `cooldownPeriod` 30 s and
`takeoverMargin` 5 s (so a takeover delay of 40 s), and the `singleton`
deployment from `manifests/` as the scale target: one replica while the
cluster holds the lease (`REPLICAS=3 ./deploy.sh ...` for three), zero
otherwise. `singleton` runs the heartbeat
example from `examples/`, built into `kfklease-heartbeat:dev` by
`build-image.sh`.

## Scenarios

`scenarios.sh` runs them all or by name. Each fails if the workload ever
runs in both clusters at once, and, since the workload is the heartbeat
example, if two lease epochs ever wrote at the same time or a stale record
got past the fence, measured by the broker's clock to 200 ms. Scenarios
with TTL 10 s, `pollingInterval: 5`, `cooldownPeriod: 30`, `takeoverDelay`
40 s (the chart's defaults are 60 and 95: the stand keeps the order
"scaler restart < cooldown < delay" at a shorter scale):

| Scenario    | What happens                                   | Result                                   |
|-------------|------------------------------------------------|------------------------------------------|
| `coldstart` | both clusters start                            | one holder within 1 s, standby idle      |
| `restart`   | every scaler pod of the holder's cluster is deleted | a new pod holds a new epoch for the same site within seconds; the workload's pod UIDs are the same 40 s later and the other cluster never starts |
| `holder-pod` | the scaler pod that holds the lease is deleted, the other scaler pod of its cluster stays | the other pod holds a new epoch for the same site within seconds; the workload's pod UIDs are the same 40 s later and the other cluster never starts |
| `bad-image` | the holder's cluster is upgraded to a scaler image tag that does not pull (`IMAGE_TAG=does-not-exist`) | the new pod waits in `ErrImageNeverPull`, the two old pods stay ready and keep the site; the workload's pod UIDs are the same 40 s later and the other cluster never starts; then rolled back |
| `crash`     | `docker compose kill` the holder's cluster     | takeover after TTL + the takeover delay; the revived cluster restarts its stale pod until its KEDA is back, then stays idle |
| `partition` | holder's cluster cut off from Kafka            | holder's pod down after its deadline + the cooldown, standby's up after TTL + the delay, no overlap |
| `pause`     | holder's cluster frozen                        | takeover after TTL + the delay into the freeze; on wake-up the frozen pod goes |
| `broker`    | Kafka stopped for 2 × TTL                      | the holder's site claims again as soon as the broker is back, the other site waits the delay: the lease stays where it was |
| `replicas`  | both clusters redeployed with `REPLICAS=3`, then the holder cut off from Kafka | holder at 3, standby at 0; after the cut the holder's pods all go before the standby's three run; back to 1 at the end |
| `scaler-down` | the holder's scaler Service loses its endpoint, the holder is cut off from Kafka and the scaler is killed (`crictl stop --timeout 0`, no release) | KEDA's fallback (`replicas: 0`) scales the holder's workload to zero on the scaler error once the cooldown has passed; the standby takes over after the term runs out and the delay |
| `standby` | the holder's cluster is redeployed with `STANDBY=true` | the handover pauses its ScaledObject, its pod goes and it yields: the other cluster runs the workload well within the delay after the rollout, no overlap; the pause is lifted, `status` names the new site, and nothing moves back once the cluster leaves standby |
| `handover-crash` | as `standby`, but a finalizer keeps the workload pod in `Terminating`, and the scaler pods are force-deleted after the pause | the other cluster does not start while the pod terminates; the restarted scalers keep the pause, yield once the finalizer goes, and the pause is gone after the takeover |

Takeover timing is TTL plus the takeover delay plus KEDA's reaction and pod
start; the standby never claims before the term in the log runs out and the
delay has passed, by which time the old holder's KEDA has scaled its
workload down. Pods are polled once a second, so a
pod-level overlap shorter than that can slip past that check; what matters
is the heartbeat check, which sees every write.

## Failure injection

| Failure                    | Command                                   |
|----------------------------|-------------------------------------------|
| Cluster dies               | `docker compose kill k3s-a`               |
| Cluster freezes            | `docker compose pause k3s-a` / `unpause`  |
| Cluster loses Kafka        | `./partition.sh cut k3s-a` / `heal k3s-a` |
| Scaler gone, no release    | `docker compose exec k3s-a sh -c 'crictl stop --timeout 0 $(crictl ps -q --name "^scaler$")'` |
| Arbiter is down            | `docker compose stop kafka` / `up -d --no-deps --no-recreate kafka` (`start` reruns the certs service and issues a new CA) |

`partition.sh` drops packets to Kafka inside the node's network namespace, so
connections hang and time out the way they do in a real partition. Do not use
`docker network disconnect` instead: bridge networks are not reliably isolated
from each other on every Docker engine, and flannel's vxlan backend panics
when an interface disappears.

## Notes

- Pods inside k3s cannot resolve compose service names, so Kafka advertises a
  static IP. Use `172.30.0.10:9092` from pods and `localhost:19092` from the
  host.
- Locally built images: `docker save <image> -o images/<name>.tar` before the
  stand starts. k3s imports every tarball from that directory on boot.
- KEDA is installed by the k3s helm-controller from `manifests/keda.yaml`.
