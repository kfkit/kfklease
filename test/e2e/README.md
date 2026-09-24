# End-to-end stand

Two independent single-node Kubernetes clusters and one Kafka broker, all from
a single compose file. The clusters compete for a lease; Kafka is the arbiter.

```
kafka   apache/kafka, KRaft, 172.30.0.10:9092   (host: localhost:19092)
k3s-a   k3s + KEDA,          172.30.0.11        (API: https://127.0.0.1:6443)
k3s-b   k3s + KEDA,          172.30.0.12        (API: https://127.0.0.1:6444)
```

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
./deploy.sh k3s-a; ./deploy.sh k3s-b
./scenarios.sh                 # failover scenarios, see below
docker compose down -v         # tear down, drop cluster state
```

`manifests/kfklease.yaml` deploys into each cluster a `kfklease-scaler`
(one lease participant, holder id `<cluster>/<pod>`), a `singleton`
deployment and a ScaledObject that keeps `singleton` at one replica while
the cluster holds the lease and at zero otherwise.

## Scenarios

`scenarios.sh` runs them all or by name. Each fails if the workload ever
runs in both clusters at once. Results with TTL 10 s, `pollingInterval: 5`,
`cooldownPeriod: 0`:

| Scenario    | What happens                                   | Result                                   |
|-------------|------------------------------------------------|------------------------------------------|
| `coldstart` | both clusters start                            | one holder within 1 s, standby idle      |
| `crash`     | `docker compose kill` the holder's cluster     | takeover after 9–12 s; revived cluster stays idle |
| `partition` | holder's cluster cut off from Kafka            | holder's pod down at ~10–12 s, standby's up ~1 s later, no overlap |
| `pause`     | holder's cluster frozen for 2 × TTL            | takeover ~10 s into the freeze; on wake-up the frozen pod goes within 1 s |

Takeover timing is TTL plus KEDA's reaction and pod start; the standby
never claims before the term in the log runs out, and the old holder stops
believing a margin earlier than that.

## Failure injection

| Failure                    | Command                                   |
|----------------------------|-------------------------------------------|
| Cluster dies               | `docker compose kill k3s-a`               |
| Cluster freezes            | `docker compose pause k3s-a` / `unpause`  |
| Cluster loses Kafka        | `./partition.sh cut k3s-a` / `heal k3s-a` |
| Arbiter restarts           | `docker compose restart kafka`            |

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
