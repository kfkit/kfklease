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
helm is not installed) with holder prefix `<cluster>` and the `singleton`
deployment from `manifests/` as the scale target: one replica while the
cluster holds the lease, zero otherwise.

## Scenarios

`scenarios.sh` runs them all or by name. Each fails if the workload ever
runs in both clusters at once. Results with TTL 10 s, `pollingInterval: 5`,
`cooldownPeriod: 0`:

| Scenario    | What happens                                   | Result                                   |
|-------------|------------------------------------------------|------------------------------------------|
| `coldstart` | both clusters start                            | one holder within 1 s, standby idle      |
| `crash`     | `docker compose kill` the holder's cluster     | takeover after 9–12 s; the revived cluster restarts its stale pod for ~30 s until its KEDA is back, then stays idle |
| `partition` | holder's cluster cut off from Kafka            | holder's pod down at ~10–12 s, standby's up ~1 s later, no overlap |
| `pause`     | holder's cluster frozen for 2 × TTL            | takeover ~10 s into the freeze; on wake-up the frozen pod goes within 1 s |
| `broker`    | Kafka stopped for 2 × TTL                      | holder's pod down within a TTL; a holder again ~10 s after the broker is back |

Takeover timing is TTL plus KEDA's reaction and pod start; the standby
never claims before the term in the log runs out, and the old holder stops
believing a margin earlier than that. Pods are polled once a second, so an
overlap shorter than that can slip past the check; the lease itself is
checked to the millisecond by the simulation and the integration tests.

## Failure injection

| Failure                    | Command                                   |
|----------------------------|-------------------------------------------|
| Cluster dies               | `docker compose kill k3s-a`               |
| Cluster freezes            | `docker compose pause k3s-a` / `unpause`  |
| Cluster loses Kafka        | `./partition.sh cut k3s-a` / `heal k3s-a` |
| Arbiter is down            | `docker compose stop kafka` / `start kafka` |

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
- CI runners start empty, so `cache-images.sh` keeps every image the stand
  needs as a tarball: the compose images in `.cache/`, KEDA and the test
  workload in `images/` for k3s to import. Both directories live in the
  Actions cache. Locally Docker's own cache does the job; do not bother.
- KEDA is installed by the k3s helm-controller from `manifests/keda.yaml`.
