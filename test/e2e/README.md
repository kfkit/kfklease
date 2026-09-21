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

Requires Docker with cgroup v2, `docker compose` and `kubectl`. About 2 GB of
memory for the whole stand.

```bash
./smoke.sh                     # up + readiness checks
kubectl --kubeconfig .out/k3s-a/kubeconfig.yaml get pods -A
kubectl --kubeconfig .out/k3s-b/kubeconfig.yaml get pods -A
docker compose down -v         # tear down, drop cluster state
```

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
