# kfklease

Distributed leases and leader election on top of Apache Kafka®, with a
[KEDA](https://keda.sh) metric for lease-driven failover.

> Status: early design. Nothing here is ready for production yet.

## Why

Some services must run in exactly one place at a time: a single cloud, a single
region, a single cluster. When that place goes away, something has to decide
who takes over, and the workload has to start there.

`kfklease` uses a Kafka cluster that already spans those places as the
coordination layer:

- a **lease** is held by one owner at a time and has to be renewed;
- when the owner stops renewing, another candidate acquires the lease;
- the current lease state is exposed as a **metric for KEDA**, so a standby
  deployment scales from zero when it becomes the owner and back to zero when
  it loses the lease.

Failover through KEDA is the first use case, not the only one: the lease is a
general building block for leader election between Kafka clients.

## Design

The lease is a deterministic fold over a Kafka log: writing a record decides
nothing, and every reader reaches the same verdict on it. The rules, and why
they hold up under delays, freezes and a compacted log, are in
[docs/protocol.md](docs/protocol.md). The state machine lives in
[`lease/`](lease) and has no dependencies; `lease.Candidate` wraps it in a
Kafka client built on [franz-go](https://github.com/twmb/franz-go).

## Usage

```go
c, err := lease.NewCandidate(lease.Config{
	Brokers:     []string{"kafka:9092"},
	Topic:       "kfklease-demo",
	CreateTopic: true,
	TTL:         10 * time.Second,
})
if err != nil {
	log.Fatal(err)
}
go c.Run(ctx) // blocks until ctx is done; releases the lease on the way out

for range c.Changed() {
	if s := c.Status(); s.Holding {
		// Act as the holder. s.Epoch is the fencing token: pass it downstream
		// and let downstream reject anything with a smaller epoch.
	}
}
```

### KEDA

`kfklease-scaler` runs one participant per cluster and serves it to KEDA as an
[external scaler](https://keda.sh/docs/latest/scalers/external-push/). A
ScaledObject with `maxReplicaCount: 1` then runs the workload where the lease
is held and nowhere else:

```yaml
triggers:
  - type: external-push
    metadata:
      scalerAddress: kfklease-scaler.kfklease.svc:9090
```

The chart in [charts/kfklease](charts/kfklease) installs the scaler, its
service and that ScaledObject; one release per cluster:

```bash
helm install kfklease charts/kfklease -n kfklease --create-namespace \
  --set brokers=kafka:9092 --set topic=my-lease --set holderPrefix=eu-west \
  --set scaledObject.target=my-singleton
```

No image is published yet: build it with `make image` and push it to your
registry, then set `image.repository` and `image.tag`.

The binary is configured by flags or environment: `KFKLEASE_BROKERS`,
`KFKLEASE_TOPIC`, `KFKLEASE_TTL`, `KFKLEASE_HOLDER` (unique per process; the
chart uses the pod name). The two-cluster stand the chart is tested on is in
[test/e2e](test/e2e).

KEDA gives failover, not mutual exclusion: the old pod is still terminating
while the new one starts, and a `cooldownPeriod` above zero stretches that.
Workloads that must not overlap check the epoch downstream.

`Status().Holding` is a belief with a deadline, not a fact: a holder that
cannot reach Kafka stops believing after one TTL minus a margin, and the next
holder is elected only after the term runs out in the log, so the two never
overlap as long as the margin covers clock skew.

## Development

```bash
make test          # unit tests, no broker needed
make integration   # starts the Kafka container from test/e2e and runs the client tests
make image         # builds the scaler image
make generate      # regenerates the KEDA gRPC stubs from proto/ (buf via go run)
```

The end-to-end stand with two Kubernetes clusters is described in
[test/e2e](test/e2e).

## Roadmap

- [x] Lease protocol on a compacted topic: acquire, renew, release, fencing
- [x] Go library
- [x] KEDA external scaler
- [x] Container image
- [x] Helm chart
- [x] Failure-mode tests: crash, partition, freeze, broker restart
- [ ] Failure-mode tests: clock skew
- [ ] Published image and chart

## License

Copyright 2026 Ivan Abramov.

Licensed under the [Apache License, Version 2.0](LICENSE).

Apache Kafka and Kafka are either registered trademarks or trademarks of The
Apache Software Foundation in the United States and other countries. This
project is not affiliated with, endorsed by, or sponsored by The Apache
Software Foundation.
