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

## Roadmap

- [ ] Lease protocol on a compacted topic: acquire, renew, release, fencing
- [ ] Go library
- [ ] KEDA external scaler / metrics endpoint
- [ ] Helm chart and container image
- [ ] Failure-mode tests (partitions, clock skew, broker loss)

## License

Copyright 2026 Ivan Abramov.

Licensed under the [Apache License, Version 2.0](LICENSE).

Apache Kafka and Kafka are either registered trademarks or trademarks of The
Apache Software Foundation in the United States and other countries. This
project is not affiliated with, endorsed by, or sponsored by The Apache
Software Foundation.
