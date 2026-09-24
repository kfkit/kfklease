# Working on kfklease

Rules for agents and humans alike. They come from what went wrong or
turned out to matter while building this; keep them short and keep them
true.

## What this is

Distributed leases on top of Apache Kafka® and a KEDA external scaler that
runs a workload in one cluster at a time. The map:

| Path | What | Depends on |
|---|---|---|
| `lease/view.go` | the fold: log records in, lease state out. Pure | nothing |
| `lease/agent.go` | the writer rules: when to claim, renew, release, believe. Pure, takes the clock as an argument | view |
| `lease/candidate.go` | the Kafka client around the agent, and `Config`, `Status`, `Auth` | franz-go |
| `lease/auth.go` | TLS, mTLS, SASL | franz-go |
| `scaler/` | KEDA external-push gRPC server, HTTP `/metrics` and `/status` | lease |
| `cmd/kfklease-scaler` | the binary: flags with `KFKLEASE_*` fallbacks | scaler |
| `charts/kfklease` | one release per cluster | |
| `examples/heartbeat` | a workload that fences by epoch, and the report that checks it | scaler's `/status` |
| `test/e2e` | the stand: two k3s clusters with KEDA and one Kafka, five failure scenarios | Docker |
| `docs/protocol.md` | why the lease is safe. Change it when the rules change | |

## Rules

- **Public repository, English only.** Code, comments, commits, docs, PR
  text. Write "Apache Kafka®" and "for Apache Kafka"; the word kafka goes
  in no name of ours (trademark of the ASF). Apache-2.0; copied code carries
  its attribution in `NOTICE`.
- **Never commit to `main`.** Branch, PR, CI green, merge. The ruleset
  requires it and allows no bypass. Conventional commits, signed.
- **Keep the pure core pure.** `view.go` and `agent.go` take time as an
  argument and do no I/O. That is what lets the simulation drive the real
  code; any rule that lands in `candidate.go` instead is a rule the tests
  cannot reach.
- **DRY, and Go as Go is written.** One place for each rule. Builtin
  `min`/`max`, `errors.Join`, `slices`, table tests. `gofmt`, `go vet`,
  `shellcheck -x` on every script.
- **Secrets never touch values or code.** The chart points at Kubernetes
  Secrets: certificates as mounted files (re-read per connection), SASL
  credentials as environment variables. No `fromEnv` toggles, no
  `extraEnv`: one way for each kind, easy to connect.
- **Measure before optimising CI.** An image cache for the stand was
  built, measured at 25 s of a six-minute job, and removed. The scenarios
  are the time; nothing else is.

## Tests, honestly

The question to ask of every test: does it exercise the code that ships,
or a copy of its rules? The simulation once checked rules written in the
test itself; the agent was extracted so it checks the agent.

- `make test`: unit tests and the simulation (`lease/sim_test.go`: lagging
  consumers, late landings, delivery timeouts, frozen and partitioned
  nodes, late joiners with compaction holes, skewed and drifting clocks).
  Safety, agreement, and a bound on noise.
- `make integration`: against the Kafka container from the stand, over
  plaintext, SASL PLAIN/SCRAM and mTLS. Wrong credentials must fail, not
  hang.
- `test/e2e/scenarios.sh`: cold start, crash, partition, freeze, broker
  outage, each ending with the heartbeat check: no two epochs ever wrote at
  once, nothing stale got past the fence, by the broker's clock.
- When a rule changes, break it on purpose and confirm a test fails. Every
  rule in `agent.go` and `view.go` has been checked this way; keep it so.
- A scenario that depends on timing is a scenario that will fail on a slow
  runner. Make the outcome deterministic (stop Kafka for 2 × TTL, not
  `restart`) or wait for the condition that actually matters (the KEDA
  operator on a revived cluster), never for a fixed number of seconds.

## The stand

`test/e2e`: `smoke.sh`, `build-image.sh`, `deploy.sh k3s-a`, `deploy.sh
k3s-b`, `scenarios.sh`, `docker compose down -v`. k3s-a authenticates over
mTLS from a mounted Secret, k3s-b over SASL from environment variables, so
every scenario also covers both ways of handing secrets to the scaler.

- Do not tear the stand down in the same command as the scenarios: a
  failure with no logs left is a failure you get to reproduce.
- `docker network disconnect` does not partition anything here (bridge
  networks are not isolated from each other on every engine) and makes
  flannel's vxlan backend panic. `partition.sh` drops packets with
  iptables inside the node; flannel runs `host-gw`.
- Pods cannot resolve compose service names; Kafka advertises a static IP.
- A topic created a moment ago has no leader for a few hundred
  milliseconds: retry, don't fail. `kadm.CreateTopic` reports
  `TOPIC_ALREADY_EXISTS` in `err`, not in the response.
- The `apache/kafka` image ignores per-listener JAAS environment variables
  and, with SCRAM enabled and no `KafkaServer` JAAS entry, does not start.
  `gen-certs.sh` writes a static `jaas.conf`; `KAFKA_OPTS` points at it.
- A revived cluster restarts its stale workload pod until its own KEDA is
  back, 20–30 s. That is Kubernetes, not the lease; it is why fencing
  exists.
- `helm` is not installed here; `deploy.sh` falls back to the
  `alpine/helm` image and renders the chart by absolute path (a relative
  path from `test/e2e` is taken for a repository name).
- Scripts are bash. On a zsh host, `$var` does not word-split: run them,
  do not paste them.

## CI and releases

- `ci.yml` runs unit, integration, image, chart and e2e; all five are
  required checks and the branch must be up to date with `main`. Stacked
  PRs that touch the same file (the README roadmap, say) conflict after
  the first merges; resolve by hand, `update-branch` cannot.
- `setup-go` installs exactly the `go` directive's version with
  `GOTOOLCHAIN=local`; the `toolchain` line in `go.mod` is what it honours.
- Releases: a `chore: vX.Y.Z` PR bumps `charts/kfklease/Chart.yaml` and
  the README, then a signed tag `vX.Y.Z` on `main`. `release.yml` builds
  the multi-arch image and the chart into GHCR, signs the image with cosign
  and writes the release. The actions in `release.yml` run only on tags, so
  a Dependabot bump of them is verified by the next release.
- GHCR packages are public because the organisation allows public package
  creation; a new package would still start private if that setting were
  off.

## Before opening a PR

```bash
make lint && make test
make integration          # needs Docker
test/e2e/build-image.sh && test/e2e/smoke.sh && test/e2e/deploy.sh k3s-a && test/e2e/deploy.sh k3s-b && test/e2e/scenarios.sh
docker compose -f test/e2e/compose.yaml down -v
```

Say in the PR what was verified and how, with numbers; say what was not.
