# Lease protocol

Status: draft. The state machine and the Kafka client described here are
implemented and tested in [`lease/`](../lease).

## The problem with Kafka

A lease needs an atomic "take it if it is free". Kafka has no conditional
produce, so two candidates can always both write "I take it".

The protocol does not try to prevent that. Instead, writing a record decides
nothing: **the state of the lease is a deterministic function of the log**.
Every participant reads the same partition in the same order and applies the
same rules, so everyone reaches the same verdict on every record, including
the writer, who learns the outcome of its own record by reading it back. The
partition's total order does the job of the missing compare-and-swap.

## Topic

- One lease per partition, all its records under one constant key, so that
  compaction keeps the latest record and nothing else is needed.
- `message.timestamp.type=LogAppendTime`. The only clock in the protocol is
  the broker's. Client wall clocks are never compared with anything.
- `cleanup.policy=compact`, with `min.compaction.lag.ms` well above the TTL so
  that the recent past is always complete.

Broker timestamps may step backwards when the partition leader changes. The
fold clamps them to be monotone. A new leader whose clock runs *ahead* by δ
lets a takeover happen δ early, so the holder's safety margin must cover the
clock skew between brokers.

## Records

| Kind      | Fields                 | Meaning                                  |
|-----------|------------------------|------------------------------------------|
| `claim`   | `holder`, `ttl_ms`     | start a new term                         |
| `renew`   | `holder`, `epoch`, `ttl_ms` | extend the term `epoch`             |
| `release` | `holder`, `epoch`      | end the term `epoch` now                 |

`holder` is unique per process incarnation, not per host: a restarted process
must not inherit the term of its previous life, because it has no idea when
that term runs out.

The TTL is a property of the lease, identical for all participants. Records
asking for more than the lease TTL are rejected by everyone.

## Fold

State: `holder`, `epoch`, `expires`. For a record with broker timestamp `T`:

- `claim`: granted iff the lease is not held at `T`. Then `holder` is the
  writer, **`epoch` is the offset of the claim**, `expires = T + ttl`.
- `renew`: valid iff the lease is held at `T` by the same holder and epoch.
  Then `expires = T + ttl`. A renew never starts or revives a term.
- `release`: valid under the same condition; frees the lease at `T`.
- Anything else has no effect.

Using the claim's offset as the epoch makes it a fencing token that is unique,
grows with every term, and is known to anyone who sees the record, without
any history. (Deleting and recreating the topic resets it. Don't.)

## Writer rules

1. Act only on a *certain* view of the log (see below).
2. One produce request in flight at a time.
3. The belief "I hold the lease" starts and is extended only when the
   writer's own fold accepts its own record, read back from the log.
4. The local deadline of that belief is `send time + ttl − margin`, measured
   on the monotonic clock, where *send time* is taken before the request is
   sent. The record lands after it was sent, so the term in the log always
   outlives the belief.
5. Past the deadline the writer is not the holder, whatever the log says. It
   renews only while it believes, and it claims otherwise.

Rule 4 is what makes freezes safe: a process suspended between the decision
and the send wakes up with a deadline in the past. Its record may still land
and may even be accepted, but the writer no longer acts on that term.

A candidate may claim whenever it likes. A premature claim is simply rejected
by the fold; it costs a record, not safety.

## The margin

Two clocks can disagree with the broker's and stretch the belief past the
term:

- a new partition leader whose clock runs ahead of the old one's by δ
  lets a takeover happen δ early;
- a client clock running slow by a factor of 1−d makes a deadline of
  `ttl − margin` last `(ttl − margin) / (1 − d)` of real time.

So the margin has to cover both, and the produce and consume latency of a
renewal on top:

	margin ≥ 2 × broker clock skew + ttl × client clock drift + latency

The default of `ttl / 5` (2 s for a 10 s lease) covers brokers within a
second of each other and a client clock off by a few percent. The holder
also drops its belief the moment it reads another holder's accepted claim,
which closes the window whenever it can still read the log; the margin is
for when it cannot.

## Reading a truncated log

Compaction removes history, so a reader usually starts in the middle. It
cannot judge the first records it sees: a `renew` may be stale, a `claim` may
have been rejected. A view therefore starts **uncertain** and becomes certain
only through a state it can prove:

- **Start.** The partition was read without gaps from offset 0: the lease was
  free before its first record.
- **Silence.** No `claim` or `renew` for a whole TTL means the lease is free:
  any live term needs a valid record within the last TTL. This applies to the
  gap between two consecutive records (by broker timestamps) and to the end of
  the log (the reader sits at the end for a TTL of real time and nothing
  arrives).
- **Anchor.** A record of a term that is followed by a `renew` or `release` of
  the same term was valid. By rules 2, 3 and 5 the writer decided on the later
  record after reading the earlier one back, while still believing the term
  was live. Had the earlier record been rejected, that belief would already
  have run out. From a proven record the state is known exactly, and the
  reader replays everything after it.

Any hole in the partition offsets (the compacted part of the log, a seek)
sends the view back to uncertain: silence cannot be measured across records
that were skipped.

All three rules yield the true state, so readers that started at different
points agree as soon as they are certain. The anchor rule depends on writers
following the rules above; the protocol assumes participants are correct, not
merely honest about who they are.

## What is tested

The writer rules live in one place, `lease/agent.go`, which does no I/O and
reads no clock: the Kafka client feeds it records, fetch results and
acknowledgements, and the tests feed it the same things from a fake log.

`lease/sim_test.go` runs real agents against a shared log with lagging
consumers, produce requests that land up to two TTLs late, delivery timeouts
after which the record lands anyway, acknowledgements that arrive after the
record was read back, processes frozen for up to three TTLs, nodes cut off
from the log for up to two TTLs, and participants that join late and see
only a tail of the log with holes in it. Clocks are perfect and the margin
is zero, so any overlap is a protocol bug. It checks that

- no two agents ever believe they hold the lease at the same instant;
- a reader started at any offset, once certain, has exactly the state of a
  reader that saw the whole log;
- candidates do not spam the log with claims they can see are hopeless.

A second run of the same simulation gives the partition leaders clocks off
by up to a fifth of a TTL and the clients clocks off by up to ten percent,
and checks both halves of the margin rule above: with a zero margin some
runs overlap, with the margin from the formula none do.

`lease/agent_test.go` pins down the rules one at a time: the deadline counts
from the send time whatever the landing and read-back times; the belief needs
both the acknowledgement and the read-back, in either order; a record the
client gave up on is never taken for the one in flight; a term that ended in
the log ends the belief; nothing is written on an uncertain view; silence is
measured only while fetches succeed; a hole makes the view uncertain; a
release drops the belief before the record is written.

Breaking a rule in `view.go` or `agent.go` (trusting a lone renew, reviving a
term with a late renew, halving the silence period, taking the deadline from
the acknowledgement time, believing on the acknowledgement alone, matching own
records by kind instead of offset, ignoring holes, claiming without looking)
fails at least one of these tests; that was checked by mutation, by hand.

## The client

`lease.Candidate` runs the rules above against a real partition:

- it consumes from the log start offset; a start offset of 0 means the
  history is complete and the view starts certain, otherwise it starts
  uncertain. A hole in the offsets (compaction, a lagging consumer) sends
  the view back to uncertain;
- it rejects a topic without `LogAppendTime` on the first record it sees;
- it measures silence at the end of the log only while fetches succeed, from
  the later of the last record and the moment it caught up; a fetch error
  restarts the measurement;
- it matches its own records by the offset the broker acknowledges, so a
  record it gave up on (delivery timeout) can never be mistaken for the one
  in flight;
- a stop releases the lease: the belief is dropped first, the record is
  written second.

`lease/candidate_integration_test.go` runs it against a broker: one holder
among two candidates, handover after a release (tens of milliseconds) and
after a crash (about one TTL, never before the crashed holder's deadline),
a late reader agreeing with the holder, epochs growing across terms.

`test/e2e` runs the whole thing, scaler and KEDA included, on two k3s
clusters: cold start, crash, partition, freeze and broker restart, with a
check that the workload never runs in both clusters at once.

## Not covered yet

- System suspend: on Linux the monotonic clock does not advance during
  suspend, so a suspended holder can wake up still believing. Container
  pauses are covered by the freeze scenario on the stand.
- Several leases sharing a partition.
