# Lease protocol

Status: draft. The state machine described here is implemented and tested in
[`lease/`](../lease); the Kafka client around it is not written yet.

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

- All records of one lease live in one partition (message key = lease name).
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

`lease/sim_test.go` runs participants that follow the writer rules against a
shared log with lagging consumers, produce requests that land up to two TTLs
late, and processes frozen for up to three TTLs. Clocks are perfect and the
margin is zero, so any overlap is a protocol bug. It checks that

- no two participants ever believe they hold the lease at the same instant;
- a reader started at any offset, once certain, has exactly the state of a
  reader that saw the whole log.

Breaking any single rule (trusting a lone renew, letting a late renew revive a
term, halving the silence period, basing the deadline on read-back time,
granting claims on a held lease, skipping the replay after an anchor) makes
the simulation fail.

## Not covered yet

- Clock drift between a client's monotonic clock and the broker's, and clock
  skew between brokers: both go into the margin, which is not modelled.
- The Kafka client: consuming, detecting offset holes, producing with a
  bounded delivery timeout, measuring silence at the end of the log.
- Several leases sharing a partition.
