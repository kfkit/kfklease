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
  that the recent past is always complete, and `segment.ms` equal to it, so
  that the active segment, which is never compacted, does not hold a week of
  renewals.
- Replicated, with `min.insync.replicas` above one and
  `unclean.leader.election.enable=false`: the epoch is the offset of the
  accepted claim, and an acknowledged record must not be lost or replaced by
  a different one at the same offset. `EnsureTopic` creates the topic so
  (three replicas, two in sync, by default) and warns about an existing
  topic that is not.

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
| `yield`   | `holder`, `epoch`      | the writer's site has stopped its workload after the term `epoch`; see [Handover](#handover) |

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
- `release`: valid under the same condition; frees the lease at `T`
  (`expires = T`).
- Anything else has no effect, a `yield` included: it is for the writer
  rules and moves neither the state nor the view's clock.

The state also keeps `last`, the holder of the latest term, after the term
ends by expiry or release, and `expires` then says when it ended. Neither
changes what the fold accepts; the site rules below read them. A view that
became certain through silence does not know the last holder: `last` is
empty and `expires` is the latest moment the unknown term can have ended,
the last record it saw plus a TTL.

Using the claim's offset as the epoch makes it a fencing token that is unique,
grows with every term, and is known to anyone who sees the record, without
any history. (Deleting and recreating the topic resets it. Don't. The
participants notice and stop; see [The client](#the-client).)

## Writer rules

1. Act only on a *certain* view of the log (see below), and claim only on
   one that has read the log up to its end offset: a reader that is behind
   knows an old state of the lease, and one that has read nothing yet knows
   none.
2. One produce request in flight at a time. A record the client gave up on
   (a delivery timeout) may still land, unread: its term gets no further
   record, and the belief in it lasts until its deadline.
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

A release is a record of a term like a renew, and goes out under the same
conditions: nothing else in flight, before the deadline of the belief it
ends, and for a term the writer's view shows as its own and live. A writer
that gave its belief up while a renew was in flight waits for the renew; if
that is rejected, or the deadline passes first, there is no release and the
term runs out by itself. This is what the anchor rule below rests on: two
records of one term in the log mean the writer read the first one back and
found it accepted.

A claim that turns out premature is simply rejected by the fold; it costs a
record, not safety.

The same freedom covers giving up. With a health gate
(`lease.Config.Health`), a holder whose workload has not been reported
healthy for `FailAfter`, counted from the later of the start of its term
and the last healthy report, drops its belief at once, writes a release
when rule 2 lets it, and claims nothing for `Cooldown`. Dropping a belief
early and claiming later are both on the safe side of rules 4 and 5; the
fold does not change.

Standby (`lease.Config.Standby`, `Candidate.SetStandby`) is the same
freedom without a clock: a participant in standby never claims, and one
that holds the lease when standby is switched on drops its belief at once
and writes a release when rule 2 lets it. A claim of its own that lands
after the switch is never believed and is released. `lease.Inspect` reads a
lease this way, as a participant in standby that stops once its view is
certain, and so never writes.

### Sites

The same freedom carries the lease from a process to a site. A holder id
is `<site>/<name>`, the site being everything before the last slash, a
cluster under the chart (an id without a slash is a site of its own). With
a takeover delay `D` (`lease.Config.TakeoverDelay`):

6. A candidate whose site held the latest term claims as soon as the lease
   is free, as before.
7. A candidate of another site claims only once the broker's clock, as it
   estimates it from the last record it read, is `D` past the end of that
   term, and never before the term's *natural end*, the end its last
   accepted claim or renew gave it, plus two poll intervals: a release ends
   a term early in the log, but a participant of that site that read the
   last renewal and missed the release goes on answering for its site
   until then (rule 9). A view that knows the release and not the renewals
   takes a TTL after the release for the natural end. A term whose holder
   the view does not know counts as another site's; if not a single record
   of an existing history is left, the delay counts from the silence that
   made the view certain, on the local clock.
8. A claim of another site that lands within `D` of the end of the previous
   term is not believed and is released at once. It was sent in time, but
   landed late, after a short term of that site. The fold accepted it, so
   the release is what frees the lease for the others.

Rules 6 to 8 only ever hold a claim back or give a term up, so safety is
untouched; what they buy is time: a site that loses its term, by expiry or
by release, has `D` to stop its workload before another site may start one.
Under the chart `D` is at least KEDA's `pollingInterval + cooldownPeriod`
plus a margin for the workload to stop, and the cooldown is longer than a
scaler restart, so a restarted scaler claims for its site again before KEDA
gives the workload up. A new scaler answers KEDA with an error, not with "not
holding", until its view has been certain once, with the log read to its
end (`Status.Ready`).

### Several participants per site

A site may run several participants, the scaler replicas of one cluster.
One holds the term and renews it; the others follow the log, and with
rule 6 the first of them to claim takes the term over at once when it
ends. For KEDA every one of them answers for the site
(`Status.SiteHolding`), so that losing one pod neither blinds KEDA nor
moves anything:

9. A participant believes its site holds the lease while it holds it
   itself, or while it has read an accepted `claim` or `renew` of another
   participant of its site, *fresh*, and the term that record gives in the
   log has not run out: the record's term counts from the moment it was
   read, for the TTL less the margin, as the holder counts its own from
   the send. Anything that ends the term in the log, a release or another
   holder, or a view that is no longer certain, ends the belief at once.
   A reader cut off from Kafka sees no more renewals, and the belief runs
   out with the term. In standby there is no site belief, and without a
   takeover delay there is none either.
10. A record is fresh when the reader sat at the end of a certain log at
   its previous loop iteration, and that iteration was no more than two
   poll intervals ago (half a renewal interval). A record read after a
   freeze, a lost connection or from a backlog may be any age and extends
   nothing.

Rule 4 bounds a holder's belief by the send of its record, which comes
before the landing. Rule 9 can only start from the read, which comes after
it: a site belief outlives the term in the log by however long the record
took to reach the reader. Rule 10 keeps that to a fetch round trip, and
what makes it safe across sites is rule 7: another site claims only `D`
after the term ended, by the broker's clock, and never before its natural
end plus the two poll intervals of rule 10, so a site belief would have to
lag by more than that to meet another site's term. That is why rule 9 needs
a delay. The simulation checks it with a delay of half a TTL too, where the
delay covers almost nothing: there the natural end keeps another site away
from a term that was released while a participant of its site did not look,
and rule 10 keeps a reader that wakes up from a pause from answering for a
term long gone. Without either, that case overlaps.

The health gate counts per site, too. Every participant whose site holds
the lease probes the workload, and one that finds it unhealthy for
`FailAfter` claims nothing for `Cooldown`, holder or not; a term that
follows another of its site within a TTL continues the run the gate
counts from. So when the holder releases because the workload cannot run,
the others of its site do not take the term over for it, and the lease
goes to another site after `D`.

### Handover

The takeover delay is a guess at how long the old site's workload takes to
stop. A site in standby can do better: stop it on purpose and say when it
is gone.

11. A participant in standby whose site held the latest term reports a
   handover (`Status.Handover`) once that term has ended in the log, by its
   estimate of the broker's clock, and until `D` has passed since, on a
   certain view caught up with the end offset, while no yield for the term
   is in the log. Whoever drives the site's workload stops it then and,
   once nothing of it runs, terminating pods included, the participant
   writes `yield` with the term's epoch (`Candidate.Yield`).
12. A yield counts only on a certain view, when it names the epoch of the
   view's state, its writer is of the site of that term's holder, the term
   had ended at the yield's timestamp, and the view knows the term's
   natural end: the end the term's last accepted `claim` or `renew` gave it,
   folded on a certain view. A view that became certain again by an anchor
   after a hole may have missed a renewal and does not count yields for
   that term. For a candidate of another site a counting yield ends rule
   7's delay at the later of the yield's timestamp and the natural end
   plus two poll intervals; rule 8 judges a landing claim by the same time.

A yield only ever brings a claim forward, so what has to hold is the
promise it carries: the old site's workload is down and stays down until
the new term has started. A workload comes back only if KEDA hears "my site
holds". The holder's own belief ended before its release; another
participant of its site (rule 9) believes until at most the natural end of
the term plus a fresh read's lag of two poll intervals, less the margin,
and the later bound of rule 12 waits that out. Under the chart the scaler
also pauses the ScaledObject at zero (KEDA's `paused-replicas`), which
holds whatever any replica answers, and lifts the pause only when its own
site holds again, by its own claim or a fresh site belief that starts
after the pause, or when another site holds the latest term. A claim of the
old site that lands after the yield simply starts a new term; the fold
accepts at most one of the two sites' claims, and a yield names a term that
is then no longer the latest.

A yield that is lost changes nothing: the other sites wait `D`. One that
lands late, after a new term, names a term that is not the latest and is
ignored. One written by another site, for another term or before the term
ended is ignored by rule 12. One written by the right site while its
workload still runs is a lie the protocol cannot catch; `Candidate.Yield`
is to be called only once nothing of the workload runs.

## The margin

Two clocks can disagree with the broker's and stretch the belief past the
term:

- a new partition leader whose clock runs ahead of the old one's by δ
  lets a takeover happen δ early;
- a client clock running slow by a factor of 1−d makes a deadline of
  `ttl − margin` last `(ttl − margin) / (1 − d)` of real time.

The readers meet the first of the two as well: timestamps step when the
leader changes, so the gap between two records can read longer than it
was. A view counts a silence between two records only when it is the margin
longer than a TTL (`View.AllowSkew`).

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
  gap between two consecutive records (by broker timestamps, with the margin
  on top for what two leaders' clocks may be apart) and to the end of the
  log (the reader sits at the end for a TTL and the margin of real time and
  nothing arrives).
- **Anchor.** A record of a term that is followed by a `renew` or `release` of
  the same term was valid. By rules 2, 3 and 5 and the rule for a release,
  the writer decided on the later record after reading the earlier one back,
  while the deadline that record gave it had not passed. Had the earlier
  record been rejected, there would be no such deadline; had the client
  given up on it, the term would have got no further record. From a proven record the state is known exactly, and the
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
from the log for up to two TTLs, processes stopped at any moment, a record
in flight or not, that come back as a new process under the same holder id,
and participants that join late and see only a tail of the log with holes
in it. Clocks are perfect and the margin is zero, so any overlap is a
protocol bug. It checks that

- no two agents ever believe they hold the lease at the same instant, and
  no agents of two sites ever believe their sites hold it at the same
  instant (rule 9, with the takeover delay of the sites and with one of
  half a TTL);
- a reader started at any offset, once certain, has exactly the state of a
  reader that saw the whole log: in every run of every flavour below, from
  every offset of its log;
- candidates do not spam the log with claims they can see are hopeless.

A second run of the same simulation gives the partition leaders clocks off
by up to a fifth of a TTL and the clients clocks off by up to ten percent,
and checks both halves of the margin rule above: with a zero margin some
runs overlap, with the margin from the formula none do, and the readers
agree from every offset although the leaders' clocks step every other TTL.

`lease/agent_test.go` pins down the rules one at a time: the deadline counts
from the send time whatever the landing and read-back times; the belief needs
both the acknowledgement and the read-back, in either order; a record the
client gave up on is never taken for the one in flight; a term that ended in
the log ends the belief; nothing is written on an uncertain view; silence is
measured only while fetches succeed; a hole makes the view uncertain; a
stop drops the belief before the release is written, waits for the record
in flight, writes no release behind a renew that is still out when the
belief would have run out, and releases a claim that lands after it; no
release follows a renew that was rejected, a renew the client gave up on, a
deadline that has passed or a term the log shows as another's; a claim
waits for the log to be read to its end; an offset already passed that
holds another record stops the participant; the health gate counts from
the start of the term, drops the belief when it fires, waits for the
record in flight before releasing, and holds claims back for the
cooldown.

A third run of the simulation turns the health gate on, with workloads that
go up and down every twenty TTLs or so: no overlap, no holder believing
longer than `FailAfter` past its last healthy report or the start of its
term, no claim within the cooldown, and the gate firing several times per
run.

A fourth run puts the nodes in two sites with a takeover delay of three
TTLs, and cuts a whole site off from the log now and then for up to twice
that: no overlap; no claim of another site accepted within the delay after
the end of a term, by the broker's clock, unless it is given up without a
single renewal (rule 8); and many takeovers by the same site within the
delay, after releases and after terms that ran out. `agent_test.go` pins
rules 6 to 8 down one case at a time: a release and an expiry by either
site, a holder known only through silence, a log whose history is gone, a
reader behind the end offset, a claim that landed early.

A fifth run, with sites, switches nodes in and out of standby every twenty
TTLs or so: no overlap, no node in standby believing or claiming, and
standby releasing terms in every run on average. `agent_test.go` pins down
that standby never claims, releases a term held, waits for the record in
flight before releasing, and never believes a claim that lands after the
switch.

A sixth run adds the handover. Each site has a workload that runs while
one of its nodes says the site holds and for 1.5 TTLs after, and a pause
that takes it down within a TTL, now and then within six TTLs. Sites switch
in and out of standby node by node, about a TTL apart, as a rolling update
does; a node in standby with a handover pauses the workload and asks for
the yield once it is down; a pause is lifted when a node starts to say its
site holds, or when another site holds the latest term. Yields get lost
and land late like any record, and nodes write bogus yields: for a term of
another site, for their own term while they hold it, for a term that is not
the latest. The workloads of two sites never run at once, the beliefs keep
the properties above, and in 100 runs about 300 yields bring about 60
takeovers within the delay, the fastest 0.2 s after the term's end.
`agent_test.go` pins rules 11 and 12 down: a yield after a release, after
an expiry, later than the natural end; yields by another site, by the
claimant's site, of another term, written while the term was held, landing
after a new term, read on an uncertain view, read after a hole; and on the
writing side a yield on request, once, not for another term, not out of
standby, not after `D`, not while a site holds.

Breaking a rule in `view.go` or `agent.go` (trusting a lone renew, reviving a
term with a late renew, halving the silence period, taking the deadline from
the acknowledgement time, believing on the acknowledgement alone, matching own
records by kind instead of offset, ignoring holes, claiming without looking,
dropping the takeover delay, the own-site exemption, the end-offset check,
the wait on a log without history, the natural end of a released term, a
claim before the log is read to its end, a release past its deadline or
behind a record in flight, the seal on a term with a record given up on,
the skew allowed for a silence between two records, the release of an
early claim, a claim
in standby, a term kept in standby or a claim in flight believed in
standby; for the handover: the site, epoch and ended checks on a yield, the
natural-end bound, forgetting the term on an uncertain view, a handover out
of standby, a yield asked for before the workload is down, lifting the
pause for as long as a site belief lasts rather than when it starts, the
last one in `scaler/handover_test.go`) fails
at least one of these tests; that was checked by mutation, by hand.

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
  written second, after the record in flight, if any, was read back, and
  not at all once the belief would have run out. With sites, the release
  lets the next pod of the same site claim at once;
- it stops with `ErrLogReplaced` when the log is not the one it read
  before: an offset it has passed holds a newer record, an own record is
  acknowledged where another was read, or the broker no longer knows the
  topic id. That is a topic deleted and created again, or a partition that
  lost acknowledged records; a new process starts from the log as it is;
- `Candidate.Yield` writes a yield through the loop, if the handover it
  names still stands when the loop picks it up.

`lease/candidate_integration_test.go` runs it against a broker: one holder
among two candidates, handover after a release (tens of milliseconds) and
after a crash (about one TTL, never before the crashed holder's deadline),
a holder stopped with a renew stalled on its way, whose release does not
land behind the next holder's claim, two participants that stop when their
topic is recreated,
a late reader agreeing with the holder, epochs growing across terms, a
holder switched to standby handing over from its release, a handover
through a yield well within the takeover delay, and `Inspect` reading a
held and a released lease without writing.

`test/e2e` runs the whole thing, scaler and KEDA included, on two k3s
clusters: cold start, a scaler restart that keeps the workload's pods, crash,
partition, freeze, a broker outage that ends with the lease on the same site,
N replicas, a scaler that is gone, a cluster put in standby with the
handover, and a handover whose scalers are killed between the pause and the
yield, with a check that the workload never runs in both clusters at once.

## Not covered yet

- System suspend: on Linux the monotonic clock does not advance during
  suspend, so a suspended holder can wake up still believing. Container
  pauses are covered by the freeze scenario on the stand.
- A topic that is deleted and not created again: the participants keep
  trying, as they do without a connection.
- Several leases sharing a partition.
