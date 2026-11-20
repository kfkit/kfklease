#!/usr/bin/env bash
# Failover scenarios on the stand. Each one reports how long the takeover
# took and fails if two clusters ever run the workload at the same time.
# The workload is the heartbeat example: after each scenario its records
# say, by the broker's clock and to 200 ms, whether two epochs ever wrote
# at once and how long nobody wrote.
#
#   ./scenarios.sh            # all
#   ./scenarios.sh crash      # one: coldstart, restart, holder-pod, bad-image, crash, partition, pause, broker, replicas, scaler-down, standby, handover-crash, recreate
#
# Expects the stand to be up, the images built and the chart deployed to
# both clusters: ./run.sh (`make e2e`) does all of that and then runs this.
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=lib.sh
source ./lib.sh

# The workload's replica count in the holding cluster; the replicas
# scenario raises it for its duration.
want=1

# Another site waits TAKEOVER_DELAY seconds after the end of a term: the
# delay the chart rendered, which deploy.sh wrote down. A takeover by the
# other site comes a TTL after the holder's last renewal at the latest, plus
# the takeover delay; TAKEOVER leaves two TTLs on top for KEDA and the pod
# start. Neither is set before the first deploy.sh: selftest.sh runs this
# file that way, and a scenario that begins fails.
if [[ -s $TAKEOVER_DELAY_FILE ]]; then
  TAKEOVER_DELAY=$(<"$TAKEOVER_DELAY_FILE")
  TAKEOVER=$(( TTL + TAKEOVER_DELAY + 2 * TTL ))
fi
# A scenario ends with WATCH seconds in which nothing may move.
WATCH=$(( 2 * TTL ))
# A workload that stays is watched for KEDA's cooldown and ten seconds more.
STAY=$(( COOLDOWN + 10 ))

# running <cluster> prints the number of Running, not terminating, singleton pods.
running() {
  kc "$1" -n "$NS" get pods -l app=singleton -o json 2>/dev/null \
    | python3 -c 'import sys,json; print(sum(1 for p in json.load(sys.stdin)["items"] if p["status"]["phase"]=="Running" and not p["metadata"].get("deletionTimestamp")))' \
    || echo "?"
}

# runs <cluster> <count> succeeds if the cluster runs exactly <count>
# workload pods.
runs() {
  [[ $(running "$1") == "$2" ]]
}

# wait_running <cluster> <count> <timeout> waits until the cluster runs
# exactly <count> workload pods and prints how long it took.
wait_running() {
  local cluster=$1 want=$2 timeout=$3 t0
  t0=$(date +%s)
  wait_until "$timeout" "${cluster} did not reach ${want} pods within ${timeout}s" runs "$cluster" "$want" || return
  echo $(( $(date +%s) - t0 ))
}

# left <t0> <seconds> prints how many of <seconds> are left, counted from
# <t0>: the timeout of a wait whose time began before the wait did.
left() {
  echo $(( $1 + $2 - $(date +%s) ))
}

# some <count> succeeds if a running() count is a number above zero.
some() {
  [[ $1 != 0 && $1 != "?" ]]
}

# transition <from> <to> <timeout> watches both clusters until <from> runs no
# workload pod and <to> runs $want, and prints how long each took from now and
# how long both ran at once. Pods are polled every half second, so the
# overlap is a lower bound.
transition() {
  local from=$1 to=$2 timeout=$3 t0 now down="" up=""
  # Not date +%s.%N: BSD date has no %N.
  t0=$(python3 -c 'import time; print(time.time())')
  while true; do
    now=$(python3 -c "import time; print(round(time.time() - $t0, 1))")
    [[ -n $down ]] || [[ $(running "$from") != 0 ]] || down=$now
    [[ -n $up ]] || [[ $(running "$to") != "$want" ]] || up=$now
    if [[ -n $down && -n $up ]]; then
      python3 -c "print(f'down={$down}s up={$up}s overlap={max(0.0, round($down - $up, 1))}s')"
      return 0
    fi
    if python3 -c "import sys; sys.exit(0 if $now >= $timeout else 1)"; then
      fail "transition ${from} -> ${to} not complete within ${timeout}s (down=${down:-?} up=${up:-?})" || return
    fi
    sleep 0.5
  done
}

# holder prints the cluster running the workload, or nothing.
holder() {
  for c in "${CLUSTERS[@]}"; do
    if some "$(running "$c")"; then echo "$c"; return; fi
  done
}

# has_holder succeeds if a cluster runs the workload.
has_holder() {
  [[ -n $(holder) ]]
}

# watch_exclusive fails if both clusters run the workload at once in the
# next WATCH seconds.
watch_exclusive() {
  local until=$(( $(date +%s) + WATCH ))
  while (( $(date +%s) < until )); do
    if some "$(running "${CLUSTERS[0]}")" && some "$(running "${CLUSTERS[1]}")"; then
      fail "workload running in both clusters" || return
    fi
    sleep 1
  done
}

settle() {
  local h
  h=$(holder)
  [[ -n $h ]] || fail "no holder" || return
  echo "$h"
}

# scaler_status <cluster> prints the scaler's /status, through the API
# server's service proxy.
scaler_status() {
  kc "$1" get --raw "/api/v1/namespaces/${NS}/services/${RELEASE}:http/proxy/status" 2>/dev/null
}

# status_get <cluster> <python expression over r, the /status JSON> prints
# the expression, or nothing when the scaler does not answer.
status_get() {
  local st
  st=$(scaler_status "$1") || return 0
  python3 -c 'import sys, json; r = json.loads(sys.argv[1]); print(eval(sys.argv[2]))' "$st" "$2" 2>/dev/null || true
}

# held_epoch <cluster> prints the epoch the cluster's site holds, as the
# scaler pod the Service picks sees it, or -1.
held_epoch() {
  local e
  e=$(status_get "$1" 'r["site_epoch"] if r["site_holding"] else -1')
  [[ $e =~ ^[0-9]+$ ]] && echo "$e" || echo -1
}

# workload_uids <cluster> prints the UIDs of the workload pods, sorted.
workload_uids() {
  kc "$1" -n "$NS" get pods -l app=singleton -o jsonpath='{range .items[*]}{.metadata.uid}{"\n"}{end}' | sort
}

# now_rfc3339 prints the current time for the heartbeat report's --since.
now_rfc3339() {
  date -u +%Y-%m-%dT%H:%M:%SZ
}

# begin starts every scenario but coldstart, on a stand where one cluster
# runs the workload. It sets, in the scenario that calls it: h, that
# cluster; o, the other one; since, the time from which the heartbeat report
# is read at the end.
begin() {
  [[ -n ${TAKEOVER_DELAY:-} ]] || fail "no ${TAKEOVER_DELAY_FILE}: deploy.sh writes it" || return
  h=$(settle) || return
  o=$(other "$h")
  since=$(now_rfc3339)
}

# begin_staying is begin for a scenario whose workload must stay where it
# is. On top it sets uids, the workload's pods, and before, the epoch the
# site of h holds; it fails if the site holds none.
begin_staying() {
  begin || return
  uids=$(workload_uids "$h") || return
  before=$(held_epoch "$h")
  (( before >= 0 )) || fail "the site ${h} does not hold the lease" || return
}

# heartbeats <since> [reset] reads the heartbeat topic from <since>, prints
# what the workload actually did, and fails on any fenced (stale) record or
# any overlap between epochs: the workload-level mutual exclusion check.
# With reset the lease topic was recreated in the meantime and its epochs
# started from 0 again, below the ones before: the fence cannot hold across
# that, and only the overlap counts.
heartbeats() {
  local since=$1 reset=${2:-} out
  out=$(docker run --rm --network "$KFK_NETWORK" "$HEARTBEAT_IMAGE" report \
    --brokers "$KAFKA_ADDR:$KAFKA_PORT" --since "$since" --json)
  python3 - "$out" "$reset" <<'PY'
import sys, json
r = json.loads(sys.argv[1])
ms = lambda ns: f"{ns/1e6:.0f}ms"
spans = ", ".join(f"epoch {e['epoch']}@{e['cluster']} x{e['count']}" for e in r["epochs"])
print(f"   heartbeats: {r['records']} records, {spans}; stale {r['stale']}, overlap {ms(r['overlap'])}, gap {ms(r['gap'])}")
sys.exit(0 if (r["stale"] == 0 or sys.argv[2] == "reset") and r["overlap"] == 0 else 1)
PY
}

scenario_coldstart() {
  echo "== coldstart: exactly one cluster runs the workload"
  local t0 h since
  since=$(now_rfc3339)
  t0=$(date +%s)
  # The topic is new and empty, which looks the same as one deleted and
  # created again: nobody claims before the takeover delay has passed.
  wait_until $(( TAKEOVER_DELAY + 3 * TTL )) "nobody took the lease" has_holder || return
  h=$(holder)
  echo "   holder: ${h} after $(( $(date +%s) - t0 ))s"
  watch_exclusive
  runs "$(other "$h")" 0 || fail "standby is running the workload" || return
  heartbeats "$since"
  echo "   ok: standby idle for ${WATCH}s"
}

# stays <cluster> <uids>: for STAY seconds, the other cluster never starts
# the workload and <cluster> keeps the workload pods <uids>. Had the scaler
# said "not holding", or failed to answer, for that long, the workload would
# be gone by then.
stays() {
  local h=$1 uids=$2 o until
  o=$(other "$h")
  until=$(( $(date +%s) + STAY ))
  while (( $(date +%s) < until )); do
    runs "$o" 0 || fail "${o} started the workload" || return
    [[ $(workload_uids "$h") == "$uids" ]] || fail "the workload pods of ${h} changed" || return
    sleep 1
  done
}

# epoch_above <cluster> <epoch> succeeds if the cluster's site holds a
# later epoch, and leaves the one it holds in the caller's epoch.
epoch_above() {
  epoch=$(held_epoch "$1")
  (( epoch > $2 ))
}

# new_epoch <cluster> <before> <what> waits until the cluster's site holds an
# epoch above <before>, of a holder of that site, and prints the holder.
new_epoch() {
  local h=$1 before=$2 what=$3 timeout=$(( 3 * TTL )) t0 epoch holder
  t0=$(date +%s)
  wait_until "$timeout" "${h} holds no new epoch ${timeout}s after ${what}" epoch_above "$h" "$before" || return
  holder=$(status_get "$h" 'r["lease"]["holder"]')
  [[ $holder == "${h}/"* ]] || fail "the lease is held by ${holder}, not by site ${h}" || return
  echo "   ${holder} holds epoch ${epoch} (was ${before}) $(( $(date +%s) - t0 ))s after ${what}" >&2
  echo "$holder"
}

scenario_restart() {
  echo "== restart: delete every scaler pod of the holder's cluster; the workload stays where it is"
  local h o since uids before
  begin_staying || return
  # The holder releases on its way out and a new pod of the same site
  # claims at once; the other site would wait the takeover delay.
  kc "$h" -n "$NS" delete pod -l "$SCALER_PODS" --wait=false >/dev/null
  new_epoch "$h" "$before" "the pods were deleted" >/dev/null || return
  stays "$h" "$uids" || return
  heartbeats "$since"
  echo "   ok: same workload pods ${STAY}s on, ${o} never started"
}

scenario_holder_pod() {
  echo "== holder-pod: delete the scaler pod that holds the lease; another of its site takes over, the workload stays"
  local h o since uids before holder pod after
  begin_staying || return
  holder=$(status_get "$h" 'r["lease"]["holder"]')
  pod=${holder#"${h}/"}
  [[ $pod != "$holder" ]] || fail "the lease is held by ${holder}, not by site ${h}" || return
  # The other replica answered KEDA for the site all along; it claims as
  # soon as the release lands.
  kc "$h" -n "$NS" delete pod "$pod" --wait=false >/dev/null
  after=$(new_epoch "$h" "$before" "${pod} was deleted") || return
  [[ $after != "$holder" ]] || fail "the deleted pod still holds the lease" || return
  stays "$h" "$uids" || return
  heartbeats "$since"
  echo "   ok: same workload pods ${STAY}s on, ${o} never started"
}

# scaler_pods <cluster> prints one line per scaler pod: name, image, ready,
# and why its container waits, if it does.
scaler_pods() {
  kc "$1" -n "$NS" get pods -l "$SCALER_PODS" -o json | python3 -c '
import sys, json
for p in json.load(sys.stdin)["items"]:
    c = (p["status"].get("containerStatuses") or [{}])[0]
    ready = any(x["type"] == "Ready" and x["status"] == "True" for x in p["status"].get("conditions", []))
    waiting = c.get("state", {}).get("waiting", {}).get("reason", "-")
    print(p["metadata"]["name"], p["spec"]["containers"][0]["image"], ready, waiting)'
}

# pull_fails <cluster> <tag> succeeds if a scaler pod with the image tag
# <tag> is not ready because its image does not pull.
pull_fails() {
  grep -qE ":$2 False (ErrImageNeverPull|ErrImagePull|ImagePullBackOff)\$" <<<"$(scaler_pods "$1")"
}

scenario_bad_image() {
  echo "== bad-image: upgrade the holder's scaler to an image that does not pull; its old pods keep the site, the workload stays"
  local h o since uids before pods tag=does-not-exist
  begin_staying || return
  IMAGE_TAG=$tag WAIT=false ./deploy.sh "$h" >/dev/null
  wait_until 60 "no pod of the new image failed to pull" pull_fails "$h" "$tag" \
    || fail "the scaler pods of ${h}: $(scaler_pods "$h")" || return
  echo "   the rollout is stuck:" && scaler_pods "$h" | sed 's/^/     /'
  stays "$h" "$uids" || return
  pods=$(scaler_pods "$h")
  [[ $(grep -cF "${SCALER_IMAGE} True" <<<"$pods") == 2 ]] || fail "the old scaler pods did not stay ready: ${pods}" || return
  (( $(held_epoch "$h") >= before )) || fail "${h} no longer holds the lease" || return
  ./deploy.sh "$h" >/dev/null
  heartbeats "$since"
  echo "   ok: same workload pods ${STAY}s on, ${o} never started, rolled back"
}

scenario_crash() {
  echo "== crash: kill the holder's cluster"
  local h o t t0 since
  begin || return
  docker compose kill "$h" >/dev/null 2>&1
  t=$(wait_running "$o" "$want" "$TAKEOVER")
  echo "   ${o} took over ${t}s after ${h} died (ttl ${TTL}s, takeover delay ${TAKEOVER_DELAY}s)"
  docker compose up -d --wait "$h" >/dev/null 2>&1
  # The revived cluster still has the workload at one replica in its own
  # state, so its kubelet restarts the stale pod until KEDA is back up and
  # scales it down. That window is a property of Kubernetes, not of the
  # lease; it is why workloads that must not overlap check the epoch.
  t0=$(date +%s)
  kc "$h" -n keda rollout status deploy/keda-operator --timeout=180s >/dev/null
  kc "$h" -n "$NS" rollout status "deploy/$RELEASE" --timeout=120s >/dev/null
  t=$(wait_running "$h" 0 $(( 3 * TTL + COOLDOWN )))
  echo "   ${h} came back and ran its stale pod for $(( $(date +%s) - t0 ))s before KEDA scaled it down"
  # The revived scaler's site no longer holds the lease: it must not take
  # it back.
  watch_exclusive
  [[ $(holder) == "$o" ]] || fail "lease moved back to the revived cluster" || return
  heartbeats "$since"
  echo "   ok: ${h} stayed idle"
}

scenario_partition() {
  echo "== partition: cut the holder's cluster off from Kafka"
  local h o since
  begin || return
  ./partition.sh cut "$h"
  echo "   ${h} -> ${o}: $(transition "$h" "$o" "$TAKEOVER") (ttl ${TTL}s, cooldown ${COOLDOWN}s, takeover delay ${TAKEOVER_DELAY}s)"
  ./partition.sh heal "$h"
  watch_exclusive
  [[ $(holder) == "$o" ]] || fail "lease moved back after the heal" || return
  heartbeats "$since"
  echo "   ok: ${h} healed and stayed idle"
}

scenario_pause() {
  echo "== pause: freeze the holder's cluster for 2 x ttl"
  local h o t since
  begin || return
  docker compose pause "$h" >/dev/null 2>&1
  t=$(wait_running "$o" "$want" "$TAKEOVER")
  echo "   ${o} took over ${t}s into the freeze"
  sleep $(( 2 * TTL - t > 0 ? 2 * TTL - t : 0 ))
  docker compose unpause "$h" >/dev/null 2>&1
  # On waking up the frozen scaler is past its deadline: the pod must go.
  # Both pods exist during the freeze, but the frozen one is not running
  # in any useful sense; what matters is how fast it goes after the wake-up.
  t=$(wait_running "$h" 0 $(( 2 * TTL + COOLDOWN )))
  echo "   ${h} stopped its pod ${t}s after waking up"
  watch_exclusive
  [[ $(holder) == "$o" ]] || fail "lease moved back after the pause" || return
  heartbeats "$since"
  echo "   ok"
}

# held_or_started <cluster> <other> ends a wait when the cluster's site
# holds the lease, and also when the other cluster runs the workload: then
# it leaves that cluster's name in the caller's started.
held_or_started() {
  (( $(held_epoch "$1") >= 0 )) && return
  runs "$2" 0 || started=$2
  [[ -n $started ]]
}

scenario_broker() {
  echo "== broker: Kafka, the arbiter, is down for 2 x ttl; the lease stays with its site"
  local h o t0 back since started=""
  begin || return
  t0=$(date +%s)
  # A restart would be over before the holder's deadline on a fast machine
  # and the holder would keep its term; a stop of a known length makes the
  # outcome deterministic: the term runs out in the log.
  docker compose stop kafka >/dev/null 2>&1
  sleep $(( 2 * TTL ))
  # Not `start kafka`: that would rerun the certs service as well, and this
  # is about Kafka alone.
  docker compose up -d --no-deps --no-recreate kafka >/dev/null 2>&1
  # Both sites see the same expired term of ${h}: its site claims at once,
  # the other one waits the takeover delay. The time counts from the
  # beginning of the outage.
  wait_until "$(left "$t0" $(( 2 * TTL + TAKEOVER_DELAY )))" "${h} did not take the lease back after the broker came back" \
    held_or_started "$h" "$o" || return
  [[ -z $started ]] || fail "${started} started the workload" || return
  back=$(( $(date +%s) - t0 ))
  echo "   ${h} holds again ${back}s after the outage began (ttl ${TTL}s), its workload at $(running "$h")"
  watch_exclusive
  [[ $(holder) == "$h" && $(running "$o") == 0 ]] || fail "the workload moved to ${o}" || return
  heartbeats "$since"
  echo "   ok: the lease stayed with ${h}"
}

# redeploy <replicas> renders the chart into both clusters with the
# workload at <replicas> while holding.
redeploy() {
  local c
  for c in "${CLUSTERS[@]}"; do
    REPLICAS=$1 ./deploy.sh "$c" >/dev/null
  done
}

scenario_replicas() {
  echo "== replicas: N = 3 where the lease is held, 0 elsewhere, and a failover"
  local h o t since
  begin || return
  want=3
  redeploy "$want"
  t=$(wait_running "$h" "$want" $(( 6 * TTL )))
  echo "   ${h} scaled to ${want} in ${t}s without moving the lease"
  runs "$o" 0 || fail "standby is running the workload" || return
  ./partition.sh cut "$h"
  echo "   ${h} -> ${o}: $(transition "$h" "$o" "$TAKEOVER") (ttl ${TTL}s, ${want} replicas)"
  ./partition.sh heal "$h"
  watch_exclusive
  [[ $(holder) == "$o" && $(running "$o") == "$want" ]] || fail "${o} does not run ${want} replicas after the heal" || return
  heartbeats "$since"
  want=1
  redeploy "$want"
  t=$(wait_running "$o" "$want" $(( 6 * TTL )))
  echo "   ok: back to ${want} replica in ${t}s"
}

# no_endpoint <cluster> succeeds if kube-proxy refuses connections to the
# gRPC port of the scaler's Service.
no_endpoint() {
  # Not a pipe into grep -q: with pipefail, an early exit is a failure.
  grep -qF "${NS}/${RELEASE}:grpc has no endpoints" <<<"$(docker compose exec -T "$1" iptables -t filter -S)"
}

# scaler_service <cluster> on|off adds a selector term no pod has to the
# scaler's Service, or takes it away. Off, the Service has no endpoint and
# kube-proxy refuses new connections to it: what KEDA sees when the scaler
# pod is gone. Waits until kube-proxy has the rule.
scaler_service() {
  local cluster=$1
  case "$2" in
    off)
      kc "$cluster" -n "$NS" patch service "$RELEASE" --type=merge \
        -p '{"spec":{"selector":{"kfklease-e2e/scaler-down":"true"}}}' >/dev/null
      wait_until 30 "kube-proxy kept the scaler's endpoint" no_endpoint "$cluster" || return
      ;;
    on)
      kc "$cluster" -n "$NS" patch service "$RELEASE" --type=merge \
        -p '{"spec":{"selector":{"kfklease-e2e/scaler-down":null}}}' >/dev/null
      ;;
  esac
}

scenario_scaler_down() {
  echo "== scaler-down: the holder's scaler is gone, its workload pods are not"
  local h o since
  begin || return
  # The scaler goes without a word, as with a lost node: KEDA can no longer
  # reach it, it can no longer reach Kafka, and it is killed, which writes
  # no release and resets KEDA's open connection to it. The kubelet's
  # restart of the container reaches neither. Only the fallback can stop
  # the holder's workload now; without it this times out.
  scaler_service "$h" off
  ./partition.sh cut "$h"
  docker compose exec -T "$h" sh -c 'crictl stop --timeout 0 $(crictl ps -q --name "^scaler$")' >/dev/null
  echo "   ${h} -> ${o}: $(transition "$h" "$o" "$TAKEOVER") (ttl ${TTL}s, cooldown ${COOLDOWN}s)"
  ./partition.sh heal "$h"
  scaler_service "$h" on
  watch_exclusive
  [[ $(holder) == "$o" ]] || fail "lease moved back after the scaler returned" || return
  heartbeats "$since"
  echo "   ok: ${h} stopped its workload without its scaler and stayed idle"
}

# lease_status prints `kfklease-scaler status` for the lease, read from
# outside both clusters.
lease_status() {
  docker run --rm --network "$KFK_NETWORK" "$SCALER_IMAGE" status \
    -brokers "$KAFKA_ADDR:$KAFKA_PORT" -topic "$TOPIC" -ttl "${TTL}s" -log-level warn
}

# paused <cluster> prints the paused-replicas annotation of the workload's
# ScaledObject, empty when there is none.
paused() {
  kc "$1" -n "$NS" get scaledobject singleton -o jsonpath='{.metadata.annotations.autoscaling\.keda\.sh/paused-replicas}'
}

# pause_is <cluster> <replicas> succeeds if the ScaledObject is paused at
# <replicas>; with an empty <replicas>, if it is not paused.
pause_is() {
  [[ $(paused "$1") == "$2" ]]
}

# wait_unpaused <cluster> waits until the ScaledObject has no pause left.
wait_unpaused() {
  wait_until $(( 2 * TTL )) "the ScaledObject of $1 is still paused" pause_is "$1" ""
}

scenario_standby() {
  echo "== standby: put the holder's cluster in standby; the workload moves through the handover, and stays moved"
  local h o since st site tr up
  begin || return
  # The scaler restarts in standby; its predecessor releases on the way out.
  # A standby pod pauses the ScaledObject at zero, waits for the workload's
  # pods to go and yields, and the other site takes over from the yield
  # rather than after the takeover delay.
  STANDBY=true ./deploy.sh "$h" >/dev/null
  [[ $(status_get "$h" 'r["standby"]') == True ]] || fail "the scaler of ${h} is not in standby" || return
  tr=$(transition "$h" "$o" "$TAKEOVER") || return
  echo "   ${h} -> ${o}: ${tr} after the rollout (takeover delay ${TAKEOVER_DELAY}s)"
  up=$(sed -E 's/.*up=([0-9.]+)s.*/\1/' <<<"$tr")
  python3 -c "import sys; sys.exit(0 if $up < $TAKEOVER_DELAY - 10 else 1)" \
    || fail "${o} came up ${up}s after the rollout: it waited for the takeover delay, not the yield" || return
  # The other site holds the latest term: the pause is lifted.
  wait_unpaused "$h" || return
  st=$(lease_status) || fail "status command failed" || return
  echo "   status: ${st}"
  site=$(python3 -c 'import sys, json; r = json.loads(sys.argv[1]); print(r["site"] if r["held"] else "")' "$st")
  [[ $site == "$o" ]] || fail "status names site '${site}', want ${o}" || return
  watch_exclusive
  # Out of standby, the cluster claims again by the usual rules: the lease
  # is held, so nothing moves back.
  ./deploy.sh "$h" >/dev/null
  watch_exclusive
  [[ $(holder) == "$o" && $(running "$h") == 0 ]] || fail "the workload moved back to ${h}" || return
  heartbeats "$since"
  echo "   ok: ${o} holds, ${h} back out of standby and idle"
}

# kafka_topics <args...> runs kafka-topics.sh in the broker's container.
kafka_topics() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server "localhost:${KAFKA_PORT}" "$@"
}

# topic_configs prints the lease topic's own settings as --config
# arguments, one per line, for a topic created again just the same.
topic_configs() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server "localhost:${KAFKA_PORT}" \
    --describe --entity-type topics --entity-name "$TOPIC" | awk '/^  [a-z.]+=/ { print "--config"; print $1 }'
}

# create_topic <args...> creates the lease topic with the arguments
# topic_configs printed. It fails while the deletion is still under way.
create_topic() {
  kafka_topics --create --topic "$TOPIC" --partitions 1 --replication-factor 1 "$@" >/dev/null 2>&1
}

# scalers <cluster> STOP|CONT freezes the scaler processes of the cluster,
# or lets them go on. A frozen scaler reads nothing, writes nothing and
# answers neither KEDA nor its probes; the liveness probe gives it 30 s.
scalers() {
  # The binary is /app in the scaler image and in the workload's: find the
  # scaler containers by name. shellcheck: expanded in the node's shell.
  # shellcheck disable=SC2016
  # Fails if there was none to signal.
  docker compose exec -T "$1" sh -c 'n=0; for c in $(crictl ps -q --name "^scaler$"); do
    kill -"$0" "$(crictl inspect --output go-template --template "{{.info.pid}}" "$c")" && n=$((n + 1)); done
    [ "$n" -gt 0 ]' "$2"
}

# scaler_restarts <cluster> prints how often the scaler containers of the
# cluster restarted, all pods together.
scaler_restarts() {
  kc "$1" -n "$NS" get pods -l "$SCALER_PODS" \
    -o jsonpath='{range .items[*]}{.status.containerStatuses[0].restartCount}{"\n"}{end}' | awk '{ n += $1 } END { print n + 0 }'
}

# restarted_ready <cluster> <restarts> succeeds once the scaler containers
# of the cluster restarted <restarts> times in all and a scaler is ready.
restarted_ready() {
  (( $(scaler_restarts "$1") >= $2 )) && [[ $(status_get "$1" 'r["ready"]') == True ]]
}

scenario_recreate() {
  echo "== recreate: the lease topic is deleted and created again; the other cluster's scalers come back first and wait the takeover delay"
  local h o since restarts pods tr overlap configs
  begin || return
  mapfile -t configs < <(topic_configs)
  (( ${#configs[@]} > 0 )) || fail "no settings read from the lease topic" || return
  pods=$(kc "$o" -n "$NS" get pods -l "$SCALER_PODS" -o name | wc -l)
  restarts=$(( $(scaler_restarts "$o") + pods ))
  # The holder's scalers are frozen until the other cluster's are back on
  # the new topic, so that the order is not a race: theirs is the view that
  # becomes certain first. The holder's workload runs on meanwhile, as it
  # would while its scalers restart.
  scalers "$h" STOP || fail "no scaler of ${h} to freeze" || return
  kafka_topics --delete --topic "$TOPIC" >/dev/null
  # Deleted alone, the topic leaves the scalers retrying: created again, it
  # has another topic id, and the other cluster's scalers stop on it and
  # restart onto the new, empty log.
  wait_until 30 "the lease topic was not created again" create_topic "${configs[@]}" \
    || { scalers "$h" CONT; return 1; }
  wait_until 30 "${o}'s scalers did not restart onto the new topic" restarted_ready "$o" "$restarts" \
    || { scalers "$h" CONT; return 1; }
  scalers "$h" CONT
  echo "   ${o}'s scalers are back on the new topic; ${h}'s go on and stop too"
  # Neither site's term is in the new log: the other cluster may claim only
  # once the takeover delay since its start has passed, by when the old
  # holder's workload is gone.
  tr=$(transition "$h" "$o" "$TAKEOVER") || return
  echo "   ${h} -> ${o}: ${tr} (takeover delay ${TAKEOVER_DELAY}s)"
  overlap=$(sed -E 's/.*overlap=([0-9.]+)s.*/\1/' <<<"$tr")
  [[ $overlap == 0.0 ]] || fail "both clusters ran the workload for ${overlap}s" || return
  watch_exclusive
  [[ $(holder) == "$o" && $(running "$h") == 0 ]] || fail "the workload is not in ${o} alone" || return
  heartbeats "$since" reset
  echo "   ok: ${o} runs the workload, ${h} idle"
}

# finalizer <cluster> add|remove holds the workload's pods in Terminating
# once deleted, or lets them go.
finalizer() {
  local cluster=$1 patch pod
  case "$2" in
    add) patch='{"metadata":{"finalizers":["kfklease-e2e/hold"]}}' ;;
    remove) patch='{"metadata":{"finalizers":null}}' ;;
  esac
  for pod in $(kc "$cluster" -n "$NS" get pods -l app=singleton -o name); do
    kc "$cluster" -n "$NS" patch "$pod" --type=merge -p "$patch" >/dev/null
  done
}

# pods_left <cluster> prints the number of workload pods, terminating ones
# included.
pods_left() {
  kc "$1" -n "$NS" get pods -l app=singleton -o name | wc -l
}

scenario_handover_crash() {
  echo "== handover-crash: kill the scalers between the pause and the yield; the handover resumes and leaves no pause behind"
  local h o since begun t0 t
  # The start in epoch seconds as well: BSD date cannot parse the RFC 3339
  # time back (it has no -d).
  begin || return
  begun=$(date +%s)
  # A finalizer keeps the workload's pod in Terminating: the handover
  # paused the ScaledObject and waits for the pod, and no yield goes out.
  finalizer "$h" add
  # Not waiting for the rollout: the handover starts when its last old pod
  # releases, and the scalers go down right after the pause.
  STANDBY=true WAIT=false ./deploy.sh "$h" >/dev/null
  # One time for both: paused and stopped within two TTLs of the rollout.
  t0=$(date +%s)
  wait_until "$(left "$t0" $(( 2 * TTL )))" "${h} did not pause its ScaledObject" pause_is "$h" 0 || return
  wait_until "$(left "$t0" $(( 2 * TTL )))" "${h} did not stop its workload" runs "$h" 0 || return
  (( $(pods_left "$h") > 0 )) || fail "the finalizer did not hold the pod" || return
  echo "   ${h} paused and its pod is terminating; killing its scaler pods"
  kc "$h" -n "$NS" delete pod -l "$SCALER_PODS" --grace-period=0 --force >/dev/null 2>&1
  kc "$h" -n "$NS" rollout status "deploy/$RELEASE" --timeout=120s >/dev/null
  # A pod that still terminates is a pod that may still run: no yield, so
  # the other site does not start.
  for _ in 1 2 3 4 5; do
    runs "$o" 0 || fail "${o} started while ${h}'s pod was still terminating" || return
    sleep 1
  done
  pause_is "$h" 0 || fail "the restarted scalers lifted the pause during the handover" || return
  finalizer "$h" remove
  t0=$(date +%s)
  t=$(wait_running "$o" "$want" "$TAKEOVER") || return
  echo "   ${o} took over ${t}s after ${h}'s last pod went, $(( t0 - begun ))s after the standby began (takeover delay ${TAKEOVER_DELAY}s)"
  wait_unpaused "$h" || return
  watch_exclusive
  ./deploy.sh "$h" >/dev/null
  [[ -z $(paused "$h") && $(holder) == "$o" ]] || fail "${h} out of standby: paused '$(paused "$h")', holder $(holder)" || return
  heartbeats "$since"
  echo "   ok: the restarted scalers finished the handover, no pause left"
}

if (( $# == 0 )); then
  set -- coldstart restart holder-pod bad-image crash partition pause broker replicas scaler-down standby handover-crash recreate
fi
# A failing command ends the run through set -e. A shell error does not:
# bash drops the whole command it happens in, here the loop, and goes on
# with the next one. So the loop clears `current` only when a scenario
# returns, and a run that ends with a scenario still current has failed.
current=""
on_exit() {
  local rc=$?
  [[ -z $current ]] || fail "scenario ${current} did not finish (exit ${rc})" || true
}
trap on_exit EXIT
for s in "$@"; do
  current=$s
  "scenario_${s//-/_}"
  current=""
done
[[ -z $current ]] || exit 1
echo "all scenarios passed"
