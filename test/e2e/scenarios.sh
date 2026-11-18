#!/usr/bin/env bash
# Failover scenarios on the stand. Each one reports how long the takeover
# took and fails if two clusters ever run the workload at the same time.
# The workload is the heartbeat example: after each scenario its records
# say, by the broker's clock and to 200 ms, whether two epochs ever wrote
# at once and how long nobody wrote.
#
#   ./scenarios.sh            # all
#   ./scenarios.sh crash      # one: coldstart, restart, holder-pod, bad-image, crash, partition, pause, broker, replicas, scaler-down, standby, handover-crash
#
# Expects the stand to be up (./smoke.sh), the image built (./build-image.sh)
# and the manifests deployed to both clusters (./deploy.sh k3s-a; ./deploy.sh k3s-b).
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=lib.sh
source ./lib.sh

# The workload's replica count in the holding cluster; the replicas
# scenario raises it for its duration.
want=1

# A takeover by the other site comes a TTL after the holder's last renewal
# at the latest, plus the takeover delay; TAKEOVER leaves two TTLs on top
# for KEDA and the pod start.
TAKEOVER=$(( TTL + TAKEOVER_DELAY + 2 * TTL ))

fail() {
  echo "FAILED: $*" >&2
  return 1
}

# running <cluster> prints the number of Running, not terminating, singleton pods.
running() {
  kc "$1" -n "$NS" get pods -l app=singleton -o json 2>/dev/null \
    | python3 -c 'import sys,json; print(sum(1 for p in json.load(sys.stdin)["items"] if p["status"]["phase"]=="Running" and not p["metadata"].get("deletionTimestamp")))' \
    || echo "?"
}

# wait_running <cluster> <count> <timeout> waits until the cluster runs
# exactly <count> workload pods and prints how long it took.
wait_running() {
  local cluster=$1 want=$2 timeout=$3 t0
  t0=$(date +%s)
  while true; do
    if [[ $(running "$cluster") == "$want" ]]; then
      echo $(( $(date +%s) - t0 ))
      return 0
    fi
    if (( $(date +%s) - t0 >= timeout )); then
      echo "FAILED: ${cluster} did not reach ${want} pods within ${timeout}s" >&2
      return 1
    fi
    sleep 1
  done
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
  t0=$(date +%s.%N)
  while true; do
    now=$(python3 -c "import time; print(round(time.time() - $t0, 1))")
    [[ -n $down ]] || [[ $(running "$from") != 0 ]] || down=$now
    [[ -n $up ]] || [[ $(running "$to") != "$want" ]] || up=$now
    if [[ -n $down && -n $up ]]; then
      python3 -c "print(f'down={$down}s up={$up}s overlap={max(0.0, round($down - $up, 1))}s')"
      return 0
    fi
    if python3 -c "import sys; sys.exit(0 if $now >= $timeout else 1)"; then
      echo "FAILED: transition ${from} -> ${to} not complete within ${timeout}s (down=${down:-?} up=${up:-?})" >&2
      return 1
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

# watch_exclusive <seconds> fails if both clusters run the workload at once.
watch_exclusive() {
  local until=$(( $(date +%s) + $1 ))
  while (( $(date +%s) < until )); do
    if some "$(running k3s-a)" && some "$(running k3s-b)"; then
      echo "FAILED: workload running in both clusters" >&2
      return 1
    fi
    sleep 1
  done
}

settle() {
  local h
  h=$(holder)
  if [[ -z $h ]]; then
    echo "FAILED: no holder" >&2
    return 1
  fi
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

# heartbeats <since> reads the heartbeat topic from <since>, prints what
# the workload actually did, and fails on any fenced (stale) record or any
# overlap between epochs: the workload-level mutual exclusion check.
heartbeats() {
  local since=$1 out
  out=$(docker run --rm --network "${COMPOSE_PROJECT}_kfk" kfklease-heartbeat:dev report \
    --brokers "$KAFKA_ADDR:$KAFKA_PORT" --since "$since" --json)
  python3 - "$out" <<'PY'
import sys, json
r = json.loads(sys.argv[1])
ms = lambda ns: f"{ns/1e6:.0f}ms"
spans = ", ".join(f"epoch {e['epoch']}@{e['cluster']} x{e['count']}" for e in r["epochs"])
print(f"   heartbeats: {r['records']} records, {spans}; stale {r['stale']}, overlap {ms(r['overlap'])}, gap {ms(r['gap'])}")
sys.exit(0 if r["stale"] == 0 and r["overlap"] == 0 else 1)
PY
}

scenario_coldstart() {
  echo "== coldstart: exactly one cluster runs the workload"
  local t0 h since
  since=$(now_rfc3339)
  t0=$(date +%s)
  until [[ -n $(holder) ]]; do
    (( $(date +%s) - t0 < 3 * TTL )) || { echo "FAILED: nobody took the lease" >&2; return 1; }
    sleep 1
  done
  h=$(holder)
  echo "   holder: ${h} after $(( $(date +%s) - t0 ))s"
  watch_exclusive $(( 2 * TTL ))
  [[ $(running "$(other "$h")") == 0 ]] || { echo "FAILED: standby is running the workload" >&2; return 1; }
  heartbeats "$since"
  echo "   ok: standby idle for $(( 2 * TTL ))s"
}

# stays <cluster> <uids>: for KEDA's cooldown and ten seconds more, the
# other cluster never starts the workload and <cluster> keeps the workload
# pods <uids>. Had the scaler said "not holding", or failed to answer, for
# that long, the workload would be gone by then.
stays() {
  local h=$1 uids=$2 o until
  o=$(other "$h")
  until=$(( $(date +%s) + COOLDOWN + 10 ))
  while (( $(date +%s) < until )); do
    [[ $(running "$o") == 0 ]] || fail "${o} started the workload" || return
    [[ $(workload_uids "$h") == "$uids" ]] || fail "the workload pods of ${h} changed" || return
    sleep 1
  done
}

# new_epoch <cluster> <before> <what> waits until the cluster's site holds an
# epoch above <before>, of a holder of that site, and prints the holder.
new_epoch() {
  local h=$1 before=$2 what=$3 t0 epoch holder
  t0=$(date +%s)
  until epoch=$(held_epoch "$h"); (( epoch > before )); do
    (( $(date +%s) - t0 < 3 * TTL )) || fail "${h} holds no new epoch $(( 3 * TTL ))s after ${what}" || return
    sleep 1
  done
  holder=$(status_get "$h" 'r["lease"]["holder"]')
  [[ $holder == "${h}/"* ]] || fail "the lease is held by ${holder}, not by site ${h}" || return
  echo "   ${holder} holds epoch ${epoch} (was ${before}) $(( $(date +%s) - t0 ))s after ${what}" >&2
  echo "$holder"
}

scenario_restart() {
  echo "== restart: delete every scaler pod of the holder's cluster; the workload stays where it is"
  local h o since uids before
  h=$(settle); o=$(other "$h"); since=$(now_rfc3339)
  uids=$(workload_uids "$h")
  before=$(held_epoch "$h")
  (( before >= 0 )) || fail "the site ${h} does not hold the lease" || return
  # The holder releases on its way out and a new pod of the same site
  # claims at once; the other site would wait the takeover delay.
  kc "$h" -n "$NS" delete pod -l app.kubernetes.io/name=kfklease --wait=false >/dev/null
  new_epoch "$h" "$before" "the pods were deleted" >/dev/null || return
  stays "$h" "$uids" || return
  heartbeats "$since"
  echo "   ok: same workload pods $(( COOLDOWN + 10 ))s on, ${o} never started"
}

scenario_holder_pod() {
  echo "== holder-pod: delete the scaler pod that holds the lease; another of its site takes over, the workload stays"
  local h o since uids before holder pod after
  h=$(settle); o=$(other "$h"); since=$(now_rfc3339)
  uids=$(workload_uids "$h")
  before=$(held_epoch "$h")
  (( before >= 0 )) || fail "the site ${h} does not hold the lease" || return
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
  echo "   ok: same workload pods $(( COOLDOWN + 10 ))s on, ${o} never started"
}

# scaler_pods <cluster> prints one line per scaler pod: name, image, ready,
# and why its container waits, if it does.
scaler_pods() {
  kc "$1" -n "$NS" get pods -l app.kubernetes.io/name=kfklease -o json | python3 -c '
import sys, json
for p in json.load(sys.stdin)["items"]:
    c = (p["status"].get("containerStatuses") or [{}])[0]
    ready = any(x["type"] == "Ready" and x["status"] == "True" for x in p["status"].get("conditions", []))
    waiting = c.get("state", {}).get("waiting", {}).get("reason", "-")
    print(p["metadata"]["name"], p["spec"]["containers"][0]["image"], ready, waiting)'
}

scenario_bad_image() {
  echo "== bad-image: upgrade the holder's scaler to an image that does not pull; its old pods keep the site, the workload stays"
  local h o since uids before t0 pods
  h=$(settle); o=$(other "$h"); since=$(now_rfc3339)
  uids=$(workload_uids "$h")
  before=$(held_epoch "$h")
  (( before >= 0 )) || fail "the site ${h} does not hold the lease" || return
  IMAGE_TAG=does-not-exist WAIT=false ./deploy.sh "$h" >/dev/null
  t0=$(date +%s)
  until grep -qE ':does-not-exist False (ErrImageNeverPull|ErrImagePull|ImagePullBackOff)$' <<<"$(scaler_pods "$h")"; do
    (( $(date +%s) - t0 < 60 )) || fail "no pod of the new image failed to pull: $(scaler_pods "$h")" || return
    sleep 1
  done
  echo "   the rollout is stuck:" && scaler_pods "$h" | sed 's/^/     /'
  stays "$h" "$uids" || return
  pods=$(scaler_pods "$h")
  [[ $(grep -c ':dev True' <<<"$pods") == 2 ]] || fail "the old scaler pods did not stay ready: ${pods}" || return
  (( $(held_epoch "$h") >= before )) || fail "${h} no longer holds the lease" || return
  ./deploy.sh "$h" >/dev/null
  heartbeats "$since"
  echo "   ok: same workload pods $(( COOLDOWN + 10 ))s on, ${o} never started, rolled back"
}

scenario_crash() {
  echo "== crash: kill the holder's cluster"
  local h o t since
  h=$(settle); o=$(other "$h"); since=$(now_rfc3339)
  docker compose kill "$h" >/dev/null 2>&1
  t=$(wait_running "$o" "$want" "$TAKEOVER")
  echo "   ${o} took over ${t}s after ${h} died (ttl ${TTL}s, takeover delay ${TAKEOVER_DELAY}s)"
  docker compose up -d --wait "$h" >/dev/null 2>&1
  # The revived cluster still has the workload at one replica in its own
  # state, so its kubelet restarts the stale pod until KEDA is back up and
  # scales it down. That window is a property of Kubernetes, not of the
  # lease; it is why workloads that must not overlap check the epoch.
  local t0
  t0=$(date +%s)
  kc "$h" -n keda rollout status deploy/keda-operator --timeout=180s >/dev/null
  kc "$h" -n "$NS" rollout status "deploy/$RELEASE" --timeout=120s >/dev/null
  t=$(wait_running "$h" 0 $(( 3 * TTL + COOLDOWN )))
  echo "   ${h} came back and ran its stale pod for $(( $(date +%s) - t0 ))s before KEDA scaled it down"
  # The revived scaler's site no longer holds the lease: it must not take
  # it back.
  watch_exclusive $(( 2 * TTL ))
  [[ $(holder) == "$o" ]] || { echo "FAILED: lease moved back to the revived cluster" >&2; return 1; }
  heartbeats "$since"
  echo "   ok: ${h} stayed idle"
}

scenario_partition() {
  echo "== partition: cut the holder's cluster off from Kafka"
  local h o since
  h=$(settle); o=$(other "$h"); since=$(now_rfc3339)
  ./partition.sh cut "$h"
  echo "   ${h} -> ${o}: $(transition "$h" "$o" "$TAKEOVER") (ttl ${TTL}s, cooldown ${COOLDOWN}s, takeover delay ${TAKEOVER_DELAY}s)"
  ./partition.sh heal "$h"
  watch_exclusive $(( 2 * TTL ))
  [[ $(holder) == "$o" ]] || { echo "FAILED: lease moved back after the heal" >&2; return 1; }
  heartbeats "$since"
  echo "   ok: ${h} healed and stayed idle"
}

scenario_pause() {
  echo "== pause: freeze the holder's cluster for 2 x ttl"
  local h o t since
  h=$(settle); o=$(other "$h"); since=$(now_rfc3339)
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
  watch_exclusive $(( 2 * TTL ))
  [[ $(holder) == "$o" ]] || { echo "FAILED: lease moved back after the pause" >&2; return 1; }
  heartbeats "$since"
  echo "   ok"
}

scenario_broker() {
  echo "== broker: Kafka, the arbiter, is down for 2 x ttl; the lease stays with its site"
  local h o t0 back since
  h=$(settle); o=$(other "$h"); since=$(now_rfc3339)
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
  # the other one waits the takeover delay.
  until (( $(held_epoch "$h") >= 0 )); do
    [[ $(running "$o") == 0 ]] || fail "${o} started the workload" || return
    (( $(date +%s) - t0 < 2 * TTL + TAKEOVER_DELAY )) || fail "${h} did not take the lease back after the broker came back" || return
    sleep 1
  done
  back=$(( $(date +%s) - t0 ))
  echo "   ${h} holds again ${back}s after the outage began (ttl ${TTL}s), its workload at $(running "$h")"
  watch_exclusive $(( 2 * TTL ))
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
  h=$(settle); since=$(now_rfc3339)
  want=3
  redeploy "$want"
  t=$(wait_running "$h" "$want" $(( 6 * TTL )))
  echo "   ${h} scaled to ${want} in ${t}s without moving the lease"
  o=$(other "$h")
  [[ $(running "$o") == 0 ]] || { echo "FAILED: standby is running the workload" >&2; return 1; }
  ./partition.sh cut "$h"
  echo "   ${h} -> ${o}: $(transition "$h" "$o" "$TAKEOVER") (ttl ${TTL}s, ${want} replicas)"
  ./partition.sh heal "$h"
  watch_exclusive $(( 2 * TTL ))
  [[ $(holder) == "$o" && $(running "$o") == "$want" ]] || { echo "FAILED: ${o} does not run ${want} replicas after the heal" >&2; return 1; }
  heartbeats "$since"
  want=1
  redeploy "$want"
  t=$(wait_running "$o" "$want" $(( 6 * TTL )))
  echo "   ok: back to ${want} replica in ${t}s"
}

# scaler_service <cluster> on|off adds a selector term no pod has to the
# scaler's Service, or takes it away. Off, the Service has no endpoint and
# kube-proxy refuses new connections to it: what KEDA sees when the scaler
# pod is gone. Waits until kube-proxy has the rule.
scaler_service() {
  local cluster=$1 t0
  case "$2" in
    off)
      kc "$cluster" -n "$NS" patch service "$RELEASE" --type=merge \
        -p '{"spec":{"selector":{"kfklease-e2e/scaler-down":"true"}}}' >/dev/null
      t0=$(date +%s)
      # Not a pipe into grep -q: with pipefail, an early exit is a failure.
      until grep -qF "${NS}/${RELEASE}:grpc has no endpoints" \
          <<<"$(docker compose exec -T "$cluster" iptables -t filter -S)"; do
        (( $(date +%s) - t0 < 30 )) || { echo "FAILED: kube-proxy kept the scaler's endpoint" >&2; return 1; }
        sleep 1
      done
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
  h=$(settle); o=$(other "$h"); since=$(now_rfc3339)
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
  watch_exclusive $(( 2 * TTL ))
  [[ $(holder) == "$o" ]] || { echo "FAILED: lease moved back after the scaler returned" >&2; return 1; }
  heartbeats "$since"
  echo "   ok: ${h} stopped its workload without its scaler and stayed idle"
}

# lease_status prints `kfklease-scaler status` for the lease, read from
# outside both clusters.
lease_status() {
  docker run --rm --network "${COMPOSE_PROJECT}_kfk" kfklease-scaler:dev status \
    -brokers "$KAFKA_ADDR:$KAFKA_PORT" -topic "$TOPIC" -ttl "${TTL}s" -log-level warn
}

# paused <cluster> prints the paused-replicas annotation of the workload's
# ScaledObject, empty when there is none.
paused() {
  kc "$1" -n "$NS" get scaledobject singleton -o jsonpath='{.metadata.annotations.autoscaling\.keda\.sh/paused-replicas}'
}

# wait_unpaused <cluster> waits until the ScaledObject has no pause left.
wait_unpaused() {
  local t0
  t0=$(date +%s)
  until [[ -z $(paused "$1") ]]; do
    (( $(date +%s) - t0 < 2 * TTL )) || fail "the ScaledObject of $1 is still paused" || return
    sleep 1
  done
}

scenario_standby() {
  echo "== standby: put the holder's cluster in standby; the workload moves through the handover, and stays moved"
  local h o since st site tr up
  h=$(settle); o=$(other "$h"); since=$(now_rfc3339)
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
  watch_exclusive $(( 2 * TTL ))
  # Out of standby, the cluster claims again by the usual rules: the lease
  # is held, so nothing moves back.
  ./deploy.sh "$h" >/dev/null
  watch_exclusive $(( 2 * TTL ))
  [[ $(holder) == "$o" && $(running "$h") == 0 ]] || fail "the workload moved back to ${h}" || return
  heartbeats "$since"
  echo "   ok: ${o} holds, ${h} back out of standby and idle"
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
  local h o since t0 t
  h=$(settle); o=$(other "$h"); since=$(now_rfc3339)
  # A finalizer keeps the workload's pod in Terminating: the handover
  # paused the ScaledObject and waits for the pod, and no yield goes out.
  finalizer "$h" add
  # Not waiting for the rollout: the handover starts when its last old pod
  # releases, and the scalers go down right after the pause.
  STANDBY=true WAIT=false ./deploy.sh "$h" >/dev/null
  t0=$(date +%s)
  until [[ $(paused "$h") == 0 ]]; do
    (( $(date +%s) - t0 < 2 * TTL )) || fail "${h} did not pause its ScaledObject" || return
    sleep 1
  done
  until [[ $(running "$h") == 0 ]]; do
    (( $(date +%s) - t0 < 2 * TTL )) || fail "${h} did not stop its workload" || return
    sleep 1
  done
  (( $(pods_left "$h") > 0 )) || fail "the finalizer did not hold the pod" || return
  echo "   ${h} paused and its pod is terminating; killing its scaler pods"
  kc "$h" -n "$NS" delete pod -l app.kubernetes.io/name=kfklease --grace-period=0 --force >/dev/null 2>&1
  kc "$h" -n "$NS" rollout status "deploy/$RELEASE" --timeout=120s >/dev/null
  # A pod that still terminates is a pod that may still run: no yield, so
  # the other site does not start.
  for _ in 1 2 3 4 5; do
    [[ $(running "$o") == 0 ]] || fail "${o} started while ${h}'s pod was still terminating" || return
    sleep 1
  done
  [[ $(paused "$h") == 0 ]] || fail "the restarted scalers lifted the pause during the handover" || return
  finalizer "$h" remove
  t0=$(date +%s)
  t=$(wait_running "$o" "$want" "$TAKEOVER") || return
  echo "   ${o} took over ${t}s after ${h}'s last pod went, $(( t0 - $(date -d "$since" +%s) ))s after the standby began (takeover delay ${TAKEOVER_DELAY}s)"
  wait_unpaused "$h" || return
  watch_exclusive $(( 2 * TTL ))
  ./deploy.sh "$h" >/dev/null
  [[ -z $(paused "$h") && $(holder) == "$o" ]] || fail "${h} out of standby: paused '$(paused "$h")', holder $(holder)" || return
  heartbeats "$since"
  echo "   ok: the restarted scalers finished the handover, no pause left"
}

if (( $# == 0 )); then
  set -- coldstart restart holder-pod bad-image crash partition pause broker replicas scaler-down standby handover-crash
fi
for s in "$@"; do
  "scenario_${s//-/_}"
done
echo "all scenarios passed"
