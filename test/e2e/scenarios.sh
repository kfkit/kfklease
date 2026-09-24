#!/usr/bin/env bash
# Failover scenarios on the stand. Each one reports how long the takeover
# took and fails if two clusters ever run the workload at the same time.
#
#   ./scenarios.sh            # all
#   ./scenarios.sh crash      # one: coldstart, crash, partition, pause
#
# Expects the stand to be up (./smoke.sh), the image built (./build-image.sh)
# and the manifests deployed to both clusters (./deploy.sh k3s-a; ./deploy.sh k3s-b).
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=lib.sh
source ./lib.sh

TTL=10

# running <cluster> prints the number of Running, not terminating, singleton pods.
running() {
  kc "$1" -n kfklease get pods -l app=singleton -o json 2>/dev/null \
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

# transition <from> <to> <timeout> watches both clusters until <from> runs no
# workload pod and <to> runs one, and prints how long each took from now and
# how long both ran at once. Pods are polled every half second, so the
# overlap is a lower bound.
transition() {
  local from=$1 to=$2 timeout=$3 t0 now down="" up=""
  t0=$(date +%s.%N)
  while true; do
    now=$(python3 -c "import time; print(round(time.time() - $t0, 1))")
    [[ -n $down ]] || [[ $(running "$from") != 0 ]] || down=$now
    [[ -n $up ]] || [[ $(running "$to") != 1 ]] || up=$now
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
    if [[ $(running "$c") == 1 ]]; then echo "$c"; return; fi
  done
}

# watch_exclusive <seconds> fails if both clusters run the workload at once.
watch_exclusive() {
  local until=$(( $(date +%s) + $1 ))
  while (( $(date +%s) < until )); do
    if [[ $(running k3s-a) == 1 && $(running k3s-b) == 1 ]]; then
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

scenario_coldstart() {
  echo "== coldstart: exactly one cluster runs the workload"
  local t0 h
  t0=$(date +%s)
  until [[ -n $(holder) ]]; do
    (( $(date +%s) - t0 < 3 * TTL )) || { echo "FAILED: nobody took the lease" >&2; return 1; }
    sleep 1
  done
  h=$(holder)
  echo "   holder: ${h} after $(( $(date +%s) - t0 ))s"
  watch_exclusive $(( 2 * TTL ))
  [[ $(running "$(other "$h")") == 0 ]] || { echo "FAILED: standby is running the workload" >&2; return 1; }
  echo "   ok: standby idle for $(( 2 * TTL ))s"
}

scenario_crash() {
  echo "== crash: kill the holder's cluster"
  local h o t
  h=$(settle); o=$(other "$h")
  docker compose kill "$h" >/dev/null 2>&1
  t=$(wait_running "$o" 1 $(( 3 * TTL )))
  echo "   ${o} took over ${t}s after ${h} died (ttl ${TTL}s)"
  docker compose up -d --wait "$h" >/dev/null 2>&1
  # The revived cluster restarts its scaler with a new holder id and must
  # not take the lease back.
  kc "$h" -n kfklease rollout status deploy/kfklease-scaler --timeout=120s >/dev/null
  wait_running "$h" 0 $(( 2 * TTL )) >/dev/null
  watch_exclusive $(( 2 * TTL ))
  [[ $(holder) == "$o" ]] || { echo "FAILED: lease moved back to the revived cluster" >&2; return 1; }
  echo "   ok: ${h} came back and stayed idle"
}

scenario_partition() {
  echo "== partition: cut the holder's cluster off from Kafka"
  local h o
  h=$(settle); o=$(other "$h")
  ./partition.sh cut "$h"
  echo "   ${h} -> ${o}: $(transition "$h" "$o" $(( 3 * TTL ))) (ttl ${TTL}s)"
  ./partition.sh heal "$h"
  watch_exclusive $(( 2 * TTL ))
  [[ $(holder) == "$o" ]] || { echo "FAILED: lease moved back after the heal" >&2; return 1; }
  echo "   ok: ${h} healed and stayed idle"
}

scenario_pause() {
  echo "== pause: freeze the holder's cluster for 2 x ttl"
  local h o t
  h=$(settle); o=$(other "$h")
  docker compose pause "$h" >/dev/null 2>&1
  t=$(wait_running "$o" 1 $(( 3 * TTL )))
  echo "   ${o} took over ${t}s into the freeze"
  sleep $(( 2 * TTL - t > 0 ? 2 * TTL - t : 0 ))
  docker compose unpause "$h" >/dev/null 2>&1
  # On waking up the frozen scaler is past its deadline: the pod must go.
  # Both pods exist during the freeze, but the frozen one is not running
  # in any useful sense; what matters is how fast it goes after the wake-up.
  t=$(wait_running "$h" 0 $(( 2 * TTL )))
  echo "   ${h} stopped its pod ${t}s after waking up"
  watch_exclusive $(( 2 * TTL ))
  [[ $(holder) == "$o" ]] || { echo "FAILED: lease moved back after the pause" >&2; return 1; }
  echo "   ok"
}

if (( $# == 0 )); then
  set -- coldstart crash partition pause
fi
for s in "$@"; do
  "scenario_$s"
done
echo "all scenarios passed"
