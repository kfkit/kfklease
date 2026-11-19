#!/usr/bin/env bash
# Brings the stand up and checks the things every scenario relies on:
# both clusters are Ready, KEDA is running in each, and pods can reach Kafka.
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=lib.sh
source ./lib.sh

# A check here is a kubectl that waits up to five seconds by itself, tried
# every five more: the timeouts below count ten seconds a try.
POLL=5

# ready <timeout seconds> <what> <command...> waits until the command
# succeeds, without its output, and says so.
ready() {
  local timeout=$1 what=$2
  shift 2
  wait_until "$timeout" "$what" quiet "$@"
  echo "ok: ${what}"
}

quiet() {
  "$@" >/dev/null 2>&1
}

mkdir -p images
for c in "${CLUSTERS[@]}"; do
  mkdir -p ".out/${c}"
done
docker compose up -d --wait

for c in "${CLUSTERS[@]}"; do
  ready 600 "${c}: node Ready" kc "$c" wait node "$c" --for=condition=Ready --timeout=5s
  ready 900 "${c}: KEDA operator" kc "$c" -n keda rollout status deploy/keda-operator --timeout=5s
  ready 600 "${c}: KEDA metrics server" kc "$c" -n keda rollout status deploy/keda-operator-metrics-apiserver --timeout=5s

  kc "$c" delete pod kafka-probe --ignore-not-found --wait >/dev/null
  kc "$c" run kafka-probe --image=busybox:1.37 --restart=Never --command -- \
    nc -z -w 5 "$KAFKA_ADDR" "$KAFKA_PORT" >/dev/null
  ready 240 "${c}: pod reaches Kafka at ${KAFKA_ADDR}:${KAFKA_PORT}" \
    kc "$c" wait pod kafka-probe --for=jsonpath='{.status.phase}'=Succeeded --timeout=5s
  kc "$c" delete pod kafka-probe --wait=false >/dev/null
done

echo "stand is up"
