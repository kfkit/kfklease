#!/usr/bin/env bash
# Brings the stand up and checks the things every scenario relies on:
# both clusters are Ready, KEDA is running in each, and pods can reach Kafka.
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=lib.sh
source ./lib.sh

wait_for() {
  local what=$1 tries=$2
  shift 2
  for _ in $(seq "$tries"); do
    if "$@" >/dev/null 2>&1; then
      echo "ok: ${what}"
      return 0
    fi
    sleep 5
  done
  echo "FAILED: ${what}" >&2
  return 1
}

mkdir -p images .out/k3s-a .out/k3s-b
docker compose up -d --wait

for c in "${CLUSTERS[@]}"; do
  wait_for "${c}: node Ready" 60 kc "$c" wait node "$c" --for=condition=Ready --timeout=5s
  wait_for "${c}: KEDA operator" 90 kc "$c" -n keda rollout status deploy/keda-operator --timeout=5s
  wait_for "${c}: KEDA metrics server" 60 kc "$c" -n keda rollout status deploy/keda-operator-metrics-apiserver --timeout=5s

  kc "$c" delete pod kafka-probe --ignore-not-found --wait >/dev/null
  kc "$c" run kafka-probe --image=busybox:1.37 --restart=Never --command -- \
    nc -z -w 5 "$KAFKA_ADDR" "$KAFKA_PORT" >/dev/null
  wait_for "${c}: pod reaches Kafka at ${KAFKA_ADDR}:${KAFKA_PORT}" 24 \
    kc "$c" wait pod kafka-probe --for=jsonpath='{.status.phase}'=Succeeded --timeout=5s
  kc "$c" delete pod kafka-probe --wait=false >/dev/null
done

echo "stand is up"
