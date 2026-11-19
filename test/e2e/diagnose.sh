#!/usr/bin/env bash
# Where to look first when a scenario failed: the containers of the stand
# and, in each cluster, the pods, the ScaledObject, the scaler's log and
# KEDA's. CI runs it after a failed `make e2e`.
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=lib.sh
source ./lib.sh

docker compose ps -a
for c in "${CLUSTERS[@]}"; do
  kc "$c" -n "$NS" get pods,scaledobject || true
  kc "$c" -n "$NS" logs -l "$SCALER_PODS" --prefix --tail=50 || true
  kc "$c" -n keda logs deploy/keda-operator --tail=30 || true
done
