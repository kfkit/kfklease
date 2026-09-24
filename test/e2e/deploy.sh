#!/usr/bin/env bash
# Deploys the scaler and the workload into one cluster:  ./deploy.sh k3s-a
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=lib.sh
source ./lib.sh

cluster=${1:?usage: deploy.sh <k3s-a|k3s-b>}

# shellcheck disable=SC2016 # the literal is the variable name for envsubst
CLUSTER="$cluster" envsubst '$CLUSTER' < manifests/kfklease.yaml | kc "$cluster" apply -f - >/dev/null
kc "$cluster" -n kfklease rollout status deploy/kfklease-scaler --timeout=120s >/dev/null
echo "deployed to ${cluster}"
