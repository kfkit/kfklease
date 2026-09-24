#!/usr/bin/env bash
# Deploys the chart and the test workload into one cluster:  ./deploy.sh k3s-a
# Uses helm when installed, the alpine/helm image otherwise.
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=lib.sh
source ./lib.sh

cluster=${1:?usage: deploy.sh <k3s-a|k3s-b>}

ROOT=$(cd ../.. && pwd)

# helm_template <values...> renders the chart from the repository root,
# whatever the working directory.
helm_template() {
  if command -v helm >/dev/null; then
    helm template "$RELEASE" "$ROOT/charts/kfklease" "$@"
  else
    docker run --rm -v "$ROOT:/src" alpine/helm:3.19.0 template "$RELEASE" /src/charts/kfklease "$@"
  fi
}

kc "$cluster" create namespace "$NS" --dry-run=client -o yaml | kc "$cluster" apply -f - >/dev/null
helm_template --namespace "$NS" \
  --set image.repository=kfklease-scaler --set image.tag=dev --set image.pullPolicy=Never \
  --set brokers="$KAFKA_ADDR:$KAFKA_PORT" --set topic=kfklease-e2e --set ttl="${TTL}s" \
  --set holderPrefix="$cluster" --set scaledObject.target=singleton --set logLevel=debug \
  | kc "$cluster" -n "$NS" apply -f - >/dev/null
# shellcheck disable=SC2016 # the literal is the variable name for envsubst
CLUSTER="$cluster" envsubst '$CLUSTER' < manifests/singleton.yaml | kc "$cluster" -n "$NS" apply -f - >/dev/null
kc "$cluster" -n "$NS" rollout status "deploy/$RELEASE" --timeout=120s >/dev/null
echo "deployed to ${cluster}"
