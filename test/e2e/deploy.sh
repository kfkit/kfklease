#!/usr/bin/env bash
# Deploys the chart and the test workload into one cluster:  ./deploy.sh k3s-a
# Uses helm when installed, the alpine/helm image otherwise. REPLICAS sets
# the workload's replica count while the cluster holds the lease (default 1),
# STANDBY=true puts the cluster in standby mode, IMAGE_TAG sets the scaler's
# image tag (default: the one build-image.sh built), and WAIT=false returns
# without waiting for the scaler's rollout.
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=lib.sh
source ./lib.sh

cluster=${1:?usage: deploy.sh <k3s-a|k3s-b>}

kc "$cluster" create namespace "$NS" --dry-run=client -o yaml | kc "$cluster" apply -f - >/dev/null

# Each cluster authenticates differently, so that the chart's auth values
# and both kinds of listener are exercised on every run.
auth=()
case "$cluster" in
  k3s-a)
    kc "$cluster" -n "$NS" create secret generic kafka-mtls --dry-run=client -o yaml \
      --from-file=ca.crt=.out/certs/ca.crt --from-file=tls.crt=.out/certs/client.crt --from-file=tls.key=.out/certs/client.key \
      | kc "$cluster" apply -f - >/dev/null
    auth=(--set brokers="$KAFKA_ADDR:$KAFKA_MTLS_PORT" --set auth.tls.enabled=true --set auth.tls.secretName=kafka-mtls
          --set auth.tls.clientCert=true)
    ;;
  k3s-b)
    # Username and password both in the Secret, as an External Secrets
    # managed Secret usually has them.
    kc "$cluster" -n "$NS" create secret generic kfklease-sasl --dry-run=client -o yaml \
      --from-literal=username="$SASL_USER" --from-literal=password="$SASL_PASSWORD" | kc "$cluster" apply -f - >/dev/null
    auth=(--set brokers="$KAFKA_ADDR:$KAFKA_SASL_PORT" --set auth.sasl.mechanism=plain --set auth.sasl.secretName=kfklease-sasl
          --set auth.sasl.usernameKey=username)
    ;;
esac

manifest=$(helm_run template "$RELEASE" "$CHART" --namespace "$NS" \
  --set image.repository="${SCALER_IMAGE%:*}" --set image.tag="${IMAGE_TAG:-${SCALER_IMAGE##*:}}" --set image.pullPolicy=Never \
  --set topic="$TOPIC" --set ttl="${TTL}s" --set replicationFactor=1 \
  --set site="$cluster" --set scaledObject.targetRef.name=singleton \
  --set scaledObject.cooldownPeriod="$COOLDOWN" --set takeoverMargin="$TAKEOVER_MARGIN" \
  --set scaledObject.replicas="${REPLICAS:-1}" --set standby="${STANDBY:-false}" --set logLevel=debug \
  "${auth[@]}")
# The takeover delay the scaler is started with, for the scenarios: what the
# chart made of the values above, not a second copy of its formula.
delay=$(awk '/name: KFKLEASE_TAKEOVER_DELAY$/ { getline; if (match($0, /"[0-9]+s"/)) print substr($0, RSTART + 1, RLENGTH - 3) }' <<<"$manifest")
[[ -n $delay ]] || fail "the chart rendered no KFKLEASE_TAKEOVER_DELAY in seconds"
echo "$delay" > "$TAKEOVER_DELAY_FILE"
kc "$cluster" -n "$NS" apply -f - <<<"$manifest" >/dev/null
# The workload once: its manifest says replicas 0, and KEDA owns the count
# from then on. Applying it again would scale a running workload to zero and
# start a new pod, which a redeploy of the chart must not do.
if ! kc "$cluster" -n "$NS" get deploy singleton >/dev/null 2>&1; then
  # shellcheck disable=SC2016 # the literals are the variable names for envsubst
  CLUSTER="$cluster" HEARTBEAT_IMAGE="$HEARTBEAT_IMAGE" HEARTBEAT_BROKERS="$KAFKA_ADDR:$KAFKA_PORT" RELEASE="$RELEASE" \
    envsubst '$CLUSTER $HEARTBEAT_IMAGE $HEARTBEAT_BROKERS $RELEASE' < manifests/singleton.yaml \
    | kc "$cluster" -n "$NS" apply -f - >/dev/null
fi
if [[ ${WAIT:-true} == true ]]; then
  kc "$cluster" -n "$NS" rollout status "deploy/$RELEASE" --timeout=120s >/dev/null
fi
echo "deployed to ${cluster}"
