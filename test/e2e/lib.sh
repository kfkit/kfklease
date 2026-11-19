#!/usr/bin/env bash
# Shared by the stand scripts; source it after cd-ing into test/e2e.

# The broker's address and ports, its SASL user, the image names and the
# compose project: .env, which compose.yaml and the Makefile read as well.
# shellcheck source=.env
source ./.env

# shellcheck disable=SC2034 # used by the sourcing scripts
{
  CLUSTERS=(k3s-a k3s-b)
  KFK_NETWORK=${COMPOSE_PROJECT_NAME}_kfk  # the broker is at KAFKA_ADDR on it
  NS=kfklease       # namespace of the scaler and the workload
  RELEASE=kfklease  # chart release: deployment and service name
  SCALER_PODS=app.kubernetes.io/name=kfklease  # selects the scaler's pods
  TOPIC=kfklease-e2e  # the lease
  TTL=10            # lease ttl in seconds
  # KEDA keeps a workload for COOLDOWN seconds after its scaler stopped
  # saying "holding" (the chart's default is 60; shorter here to keep the
  # scenarios short, still longer than a scaler restart). TAKEOVER_MARGIN
  # covers the test workload's 2 s grace period. From the two and KEDA's
  # polling interval the chart derives the takeover delay, how long another
  # site waits after the end of a term; deploy.sh writes what the chart
  # rendered to TAKEOVER_DELAY_FILE, in seconds, and scenarios.sh reads it.
  COOLDOWN=30
  TAKEOVER_MARGIN=5
  TAKEOVER_DELAY_FILE=.out/takeover-delay
  POLL=1            # seconds between two tries of wait_until
}

# fail <message...> prints the failure and returns 1:
#   [[ it is fine ]] || fail "why it is not" || return
fail() {
  echo "FAILED: $*" >&2
  return 1
}

# kc <cluster> <kubectl args...> runs kubectl against one cluster.
kc() {
  local cluster=$1
  shift
  kubectl --kubeconfig ".out/${cluster}/kubeconfig.yaml" "$@"
}

# other <cluster> prints the other cluster.
other() {
  if [[ $1 == "${CLUSTERS[0]}" ]]; then echo "${CLUSTERS[1]}"; else echo "${CLUSTERS[0]}"; fi
}

# wait_until <timeout seconds> <what> <command...> runs the command every
# POLL seconds until it succeeds. When <timeout> has passed and it still
# fails, so does the wait, with <what>: the words for what did not happen.
# The command runs in this shell, so a function can leave what it saw in a
# variable of its caller.
wait_until() {
  local timeout=$1 what=$2 t0
  shift 2
  t0=$(date +%s)
  until "$@"; do
    (( $(date +%s) - t0 < timeout )) || fail "$what" || return
    sleep "$POLL"
  done
}
