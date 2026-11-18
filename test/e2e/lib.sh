#!/usr/bin/env bash
# Shared by the stand scripts; source it after cd-ing into test/e2e.

# shellcheck disable=SC2034 # used by the sourcing scripts
{
  CLUSTERS=(k3s-a k3s-b)
  KAFKA_ADDR=172.30.0.10
  KAFKA_PORT=9092
  NS=kfklease       # namespace of the scaler and the workload
  RELEASE=kfklease  # chart release: deployment and service name
  TOPIC=kfklease-e2e  # the lease
  TTL=10            # lease ttl in seconds
  # KEDA keeps a workload for COOLDOWN seconds after its scaler stopped
  # saying "holding" (the chart's default is 60; shorter here to keep the
  # scenarios short, still longer than a scaler restart). Another site waits
  # TAKEOVER_DELAY after the end of a term: polling 5 s + COOLDOWN +
  # TAKEOVER_MARGIN, which covers the test workload's 2 s grace period.
  COOLDOWN=30
  TAKEOVER_MARGIN=5
  TAKEOVER_DELAY=$(( 5 + COOLDOWN + TAKEOVER_MARGIN ))
  COMPOSE_PROJECT=kfklease-e2e  # the name: in compose.yaml
}

# kc <cluster> <kubectl args...> runs kubectl against one cluster.
kc() {
  local cluster=$1
  shift
  kubectl --kubeconfig ".out/${cluster}/kubeconfig.yaml" "$@"
}

# other <cluster> prints the other cluster.
other() {
  if [[ $1 == k3s-a ]]; then echo k3s-b; else echo k3s-a; fi
}
