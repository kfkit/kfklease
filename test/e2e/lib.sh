#!/usr/bin/env bash
# Shared by the stand scripts; source it after cd-ing into test/e2e.

# shellcheck disable=SC2034 # used by the sourcing scripts
{
  CLUSTERS=(k3s-a k3s-b)
  KAFKA_ADDR=172.30.0.10
  KAFKA_PORT=9092
  NS=kfklease       # namespace of the scaler and the workload
  RELEASE=kfklease  # chart release: deployment and service name
  TTL=10            # lease ttl in seconds
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
