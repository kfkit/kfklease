#!/usr/bin/env bash
# Cuts a cluster off from Kafka, or KEDA off from the scaler, or heals it.
#
#   ./partition.sh cut  k3s-a           # the node from Kafka
#   ./partition.sh heal k3s-a
#   ./partition.sh cut  k3s-a scaler    # KEDA from the scaler's gRPC port
#   ./partition.sh heal k3s-a scaler
#
# kafka: packets are dropped in the raw table of the node's network
# namespace, so both pod traffic (PREROUTING) and host-network traffic
# (OUTPUT) black-hole the way they would in a real partition: connections
# hang and time out, nothing is refused. The cluster itself and its API
# server stay up.
#
# scaler: what KEDA sees when the scaler pod is gone. Pod traffic to the
# scaler's Service is routed through the node, so FORWARD sees it after the
# Service address was translated; it is answered with a TCP reset, as a
# missing endpoint would be. The kubelet's probes come from the node itself
# (OUTPUT, not FORWARD) and the workload reads /status on another port, so
# the scaler stays Ready and the workload keeps its view of the lease.
set -euo pipefail

cd "$(dirname "$0")"

KAFKA_ADDR=172.30.0.10
SCALER_PORT=9090  # the chart's service.port

action=${1:?usage: partition.sh cut|heal <service> [kafka|scaler]}
service=${2:?usage: partition.sh cut|heal <service> [kafka|scaler]}
what=${3:-kafka}

case "$what" in
  kafka)
    table=raw
    chains=(PREROUTING OUTPUT)
    rule=(-d "$KAFKA_ADDR" -j DROP)
    ;;
  scaler)
    table=filter
    chains=(FORWARD)
    rule=(-p tcp --dport "$SCALER_PORT" -j REJECT --reject-with tcp-reset)
    ;;
  *)
    echo "unknown target: $what" >&2
    exit 2
    ;;
esac

ipt() {
  docker compose exec -T "$service" iptables -t "$table" "$@"
}

case "$action" in
  cut)
    for chain in "${chains[@]}"; do
      ipt -C "$chain" "${rule[@]}" 2>/dev/null || ipt -I "$chain" "${rule[@]}"
    done
    ;;
  heal)
    for chain in "${chains[@]}"; do
      while ipt -D "$chain" "${rule[@]}" 2>/dev/null; do :; done
    done
    ;;
  *)
    echo "unknown action: $action" >&2
    exit 2
    ;;
esac
