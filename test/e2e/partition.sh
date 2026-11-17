#!/usr/bin/env bash
# Cuts a cluster off from Kafka, or heals it.
#
#   ./partition.sh cut  k3s-a
#   ./partition.sh heal k3s-a
#
# Packets are dropped in the raw table of the node's network namespace, so both
# pod traffic (PREROUTING) and host-network traffic (OUTPUT) black-hole the way
# they would in a real partition: connections hang and time out, nothing is
# refused. The cluster itself and its API server stay up.
set -euo pipefail

cd "$(dirname "$0")"

KAFKA_ADDR=172.30.0.10

action=${1:?usage: partition.sh cut|heal <service>}
service=${2:?usage: partition.sh cut|heal <service>}

ipt() {
  docker compose exec -T "$service" iptables -t raw "$@"
}

case "$action" in
  cut)
    for chain in PREROUTING OUTPUT; do
      ipt -C "$chain" -d "$KAFKA_ADDR" -j DROP 2>/dev/null || ipt -I "$chain" -d "$KAFKA_ADDR" -j DROP
    done
    ;;
  heal)
    for chain in PREROUTING OUTPUT; do
      while ipt -D "$chain" -d "$KAFKA_ADDR" -j DROP 2>/dev/null; do :; done
    done
    ;;
  *)
    echo "unknown action: $action" >&2
    exit 2
    ;;
esac
