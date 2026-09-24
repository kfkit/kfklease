#!/usr/bin/env bash
# Builds the scaler and the heartbeat workload images and makes them
# available to both clusters: as tarballs k3s imports on boot, and by direct
# import when a cluster is up.
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=lib.sh
source ./lib.sh

mkdir -p images
# name=package; deploy.sh sets image.repository and image.tag to name:dev.
for build in kfklease-scaler=./cmd/kfklease-scaler kfklease-heartbeat=./examples/heartbeat; do
  name=${build%%=*}
  pkg=${build#*=}
  docker build -q -t "$name:dev" --build-arg "PKG=$pkg" ../.. >/dev/null
  docker save "$name:dev" -o "images/$name.tar"
  echo "saved images/$name.tar"
  for c in "${CLUSTERS[@]}"; do
    if docker compose ps --status running --format '{{.Service}}' 2>/dev/null | grep -qx "$c"; then
      docker compose exec -T "$c" ctr -n k8s.io images import "/var/lib/rancher/k3s/agent/images/$name.tar" >/dev/null
      echo "imported $name into $c"
    fi
  done
done
