#!/usr/bin/env bash
# Builds the scaler and the heartbeat workload images and makes them
# available to both clusters: as tarballs k3s imports on boot, and by direct
# import when a cluster is up.
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=lib.sh
source ./lib.sh

mkdir -p images
# image=package; deploy.sh points the chart and the workload at the images.
for build in "$SCALER_IMAGE=./cmd/kfklease-scaler" "$HEARTBEAT_IMAGE=./examples/heartbeat"; do
  image=${build%%=*}
  pkg=${build#*=}
  name=${image%:*}
  docker build -q -t "$image" --build-arg "PKG=$pkg" ../.. >/dev/null
  docker save "$image" -o "images/$name.tar"
  echo "saved images/$name.tar"
  for c in "${CLUSTERS[@]}"; do
    if docker compose ps --status running --format '{{.Service}}' 2>/dev/null | grep -qx "$c"; then
      docker compose exec -T "$c" ctr -n k8s.io images import "/var/lib/rancher/k3s/agent/images/$name.tar" >/dev/null
      echo "imported $name into $c"
    fi
  done
done
