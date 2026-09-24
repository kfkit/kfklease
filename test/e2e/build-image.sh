#!/usr/bin/env bash
# Builds the scaler image and makes it available to both clusters: as a
# tarball k3s imports on boot, and by direct import when a cluster is up.
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=lib.sh
source ./lib.sh

IMAGE=kfklease-scaler:dev

docker build -q -t "$IMAGE" ../.. >/dev/null
mkdir -p images
docker save "$IMAGE" -o images/kfklease-scaler.tar
echo "saved images/kfklease-scaler.tar"

for c in "${CLUSTERS[@]}"; do
  if docker compose ps --status running --format '{{.Service}}' 2>/dev/null | grep -qx "$c"; then
    docker compose exec -T "$c" ctr -n k8s.io images import /var/lib/rancher/k3s/agent/images/kfklease-scaler.tar >/dev/null
    echo "imported into $c"
  fi
done
