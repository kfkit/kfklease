#!/usr/bin/env bash
# For CI, where every run starts on an empty machine: keeps every image the
# stand needs as a tarball, so that the runner loads them instead of
# pulling.
#
#   images/*.tar   what k3s imports on boot: KEDA, the test workload
#   .cache/*.tar   what compose runs: k3s, Kafka, openssl, helm
#
# With the tarballs present it only loads them; the workflow caches both
# directories keyed on this file. Locally, Docker's own cache is enough.
set -euo pipefail

cd "$(dirname "$0")"

# Versions follow compose.yaml, manifests/keda.yaml and deploy.sh.
COMPOSE_IMAGES=(
  rancher/k3s:v1.35.8-k3s1
  apache/kafka:4.3.1
  alpine/openssl:3.5.4
  alpine/helm:3.19.0
)
CLUSTER_IMAGES=(
  ghcr.io/kedacore/keda:2.20.2
  ghcr.io/kedacore/keda-metrics-apiserver:2.20.2
  ghcr.io/kedacore/keda-admission-webhooks:2.20.2
  busybox:1.37
)

# tar_name <image> maps an image reference to a file name.
tar_name() {
  echo "${1//[\/:]/_}.tar"
}

# ensure <dir> <image...> pulls and saves what is missing, and loads what
# is present into the local Docker so compose finds it.
ensure() {
  local dir=$1
  shift
  mkdir -p "$dir"
  local image tar
  for image in "$@"; do
    tar="$dir/$(tar_name "$image")"
    if [[ -f $tar ]]; then
      docker load -q -i "$tar" >/dev/null
      echo "loaded $image"
    else
      docker pull -q "$image" >/dev/null
      docker save "$image" -o "$tar"
      echo "saved $image"
    fi
  done
}

ensure .cache "${COMPOSE_IMAGES[@]}"
ensure images "${CLUSTER_IMAGES[@]}"
