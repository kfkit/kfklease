#!/usr/bin/env bash
# Shared by the stand's scripts (test/e2e) and the chart's tests
# (test/chart); source it.

# shellcheck disable=SC2034 # used by the sourcing scripts
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

# fail <message...> prints the failure and returns 1:
#   [[ it is fine ]] || fail "why it is not" || return
fail() {
  echo "FAILED: $*" >&2
  return 1
}

# helm_run <helm arguments...> runs helm when it is installed, the
# alpine/helm image otherwise. CHART is the chart's path for whichever runs:
# absolute, because helm takes a relative path it does not find for the name
# of a repository.
if command -v helm >/dev/null; then
  CHART=$ROOT/charts/kfklease
  helm_run() { helm "$@"; }
else
  CHART=/src/charts/kfklease
  helm_run() { docker run --rm -v "$ROOT:/src" alpine/helm:3.19.0 "$@"; }
fi
