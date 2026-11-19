#!/usr/bin/env bash
# The stand from nothing to "all scenarios passed": the images, the stand
# itself, the chart in both clusters, the scenarios. `make e2e` and CI run
# this; the order of the steps is written here and nowhere else.
#
#   ./run.sh            # every scenario
#   ./run.sh crash      # the ones named, see scenarios.sh
#
# The stand stays up afterwards, passed or failed: a failure with no logs
# left is a failure you get to reproduce. ./diagnose.sh prints where to
# look first, `docker compose down -v` (`make e2e-down`) tears it down.
set -euo pipefail

cd "$(dirname "$0")"
# shellcheck source=lib.sh
source ./lib.sh

# step <command...> runs one step and says how long it took: the scenarios
# are the time, and this is where to see it.
step() {
  local t0
  t0=$(date +%s)
  "$@"
  echo "-- $* took $(( $(date +%s) - t0 ))s"
}

step ./build-image.sh
step ./smoke.sh
for c in "${CLUSTERS[@]}"; do
  step ./deploy.sh "$c"
done
step ./scenarios.sh "$@"
