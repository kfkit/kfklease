#!/usr/bin/env bash
# Checks scenarios.sh itself, without the stand: a scenario that fails, or
# that dies of a shell error, must fail the run. The scenarios here are
# exported functions, which scenarios.sh inherits; it runs only the ones
# named, so nothing touches a cluster. Run by `make lint`.
set -euo pipefail

cd "$(dirname "$0")"

# shellcheck disable=SC2317,SC2329 # called by scenarios.sh, through export -f
{
  scenario_selftest_ok() { echo "   ok"; }
  # What a GNU-only `date -d` did on macOS: the command substitution is
  # empty, the arithmetic expansion is a syntax error, and bash drops the
  # command it happens in.
  scenario_selftest_shell_error() { local t0=1; echo "   $(( t0 - $(false) ))s"; }
  scenario_selftest_fail() { echo "FAILED: on purpose" >&2; return 1; }
}
export -f scenario_selftest_ok scenario_selftest_shell_error scenario_selftest_fail

failed=0

# expect <want exit: 0 or nonzero> <scenarios...>
expect() {
  local want=$1 out rc=0
  shift
  out=$(./scenarios.sh "$@" 2>&1) || rc=$?
  if [[ $want == 0 && $rc == 0 && $out == *"all scenarios passed"* ]] \
      || [[ $want != 0 && $rc != 0 && $out != *"all scenarios passed"* ]]; then
    echo "ok: scenarios.sh $* exits ${rc}"
  else
    echo "FAILED: scenarios.sh $* exits ${rc}, want ${want}:" >&2
    echo "$out" >&2
    failed=1
  fi
}

expect 0 selftest-ok selftest-ok
expect nonzero selftest-ok selftest-shell-error selftest-ok
expect nonzero selftest-shell-error
expect nonzero selftest-ok selftest-fail selftest-ok
expect nonzero no-such-scenario
exit "$failed"
