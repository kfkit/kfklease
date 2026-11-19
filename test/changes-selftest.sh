#!/usr/bin/env bash
# Checks changes.sh: which sets of paths count as documents only. Run by
# `make lint`.
set -euo pipefail

cd "$(dirname "$0")"

failed=0

# expect <true|false> <paths...>
expect() {
  local want=$1 got
  shift
  got=$(printf '%s\n' "$@" | ./changes.sh --stdin)
  if [[ $got == "code=$want" ]]; then
    echo "ok: ${*:-no paths} is $got"
  else
    echo "FAILED: ${*:-no paths} is $got, want code=$want" >&2
    failed=1
  fi
}

expect false README.md
expect false README.md CHANGELOG.md AGENTS.md docs/install.md docs/protocol.md test/e2e/README.md
expect false docs/provenance/release-hashes.txt docs/provenance/release-hashes.txt.ots
expect false LICENSE NOTICE .github/ISSUE_TEMPLATE/task.yml .github/PULL_REQUEST_TEMPLATE.md
expect false claude-plugin/skills/kfklease/SKILL.md claude-plugin/.claude-plugin/plugin.json .claude-plugin/marketplace.json
expect true README.md lease/agent.go
expect true charts/kfklease/values.yaml
expect true charts/kfklease/templates/NOTES.txt
expect true .github/workflows/ci.yml
expect true Makefile
expect true test/e2e/scenarios.sh
expect true go.mod
expect true

exit "$failed"
