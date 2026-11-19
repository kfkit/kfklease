#!/usr/bin/env bash
# Tells CI whether a pull request touches more than documents. The jobs that
# build the image, render the chart, start a broker or the stand are skipped
# when it does not: they would test the same code as on main. The unit job
# always runs: it lints, and a test of it reads docs/install.md.
#
#   changes.sh           in CI: the paths the pull request changes, by git
#   changes.sh --stdin   the paths on stdin, one per line
#
# Prints code=true or code=false, the line a job output takes. Anything it
# cannot tell is code=true: the slow jobs run rather than not.
set -euo pipefail

classify() {
  local path code=false seen=false
  while IFS= read -r path; do
    [[ -n $path ]] || continue
    seen=true
    case $path in
      *.md | docs/* | LICENSE | NOTICE | .github/ISSUE_TEMPLATE/* | claude-plugin/* | .claude-plugin/*) ;;
      *) code=true ;;
    esac
  done
  [[ $seen == true ]] || code=true
  echo "code=$code"
}

if [[ ${1:-} == --stdin ]]; then
  classify
elif [[ ${GITHUB_EVENT_NAME:-} == pull_request ]] && paths=$(git diff --name-only HEAD^1 HEAD 2>/dev/null); then
  # A pull request is checked out as its merge into the base branch: the
  # first parent is the base.
  classify <<<"$paths"
else
  echo code=true
fi
