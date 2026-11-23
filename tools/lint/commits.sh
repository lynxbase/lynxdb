#!/bin/sh
# usage: commits.sh [<revision range>]
# The range defaults to $LYNXDB_COMMIT_RANGE, then to what main lacks.
set -eu

range=${1:-${LYNXDB_COMMIT_RANGE:-origin/main..HEAD}}
types='feat|fix|chore|refactor|docs|test|perf'
failed=0

for commit in $(git rev-list --no-merges "$range"); do
    # Dependabot names what it moved, and the paths of modules run long.
    [ "$(git log -1 --format=%an "$commit")" != 'dependabot[bot]' ] || continue
    subject=$(git log -1 --format=%s "$commit")
    reason=
    if ! printf '%s\n' "$subject" | grep -Eq "^($types)!?: [a-z0-9]"; then
        reason="must read '<type>: <subject>' with a lowercase subject and one of: $types"
    elif [ "${#subject}" -gt 72 ]; then
        reason='is longer than 72 characters'
    elif printf '%s\n' "$subject" | grep -q '\.$'; then
        reason='ends with a period'
    fi
    if [ -n "$reason" ]; then
        printf '%s %s\n    %s\n' "$(git rev-parse --short "$commit")" "$subject" "$reason" >&2
        failed=1
    fi
done

exit "$failed"
