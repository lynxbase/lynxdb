#!/bin/sh
# usage: notes.sh [<tag> <previous tag or ""> <commit> [<changelog>]]
# Prints the notes of the release that tags <commit> as <tag>, from the commits
# after <previous tag>. With <changelog>, adds them to that file instead.
# Without arguments, prints what a release cut now would say.
set -eu

cd "$(dirname -- "$0")/../.."

# GitHub refuses a release whose notes are longer than 125000 characters.
limit=120000

tag=${1:-}
previous=${2:-}
commit=$(git rev-parse --verify "${3:-HEAD}^{commit}")
changelog=${4:-}

if [ $# -eq 0 ]; then
    previous=$(git describe --tags --abbrev=0 --first-parent \
        --match 'v[0-9]*-stable' --match 'v[0-9]*-lts' "$commit" 2>/dev/null || true)
fi
# The first release numbered by date follows the last one numbered
# major.minor.patch.
if [ -z "$previous" ]; then
    previous=$(git describe --tags --abbrev=0 --first-parent \
        --match 'v[0-9]*.[0-9]*.[0-9]*' --exclude '*-*' "$commit" 2>/dev/null || true)
fi
range=$commit
[ -z "$previous" ] || range=$previous..$commit

if [ -z "$tag" ]; then
    exec git-cliff --strip all "$range"
fi
if [ -n "$changelog" ]; then
    exec git-cliff --tag "$tag" --prepend "$changelog" "$range"
fi

# Behind a pipe, a failure of git-cliff would leave notes that say nothing.
notes=$(mktemp)
trap 'rm -f "$notes"' EXIT
git-cliff --tag "$tag" --strip all "$range" >"$notes"
LC_ALL=C awk -v limit="$limit" '
    { size += length($0) + 1 }
    size > limit { cut = 1; exit }
    { print }
    END { if (cut) print "\nThe list is cut short here; the link below shows every change." }' "$notes"
[ -z "$previous" ] ||
    printf '\n[Every change](https://github.com/lynxbase/lynxdb/compare/%s...%s)\n' \
        "$previous" "$tag"
