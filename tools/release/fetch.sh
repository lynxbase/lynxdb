#!/bin/sh
# usage: fetch.sh <directory>
# Downloads the files of every published release, candidates included, into
# <directory>/<tag>/, the layout site.sh reads.
# Needs gh, signed in to the repository.
set -eu

[ $# -eq 1 ] || {
    echo "usage: fetch.sh <directory>" >&2
    exit 2
}

gh release list --exclude-drafts --limit 1000 --json tagName --jq '.[].tagName' |
    while read -r tag; do
        case $tag in
        v*-stable | v*-lts | v*-prestable) ;;
        *) continue ;;
        esac
        mkdir -p "$1/$tag"
        gh release download "$tag" --dir "$1/$tag" --clobber
    done
