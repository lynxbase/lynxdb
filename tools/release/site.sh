#!/bin/sh
# usage: site.sh <releases directory> <site directory>
# Builds the download site from <releases directory>/<tag>/<release assets>:
# the files of every release under dist/<tag>/, the installer of the newest
# stable or lts one, and the files install.sh reads to find a version:
# channels/<channel> names the newest version of a channel and
# versions/<version> the tag that holds a version.
set -eu

[ $# -eq 2 ] || {
    echo "usage: site.sh <releases directory> <site directory>" >&2
    exit 2
}

releases=$(CDPATH='' cd -- "$1" && pwd)
rm -rf "$2"
mkdir -p "$2/dist" "$2/versions"
site=$(CDPATH='' cd -- "$2" && pwd)

# newest <channel>...: the tag, without its v, of the highest version in these channels.
newest()
{
    for channel in "$@"; do
        for release in "$releases"/v*-"$channel"; do
            [ -d "$release" ] || continue
            tag=${release##*/}
            printf '%s\n' "${tag#v}"
        done
    done | sort -t. -k1,1n -k2,2n -k3,3n -k4,4n | tail -n 1
}

# A release cut from the commit of a candidate carries the candidate's
# version, so the candidates are written first and the release replaces them.
for channel in prestable stable lts; do
    for release in "$releases"/v*-"$channel"; do
        [ -d "$release" ] || continue
        cp -R "$release" "$site/dist/"
        tag=${release##*/}
        version=${tag#v}
        printf '%s\n' "$tag" >"$site/versions/${version%-*}"
    done
done
if [ -z "$(ls "$site/dist")" ]; then
    echo "site.sh: no release under $releases" >&2
    exit 1
fi

latest=$(newest stable lts)
# Candidates alone give no version to install by channel.
[ -n "$latest" ] || exit 0

mkdir "$site/channels"
printf '%s\n' "${latest%-*}" >"$site/channels/stable"
latest_lts=$(newest lts)
[ -z "$latest_lts" ] || printf '%s\n' "${latest_lts%-*}" >"$site/channels/lts"

install -m 0755 "$releases/v$latest/install.sh" "$site/install.sh"
