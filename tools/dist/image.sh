#!/bin/sh
# usage: image.sh [--push] [<tag>...]
# Builds the container image from the Linux binaries under dist/stage and
# names it <image>:<tag> for every tag, or <image>:dev without one. <image> is
# ghcr.io/lynxbase/lynxdb unless LYNXDB_IMAGE names another.
# As is, it builds for this machine and loads the image into Docker. With
# --push it builds for amd64 and arm64, pushes the image and prints its digest.
# LYNXDB_VERSION overrides the version git reports.
set -eu

root=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"

push=0
if [ "${1:-}" = --push ]; then
    push=1
    shift
fi
[ $# -gt 0 ] || set -- dev

image=${LYNXDB_IMAGE:-ghcr.io/lynxbase/lynxdb}
version=${LYNXDB_VERSION:-$(tools/release/version.sh current)}

names=
for tag in "$@"; do
    names="$names --tag $image:$tag"
done
# shellcheck disable=SC2086 # every name is one word
set -- $names \
    --label "org.opencontainers.image.source=https://github.com/lynxbase/lynxdb" \
    --label "org.opencontainers.image.version=$version" \
    --label "org.opencontainers.image.revision=$(git rev-parse HEAD)" \
    --file packaging/docker/Dockerfile dist/stage

if [ "$push" -eq 0 ]; then
    exec docker build "$@"
fi

# The builder Docker starts with cannot push an image of several platforms.
# The one created here runs the BuildKit that buildkit.Dockerfile pins.
builder=lynxdb-release
buildkit=$(sed -n 's/^FROM //p' packaging/docker/buildkit.Dockerfile)
docker buildx inspect "$builder" >/dev/null 2>&1 ||
    docker buildx create --name "$builder" --driver docker-container \
        --driver-opt "image=$buildkit" >/dev/null
metadata=$(mktemp)
# The release workflow attests the image itself; an attestation from the
# builder would show up as one more platform of the image.
docker buildx build --builder "$builder" --platform linux/amd64,linux/arm64 --push \
    --provenance=false --sbom=false --metadata-file "$metadata" "$@" >&2
sed -n 's/.*"containerimage.digest": *"\([^"]*\)".*/\1/p' "$metadata"
rm "$metadata"
