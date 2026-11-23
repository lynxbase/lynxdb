#!/bin/sh
# usage: image.sh
# Builds the container image for this machine from the Linux binary under
# dist/stage and checks that it reports the version it was built as and that
# the server it starts answers.
set -eu

root=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"

case $(docker info --format '{{.Architecture}}') in
aarch64 | arm64) arch=arm64 ;;
x86_64 | amd64) arch=amd64 ;;
*)
    echo "image.sh: unsupported Docker host architecture" >&2
    exit 1
    ;;
esac
[ -f "dist/stage/linux-$arch/lynxdb" ] || {
    echo "image.sh: dist/stage/linux-$arch holds no binary; run tools/dist/build.sh first" >&2
    exit 1
}

image=lynxdb-test
version=${LYNXDB_VERSION:-$(tools/release/version.sh current)}
LYNXDB_IMAGE=$image LYNXDB_VERSION=$version tools/dist/image.sh test >/dev/null

reported=$(docker run --rm "$image:test" version --short)
if [ "$reported" != "$version" ]; then
    echo "image.sh: the image runs '$reported', expected '$version'" >&2
    exit 1
fi

# The way the Docker guide starts it.
container=$(docker run --detach --publish 127.0.0.1::3100 \
    --env LYNXDB_LISTEN=0.0.0.0:3100 --env LYNXDB_DATA_DIR=/data "$image:test")
trap 'docker rm --force "$container" >/dev/null' EXIT
address=$(docker port "$container" 3100/tcp | head -n 1)
tries=0
until curl -fsS "http://$address/health" >/dev/null 2>&1; do
    tries=$((tries + 1))
    if [ "$tries" -ge 30 ]; then
        echo "image.sh: the server in the image did not answer on /health" >&2
        docker logs "$container" >&2
        exit 1
    fi
    sleep 1
done
echo "image: $version answers on /health"
