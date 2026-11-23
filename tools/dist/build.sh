#!/bin/sh
# usage: build.sh [<os>-<arch>...]
# Builds the release binary of every platform, or of the ones named, and
# stages each in dist/stage/<os>-<arch>/. LYNXDB_VERSION and LYNXDB_COMMIT
# override what git reports.
set -eu

root=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"

platforms='linux-amd64 linux-arm64 linux-armv7 darwin-amd64 darwin-arm64 windows-amd64'
[ $# -eq 0 ] || platforms=$*

version=${LYNXDB_VERSION:-$(tools/release/version.sh current)}
ldflags="-s -w $(tools/dist/ldflags.sh)"
host=$(go env GOHOSTOS)-$(go env GOHOSTARCH)

for platform in $platforms; do
    os=${platform%-*}
    arch=${platform#*-}
    case $platform in
    linux-amd64 | linux-arm64 | darwin-amd64 | darwin-arm64 | windows-amd64) arm= ;;
    linux-armv7)
        arch=arm
        arm=7
        ;;
    *)
        echo "build.sh: unknown platform: $platform" >&2
        exit 1
        ;;
    esac
    binary=lynxdb
    [ "$os" != windows ] || binary=lynxdb.exe

    stage=dist/stage/$platform
    rm -rf "$stage"
    mkdir -p "$stage"
    env CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" ${arm:+GOARM="$arm"} \
        go build -trimpath -ldflags "$ldflags" -o "$stage/$binary" ./cmd/lynxdb

    if [ "$platform" = "$host" ]; then
        reported=$("$stage/$binary" version --short)
        if [ "$reported" != "$version" ]; then
            echo "build.sh: built '$reported', expected '$version'" >&2
            exit 1
        fi
    fi
done

ls -l dist/stage/*/
