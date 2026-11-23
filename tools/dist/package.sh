#!/bin/sh
# usage: package.sh [<output directory>]
# Turns every tree under dist/stage into a release archive in dist/, or in the
# directory given: a tarball per platform and a zip for Windows, each holding
# the binary, the README and the license. Adds the installer and SHA256SUMS,
# which lists them all. LYNXDB_VERSION overrides the version git reports.
set -eu

root=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"

version=${LYNXDB_VERSION:-$(tools/release/version.sh current)}
SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH:-$(git log -1 --format=%ct)}
export SOURCE_DATE_EPOCH

stages=$root/dist/stage
dist=$root/dist
if [ $# -gt 0 ]; then
    mkdir -p "$1"
    dist=$(CDPATH='' cd -- "$1" && pwd)
fi

# stamp <tree>: gives every file the time of the commit, which is what a zip
# records for it.
stamp()
{
    if touch -d "@$SOURCE_DATE_EPOCH" "$1" 2>/dev/null; then
        find "$1" -exec touch -d "@$SOURCE_DATE_EPOCH" {} +
    else
        find "$1" -exec env TZ=UTC0 \
            touch -t "$(date -u -r "$SOURCE_DATE_EPOCH" +%Y%m%d%H%M.%S)" {} +
    fi
}

# archive <parent directory> <entry> <output.tar.gz>
archive()
{
    if tar --version 2>/dev/null | grep -q 'GNU tar'; then
        tar --sort=name --owner=0 --group=0 --numeric-owner --mtime="@$SOURCE_DATE_EPOCH" \
            -C "$1" -cf "$3.tar" "$2"
    else
        tar --no-xattrs --no-mac-metadata --uid 0 --gid 0 --numeric-owner \
            -C "$1" -cf "$3.tar" "$2"
    fi
    gzip -n -9 <"$3.tar" >"$3"
    rm "$3.tar"
}

# archive_zip <parent directory> <entry> <output.zip>
archive_zip()
{
    stamp "$1/$2"
    (cd "$1" && find "$2" | LC_ALL=C sort | TZ=UTC0 zip -q -X -@ "$3")
}

rm -rf "$dist/work"
rm -f "$dist"/*.tar.gz "$dist"/*.zip "$dist/install.sh" "$dist/SHA256SUMS"

found=0
for stage in "$stages"/*; do
    [ -d "$stage" ] || continue
    found=1
    platform=$(basename "$stage")
    name=lynxdb-$version-$platform
    tree=$dist/work/$name
    mkdir -p "$tree"
    install -m 0644 README.md LICENSE "$tree/"
    # The mode is set here and not copied: an artifact store between the build
    # and this script does not keep it.
    case $platform in
    windows-*)
        install -m 0755 "$stage/lynxdb.exe" "$tree/"
        archive_zip "$dist/work" "$name" "$dist/$name.zip"
        ;;
    *)
        install -m 0755 "$stage/lynxdb" "$tree/"
        archive "$dist/work" "$name" "$dist/$name.tar.gz"
        ;;
    esac
done
if [ "$found" -eq 0 ]; then
    echo "package.sh: nothing is staged under dist/stage; run tools/dist/build.sh first" >&2
    exit 1
fi
rm -rf "$dist/work"

install -m 0755 packaging/install.sh "$dist/install.sh"

(
    cd "$dist"
    set --
    for file in lynxdb-*.tar.gz lynxdb-*.zip install.sh; do
        if [ -f "$file" ]; then
            set -- "$@" "$file"
        fi
    done
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$@" >SHA256SUMS
    else
        shasum -a 256 "$@" >SHA256SUMS
    fi
)

find "$dist" -maxdepth 1 -type f | sort
