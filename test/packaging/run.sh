#!/bin/sh
# usage: run.sh [<case>...]
# Installs LynxDB from the archives in dist/ with the installer and checks the
# result. The archives are laid out as the download site a release publishes,
# next to a candidate and an older release, and the installer reads that site
# from the disk.
# Cases: host runs the installer on this machine; debian and alpine run it in
# containers, as root under dash and as an ordinary user under BusyBox, and
# need Docker. The default is all of them.
set -eu

root=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"

[ $# -gt 0 ] || set -- host debian alpine

version=
for archive in dist/lynxdb-*-linux-amd64.tar.gz; do
    [ -f "$archive" ] || continue
    if [ -n "$version" ]; then
        echo "run.sh: dist/ holds the archives of more than one version" >&2
        exit 1
    fi
    version=${archive#dist/lynxdb-}
    version=${version%-linux-amd64.tar.gz}
done
if [ -z "$version" ]; then
    echo "run.sh: dist/ holds no archive; run tools/dist/package.sh first" >&2
    exit 1
fi

work=$root/dist/test
rm -rf "$work"
mkdir -p "$work/releases/v$version-stable"
cp dist/lynxdb-"$version"-* dist/install.sh dist/SHA256SUMS "$work/releases/v$version-stable/"
# The releases around it only have to be told apart, so they hold no archive.
for tag in "v$version-prestable" v0.0.0.1-stable v99.1.1.1-prestable; do
    mkdir "$work/releases/$tag"
    echo "the installer of $tag" >"$work/releases/$tag/install.sh"
done

site=$work/site
tools/release/site.sh "$work/releases" "$site"

expect()
{
    if [ "$(cat "$site/$1")" != "$2" ]; then
        echo "run.sh: $1 holds '$(cat "$site/$1")', expected '$2'" >&2
        exit 1
    fi
}
expect channels/stable "$version"
expect "versions/$version" "v$version-stable"
expect versions/0.0.0.1 v0.0.0.1-stable
expect versions/99.1.1.1 v99.1.1.1-prestable
cmp -s "$site/install.sh" dist/install.sh || {
    echo "run.sh: the site does not serve the installer of the newest release" >&2
    exit 1
}
if [ -e "$site/channels/lts" ]; then
    echo "run.sh: the site names an lts release where there is none" >&2
    exit 1
fi

# The same site with archives that do not match their checksums.
bad=$work/bad
mkdir -p "$bad/dist/v$version-stable"
cp -R "$site/channels" "$site/versions" "$bad/"
cp "$site/dist/v$version-stable/SHA256SUMS" "$bad/dist/v$version-stable/"
for archive in "$site/dist/v$version-stable"/lynxdb-*; do
    echo 'not the archive' >"$bad/dist/v$version-stable/$(basename "$archive")"
done

for case in "$@"; do
    echo "== $case"
    case $case in
    host)
        mkdir -p "$work/home"
        env -i HOME="$work/home" PATH=/usr/bin:/bin:/usr/sbin:/sbin \
            sh test/packaging/check.sh "$site" "$bad" "$version"
        ;;
    debian | alpine)
        image=lynxdb-test-$case
        docker build -q -t "$image" -f "test/packaging/images/$case.Dockerfile" \
            test/packaging/images >/dev/null
        docker run --rm --entrypoint sh \
            -v "$work:/work:ro" -v "$root/test/packaging:/checks:ro" \
            "$image" /checks/check.sh --default-dir /work/site /work/bad "$version"
        ;;
    *)
        echo "run.sh: unknown case: $case" >&2
        exit 1
        ;;
    esac
done
