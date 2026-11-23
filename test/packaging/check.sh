#!/bin/sh
# usage: check.sh [--default-dir] <site> <bad site> <version>
# Runs the installer of the download site in the directory <site> and checks
# what it installs. <bad site> serves archives that do not match their
# checksums. With --default-dir it also installs where the installer chooses
# to, which only a throwaway machine should allow.
set -eu

default_dir=0
if [ "${1:-}" = --default-dir ]; then
    default_dir=1
    shift
fi
site=$1
bad=$2
version=$3

scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT

fail()
{
    echo "check.sh: $*" >&2
    exit 1
}

# install <site> <directory> [<installer option>...]
install()
{
    base=$1
    dir=$2
    shift 2
    LYNXDB_BASE_URL="file://$base" LYNXDB_INSTALL_DIR="$dir" LYNXDB_NO_MODIFY_PATH=1 \
        sh "$site/install.sh" --quiet "$@"
}

# installed <directory>: stops unless it holds the version under test.
installed()
{
    reported=$("$1/lynxdb" version --short) || fail "$1/lynxdb does not run"
    [ "$reported" = "$version" ] || fail "$1/lynxdb reports '$reported', expected '$version'"
}

install "$site" "$scratch/channel" >/dev/null
installed "$scratch/channel"

install "$site" "$scratch/pinned" --version "$version" >/dev/null
installed "$scratch/pinned"

install "$site" "$scratch/tagged" --version "v$version-stable" >/dev/null
installed "$scratch/tagged"

out=$(PATH="$scratch/channel:$PATH" install "$site" "$scratch/again")
case $out in
*'already installed'*) ;;
*) fail "a second run does not say the version is already installed: $out" ;;
esac
[ ! -e "$scratch/again" ] || fail 'a second run installed the same version again'

if install "$bad" "$scratch/bad" >/dev/null 2>&1; then
    fail 'an archive that does not match its checksum was installed'
fi
[ ! -e "$scratch/bad/lynxdb" ] || fail 'a binary from a refused archive was left behind'

if install "$site" "$scratch/missing" --version 1.2.3.4 >/dev/null 2>&1; then
    fail 'a version that was never published was installed'
fi

if install "$site" "$scratch/lts" --channel lts >/dev/null 2>&1; then
    fail 'a channel without a release was installed from'
fi

status=0
install "$site" "$scratch/option" --no-such-option >/dev/null 2>&1 || status=$?
[ "$status" -eq 2 ] || fail "an unknown option ends with status $status, expected 2"

if [ "$default_dir" -eq 1 ]; then
    if [ "$(id -u)" -eq 0 ]; then
        chosen=/usr/local/bin
    else
        chosen=$HOME/.local/bin
    fi
    LYNXDB_BASE_URL="file://$site" LYNXDB_NO_MODIFY_PATH=1 sh "$site/install.sh" --quiet >/dev/null
    installed "$chosen"
fi

echo "installer: $version installs from its channel, by version and by tag"
