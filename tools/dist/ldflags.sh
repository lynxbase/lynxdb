#!/bin/sh
# usage: ldflags.sh
# Prints the linker flags that stamp a binary with its version, its commit and
# the time of that commit. LYNXDB_VERSION and LYNXDB_COMMIT override what git
# reports.
set -eu

cd "$(dirname -- "$0")/../.."

package=github.com/lynxbase/lynxdb/internal/buildinfo
version=${LYNXDB_VERSION:-$(tools/release/version.sh current)}
commit=${LYNXDB_COMMIT:-$(git rev-parse --short=12 HEAD)}
date=$(TZ=UTC0 git log -1 --date=format-local:%Y-%m-%dT%H:%M:%SZ --format=%cd)

printf -- '-X %s.Version=%s -X %s.Commit=%s -X %s.Date=%s\n' \
    "$package" "$version" "$package" "$commit" "$package" "$date"
