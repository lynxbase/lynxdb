#!/bin/sh
# usage: formula.sh [--line] <version> <tag> <SHA256SUMS>
# Prints the Homebrew formula lynxdb, which installs the archives of a
# release. With --line, prints the keg-only lynxdb@<line> instead, which
# follows the releases of one line. LYNXDB_RELEASE_URL overrides where the
# directory of each tag is served.
set -eu

line=
if [ "${1:-}" = --line ]; then
    line=$(printf '%s\n' "${2:-}" | cut -d. -f1-2)
    shift
fi
if [ $# -ne 3 ]; then
    echo "usage: formula.sh [--line] <version> <tag> <SHA256SUMS>" >&2
    exit 2
fi

version=$1
tag=$2
sums=$3
base=${LYNXDB_RELEASE_URL:-https://github.com/lynxbase/lynxdb/releases/download}/$tag

class=Lynxdb
keg_only=
if [ -n "$line" ]; then
    # Homebrew names the class of lynxdb@26.11 LynxdbAT2611.
    class=LynxdbAT$(printf '%s\n' "$line" | tr -d .)
    keg_only='
  keg_only :versioned_formula
'
fi

# archive <platform>: the url and sha256 lines of one archive.
archive()
{
    name=lynxdb-$version-$1.tar.gz
    sum=$(awk -v name="$name" '$2 == name { print $1 }' "$sums")
    if [ -z "$sum" ]; then
        echo "formula.sh: $sums has no checksum for $name" >&2
        exit 1
    fi
    printf '      url "%s/%s"\n      sha256 "%s"\n' "$base" "$name" "$sum"
}

# A failure inside the here-document below would not stop the script.
darwin_arm64=$(archive darwin-arm64)
darwin_amd64=$(archive darwin-amd64)
linux_arm64=$(archive linux-arm64)
linux_amd64=$(archive linux-amd64)

cat <<FORMULA
class $class < Formula
  desc "Log analytics database in one binary, queried with OpenSearch PPL"
  homepage "https://lynxdb.org"
  version "$version"
  license "Apache-2.0"
$keg_only
  on_macos do
    on_arm do
$darwin_arm64
    end
    on_intel do
$darwin_amd64
    end
  end

  on_linux do
    on_arm do
$linux_arm64
    end
    on_intel do
$linux_amd64
    end
  end

  def install
    bin.install "lynxdb"
  end

  test do
    assert_equal version.to_s, shell_output("#{bin}/lynxdb version --short").strip
  end
end
FORMULA
