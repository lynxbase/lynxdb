#!/usr/bin/env bash
set -euo pipefail

# Sync LynxDB's pinned rsigma golden corpus.
#
# Required tools:
# - git, for cloning rsigma
# - cargo, for building the rsigma CLI
# - go, for the final SPL2 parse check
#
# This script is developer-facing only. It is not intended for CI.

rsigma_ref="v0.9.0"

while (($# > 0)); do
  case "$1" in
    --rsigma-ref)
      if (($# < 2)); then
        echo "--rsigma-ref requires a tag or sha" >&2
        exit 2
      fi
      rsigma_ref="$2"
      shift 2
      ;;
    -h|--help)
      echo "usage: scripts/sync_rsigma_golden.sh [--rsigma-ref <tag-or-sha>]"
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo is required to build rsigma-cli" >&2
  exit 1
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
golden_dir="$repo_root/pkg/sigmaqueries/testdata/golden"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

mkdir -p "$golden_dir"

git clone --depth 1 --branch "$rsigma_ref" https://github.com/timescale/rsigma "$tmpdir/rsigma"

(cd "$tmpdir/rsigma" && cargo build --release -p rsigma)

src_dir="$tmpdir/rsigma/crates/rsigma-convert/tests/golden/lynxdb"
rsigma_bin="$tmpdir/rsigma/target/release/rsigma"

yaml_files=()
while IFS= read -r file; do
  yaml_files+=("$file")
done < <(find "$src_dir" -maxdepth 1 -name '*.yml' -type f | sort)
if ((${#yaml_files[@]} == 0)); then
  echo "no rsigma LynxDB golden YAML files found under $src_dir" >&2
  exit 1
fi

printf '{\n  "rsigma_version": "%s",\n  "queries": [\n' "${rsigma_ref#v}" >"$golden_dir/manifest.json"

for i in "${!yaml_files[@]}"; do
  yml="${yaml_files[$i]}"
  name="$(basename "$yml" .yml)"
  cp "$yml" "$golden_dir/$name.yml"
  "$rsigma_bin" convert -t lynxdb -f default "$yml" >"$golden_dir/$name.spl2"
  printf '{"events": [], "matches": []}\n' >"$golden_dir/$name.matches.json"

  title="$(awk -F': ' '/^title:/ {print $2; exit}' "$yml")"
  comma=","
  if ((i == ${#yaml_files[@]} - 1)); then
    comma=""
  fi
  cat >>"$golden_dir/manifest.json" <<EOF
    {
      "fixture": "$name",
      "line": 1,
      "rule_id": "",
      "title": "$title",
      "level": "",
      "tags": []
    }$comma
EOF
done

printf '  ],\n  "fixtures": {\n' >>"$golden_dir/manifest.json"

for i in "${!yaml_files[@]}"; do
  yml="${yaml_files[$i]}"
  name="$(basename "$yml" .yml)"
  title="$(awk -F': ' '/^title:/ {print $2; exit}' "$yml")"
  comma=","
  if ((i == ${#yaml_files[@]} - 1)); then
    comma=""
  fi
  cat >>"$golden_dir/manifest.json" <<EOF
    "$name": {
      "line": 1,
      "rule_id": "",
      "title": "$title",
      "level": "",
      "tags": []
    }$comma
EOF
done

printf '  }\n}\n' >>"$golden_dir/manifest.json"

"$repo_root/scripts/check_rsigma_golden_parses.sh"

git -C "$repo_root" status --short pkg/sigmaqueries/testdata/golden/
