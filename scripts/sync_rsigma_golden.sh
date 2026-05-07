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
with_matches=false
output_dir=""

while (($# > 0)); do
  case "$1" in
    --with-matches)
      with_matches=true
      shift
      ;;
    --rsigma-ref)
      if (($# < 2)); then
        echo "--rsigma-ref requires a tag or sha" >&2
        exit 2
      fi
      rsigma_ref="$2"
      shift 2
      ;;
    --output-dir)
      if (($# < 2)); then
        echo "--output-dir requires a directory" >&2
        exit 2
      fi
      output_dir="$2"
      shift 2
      ;;
    -h|--help)
      echo "usage: scripts/sync_rsigma_golden.sh [--with-matches] [--rsigma-ref <tag-or-sha>] [--output-dir <dir>]"
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
golden_dir="${output_dir:-$repo_root/pkg/sigmaqueries/testdata/golden}"
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
  if [[ "$with_matches" == false ]]; then
    printf '{"events": [], "matches": []}\n' >"$golden_dir/$name.matches.json"
  fi

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

"$repo_root/scripts/check_rsigma_golden_parses.sh" "$golden_dir"

if [[ "$with_matches" == true ]]; then
  cat >"$tmpdir/write_rsigma_matches.go" <<'GO'
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lynxbase/lynxdb/test/integration/sigmacompat"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: write_rsigma_matches <golden-dir>")
		os.Exit(2)
	}
	refs, err := sigmacompat.AllReferences()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, ref := range refs {
		data, err := json.MarshalIndent(ref, "", "  ")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		data = append(data, '\n')
		path := filepath.Join(os.Args[1], ref.Fixture+".matches.json")
		if err := os.WriteFile(path, data, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}
GO
  echo "writing reference match sets using local_reference_evaluator"
  echo "rsigma ${rsigma_ref} convert is still used for SPL2; this repository's sync path does not have a matched-indices rsigma eval mode, so --with-matches maps the deterministic synthetic datasets with the local reference evaluator."
  (cd "$repo_root" && go run "$tmpdir/write_rsigma_matches.go" "$golden_dir")
fi

if [[ "$golden_dir" == "$repo_root"/* ]]; then
  git -C "$repo_root" status --short "$golden_dir"
else
  echo "wrote rsigma golden corpus to $golden_dir"
fi
