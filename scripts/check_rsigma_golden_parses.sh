#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
golden_dir="$repo_root/pkg/sigmaqueries/testdata/golden"

if ! command -v go >/dev/null 2>&1; then
  echo "go is required to parse-check rsigma golden fixtures" >&2
  exit 1
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

cat >"$tmpdir/check_rsigma_golden_parses.go" <<'GO'
package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/spl2"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "no .spl2 files provided")
		os.Exit(1)
	}

	var failures int
	for _, path := range os.Args[1:] {
		if err := checkFile(path); err != nil {
			fmt.Fprintln(os.Stderr, err)
			failures++
		}
	}
	if failures > 0 {
		os.Exit(1)
	}
}

func checkFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)

	var failures int
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if _, err := spl2.ParseProgram(line); err != nil {
			fmt.Fprintf(os.Stderr, "%s:%d: %v\n", path, lineNo, err)
			failures++
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if failures > 0 {
		return fmt.Errorf("%s: %d parse error(s)", path, failures)
	}
	return nil
}
GO

files=()
while IFS= read -r file; do
  files+=("$file")
done < <(find "$golden_dir" -maxdepth 1 -name '*.spl2' -type f | sort)
if ((${#files[@]} == 0)); then
  echo "no .spl2 files found under $golden_dir" >&2
  exit 1
fi

(cd "$repo_root" && go run "$tmpdir/check_rsigma_golden_parses.go" "${files[@]}")
echo "parsed ${#files[@]} rsigma golden .spl2 fixture(s)"
