// genbench writes testdata/bench/json_100k.log and text_100k.log, the
// fixtures of the profiling scripts; every run writes the same bytes.
//
// Usage, from the repository root:
//
//	go run ./tools/dev/genbench
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/lynxbase/lynxdb/internal/benchdata"
)

func main() {
	dir := "testdata/bench"
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir: %v\n", err)
		os.Exit(1)
	}

	const events = 100000
	write(filepath.Join(dir, "json_100k.log"), benchdata.AppendJSONLogs(nil, events))
	write(filepath.Join(dir, "text_100k.log"), benchdata.AppendTextLogs(nil, events))
}

func write(path string, data []byte) {
	if err := os.WriteFile(path, data, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write %s: %v\n", path, err)
		os.Exit(1)
	}
	fmt.Printf("wrote %s: %d bytes\n", path, len(data))
}
