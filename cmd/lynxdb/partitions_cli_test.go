package main

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/client"
)

func TestPartitionsCLITransfer(t *testing.T) {
	source := setupServerWithData(t)
	destination := setupServerWithData(t)
	archive := filepath.Join(t.TempDir(), "detached web logs")

	run := func(server string, args ...string) string {
		t.Helper()
		stdout, _, err := runCmd(t, append([]string{"--server", server, "--format", "json", "partitions"}, args...)...)
		if err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		return stdout
	}
	partition := func(server string, args ...string) client.PartitionInfo {
		t.Helper()
		var p client.PartitionInfo
		if err := json.Unmarshal([]byte(run(server, args...)), &p); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		if p.Start == "" || p.End <= p.Start || p.Events != 1000 || p.Bytes <= 0 || p.Parts < 1 {
			t.Fatalf("%v: unexpected partition %+v", args, p)
		}
		return p
	}

	p := partition(source, "list", "main")
	partition(source, "detach", "main", p.Start, "--to", archive)
	if out := run(source, "list", "main"); out != "" {
		t.Fatalf("partitions after detach: %s", out)
	}

	if _, _, err := runCmd(t, "--server", destination, "partitions", "attach", "main", archive); err == nil {
		t.Fatal("attach over existing events succeeded")
	}
	start, err := time.Parse(time.RFC3339Nano, p.Start)
	if err != nil {
		t.Fatal(err)
	}
	partition(destination, "drop", "main", start.Format(time.DateOnly), "--yes")
	imported := partition(destination, "attach", "main", archive)
	if imported.Start != p.Start || imported.End != p.End {
		t.Fatalf("attached partition %+v, want span [%s, %s)", imported, p.Start, p.End)
	}

	stdout, _, err := runCmd(t, "--server", destination, "query", "--format", "json", "source=main level=ERROR | stats count()")
	if err != nil {
		t.Fatal(err)
	}
	if n := jsonCount(t, stdout); n != 294 {
		t.Fatalf("destination has %d error events, want 294", n)
	}
}
