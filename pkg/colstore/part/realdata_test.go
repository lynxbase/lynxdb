package part

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
)

// realData returns the NDJSON file named by LYNXDB_BENCH_NDJSON.
func realData(t testing.TB) []byte {
	t.Helper()
	path := os.Getenv("LYNXDB_BENCH_NDJSON")
	if path == "" {
		t.Skip("set LYNXDB_BENCH_NDJSON to an NDJSON file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	return data
}

func encodeNDJSON(t testing.TB, data []byte, blockRows int, opts Options, timeField string) []byte {
	t.Helper()
	var buf bytes.Buffer
	buf.Grow(len(data) / 3)
	w := NewWriter(&buf, opts)
	batch := shred.NewBatch(shred.Options{TimeField: timeField})
	for len(data) > 0 {
		var chunk []byte
		chunk, data = shred.SplitLines(data, blockRows)
		if errs := batch.AddNDJSON(chunk, 0, nil); len(errs) != 0 {
			t.Fatalf("line %d: %v", errs[0].Line, errs[0].Err)
		}
		if err := w.WriteBatch(batch); err != nil {
			t.Fatal(err)
		}
		batch.Reset()
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	return buf.Bytes()
}

func TestRealDataRoundTrip(t *testing.T) {
	data := realData(t)
	if len(data) > 96<<20 {
		data = data[:bytes.LastIndexByte(data[:96<<20], '\n')+1]
	}
	p, err := Open(encodeNDJSON(t, data, 16384, Options{}, ""))
	if err != nil {
		t.Fatal(err)
	}
	got, _ := readAll(t, p)
	lines := bytes.Split(bytes.TrimSpace(data), []byte{'\n'})
	if len(got) != len(lines) {
		t.Fatalf("rows %d, want %d", len(got), len(lines))
	}
	for i := range lines {
		if !sameJSON(t, got[i], string(lines[i])) {
			t.Fatalf("row %d\n got  %s\n want %s", i, got[i], lines[i])
		}
	}
	t.Logf("%d documents round-tripped, %d bytes of JSON in %d bytes", len(lines), len(data), p.Size())
}

func TestRealDataSize(t *testing.T) {
	data := realData(t)
	rows := bytes.Count(data, []byte{'\n'})
	for _, tc := range []struct {
		name      string
		blockRows int
		opts      Options
	}{
		{"8k fast", 8192, Options{}},
		{"16k fast", 16384, Options{}},
		{"64k fast", 65536, Options{}},
		{"16k fast, no filter", 16384, Options{NoTokenFilter: true}},
		{"16k compact", 16384, Options{Level: LevelCompact}},
		{"64k compact", 65536, Options{Level: LevelCompact}},
		{"64k compact, no filter", 65536, Options{Level: LevelCompact, NoTokenFilter: true}},
	} {
		start := time.Now()
		out := encodeNDJSON(t, data, tc.blockRows, tc.opts, "time_us")
		took := time.Since(start)
		t.Logf("%-24s %7.1f MB  %5.1f bytes/row  ratio %4.2fx  %5.2fs  %4.0fk docs/s/core",
			tc.name, float64(len(out))/1e6, float64(len(out))/float64(rows),
			float64(len(data))/float64(len(out)), took.Seconds(), float64(rows)/took.Seconds()/1e3)
	}
}
