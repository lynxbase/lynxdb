package part

import (
	"bytes"
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/lynxbase/lynxdb/internal/benchdata"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
)

// The sample logs cover shapes unlike each other: real OpenSSH and OpenStack
// lines, access logs and files that mix JSON with text, and with the two
// generated samples flat JSON application logs and syslog-style text of
// many blocks each.
var sampleFiles = []string{
	"logs/OpenSSH_2k.log", "logs/OpenStack_2k.log", "logs/nginx_access.log", "logs/access.log",
	"logs/mixed_json.log", "logs/audit_security.log", "logs/audit_transactions.log",
	"logs/backend_server.log", "logs/frontend_console.log",
}

func TestSampleLogsRoundTrip(t *testing.T) {
	root := filepath.Join("..", "..", "..", "testdata")
	zenc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	samples := map[string][]byte{
		"generated JSON logs": benchdata.AppendJSONLogs(nil, 100000),
		"generated text logs": benchdata.AppendTextLogs(nil, 100000),
	}
	for _, name := range sampleFiles {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		samples[name] = data
	}
	for _, name := range slices.Sorted(maps.Keys(samples)) {
		data := samples[name]
		lines := bytes.Split(bytes.TrimRight(data, "\n"), []byte{'\n'})

		start := time.Now()
		var buf bytes.Buffer
		w := NewWriter(&buf, Options{})
		batch := shred.NewBatch(shred.Options{})
		isJSON := make([]bool, len(lines))
		for i, line := range lines {
			// A line that is not a JSON object is kept as text.
			if err := batch.AddJSON(line, int64(i)); err == nil {
				isJSON[i] = true
			} else {
				batch.AddText(line, int64(i))
			}
			if batch.Rows == 16384 || i == len(lines)-1 {
				if err := w.WriteBatch(batch); err != nil {
					t.Fatal(err)
				}
				batch.Reset()
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		took := time.Since(start)

		p, err := Open(buf.Bytes())
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		got, times := readAll(t, p)
		if len(got) != len(lines) {
			t.Fatalf("%s: %d rows, want %d", name, len(got), len(lines))
		}
		jsonRows := 0
		for i, line := range lines {
			if times[i] != int64(i) {
				t.Fatalf("%s row %d: time %d", name, i, times[i])
			}
			if isJSON[i] {
				jsonRows++
				if !sameJSON(t, got[i], string(line)) {
					t.Fatalf("%s row %d\n got  %s\n want %s", name, i, got[i], line)
				}

				continue
			}
			var row map[string]string
			if err := json.Unmarshal([]byte(got[i]), &row); err != nil || len(row) != 1 || row[shred.RawPath] != string(bytes.ToValidUTF8(line, []byte("�"))) {
				t.Fatalf("%s row %d\n got  %s\n want text %q", name, i, got[i], line)
			}
		}
		plain := len(zenc.EncodeAll(data, nil))
		t.Logf("%-28s %6d rows (%3.0f%% JSON)  raw %8d  part %8d  ratio %5.1fx  (whole-file zstd %5.1fx)  %4.0fk rows/s",
			name, len(lines), 100*float64(jsonRows)/float64(len(lines)), len(data), p.Size(),
			float64(len(data))/float64(p.Size()), float64(len(data))/float64(plain), float64(len(lines))/took.Seconds()/1e3)
	}
}
