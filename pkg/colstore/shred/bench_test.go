package shred

import (
	"bytes"
	"os"
	"testing"
)

// benchLines returns the documents of the NDJSON file named by
// LYNXDB_BENCH_NDJSON, capped at limit bytes.
func benchLines(b *testing.B, limit int) (lines [][]byte, total int) {
	b.Helper()
	path := os.Getenv("LYNXDB_BENCH_NDJSON")
	if path == "" {
		b.Skip("set LYNXDB_BENCH_NDJSON to an NDJSON file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		b.Fatal(err)
	}
	if len(data) > limit {
		data = data[:bytes.LastIndexByte(data[:limit], '\n')+1]
	}
	for len(data) > 0 {
		end := bytes.IndexByte(data, '\n')
		if end < 0 {
			end = len(data)
		}
		if end > 0 {
			lines = append(lines, data[:end])
			total += end
		}
		data = data[min(end+1, len(data)):]
	}

	return lines, total
}

func BenchmarkAddJSON(b *testing.B) {
	lines, total := benchLines(b, 64<<20)
	batch := NewBatch(Options{TimeField: "time_us"})
	b.SetBytes(int64(total))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, line := range lines {
			if batch.Full() {
				batch.Reset()
			}
			if err := batch.AddJSON(line, 0); err != nil {
				b.Fatal(err)
			}
		}
		batch.Reset()
	}
	b.ReportMetric(float64(len(lines))*float64(b.N)/b.Elapsed().Seconds(), "docs/s")
}

func benchNDJSON(b *testing.B, add func(batch *Batch, chunk []byte)) {
	path := os.Getenv("LYNXDB_BENCH_NDJSON")
	if path == "" {
		b.Skip("set LYNXDB_BENCH_NDJSON to an NDJSON file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		b.Fatal(err)
	}
	if len(data) > 64<<20 {
		data = data[:bytes.LastIndexByte(data[:64<<20], '\n')+1]
	}
	batch := NewBatch(Options{TimeField: "time_us"})
	docs := 0
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rest := data
		for len(rest) > 0 {
			var chunk []byte
			chunk, rest = SplitLines(rest, 8192)
			add(batch, chunk)
			docs += batch.Rows
			batch.Reset()
		}
	}
	b.ReportMetric(float64(docs)/b.Elapsed().Seconds(), "docs/s")
}

func BenchmarkAddNDJSON(b *testing.B) {
	benchNDJSON(b, func(batch *Batch, chunk []byte) { batch.AddNDJSON(chunk, 0, nil) })
}
