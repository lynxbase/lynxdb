package part

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
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
	writeNDJSON(t, NewWriter(&buf, opts), data, blockRows, timeField)

	return buf.Bytes()
}

// writeNDJSON writes the lines of data through w in blocks of blockRows rows
// and closes it.
func writeNDJSON(t testing.TB, w *Writer, data []byte, blockRows int, timeField string) {
	t.Helper()
	batch := shred.NewBatch(shred.Options{TimeField: timeField})
	for len(data) > 0 {
		var chunk []byte
		chunk, data = shred.SplitLines(data, blockRows)
		if errs, _ := batch.AddNDJSON(chunk, 0, shred.Intake{}, nil); len(errs) != 0 {
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

// TestRealDataFilterGroups reports what filter groups of several shapes cost
// in space and what token lookups read from them, and holds the shape the
// writer uses to its bounds.
func TestRealDataFilterGroups(t *testing.T) {
	data := realData(t)
	const blockRows = 16384
	encode := func(opts Options, group, size, stripes int) *Part {
		var buf bytes.Buffer
		buf.Grow(len(data) / 3)
		w := NewWriter(&buf, opts)
		w.groupBlocks, w.groupBytes, w.fixedStripes, w.columnBytes = group, size, stripes, math.MaxInt
		writeNDJSON(t, w, data, blockRows, "time_us")
		p, err := Open(buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}

		return p
	}
	// A filter of its natural size behind every block, with a checksum per
	// page, is what a part without stripes holds.
	separate := encode(Options{NoTokenFilter: true}, 1, 1, 1).Size()
	natural := encode(Options{}, 1, 1, 1)
	blocks, filters := natural.NumBlocks(), 0
	for i := 0; i < blocks; i++ {
		size := filterSize(natural, i) * tokenfilter.BlockBytes
		filters += size
		separate += size + 4*((size+filterPage-1)/filterPage) - 4
	}
	t.Logf("%d blocks, %d bytes with separate filters of %d bytes on average, which take a read for every block and token",
		blocks, separate, filters/blocks)

	const unbound = 1 << 40
	for _, shape := range []struct{ group, bytes, stripes int }{
		{32, unbound, 16}, {32, unbound, 64}, {32, unbound, 128}, {64, unbound, 128}, {64, unbound, 256}, {128, unbound, 128},
		{filterGroupBlocks, filterGroupBytes, filterStripes},
	} {
		p := encode(Options{}, shape.group, shape.bytes, shape.stripes)
		growth := float64(p.Size()-separate) / float64(separate)
		runs := groupRuns(p)
		stripe := uint64(0)
		for g := range p.groups {
			stripe = max(stripe, p.groups[g].stripe*tokenfilter.BlockBytes)
		}
		r := rand.New(rand.NewSource(1))
		const lookups = 300
		var one, three struct{ pages, faults, most int }
		for n := 0; n < lookups; n++ {
			hashes := [3]uint64{r.Uint64(), r.Uint64(), r.Uint64()}
			for _, run := range runs {
				reads := newFileReads()
				reads.lookup(p, hashes[0], run[0], run[1])
				one.pages, one.faults, one.most = one.pages+len(reads.pages), one.faults+reads.faults, max(one.most, reads.faults)
				reads.lookup(p, hashes[1], run[0], run[1])
				reads.lookup(p, hashes[2], run[0], run[1])
				three.pages, three.faults, three.most = three.pages+len(reads.pages), three.faults+reads.faults, max(three.most, reads.faults)
			}
		}
		bound := "any size"
		if shape.bytes != unbound {
			bound = fmt.Sprintf("%d MB", shape.bytes>>20)
		}
		t.Logf("groups of %3d blocks and %s in %3d stripes: %d groups, %d bytes (%+.3f%%), stripes of up to %6d bytes; one token reads %5.1f pages in %5.2f reads (at most %d a group), three tokens %5.1f pages in %5.2f reads (at most %d)",
			shape.group, bound, shape.stripes, len(runs), p.Size(), 100*growth, stripe,
			float64(one.pages)/lookups, float64(one.faults)/lookups, one.most,
			float64(three.pages)/lookups, float64(three.faults)/lookups, three.most)
		if shape.bytes == unbound {
			continue
		}
		if growth > 0.005 {
			t.Fatalf("striping grows the part by %.3f%%", 100*growth)
		}
		if one.most > 1 || three.most > 3 {
			t.Fatalf("a group takes %d reads for one token and %d for three", one.most, three.most)
		}
	}
}
