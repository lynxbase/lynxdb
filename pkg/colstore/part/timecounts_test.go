package part

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// blockTimes returns the row times of every block of p.
func blockTimes(t *testing.T, p *Part) [][]int64 {
	t.Helper()
	var b Block
	out := make([][]int64, p.NumBlocks())
	for i := range out {
		if err := p.OpenBlock(i, &b); err != nil {
			t.Fatal(err)
		}
		ref, _ := b.Column(0, vec.KindInt)
		var col vec.Column
		if err := b.Decode(ref, &col); err != nil {
			t.Fatal(err)
		}
		for row := 0; row < b.Rows; row++ {
			out[i] = append(out[i], col.Int(row))
		}
	}

	return out
}

// checkTimeCounts requires every record of every block of p to count the
// block's rows in buckets of its width, and the narrowest of block i to be
// want[i] wide.
func checkTimeCounts(t *testing.T, p *Part, want []int64) {
	t.Helper()
	for i, times := range blockTimes(t, p) {
		narrowest := int64(0)
		for _, granule := range []int64{86400e9, 3600e9, 60e9} {
			counts, width := p.TimeCounts(i, granule, nil)
			if width == 0 {
				continue
			}
			if granule%width != 0 {
				t.Fatalf("block %d: buckets of %d for a granule of %d", i, width, granule)
			}
			narrowest = width
			expect := map[int64]uint32{}
			for _, ts := range times {
				expect[floorDiv(ts, width)]++
			}
			got := map[int64]uint32{}
			for k, c := range counts {
				if k > 0 && c.Bucket <= counts[k-1].Bucket {
					t.Fatalf("block %d: buckets out of order", i)
				}
				got[c.Bucket] = c.Rows
			}
			if !reflect.DeepEqual(got, expect) {
				t.Fatalf("block %d: counts %v, want %v", i, got, expect)
			}
		}
		if i < len(want) && narrowest != want[i] {
			t.Fatalf("block %d counts in buckets of %d at the narrowest, want %d", i, narrowest, want[i])
		}
	}
}

// BenchmarkTimeCounts records the rows of a full block by bucket of time,
// for a block in key order whose rows lie anywhere in a day, and for one in
// time order whose rows lie in a few minutes.
func BenchmarkTimeCounts(b *testing.B) {
	r := rand.New(rand.NewSource(1))
	const day = int64(86400e9)
	from := 20 * 365 * day
	for _, c := range []struct {
		name string
		span int64
	}{{"day", day}, {"minutes", 5 * 60e9}} {
		times := make([]int64, shred.MaxRows)
		for k := range times {
			times[k] = from + r.Int63n(c.span)
		}
		b.Run(c.name, func(b *testing.B) {
			var dst []byte
			var s bucketScratch
			b.ReportAllocs()
			for b.Loop() {
				dst = appendTimeCounts(dst[:0], times, from, from+c.span-1, &s)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(len(times)), "ns/row")
			b.ReportMetric(float64(len(dst)), "bytes")
		})
	}
}

func TestBlocksCountTheirRowsByBucketOfTime(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	const minute, hour, day = int64(60e9), int64(3600e9), int64(86400e9)
	spans := []struct {
		from, span int64
		rows       int
		width      int64
	}{
		{1_700_000_000e9, 50 * minute, 4000, minute},
		// Before the epoch the buckets round down.
		{-3 * hour, 2 * hour, 3000, minute},
		// Too many minutes hold rows for the record of one block.
		{1_700_000_000e9, 30 * day, 16384, hour},
		{1_700_000_000e9, 300 * day, 16384, day},
		// Even the days are too many.
		{0, 40 * 365 * day, 16384, 0},
	}
	var buf bytes.Buffer
	w := NewWriter(&buf, Options{})
	var want []int64
	for _, s := range spans {
		batch := shred.NewBatch(shred.Options{})
		for i := 0; i < s.rows; i++ {
			if err := batch.AddJSON([]byte(fmt.Sprintf(`{"n":%d}`, i)), s.from+r.Int63n(s.span)); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.WriteBatch(batch); err != nil {
			t.Fatal(err)
		}
		want = append(want, s.width)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	p, err := Open(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	checkTimeCounts(t, p, want)

	// A merge counts the blocks it writes and carries those it copies.
	day1, day2 := orderedTimes(1_700_000_000e9, 3000, day), orderedTimes(1_700_000_000e9+3*day, 3000, day)
	a := orderedPart(t, mergeDocs(3000, "a"), day1)
	b := orderedPart(t, mergeDocs(3000, "b"), day1)
	c := orderedPart(t, mergeDocs(3000, "z"), day2)
	merged := mergeParts(t, []*Part{a, b}, 1000)
	checkTimeCounts(t, merged, nil)
	copied := mergeParts(t, []*Part{a, c}, 4096)
	checkTimeCounts(t, copied, nil)
}

// orderedTimes returns n times from from on within span.
func orderedTimes(from int64, n int, span int64) []int64 {
	r := rand.New(rand.NewSource(from))
	out := make([]int64, n)
	for i := range out {
		out[i] = from + r.Int63n(span)
	}

	return out
}

// mergeDocs returns n documents of the service svc.
func mergeDocs(n int, svc string) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf(`{"svc":%q,"level":%d,"seq":%d}`, svc, i%3, i)
	}

	return out
}
