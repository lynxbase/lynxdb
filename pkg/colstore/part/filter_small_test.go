package part

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
)

// logLines returns n lines of NDJSON shaped like the events of a service:
// a few fields of few values, identifiers of their own and a message of
// common words.
func logLines(r *rand.Rand, n int) []byte {
	words := make([]string, 400)
	for i := range words {
		words[i] = fmt.Sprintf("w%x", r.Intn(1<<24))
	}
	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		fmt.Fprintf(&buf, `{"ts":%d,"level":"%s","service":"svc-%d","host":"node-%02d.example","request":"%016x%016x","user":"u%d","status":%d,"path":"/api/v1/items/%d","message":"`,
			1_700_000_000_000+int64(i)*37, []string{"info", "info", "warn", "error"}[r.Intn(4)], r.Intn(6), r.Intn(20), r.Uint64(), r.Uint64(), r.Intn(500), 200+r.Intn(4)*100, r.Intn(5000))
		for k, m := 0, 4+r.Intn(8); k < m; k++ {
			buf.WriteString(words[r.Intn(len(words))])
			buf.WriteByte(' ')
		}
		buf.WriteString("\"}\n")
	}

	return buf.Bytes()
}

// stripedGrowth writes the lines of data as one part of blocks of blockRows
// rows and returns its size, and that of the same part with the filters at
// their natural sizes, each behind its block with four bytes of checksum for
// every page of it.
func stripedGrowth(t testing.TB, data []byte, blockRows int, timeField string) (striped, separate int) {
	t.Helper()
	encode := func(opts Options, natural bool) *Part {
		var buf bytes.Buffer
		w := NewWriter(&buf, opts)
		if natural {
			w.groupBlocks, w.fixedStripes = 1, 1
		}
		writeNDJSON(t, w, data, blockRows, timeField)
		p, err := Open(buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}

		return p
	}
	separate = encode(Options{NoTokenFilter: true}, false).Size()
	natural := encode(Options{}, true)
	for i := 0; i < natural.NumBlocks(); i++ {
		for g, f, end := natural.blockFilters(i); f < end; f++ {
			size := natural.filterBlocks(g, f) * tokenfilter.BlockBytes
			separate += size + 4*((size+filterPage-1)/filterPage)
		}
	}

	return encode(Options{}, false).Size(), separate
}

// A part of a few hundred rows, as a quiet source flushes them, and one of a
// few thousand pay little for the stripes that large parts are read by.
func TestSmallPartsTakeTheSpaceOfFiltersOfTheirOwnSize(t *testing.T) {
	r := rand.New(rand.NewSource(21))
	for _, rows := range []int{20, 200, 3000, 20000} {
		data := logLines(r, rows)
		striped, separate := stripedGrowth(t, data, 16384, "ts")
		growth := float64(striped-separate) / float64(separate)
		t.Logf("%5d rows: %7d bytes, %7d with filters of their own size, %+.2f%%", rows, striped, separate, 100*growth)
		if growth > 0.05 {
			t.Errorf("a part of %d rows is %.2f%% larger than with filters of their own size", rows, 100*growth)
		}
	}
}

// TestRealDataSmallParts reports what the first rows of a real file take as
// parts of several sizes beside the same parts with filters of their own
// size.
func TestRealDataSmallParts(t *testing.T) {
	data := realData(t)
	for _, rows := range []int{50, 200, 1000, 3000, 16384, 100000} {
		head, _ := shred.SplitLines(data, rows)
		striped, separate := stripedGrowth(t, head, 16384, "time_us")
		growth := float64(striped-separate) / float64(separate)
		t.Logf("%6d rows: %8d bytes, %8d with filters of their own size, %+.2f%%", rows, striped, separate, 100*growth)
		if growth > 0.05 {
			t.Errorf("a part of %d rows is %.2f%% larger than with filters of their own size", rows, 100*growth)
		}
	}
}

// A block gets the stripes that suit the size of its filters; it joins the
// group being built when that group's stripes are within a factor of two of
// them and starts another otherwise, behind which the block is written.
func TestGroupsHaveTheStripesThatSuitTheirBlocks(t *testing.T) {
	r := rand.New(rand.NewSource(22))
	sizes := []int{30, 40, 3000, 2500, 20, 6000, 5000, 4000, 60, 1, 16384, 16384}
	total := 0
	for _, n := range sizes {
		total += n
	}
	data := logLines(r, total)
	encode := func(natural bool) *Part {
		var buf bytes.Buffer
		w := NewWriter(&buf, Options{})
		if natural {
			w.groupBlocks, w.fixedStripes = 1, 1
		}
		batch := shred.NewBatch(shred.Options{TimeField: "ts"})
		rest := data
		for _, n := range sizes {
			var chunk []byte
			chunk, rest = shred.SplitLines(rest, n)
			if errs := batch.AddNDJSON(chunk, 0, nil); len(errs) != 0 {
				t.Fatal(errs[0].Err)
			}
			if err := w.WriteBatch(batch); err != nil {
				t.Fatal(err)
			}
			batch.Reset()
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		p, err := Open(buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}

		return p
	}
	natural, p := encode(true), encode(false)
	if docs, _ := readAll(t, p); len(docs) != total {
		t.Fatalf("%d rows read back, want %d", len(docs), total)
	}

	// The groups that the sizes of the filters ask for.
	var want [][2]int
	seen := map[int]bool{}
	for i := range sizes {
		blocks := 0
		for g, f, end := natural.blockFilters(i); f < end; f++ {
			blocks += natural.filterBlocks(g, f)
		}
		stripes := 1
		for 2*stripes <= blocks/filterRounding && stripes < filterStripes {
			stripes *= 2
		}
		if last := len(want) - 1; last < 0 || stripes > 2*want[last][0] || want[last][0] > 2*stripes {
			want = append(want, [2]int{stripes, 0})
		}
		want[len(want)-1][1]++
		seen[stripes] = true
	}
	if !seen[1] || len(seen) < 4 {
		t.Fatalf("blocks ask for stripes %v; the test needs one and several others", seen)
	}
	runs := groupRuns(p)
	if len(runs) != len(want) {
		t.Fatalf("%d groups, want %d: %v", len(runs), len(want), want)
	}
	for g, run := range runs {
		if int(p.groups[g].stripes) != want[g][0] || run[1]-run[0]+1 != want[g][1] {
			t.Fatalf("group %d has %d stripes for %d blocks, want %v", g, p.groups[g].stripes, run[1]-run[0]+1, want[g])
		}
	}
	if rate := columnAnswers(t, p, filterColumnBytes, r); rate > 0.02 {
		t.Fatalf("filters let %.2f%% of the absent tokens pass", 100*rate)
	}
}

// Copied blocks keep the stripes that suit them: those of small blocks and
// those of large ones land in groups apart.
func TestMergeGroupsCopiedBlocksByTheirStripes(t *testing.T) {
	r := rand.New(rand.NewSource(23))
	// A merge copies short blocks as long as they stay under one in four.
	srcs := []*Part{columnSource(t, r, 0, 9*6000, 6000, filterColumnBytes), columnSource(t, r, 1, 3*40, 40, filterColumnBytes)}
	blocks := srcs[0].NumBlocks() + srcs[1].NumBlocks()
	var buf bytes.Buffer
	w := NewWriter(&buf, Options{Ordered: true, SortKey: mergeKey})
	if err := Merge(w, srcs, 6000); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if w.copied != blocks {
		t.Fatalf("%d of %d blocks copied", w.copied, blocks)
	}
	p, err := Open(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	counts := groupStripes(t, p, func(cells int) int { return min(cells, filterStripes) })
	least, most := filterStripes, 1
	for stripes := range counts {
		least, most = min(least, stripes), max(most, stripes)
	}
	if most < 4*least {
		t.Fatalf("groups by their stripes: %v; the test needs small and large ones", counts)
	}
	if rate := columnAnswers(t, p, filterColumnBytes, r); rate > 0.02 {
		t.Fatalf("filters let %.2f%% of the absent tokens pass", 100*rate)
	}
}
