package part

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// orderedBlocks shreds docs, sorts them by mergeKey and writes blocks of
// blockRows rows.
func orderedBlocks(t testing.TB, docs []string, times []int64, blockRows int) *Part {
	t.Helper()
	all := shred.NewBatch(shred.Options{})
	for i, d := range docs {
		if err := all.AddJSON([]byte(d), times[i]); err != nil {
			t.Fatal(err)
		}
	}
	all.SortBy(mergeKey)
	var buf bytes.Buffer
	w := NewWriter(&buf, Options{Ordered: true, SortKey: mergeKey})
	block := shred.NewBatch(shred.Options{})
	for from := 0; from < all.Rows; from += blockRows {
		block.Reset()
		all.CopyRows(block, from, min(from+blockRows, all.Rows))
		if err := w.WriteBatch(block); err != nil {
			t.Fatal(err)
		}
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

// mergeWith merges srcs into a part written with opts.
func mergeWith(t testing.TB, srcs []*Part, blockRows int, lo, hi []byte, opts Options) *Part {
	t.Helper()
	opts.Ordered, opts.SortKey = true, mergeKey
	var buf bytes.Buffer
	w := NewWriter(&buf, opts)
	if err := MergeRange(w, srcs, blockRows, lo, hi); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	p, err := Open(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	copies.Store(p, w.copied)

	return p
}

// copies holds, for every part mergeWith wrote, the number of its blocks
// that were copied as stored.
var copies sync.Map

// encodedAgain merges srcs with every block written anew, which a writer
// with another codec than the sources' does.
func encodedAgain(t testing.TB, srcs []*Part, blockRows int, lo, hi []byte) *Part {
	t.Helper()
	p := mergeWith(t, srcs, blockRows, lo, hi, Options{Codec: CodecLZ4})
	if n := copiedBlocks(t, p); n != 0 {
		t.Fatalf("%d blocks copied into a part of another codec", n)
	}

	return p
}

// copiedBlocks returns the number of blocks of dst, which mergeWith wrote,
// that were copied as stored.
func copiedBlocks(t testing.TB, dst *Part) int {
	t.Helper()
	n, ok := copies.Load(dst)
	if !ok {
		t.Fatal("part was not written by mergeWith")
	}

	return n.(int)
}

// sameRows requires got to hold the rows of want in the same order, and
// that order to be the merge order.
func sameRows(t testing.TB, got, want *Part) []mergedRow {
	t.Helper()
	g, w := decodeRows(t, got), decodeRows(t, want)
	if len(g) != len(w) || got.Rows != uint64(len(g)) {
		t.Fatalf("%d rows (footer says %d), want %d", len(g), got.Rows, len(w))
	}
	for i := range g {
		if g[i].text != w[i].text || g[i].time != w[i].time {
			t.Fatalf("row %d differs:\n got  %s @%d\n want %s @%d", i, g[i].text, g[i].time, w[i].text, w[i].time)
		}
		if i > 0 && !ordered(g[i-1], g[i]) {
			t.Fatalf("rows %d and %d are out of order:\n%s\n%s", i-1, i, g[i-1].text, g[i].text)
		}
	}
	gotFirst, gotLast := got.KeyRange()
	wantFirst, wantLast := want.KeyRange()
	if !bytes.Equal(gotFirst, wantFirst) || !bytes.Equal(gotLast, wantLast) {
		t.Fatalf("key range %x .. %x, want %x .. %x", gotFirst, gotLast, wantFirst, wantLast)
	}
	if got.MinTime != want.MinTime || got.MaxTime != want.MaxTime || got.RawBytes != want.RawBytes {
		t.Fatalf("times %d .. %d and %d raw bytes, want %d .. %d and %d",
			got.MinTime, got.MaxTime, got.RawBytes, want.MinTime, want.MaxTime, want.RawBytes)
	}

	return g
}

// disjointSources returns n sources of rows rows each whose services do not
// overlap, each with its own member order and members of its own.
func disjointSources(t testing.TB, n, rows, blockRows int) []*Part {
	t.Helper()
	r := rand.New(rand.NewSource(5))
	var srcs []*Part
	for src := 0; src < n; src++ {
		var docs []string
		var times []int64
		for seq := 0; seq < rows; seq++ {
			svc := fmt.Sprintf(`"svc":"%c%02d"`, 'a'+src, r.Intn(20))
			doc := fmt.Sprintf(`{"src":%d,"seq":%d,%s,"msg":"m%d"`, src, seq, svc, r.Intn(50))
			if src%2 == 1 {
				doc = fmt.Sprintf(`{"only%d":"x%d","msg":"m%d",%s,"seq":%d,"src":%d`, src, r.Intn(9), r.Intn(50), svc, seq, src)
			}
			if r.Intn(4) != 0 {
				doc += fmt.Sprintf(`,"level":%d`, r.Intn(6))
			}
			if src == 1 && seq == rows/2 {
				doc += `,"note":"needle"`
			}
			docs = append(docs, doc+"}")
			times = append(times, int64(r.Intn(1000)))
		}
		srcs = append(srcs, orderedBlocks(t, docs, times, blockRows))
	}

	return srcs
}

func TestMergeCopiesBlocksOfSourcesThatDoNotInterleave(t *testing.T) {
	srcs := disjointSources(t, 3, 2000, 256)
	total := 0
	for _, p := range srcs {
		total += p.NumBlocks()
	}
	want := encodedAgain(t, srcs, 256, nil, nil)

	// A block above the limit of rows is cut again.
	if p := mergeWith(t, srcs, 255, nil, nil, Options{}); copiedBlocks(t, p) != len(srcs) {
		t.Fatalf("%d blocks copied under a limit only the last block of each source meets", copiedBlocks(t, p))
	}

	got := mergeWith(t, srcs, 256, nil, nil, Options{})
	if n := copiedBlocks(t, got); n != total || got.NumBlocks() != total {
		t.Fatalf("%d of %d blocks copied into %d", n, total, got.NumBlocks())
	}
	rows := sameRows(t, got, want)

	// Sources number their paths differently; a copied block reads through
	// the destination's numbering.
	var b Block
	hash := tokenfilter.AppendHashes(nil, []byte("needle"))[0]
	found := false
	for i, at := 0, 0; i < got.NumBlocks(); i++ {
		if err := got.OpenBlock(i, &b); err != nil {
			t.Fatal(err)
		}
		refs := b.Columns()
		for j := range refs {
			if ref, ok := b.Column(refs[j].PathID, refs[j].Kind); !ok || ref != &refs[j] {
				t.Fatalf("block %d: column %s/%s is not where the directory order puts it", i, got.Paths()[refs[j].PathID], refs[j].Kind)
			}
		}
		for _, row := range rows[at : at+b.Rows] {
			if bytes.Contains([]byte(row.text), []byte("needle")) {
				found = true
				if ok, err := b.MayContainToken(hash); err != nil || !ok {
					t.Fatalf("block %d holds the token and its filter says %v, %v", i, ok, err)
				}
			}
		}
		at += b.Rows
	}
	if !found {
		t.Fatal("no row holds the token")
	}

	// A destination without filters drops them and keeps the columns.
	bare := mergeWith(t, srcs, 256, nil, nil, Options{NoTokenFilter: true})
	if n := copiedBlocks(t, bare); n != total {
		t.Fatalf("%d of %d blocks copied without filters", n, total)
	}
	sameRows(t, bare, want)
	if len(bare.groups) != 0 || bare.Size() >= got.Size() {
		t.Fatalf("%d filter groups kept in %d bytes, %d with filters", len(bare.groups), bare.Size(), got.Size())
	}
	absent := tokenfilter.AppendHashes(nil, []byte("nosuchtokenanywhere"))
	for i := 0; i < bare.NumBlocks(); i++ {
		if may, err := bare.MayContainToken(i, absent[0]); !may || err != nil {
			t.Fatalf("block %d without a filter excludes a token: %v, %v", i, may, err)
		}
		if err := bare.OpenBlock(i, &b); err != nil {
			t.Fatal(err)
		}
		refs := b.Columns()
		bits, err := b.ColumnsMayContain(nil, absent)
		if err != nil || bits.Count() != len(refs) {
			t.Fatalf("block %d without a filter excludes a token from %d of %d columns: %v", i, len(refs)-bits.Count(), len(refs), err)
		}
		for j := range refs {
			may, err := b.MayContainTokensIn(refs[j].PathID, refs[j].Kind, absent)
			if !may || err != nil || b.OwnFilter(refs[j].PathID, refs[j].Kind) {
				t.Fatalf("block %d without a filter excludes a token from a column: %v, %v", i, may, err)
			}
		}
		b.FilterColumns(func(uint32, vec.Kind) { t.Fatalf("block %d without a filter lists one", i) })
	}

	// Sections of another level are written again, as those of another
	// codec are.
	compact := mergeWith(t, srcs, 256, nil, nil, Options{Level: LevelCompact})
	if n := copiedBlocks(t, compact); n != 0 {
		t.Fatalf("%d blocks copied into a part of another level", n)
	}
	sameRows(t, compact, want)
	// So is a block of a source written without filters.
	plain := []*Part{mergeWith(t, srcs[:1], 256, nil, nil, Options{NoTokenFilter: true}), srcs[1]}
	p := mergeWith(t, plain, 256, nil, nil, Options{})
	if n := copiedBlocks(t, p); n != plain[1].NumBlocks() {
		t.Fatalf("%d blocks copied, of which %d of the source with filters", n, plain[1].NumBlocks())
	}
	sameRows(t, p, encodedAgain(t, plain, 256, nil, nil))
}

func TestMergeEncodesBlocksAgainWhereSourcesInterleave(t *testing.T) {
	r := rand.New(rand.NewSource(11))
	var srcs []*Part
	for src := 0; src < 3; src++ {
		var docs []string
		var times []int64
		for seq := 0; seq < 1500; seq++ {
			docs = append(docs, fmt.Sprintf(`{"src":%d,"seq":%d,"svc":"s%02d","level":%d}`, src, seq, r.Intn(40), r.Intn(5)))
			times = append(times, int64(r.Intn(50)))
		}
		srcs = append(srcs, orderedBlocks(t, docs, times, 256))
	}
	got := mergeWith(t, srcs, 256, nil, nil, Options{})
	if n := copiedBlocks(t, got); n != 0 {
		t.Fatalf("%d blocks of interleaving sources were copied", n)
	}
	sameRows(t, got, encodedAgain(t, srcs, 256, nil, nil))
	for i := 0; i < got.NumBlocks()-1; i++ {
		if rows, _, _ := got.BlockStats(i); rows != 256 {
			t.Fatalf("block %d holds %d rows", i, rows)
		}
	}

	// A source that starts on the key another one ends on: the blocks apart
	// from that key are copied, the ones holding it are merged by time.
	var docs []string
	var times []int64
	for seq := 0; seq < 1500; seq++ {
		docs = append(docs, fmt.Sprintf(`{"src":3,"seq":%d,"svc":"s%02d","level":4}`, seq, 39+seq/300))
		times = append(times, int64(r.Intn(50)))
	}
	touching := []*Part{srcs[0], orderedBlocks(t, docs, times, 256)}
	got = mergeWith(t, touching, 256, nil, nil, Options{})
	sameRows(t, got, encodedAgain(t, touching, 256, nil, nil))
	if n := copiedBlocks(t, got); n < 6 || n > 10 {
		t.Fatalf("%d of 12 blocks copied from sources that touch at one key", n)
	}

	// A range bound inside a block cuts it; the blocks between the bounds
	// are whole.
	one := srcs[:1]
	lo, hi := AppendKeyString(nil, []byte("s07")), AppendKeyString(nil, []byte("s31"))
	cut := mergeWith(t, one, 256, lo, hi, Options{})
	rows := sameRows(t, cut, encodedAgain(t, one, 256, lo, hi))
	if len(rows) == 0 || *rows[0].Svc != "s07" || *rows[len(rows)-1].Svc != "s30" {
		t.Fatalf("range holds %d rows from %s", len(rows), rows[0].text)
	}
	inside := 0
	for i := 0; i < one[0].NumBlocks(); i++ {
		if bytes.Compare(one[0].firstKeys[i], lo) >= 0 && bytes.Compare(one[0].upperKey(i), hi) < 0 {
			inside++
		}
	}
	if n := copiedBlocks(t, cut); n != inside || inside == 0 || inside > one[0].NumBlocks()-2 {
		t.Fatalf("%d blocks copied, %d of %d lie inside the range", n, inside, one[0].NumBlocks())
	}
}

func TestMergeCopiesBlocksOfOneKeyOnlyInTimeOrder(t *testing.T) {
	source := func(src int, from, to int64) *Part {
		var docs []string
		var times []int64
		for seq := 0; seq < 1000; seq++ {
			docs = append(docs, fmt.Sprintf(`{"src":%d,"seq":%d,"svc":"api","level":3}`, src, seq))
			times = append(times, from+int64(seq)*(to-from)/1000)
		}

		return orderedBlocks(t, docs, times, 200)
	}
	early, across, late := source(0, 0, 1000), source(1, 500, 1500), source(2, 1000, 2000)

	apart := []*Part{late, early}
	got := mergeWith(t, apart, 200, nil, nil, Options{})
	if n := copiedBlocks(t, got); n != 10 {
		t.Fatalf("%d of 10 blocks copied from sources apart in time", n)
	}
	sameRows(t, got, encodedAgain(t, apart, 200, nil, nil))

	// The last time of one source is the first of the next, and the lower
	// source goes first.
	touching := []*Part{source(0, 0, 1000), source(1, 999, 2000)}
	got = mergeWith(t, touching, 200, nil, nil, Options{})
	if n := copiedBlocks(t, got); n != 10 {
		t.Fatalf("%d of 10 blocks copied from sources that touch in time", n)
	}
	sameRows(t, got, encodedAgain(t, touching, 200, nil, nil))

	mixed := []*Part{early, across, late}
	got = mergeWith(t, mixed, 200, nil, nil, Options{})
	sameRows(t, got, encodedAgain(t, mixed, 200, nil, nil))
	if n := copiedBlocks(t, got); n == 0 || n > 8 {
		t.Fatalf("%d blocks copied from sources overlapping in time", n)
	}
}

func TestMergeKeepsShortBlocksRare(t *testing.T) {
	short := func(p *Part, blockRows int) (n int) {
		for i := 0; i < p.NumBlocks()-1; i++ {
			if rows, _, _ := p.BlockStats(i); rows < blockRows/2 {
				n++
			}
		}

		return n
	}

	// Many small sources fill whole blocks.
	small := disjointSources(t, 12, 40, 256)
	got := mergeWith(t, small, 256, nil, nil, Options{})
	sameRows(t, got, encodedAgain(t, small, 256, nil, nil))
	if got.NumBlocks() != 2 || short(got, 256) != 0 {
		t.Fatalf("480 rows of 12 sources in %d blocks, %d of them short", got.NumBlocks(), short(got, 256))
	}

	// Sources that end with a short block: most blocks are copied, and at
	// most one block in four is short.
	tailed := disjointSources(t, 6, 64*9+10, 64)
	got = mergeWith(t, tailed, 64, nil, nil, Options{})
	sameRows(t, got, encodedAgain(t, tailed, 64, nil, nil))
	copied := copiedBlocks(t, got)
	if n := short(got, 64); shortShare*n > got.NumBlocks() || copied < 6*9*2/3 {
		t.Fatalf("%d of %d blocks short, %d copied", n, got.NumBlocks(), copied)
	}
}

func TestMergesRunSideBySide(t *testing.T) {
	srcs := disjointSources(t, 4, 3000, 128)
	r := rand.New(rand.NewSource(23))
	var docs []string
	var times []int64
	for seq := 0; seq < 3000; seq++ {
		docs = append(docs, fmt.Sprintf(`{"src":4,"seq":%d,"svc":"%c%02d","msg":"m%d"}`, seq, 'a'+r.Intn(4), r.Intn(20), r.Intn(50)))
		times = append(times, int64(r.Intn(1000)))
	}
	srcs = append(srcs, orderedBlocks(t, docs, times, 128))
	cuts := SplitKeys(srcs, 8)
	merge := func(i int) *Part {
		var lo, hi []byte
		if i > 0 {
			lo = cuts[i-1]
		}
		if i < len(cuts) {
			hi = cuts[i]
		}

		return mergeWith(t, srcs, 128, lo, hi, Options{})
	}
	want := make([]*Part, len(cuts)+1)
	for i := range want {
		want[i] = merge(i)
	}
	for round := 0; round < 4; round++ {
		got := make([]*Part, len(want))
		var wg sync.WaitGroup
		for i := range got {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				got[i] = merge(i)
			}(i)
		}
		wg.Wait()
		for i := range got {
			sameRows(t, got[i], want[i])
			if a, b := copiedBlocks(t, got[i]), copiedBlocks(t, want[i]); a != b {
				t.Fatalf("round %d: range %d copied %d blocks side by side and %d alone", round, i, a, b)
			}
		}
	}
}
