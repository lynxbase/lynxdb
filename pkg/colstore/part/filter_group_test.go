package part

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// stripedWriter returns a writer whose filter groups take group blocks,
// whatever the size of their filters, and have stripes stripes.
func stripedWriter(buf *bytes.Buffer, opts Options, group, stripes int) *Writer {
	w := NewWriter(buf, opts)
	w.groupBlocks, w.groupBytes, w.stripes = group, 1<<40, stripes

	return w
}

// filterSize returns the number of bloom blocks of the first filter of block
// i of p, zero for a part without filters.
func filterSize(p *Part, i int) int {
	if len(p.groups) == 0 {
		return 0
	}
	g, f, _ := p.blockFilters(i)

	return p.filterBlocks(g, f)
}

// tokensOf returns the distinct token hashes of the string values of docs.
func tokensOf(t testing.TB, docs []string) []uint64 {
	t.Helper()
	var set tokenfilter.Set
	var hashes []uint64
	for _, d := range docs {
		var v any
		if err := json.Unmarshal([]byte(d), &v); err != nil {
			t.Fatal(err)
		}
		for _, s := range stringsIn(v, nil) {
			hashes = tokenfilter.AppendHashes(hashes[:0], []byte(s))
			set.Add(hashes)
		}
	}

	return append([]uint64(nil), set.Keys()...)
}

// ownFilters builds for every block of p the filter of the tokens of its
// rows, on its own, and returns it with those tokens. A filter is sized to a
// multiple of the one of units that gives it the size it has in p.
func ownFilters(t testing.TB, p *Part, units ...int) (filters [][]byte, tokens [][]uint64) {
	t.Helper()
	docs, _ := readAll(t, p)
	for i, at := 0, 0; i < p.NumBlocks(); i++ {
		rows, _, _ := p.BlockStats(i)
		keys := tokensOf(t, docs[at:at+rows])
		at += rows
		var own []byte
		for _, unit := range units {
			if own = tokenfilter.Append(own[:0], keys, unit); len(own) == filterSize(p, i)*tokenfilter.BlockBytes {
				break
			}
		}
		if len(own) != filterSize(p, i)*tokenfilter.BlockBytes {
			t.Fatalf("block %d: a filter of %d bloom blocks for %d tokens fits none of the units %v",
				i, filterSize(p, i), len(keys), units)
		}
		filters, tokens = append(filters, own), append(tokens, keys)
	}

	return filters, tokens
}

// sameAnswers requires every block of p to hold, in pieces, the filter that
// its rows make on their own, and to answer lookups of its tokens and of
// random ones as that filter does.
func sameAnswers(t testing.TB, p *Part, r *rand.Rand, units ...int) {
	t.Helper()
	filters, tokens := ownFilters(t, p, units...)
	for g, run := range groupRuns(p) {
		e := p.index[run[1]*indexEntrySize:]
		end := binary.LittleEndian.Uint64(e[20:]) + uint64(binary.LittleEndian.Uint32(e[28:]))
		if want := (end + filterPage - 1) &^ (filterPage - 1); p.groups[g].off != want && (p.groups[g].size > filterPage || p.groups[g].off != end) {
			t.Fatalf("group %d of %d bytes starts at %d, its blocks end at %d", g, p.groups[g].size, p.groups[g].off, end)
		}
	}
	var b Block
	for i, own := range filters {
		g, f, end := p.blockFilters(i)
		got, ok := p.appendFilter(nil, g, f, make(vec.Bitmap, vec.BitmapWords(g.pages())))
		if !ok || !bytes.Equal(got, own) || end != f+1 || binary.LittleEndian.Uint32(p.filters[f*filterEntrySize:]) != noPath {
			t.Fatalf("block %d: the pieces make another filter than the block's own (intact %v)", i, ok)
		}
		if err := p.OpenBlock(i, &b); err != nil {
			t.Fatal(err)
		}
		hashes := tokens[i]
		for n := 0; n < 500; n++ {
			hashes = append(hashes, r.Uint64())
		}
		for _, h := range hashes {
			want := tokenfilter.MayContain(own, h)
			if may, err := p.MayContainToken(i, h); may != want || err != nil {
				t.Fatalf("block %d, hash %x: %v, %v; its own filter says %v", i, h, may, err, want)
			}
			if may, err := b.MayContainToken(h); may != want || err != nil {
				t.Fatalf("block %d, hash %x: the opened block says %v, %v, want %v", i, h, may, err, want)
			}
		}
	}
}

// wordySource returns an ordered part of rows rows in blocks of blockRows
// whose services start with the letter of src, so that sources do not
// interleave, and whose rows hold words distinct words for the first
// service and as many more for every later one, so that the filters of its
// blocks differ in size.
func wordySource(t testing.TB, r *rand.Rand, src, rows, blockRows, words, group, stripes int) *Part {
	t.Helper()
	all := shred.NewBatch(shred.Options{})
	for seq := 0; seq < rows; seq++ {
		svc := r.Intn(20)
		doc := fmt.Appendf(nil, `{"src":%d,"seq":%d,"svc":"%c%02d","level":%d,"msg":"`, src, seq, 'a'+src, svc, r.Intn(6))
		for k := 0; k < words*(1+svc); k++ {
			doc = strconv.AppendUint(append(doc, 'w'), r.Uint64()>>24, 36)
			doc = append(doc, ' ')
		}
		if err := all.AddJSON(append(doc, `"}`...), int64(r.Intn(1000))); err != nil {
			t.Fatal(err)
		}
	}
	all.SortBy(mergeKey)
	var buf bytes.Buffer
	w := stripedWriter(&buf, Options{Ordered: true, SortKey: mergeKey}, group, stripes)
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

// groupRuns returns the first and the last block of every filter group of p.
func groupRuns(p *Part) [][2]int {
	runs := make([][2]int, len(p.groups))
	for i := p.NumBlocks() - 1; i >= 0; i-- {
		runs[binary.LittleEndian.Uint32(p.index[i*indexEntrySize+32:])][0] = i
	}
	for i := 0; i < p.NumBlocks(); i++ {
		runs[binary.LittleEndian.Uint32(p.index[i*indexEntrySize+32:])][1] = i
	}

	return runs
}

// mergeStriped merges srcs into a part with filter groups of group blocks
// and stripes stripes, and returns it with the number of blocks it copied.
func mergeStriped(t testing.TB, srcs []*Part, blockRows int, opts Options, group, stripes int) (*Part, int) {
	t.Helper()
	opts.Ordered, opts.SortKey = true, mergeKey
	var buf bytes.Buffer
	w := stripedWriter(&buf, opts, group, stripes)
	if err := Merge(w, srcs, blockRows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	p, err := Open(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	return p, w.copied
}

func TestStripedFiltersAnswerAsEachBlocksOwnFilter(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	for _, tc := range []struct{ group, stripes, blocks int }{
		{1, 1, 3},
		{3, 4, 10},
		{4, 16, 8},
		{5, 7, 4},
		{2, 128, 5},
		{filterGroupBlocks, filterStripes, 12},
	} {
		const blockRows = 60
		var srcs []*Part
		total := 0
		for src := 0; src < 2; src++ {
			p := wordySource(t, r, src, tc.blocks*blockRows-src*blockRows/2, blockRows, 3, tc.group, tc.stripes)
			if want := (tc.blocks + tc.group - 1) / tc.group; len(p.groups) != want || p.NumBlocks() != tc.blocks {
				t.Fatalf("%+v: %d groups for %d blocks, want %d", tc, len(p.groups), p.NumBlocks(), want)
			}
			sameAnswers(t, p, r, tc.stripes)
			srcs = append(srcs, p)
			total += p.NumBlocks()
		}

		// Copied blocks land in groups cut elsewhere than in their sources.
		copied, n := mergeStriped(t, srcs, blockRows, Options{}, tc.group+1, tc.stripes)
		if n != total || copied.NumBlocks() != total {
			t.Fatalf("%+v: %d of %d blocks copied into %d", tc, n, total, copied.NumBlocks())
		}
		sameAnswers(t, copied, r, tc.stripes)

		again, n := mergeStriped(t, srcs, 2*blockRows, Options{Codec: CodecLZ4}, tc.group, tc.stripes)
		if n != 0 || again.NumBlocks() >= total {
			t.Fatalf("%+v: %d blocks copied into the %d of a part of another codec", tc, n, again.NumBlocks())
		}
		sameAnswers(t, again, r, tc.stripes)
		if again.Rows != copied.Rows {
			t.Fatalf("%+v: %d rows encoded again, %d copied", tc, again.Rows, copied.Rows)
		}
	}
}

// A filter is copied only into a group whose stripes cut it at bloom block
// boundaries; the other blocks are encoded again.
func TestMergeCopiesFiltersThatTheStripesOfTheDestinationDivide(t *testing.T) {
	r := rand.New(rand.NewSource(4))
	const blockRows = 60
	srcs := []*Part{wordySource(t, r, 0, 12*blockRows, blockRows, 3, 3, 6), wordySource(t, r, 1, 12*blockRows, blockRows, 3, 5, 6)}
	fits := 0
	for _, p := range srcs {
		for i := 0; i < p.NumBlocks(); i++ {
			if filterSize(p, i)%4 == 0 {
				fits++
			}
		}
	}
	if fits == 0 || fits == 24 {
		t.Fatalf("%d of 24 filters fit four stripes; the test needs both kinds", fits)
	}
	got, copied := mergeStriped(t, srcs, blockRows, Options{}, 4, 4)
	if copied != fits {
		t.Fatalf("%d blocks copied, %d have filters that four stripes divide", copied, fits)
	}
	sameAnswers(t, got, r, 4, 6)

	// Every filter that 6 stripes divide, 3 do.
	got, copied = mergeStriped(t, srcs, blockRows, Options{}, 4, 3)
	if copied != 24 {
		t.Fatalf("%d of 24 blocks copied into a part of three stripes", copied)
	}
	sameAnswers(t, got, r, 6)
}

// A copied filter gets checksums of its own in the destination, so damage
// in the source must stop the copy and not pass for sound from then on.
func TestMergeFailsOnDamagedFiltersItWouldCopy(t *testing.T) {
	r := rand.New(rand.NewSource(8))
	// The pieces of the later filters span several pages.
	const blockRows, words, group, stripes = 60, 40, 3, 4
	srcs := []*Part{wordySource(t, r, 0, 7*blockRows, blockRows, words, group, stripes), wordySource(t, r, 1, 2*blockRows, blockRows, words, group, stripes)}
	if piece := filterSize(srcs[0], 6) * tokenfilter.BlockBytes / stripes; piece < 2*filterPage {
		t.Fatalf("pieces of %d bytes", piece)
	}
	for trial := 0; trial < 40; trial++ {
		g := &srcs[0].groups[r.Intn(len(srcs[0].groups))]
		at := g.off + uint64(r.Intn(int(g.size)))
		bit := byte(1) << r.Intn(8)
		srcs[0].data[at] ^= bit

		var buf bytes.Buffer
		w := stripedWriter(&buf, Options{Ordered: true, SortKey: mergeKey}, group+1, stripes)
		if err := Merge(w, srcs, blockRows); err != ErrCorrupt {
			t.Fatalf("trial %d: copying a filter with byte %d damaged: %v", trial, at, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("trial %d: closing the writer of a failed merge: %v", trial, err)
		}
		// Blocks that are encoded again get filters from their rows.
		if trial == 0 {
			again, copied := mergeStriped(t, srcs, 2*blockRows, Options{Codec: CodecLZ4}, group, stripes)
			if copied != 0 {
				t.Fatalf("%d blocks copied into a part of another codec", copied)
			}
			sameAnswers(t, again, r, stripes)
		}
		srcs[0].data[at] ^= bit
	}
	if _, copied := mergeStriped(t, srcs, blockRows, Options{}, group+1, stripes); copied != 9 {
		t.Fatalf("%d of 9 intact blocks copied", copied)
	}
}

// A group closes early once its filters reach the size a writer may hold.
func TestFilterGroupsCloseAtTheirSizeBound(t *testing.T) {
	r := rand.New(rand.NewSource(10))
	const blockRows, stripes, bound = 60, 4, 24 << 10
	src := wordySource(t, r, 0, 40*blockRows, blockRows, 3, 2, stripes)
	var buf bytes.Buffer
	w := stripedWriter(&buf, Options{Ordered: true, SortKey: mergeKey}, 8, stripes)
	w.groupBytes = bound
	if err := Merge(w, []*Part{src}, blockRows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	p, err := Open(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	sameAnswers(t, p, r, stripes)
	sizes := make([]int, len(p.groups))
	blocks := make([]int, len(p.groups))
	last := make([]int, len(p.groups))
	for i := 0; i < p.NumBlocks(); i++ {
		g := binary.LittleEndian.Uint32(p.index[i*indexEntrySize+32:])
		last[g] = filterSize(p, i) * tokenfilter.BlockBytes
		sizes[g] += last[g]
		blocks[g]++
	}
	early, full := 0, 0
	for g := range sizes {
		switch final := g == len(sizes)-1; {
		case blocks[g] > 8 || sizes[g]-last[g] >= bound:
			t.Fatalf("group %d went on to %d blocks and %d bytes of filters", g, blocks[g], sizes[g])
		case blocks[g] == 8:
			full++
		case sizes[g] >= bound:
			early++
		case !final:
			t.Fatalf("group %d closed at %d blocks and %d bytes of filters", g, blocks[g], sizes[g])
		}
	}
	if early == 0 || full == 0 {
		t.Fatalf("%d groups closed for their size and %d for their blocks; the test needs both", early, full)
	}
}

// The runs of block data and the filter groups between them cover everything
// before the directories, and lookups read the groups only.
func TestBlockRangesLeaveOutTheFilterGroups(t *testing.T) {
	r := rand.New(rand.NewSource(9))
	for _, opts := range []Options{{}, {NoTokenFilter: true}} {
		var buf bytes.Buffer
		w := stripedWriter(&buf, opts, 3, 8)
		batch := shred.NewBatch(shred.Options{})
		for i, d := range sampleDocs(1100) {
			if err := batch.AddJSON([]byte(d), int64(i)); err != nil {
				t.Fatal(err)
			}
			if batch.Rows == 100 {
				if err := w.WriteBatch(batch); err != nil {
					t.Fatal(err)
				}
				batch.Reset()
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		p, err := Open(buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		type run struct{ off, end int }
		var runs []run
		end := -1
		p.BlockRanges(func(off, n int) {
			if n <= 0 || off <= end {
				t.Fatalf("run of %d bytes at %d follows one that ends at %d", n, off, end)
			}
			runs, end = append(runs, run{off, off + n}), off+n
		})
		groups := 0
		if !opts.NoTokenFilter {
			groups = (p.NumBlocks() + 2) / 3
		}
		if len(runs) != max(groups, 1) || runs[0].off != 0 || len(p.groups) != groups {
			t.Fatalf("%d runs from %d and %d groups for %d blocks", len(runs), runs[0].off, len(p.groups), p.NumBlocks())
		}
		covered := 0
		for _, run := range runs {
			covered += run.end - run.off
		}
		for g := range p.groups {
			covered += int(p.groups[g].size)
		}
		if covered != p.dirsOff {
			t.Fatalf("runs and groups cover %d bytes of the %d before the directories", covered, p.dirsOff)
		}
		inRun := func(at int) bool {
			for _, run := range runs {
				if at >= run.off && at < run.end {
					return true
				}
			}

			return false
		}
		for i := 0; i < p.NumBlocks(); i++ {
			e := p.index[i*indexEntrySize:]
			start := int(binary.LittleEndian.Uint64(e[20:]))
			if end := start + int(binary.LittleEndian.Uint32(e[28:])); !inRun(start) || !inRun(end-1) {
				t.Fatalf("block %d at %d..%d lies outside the runs", i, start, end)
			}
			if opts.NoTokenFilter {
				continue
			}
			for n := 0; n < 200; n++ {
				block, page, end := probeOf(p, i, r.Uint64())
				for _, at := range []uint64{block, page, end - 1} {
					if inRun(int(at)) {
						t.Fatalf("block %d: a lookup reads byte %d of a run of block data", i, at)
					}
				}
			}
		}
	}
}

// probeOf is Part.probe for the first filter of block i.
func probeOf(p *Part, i int, hash uint64) (block, page, end uint64) {
	g, f, _ := p.blockFilters(i)

	return p.probe(g, f, hash)
}
