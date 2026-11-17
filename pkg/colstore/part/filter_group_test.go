package part

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// stripedWriter returns a writer that gives a block one filter for all its
// columns and whose filter groups take group blocks, whatever the size of
// their filters, and have stripes stripes.
func stripedWriter(buf *bytes.Buffer, opts Options, group, stripes int) *Writer {
	w := NewWriter(buf, opts)
	w.groupBlocks, w.groupBytes, w.fixedStripes, w.columnBytes = group, 1<<40, stripes, math.MaxInt

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
		want := end
		if p.groups[g].size >= filterAligned {
			want = (end + filterPage - 1) &^ (filterPage - 1)
		}
		if p.groups[g].off != want {
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

// groupStripes requires the stripes of every group of p to cut each filter
// of its blocks at bloom block boundaries, into pieces at most twice as
// large as the most stripes that want allows the block would give, and
// returns how many groups have each number of stripes.
func groupStripes(t testing.TB, p *Part, want func(common int) int) map[int]int {
	t.Helper()
	counts := map[int]int{}
	for g := range p.groups {
		counts[int(p.groups[g].stripes)]++
	}
	for i := 0; i < p.NumBlocks(); i++ {
		g, f, end := p.blockFilters(i)
		common := 0
		for ; f < end; f++ {
			size := p.filterBlocks(g, f)
			if size%int(g.stripes) != 0 {
				t.Fatalf("block %d: %d stripes cut a filter of %d bloom blocks", i, g.stripes, size)
			}
			for common != 0 {
				size, common = common, size%common
			}
			common = size
		}
		if most := want(common); most > 2*int(g.stripes) {
			t.Fatalf("block %d: filters that %d stripes divide lie in %d stripes", i, most, g.stripes)
		}
	}

	return counts
}

// largestDivisor returns the largest divisor of n that is at most the
// number of stripes of a large block.
func largestDivisor(n int) int {
	d := min(n, filterStripes)
	for n%d != 0 {
		d--
	}

	return d
}

// A filter is copied into a group whose stripes cut it at bloom block
// boundaries: the group being built when its stripes do, and another
// otherwise.
func TestMergeCopiesFiltersIntoGroupsWhoseStripesDivideThem(t *testing.T) {
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
	// Four stripes take the filters they divide; the others get the most
	// stripes that divide them.
	got, copied := mergeStriped(t, srcs, blockRows, Options{}, 4, 4)
	if copied != 24 {
		t.Fatalf("%d of 24 blocks copied into a part of four stripes", copied)
	}
	sameAnswers(t, got, r, 6)
	counts := groupStripes(t, got, func(common int) int {
		if common%4 == 0 {
			return 4
		}

		return largestDivisor(common)
	})
	if counts[4] == 0 || len(counts) < 2 {
		t.Fatalf("groups by their stripes: %v; the test needs four stripes and others", counts)
	}

	// Every filter that 6 stripes divide, 3 do.
	got, copied = mergeStriped(t, srcs, blockRows, Options{}, 4, 3)
	if copied != 24 {
		t.Fatalf("%d of 24 blocks copied into a part of three stripes", copied)
	}
	sameAnswers(t, got, r, 6)
	if counts = groupStripes(t, got, func(int) int { return 3 }); counts[3] != 6 || len(counts) != 1 {
		t.Fatalf("groups by their stripes: %v, want 6 of three", counts)
	}

	// A writer that is free to choose gives every block the most stripes.
	got, copied = mergeStriped(t, srcs, blockRows, Options{}, 4, 0)
	if copied != 24 {
		t.Fatalf("%d of 24 blocks copied", copied)
	}
	sameAnswers(t, got, r, 6)
	groupStripes(t, got, largestDivisor)
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
	// The bound of a group is the share of groupBytes that its stripes are
	// of those of a large block.
	w.groupBytes = bound * filterStripes / stripes
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

// A part with block data of its own reads column chunks from it and
// everything else from the bytes it was opened on.
func TestPartReadsChunksFromItsBlockData(t *testing.T) {
	r := rand.New(rand.NewSource(9))
	whole := wordySource(t, r, 0, 11*60, 60, 3, 3, 8)
	want, _ := readAll(t, whole)
	// One copy keeps the column chunks only, the other all but them.
	chunks := make([]byte, len(whole.data))
	rest := append([]byte(nil), whole.data...)
	for i := 0; i < whole.NumBlocks(); i++ {
		e := whole.index[i*indexEntrySize:]
		start := binary.LittleEndian.Uint64(e[20:])
		end := start + uint64(binary.LittleEndian.Uint32(e[28:]))
		copy(chunks[start:end], rest[start:end])
		clear(rest[start:end])
	}
	p, err := Open(rest)
	if err != nil {
		t.Fatal(err)
	}
	p.SetBlockData(chunks[:len(chunks)-1])
	if _, err := p.MayContainToken(0, 1); err != nil {
		t.Fatalf("block data of another length took the place of the part's: %v", err)
	}
	p.SetBlockData(chunks)
	if got, _ := readAll(t, p); !reflect.DeepEqual(got, want) {
		t.Fatal("rows read from the block data differ")
	}
	sameAnswers(t, p, r, 8)
	var buf bytes.Buffer
	w := stripedWriter(&buf, Options{Ordered: true, SortKey: mergeKey}, 4, 8)
	if err := Merge(w, []*Part{p}, 60); err != nil || w.Close() != nil || w.copied != p.NumBlocks() {
		t.Fatalf("%d of %d blocks copied: %v", w.copied, p.NumBlocks(), err)
	}
	merged, err := Open(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := readAll(t, merged); !reflect.DeepEqual(got, want) {
		t.Fatal("rows copied from the block data differ")
	}
}

// probeOf is Part.probe for the first filter of block i.
func probeOf(p *Part, i int, hash uint64) (block, page, end uint64) {
	g, f, _ := p.blockFilters(i)

	return p.probe(g, f, hash)
}

// wordBatch returns a batch of rows documents that hold words distinct
// words each and pad random characters that make one token, with the number
// of distinct tokens in it.
func wordBatch(t testing.TB, r *rand.Rand, rows, words, pad int) (*shred.Batch, int) {
	t.Helper()
	batch := shred.NewBatch(shred.Options{})
	var docs []string
	var doc []byte
	for i := 0; i < rows; i++ {
		doc = append(doc[:0], `{"m":"`...)
		for k := 0; k < words; k++ {
			doc = strconv.AppendUint(append(doc, 'w'), uint64(i*words+k), 36)
			doc = append(doc, ' ')
		}
		doc = append(doc, `","sig":"`...)
		for k := 0; k < pad; k++ {
			doc = append(doc, "0123456789abcdef"[r.Intn(16)])
		}
		doc = append(doc, `"}`...)
		if err := batch.AddJSON(doc, int64(i)); err != nil {
			t.Fatal(err)
		}
		docs = append(docs, string(doc))
	}

	return batch, len(tokensOf(t, docs))
}

// repeatedBlocks writes batch as every one of blocks blocks, each with one
// filter for all its columns.
func repeatedBlocks(t testing.TB, batch *shred.Batch, blocks int, opts Options) *Part {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf, opts)
	w.columnBytes = math.MaxInt
	for i := 0; i < blocks; i++ {
		if err := w.WriteBatch(batch); err != nil {
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

// Striping costs the rounding of every filter to a whole number of bloom
// blocks per stripe, the room of a bloom block for the checksum of a page,
// what fills the file up to the page a group starts on, four bytes of index
// per block and a table of groups. Blocks of the benchmark's size do not
// notice even when the rounding is the most it can be, as it is for the
// row more than the usual 16384 that these blocks hold.
func TestStripedFiltersTakeTheSpaceOfSeparateOnes(t *testing.T) {
	batch, tokens := wordBatch(t, rand.New(rand.NewSource(2)), 16385, 4, 200)
	const blocks = 5
	striped := repeatedBlocks(t, batch, blocks, Options{})
	bare := repeatedBlocks(t, batch, blocks, Options{NoTokenFilter: true})

	// A filter of its natural size behind its block, with a checksum per
	// page.
	natural := len(tokenfilter.Append(nil, make([]uint64, tokens), 1))
	if rounded := filterSize(striped, 0) * tokenfilter.BlockBytes; rounded != natural+(filterStripes-1)*tokenfilter.BlockBytes {
		t.Fatalf("a filter of %d bytes was rounded to %d", natural, rounded)
	}
	separate := bare.Size() + blocks*(natural+4*((natural+filterPage-1)/filterPage)-4)
	growth := float64(striped.Size()-separate) / float64(separate)
	t.Logf("%d blocks of %d tokens: %d bytes striped, %d with separate filters of %d bytes, %+.3f%%",
		blocks, tokens, striped.Size(), separate, natural, 100*growth)
	if growth > 0.005 {
		t.Fatalf("striping grows the part by %.3f%%", 100*growth)
	}
}

// readWindow is the kernel's default read-ahead: a fault on a page of a
// mapped file that is not in memory reads the window around it, half of it
// before the page.
const (
	readWindow = 128 << 10
	kernelPage = 4096
)

// fileReads plays token lookups against a mapped part of which no page is in
// memory.
type fileReads struct {
	cached map[uint64]bool
	// pages are the pages of the file that the lookups read, and faults
	// counts those for which the kernel reads from the file.
	pages  map[uint64]bool
	faults int
}

func newFileReads() *fileReads {
	return &fileReads{cached: map[uint64]bool{}, pages: map[uint64]bool{}}
}

// lookup reads what looking hash up in every filter of blocks first to last
// of p reads.
func (f *fileReads) lookup(p *Part, hash uint64, first, last int) {
	const half = readWindow / kernelPage / 2
	for i := first; i <= last; i++ {
		for g, filter, end := p.blockFilters(i); filter < end; filter++ {
			_, page, pageEnd := p.probe(g, filter, hash)
			for at := page / kernelPage; at <= (pageEnd-1)/kernelPage; at++ {
				f.pages[at] = true
				if f.cached[at] {
					continue
				}
				f.faults++
				for near := max(at, half) - half; near < at+half; near++ {
					f.cached[near] = true
				}
			}
		}
	}
}

// windows returns how many windows of readWindow bytes, counted from the
// start of the file, hold a page that the lookups read.
func (f *fileReads) windows() int {
	seen := map[uint64]bool{}
	for page := range f.pages {
		seen[page*kernelPage/readWindow] = true
	}

	return len(seen)
}

// Looking a token up in every block of a group reads the pages of one
// stripe, which lie side by side in the file and come in with one fault: one
// read of the file a group for one token, in one or two windows, and three
// for three tokens. A filter stored behind its block takes a read for every
// block and token.
func TestTokenLookupReadsOneStripeOfEveryGroup(t *testing.T) {
	batch, tokens := wordBatch(t, rand.New(rand.NewSource(1)), 2000, 40, 0)
	const blocks = 60
	p := repeatedBlocks(t, batch, blocks, Options{})
	runs := groupRuns(p)
	if tokens != 80000 || len(runs) != 2 || runs[0][1] >= filterGroupBlocks-1 {
		t.Fatalf("%d tokens a block, groups %v: the first is to close for its size", tokens, runs)
	}
	r := rand.New(rand.NewSource(7))
	const lookups = 500
	var one, three struct{ pages, faults, windows int }
	for n := 0; n < lookups; n++ {
		hashes := [3]uint64{r.Uint64(), r.Uint64(), r.Uint64()}
		for g, run := range runs {
			for i := run[0]; i <= run[1]; i++ {
				block, page, end := probeOf(p, i, hashes[0])
				if page%kernelPage != 0 || end-page > kernelPage || block < page+tokenfilter.BlockBytes || block+tokenfilter.BlockBytes > end {
					t.Fatalf("block %d: bloom block at %d of the page %d..%d", i, block, page, end)
				}
			}
			// A stripe need not start or end on a page.
			stripe := int(p.groups[g].stripe+pageBlocks-1)/pageBlocks + 1
			reads := newFileReads()
			reads.lookup(p, hashes[0], run[0], run[1])
			if reads.faults != 1 || len(reads.pages) > stripe || reads.windows() > 2 {
				t.Fatalf("one token in the %d blocks of group %d: %d reads of the file for %d pages in %d windows, a stripe has %d pages",
					run[1]-run[0]+1, g, reads.faults, len(reads.pages), reads.windows(), stripe)
			}
			one.pages, one.faults, one.windows = one.pages+len(reads.pages), one.faults+reads.faults, one.windows+reads.windows()
			reads.lookup(p, hashes[1], run[0], run[1])
			reads.lookup(p, hashes[2], run[0], run[1])
			if reads.faults > 3 || len(reads.pages) > 3*stripe || reads.windows() > 6 {
				t.Fatalf("three tokens in the %d blocks of group %d: %d reads of the file for %d pages in %d windows, a stripe has %d pages",
					run[1]-run[0]+1, g, reads.faults, len(reads.pages), reads.windows(), stripe)
			}
			three.pages, three.faults, three.windows = three.pages+len(reads.pages), three.faults+reads.faults, three.windows+reads.windows()
		}
	}
	per := float64(lookups * len(runs))
	t.Logf("groups of %d and %d blocks, stripes of %d bloom blocks: a group gives one token %.1f pages in %.2f reads and %.2f windows of %d KiB, three tokens %.1f pages in %.2f reads and %.2f windows",
		runs[0][1]+1, runs[1][1]-runs[1][0]+1, p.groups[0].stripe, float64(one.pages)/per, float64(one.faults)/per, float64(one.windows)/per,
		readWindow>>10, float64(three.pages)/per, float64(three.faults)/per, float64(three.windows)/per)
}

// BenchmarkMayContainToken probes three tokens against every block of a part
// of 320 blocks whose filters hold 80000 tokens each.
func BenchmarkMayContainToken(b *testing.B) {
	batch, _ := wordBatch(b, rand.New(rand.NewSource(1)), 2000, 40, 0)
	p := repeatedBlocks(b, batch, 320, Options{})
	hashes := tokenfilter.AppendHashes(nil, []byte("absent w7zzzzzzz missing"))
	if len(hashes) != 3 {
		b.Fatalf("%d tokens", len(hashes))
	}
	blocks := p.NumBlocks()
	b.ResetTimer()
	for b.Loop() {
		for i := 0; i < blocks; i++ {
			for _, h := range hashes {
				if _, err := p.MayContainToken(i, h); err != nil {
					b.Fatal(err)
				}
			}
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*blocks*len(hashes)), "ns/probe")
}
