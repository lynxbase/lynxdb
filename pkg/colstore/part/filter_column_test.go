package part

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/colstore/variant"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// column names a column of a block.
type column struct {
	path uint32
	kind vec.Kind
}

// columnTokens decodes every string and variant column of block i of p and
// returns the token hashes of its values.
func columnTokens(t testing.TB, p *Part, i int) map[column]map[uint64]bool {
	t.Helper()
	var b Block
	if err := p.OpenBlock(i, &b); err != nil {
		t.Fatal(err)
	}
	found := map[column]map[uint64]bool{}
	refs := b.Columns()
	for j := range refs {
		ref := &refs[j]
		if ref.Kind != vec.KindString && ref.Kind != vec.KindVariant {
			continue
		}
		var col vec.Column
		if err := b.DecodeValues(ref, &col); err != nil {
			t.Fatal(err)
		}
		tokens := map[uint64]bool{}
		add := func(s []byte) {
			for _, h := range tokenfilter.AppendHashes(nil, s) {
				tokens[h] = true
			}
		}
		for v := 0; v < ref.Count; v++ {
			if ref.Kind == vec.KindVariant {
				variant.ForEachString(col.Bytes(v), add)
			} else {
				add(col.Bytes(v))
			}
		}
		found[column{ref.PathID, ref.Kind}] = tokens
	}

	return found
}

// columnAnswers requires every column of every block of p to answer token
// lookups as its values do: a column encoded in at least own bytes from a
// filter of its own, the others from the one they share. It returns the
// share of lookups of absent tokens that a filter lets pass.
func columnAnswers(t testing.TB, p *Part, own int, r *rand.Rand) float64 {
	t.Helper()
	var b Block
	absent, passed := 0, 0
	for i := 0; i < p.NumBlocks(); i++ {
		truth := columnTokens(t, p, i)
		filtered := map[column]bool{}
		p.FilterColumns(i, func(path uint32, kind vec.Kind) { filtered[column{path, kind}] = true })
		if err := p.OpenBlock(i, &b); err != nil {
			t.Fatal(err)
		}
		shared, hashes := map[uint64]bool{}, []uint64{}
		for col, tokens := range truth {
			ref, ok := b.Column(col.path, col.kind)
			if !ok || filtered[col] != (int(ref.length) >= own) {
				t.Fatalf("block %d: column %s/%s of %d bytes has a filter of its own: %v", i, p.Paths()[col.path], col.kind, ref.length, filtered[col])
			}
			for h := range tokens {
				hashes = append(hashes, h)
				if !filtered[col] {
					shared[h] = true
				}
			}
		}
		for n := 0; n < 300; n++ {
			hashes = append(hashes, r.Uint64())
		}
		for _, h := range hashes {
			held, any := false, false
			for col, tokens := range truth {
				want := tokens[h]
				if !filtered[col] {
					want = shared[h]
				}
				got, err := p.MayContainTokenIn(i, col.path, col.kind, h)
				if again, berr := b.MayContainTokenIn(col.path, col.kind, h); err != nil || berr != nil || again != got {
					t.Fatalf("block %d, column %s: %v, %v from the part and %v, %v from the block", i, p.Paths()[col.path], got, err, again, berr)
				}
				if want && !got {
					t.Fatalf("block %d: column %s/%s holds token %x and its filter denies it", i, p.Paths()[col.path], col.kind, h)
				}
				if !want {
					absent++
					if got {
						passed++
					}
				}
				held, any = held || tokens[h], any || got
			}
			// Any string of the block is any of its columns.
			if got, err := p.MayContainToken(i, h); err != nil || got != any || (held && !got) {
				t.Fatalf("block %d, token %x: the block says %v, %v, its columns %v, their values %v", i, h, got, err, any, held)
			}
		}
		columnSets(t, p, &b, filtered, hashes, r)
	}

	return float64(passed) / float64(absent)
}

// columnSets requires lookups of several tokens at once in the columns of
// the open block b to answer as lookups of each token do, and every way of
// asking which columns have a filter of their own to name those of filtered.
func columnSets(t testing.TB, p *Part, b *Block, filtered map[column]bool, hashes []uint64, r *rand.Rand) {
	t.Helper()
	i := b.Index()
	listed := 0
	b.FilterColumns(func(path uint32, kind vec.Kind) {
		if listed++; !filtered[column{path, kind}] {
			t.Fatalf("block %d lists a filter of column %s/%s", i, p.Paths()[path], kind)
		}
	})
	if listed != len(filtered) {
		t.Fatalf("block %d lists %d filters of %d", i, listed, len(filtered))
	}
	refs := b.Columns()
	var bits vec.Bitmap
	for n := 0; n < 200; n++ {
		set := []uint64{hashes[r.Intn(len(hashes))], hashes[r.Intn(len(hashes))], hashes[r.Intn(len(hashes))]}[:1+n%3]
		var err error
		if bits, err = b.ColumnsMayContain(bits, set); err != nil {
			t.Fatal(err)
		}
		any := false
		for j := range refs {
			ref := &refs[j]
			if own := b.OwnFilter(ref.PathID, ref.Kind); own != filtered[column{ref.PathID, ref.Kind}] || own != p.OwnFilter(i, ref.PathID, ref.Kind) {
				t.Fatalf("block %d: column %s/%s has a filter of its own: %v", i, p.Paths()[ref.PathID], ref.Kind, own)
			}
			each := true
			for _, h := range set {
				may, err := p.MayContainTokenIn(i, ref.PathID, ref.Kind, h)
				if err != nil {
					t.Fatal(err)
				}
				each = each && may
			}
			got, err := p.MayContainTokensIn(i, ref.PathID, ref.Kind, set)
			if again, berr := b.MayContainTokensIn(ref.PathID, ref.Kind, set); err != nil || berr != nil || got != each || again != each || bits.Get(j) != each {
				t.Fatalf("block %d, column %s/%s, %d tokens: %v, %v from the part, %v, %v from the block and %v among its columns, %v one by one",
					i, p.Paths()[ref.PathID], ref.Kind, len(set), got, err, again, berr, bits.Get(j), each)
			}
			any = any || each
		}
		if got, err := p.MayContainTokens(i, set); err != nil || got != any {
			t.Fatalf("block %d, %d tokens: the block says %v, %v, its columns %v", i, len(set), got, err, any)
		}
	}
}

// columnSource returns an ordered part of rows rows in blocks of blockRows,
// of a service of its own, whose columns differ in size; a column of a block
// gets its own filter from own encoded bytes on.
func columnSource(t testing.TB, r *rand.Rand, src, rows, blockRows, own int) *Part {
	t.Helper()
	all := shred.NewBatch(shred.Options{})
	for seq := 0; seq < rows; seq++ {
		doc := fmt.Sprintf(`{"src":%d,"seq":%d,"svc":"%c%02d","level":%d,"id":"u%dx%06d","user":"did:plc:%04x","kind":"%s","tags":["t%d","x"],"text":"`,
			src, seq, 'a'+src, r.Intn(20), r.Intn(6), src, seq, r.Intn(300), []string{"post", "like", "repost"}[r.Intn(3)], r.Intn(5))
		for k := 0; k < 6; k++ {
			doc += fmt.Sprintf("w%d ", r.Intn(50000))
		}
		if seq%97 == 0 {
			doc += `","note":"needle in the hay`
		}
		if err := all.AddJSON([]byte(doc+`"}`), int64(r.Intn(1000))); err != nil {
			t.Fatal(err)
		}
	}
	all.SortBy(mergeKey)
	var buf bytes.Buffer
	w := NewWriter(&buf, Options{Ordered: true, SortKey: mergeKey})
	w.columnBytes = own
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

func TestColumnFiltersAnswerAsTheValuesOfTheirColumns(t *testing.T) {
	r := rand.New(rand.NewSource(12))
	const blockRows, own = 400, 250
	srcs := []*Part{columnSource(t, r, 0, 9*blockRows, blockRows, own), columnSource(t, r, 1, 5*blockRows, blockRows, own)}
	several := 0
	for _, p := range srcs {
		if rate := columnAnswers(t, p, own, r); rate > 0.02 {
			t.Fatalf("filters let %.2f%% of the absent tokens pass", 100*rate)
		}
		for i := 0; i < p.NumBlocks(); i++ {
			if _, first, end := p.blockFilters(i); end-first >= 3 {
				several++
			}
		}
	}
	if several < 10 {
		t.Fatalf("%d blocks have three filters or more", several)
	}

	// A copied block carries its filters under the path ids of the part it
	// lands in; a block encoded again gets filters from its rows.
	for _, opts := range []Options{{}, {Codec: CodecLZ4}} {
		opts.Ordered, opts.SortKey = true, mergeKey
		var buf bytes.Buffer
		w := NewWriter(&buf, opts)
		w.columnBytes = own
		if err := Merge(w, []*Part{srcs[1], srcs[0]}, blockRows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if want := 14 * int(1-opts.Codec/CodecLZ4); w.copied != want {
			t.Fatalf("codec %d: %d blocks copied, want %d", opts.Codec, w.copied, want)
		}
		p, err := Open(buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		if rate := columnAnswers(t, p, own, r); rate > 0.02 {
			t.Fatalf("codec %d: filters let %.2f%% of the absent tokens pass", opts.Codec, 100*rate)
		}
	}
}

// The filters of all columns of all blocks of a group have the bloom blocks
// of one token in one stripe.
func TestTokenLookupInEveryColumnReadsOneStripe(t *testing.T) {
	r := rand.New(rand.NewSource(13))
	p := columnSource(t, r, 0, 40*400, 400, 250)
	for n := 0; n < 300; n++ {
		h := r.Uint64()
		for g, run := range groupRuns(p) {
			reads := newFileReads()
			reads.lookup(p, h, run[0], run[1])
			if stripe := int(p.groups[g].stripe+pageBlocks-1)/pageBlocks + 1; reads.faults != 1 || len(reads.pages) > stripe {
				t.Fatalf("group %d: %d reads of the file for %d pages, a stripe has %d", g, reads.faults, len(reads.pages), stripe)
			}
		}
	}
}
