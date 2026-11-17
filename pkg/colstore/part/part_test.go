package part

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// buildPart shreds docs into blocks of blockRows rows and returns the part.
func buildPart(t testing.TB, docs []string, blockRows int, opts Options) *Part {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf, opts)
	batch := shred.NewBatch(shred.Options{})
	for i, d := range docs {
		if err := batch.AddJSON([]byte(d), int64(1000+i)); err != nil {
			t.Fatalf("doc %d: %v", i, err)
		}
		if batch.Rows == blockRows || i == len(docs)-1 {
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
		t.Fatalf("Open: %v", err)
	}

	return p
}

// readAll decodes every column of every block and renders each row.
func readAll(t testing.TB, p *Part) (docs []string, times []int64) {
	t.Helper()
	var b Block
	var r Renderer
	for i := 0; i < p.NumBlocks(); i++ {
		if err := p.OpenBlock(i, &b); err != nil {
			t.Fatalf("OpenBlock(%d): %v", i, err)
		}
		var paths []string
		var cols []*vec.Column
		refs := b.Columns()
		for j := range refs {
			col := new(vec.Column)
			if err := b.Decode(&refs[j], col); err != nil {
				t.Fatalf("block %d column %s/%s: %v", i, p.Paths()[refs[j].PathID], refs[j].Kind, err)
			}
			if refs[j].PathID == 0 {
				for row := 0; row < b.Rows; row++ {
					times = append(times, col.Int(row))
				}

				continue
			}
			paths = append(paths, p.Paths()[refs[j].PathID])
			cols = append(cols, col)
		}
		r.Reset(paths, cols)
		for row := 0; row < b.Rows; row++ {
			docs = append(docs, string(r.AppendRow(nil, row)))
		}
	}

	return docs, times
}

func sameJSON(t testing.TB, got, want string) bool {
	t.Helper()
	var g, w any
	if err := json.Unmarshal([]byte(got), &g); err != nil {
		t.Fatalf("rendered row is not JSON: %v\n%s", err, got)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatal(err)
	}

	return reflect.DeepEqual(g, w)
}

func sampleDocs(n int) []string {
	r := rand.New(rand.NewSource(11))
	kinds := []string{"commit", "identity", "account"}
	docs := []string{
		`{}`,
		`{"only":"here","deep":{"a":{"b":{"c":[1,2,{"x":null}]}}},"empty":{},"list":[]}`,
		`{"esc":"tab\t quote\" back\\ nl\n é 😀","neg":-9223372036854775808,"f":-0.5e-7,"t":true,"n":null}`,
	}
	for i := 0; i < n; i++ {
		var sb strings.Builder
		fmt.Fprintf(&sb, `{"did":"did:plc:%016x","seq":%d,"kind":"%s","region":"eu"`, r.Uint64(), 1_000_000+i*3, kinds[r.Intn(len(kinds))])
		// A few hundred values of varying length exercise wide dictionaries.
		fmt.Fprintf(&sb, `,"host":"node-%s","shard":"s%d"`, strings.Repeat("x", r.Intn(40)), r.Intn(400)*r.Intn(400))
		if r.Intn(4) != 0 {
			fmt.Fprintf(&sb, `,"commit":{"collection":"app.bsky.feed.%s","rev":"%x","size":%d,"ok":%v}`,
				[]string{"post", "like", "repost"}[r.Intn(3)], r.Uint32(), r.Intn(900), r.Intn(10) != 0)
		}
		if r.Intn(20) == 0 {
			fmt.Fprintf(&sb, `,"rare":{"note":"Hello World %d","score":%g,"tags":["a","B%d"]}`, i, r.Float64()*100, i%3)
		}
		if r.Intn(7) == 0 {
			sb.WriteString(`,"mixed":"text"`)
		} else if r.Intn(7) == 0 {
			fmt.Fprintf(&sb, `,"mixed":%d`, i)
		}
		sb.WriteString("}")
		docs = append(docs, sb.String())
	}

	return docs
}

func TestPartRoundTripsDocuments(t *testing.T) {
	docs := sampleDocs(3000)
	for _, opts := range []Options{{}, {Level: LevelCompact}, {NoTokenFilter: true}, {Codec: CodecLZ4}, {Codec: CodecNone}} {
		for _, blockRows := range []int{1, 7, 1000, 1 << 16} {
			if blockRows == 1 && opts.Level != LevelFast {
				continue
			}
			p := buildPart(t, docs, blockRows, opts)
			got, times := readAll(t, p)
			if len(got) != len(docs) || p.Rows != uint64(len(docs)) {
				t.Fatalf("rows: got %d, part says %d, want %d", len(got), p.Rows, len(docs))
			}
			for i := range docs {
				if !sameJSON(t, got[i], docs[i]) {
					t.Fatalf("opts %+v blockRows %d row %d\n got  %s\n want %s", opts, blockRows, i, got[i], docs[i])
				}
				if times[i] != int64(1000+i) {
					t.Fatalf("row %d time %d", i, times[i])
				}
			}
			if p.MinTime != 1000 || p.MaxTime != int64(1000+len(docs)-1) {
				t.Fatalf("time bounds %d..%d", p.MinTime, p.MaxTime)
			}
		}
	}
}

func TestPartKeepsLargeIntegersExact(t *testing.T) {
	docs := []string{
		`{"u":18446744073709551615,"ok":9223372036854775807}`,
		`{"u":-9223372036854775809,"list":[18446744073709551616,2]}`,
	}
	got, _ := readAll(t, buildPart(t, docs, 10, Options{}))
	want := []string{
		`{"ok":9223372036854775807,"u":18446744073709551615}`,
		`{"list":[18446744073709551616,2],"u":-9223372036854775809}`,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestBlockPicksEncodingsAndStatistics(t *testing.T) {
	p := buildPart(t, sampleDocs(3000)[3:], 1<<16, Options{})
	var b Block
	if err := p.OpenBlock(0, &b); err != nil {
		t.Fatal(err)
	}
	col := func(path string, kind vec.Kind) (*ColumnRef, *vec.Column) {
		t.Helper()
		id, ok := p.PathID(path)
		if !ok {
			t.Fatalf("no path %s", path)
		}
		ref, ok := b.Column(id, kind)
		if !ok {
			t.Fatalf("no %s column for %s", kind, path)
		}
		c := new(vec.Column)
		if err := b.Decode(ref, c); err != nil {
			t.Fatal(err)
		}

		return ref, c
	}

	if ref, c := col("region", vec.KindString); !ref.Const() || string(ref.ConstBytes()) != "eu" || c.Layout != vec.Const {
		t.Errorf("region is not a const column")
	}
	if _, c := col("kind", vec.KindString); c.Layout != vec.Dict || c.DictLen() != 3 || len(c.Codes8) != b.Rows ||
		string(c.DictValue(0)) != "account" || string(c.DictValue(2)) != "identity" {
		t.Errorf("kind is not a sorted 3-entry dictionary: layout %d, %d entries", c.Layout, c.DictLen())
	}
	if _, c := col("did", vec.KindString); c.Layout != vec.Flat {
		t.Errorf("unique did values were dictionary encoded")
	}
	if ref, c := col("seq", vec.KindInt); ref.MinInt != 1_000_000 || ref.MaxInt != int64(1_000_000+2999*3) || c.Valid != nil {
		t.Errorf("seq stats %d..%d", ref.MinInt, ref.MaxInt)
	}
	if ref, c := col("commit.size", vec.KindInt); ref.Count >= b.Rows || c.Valid == nil || c.Valid.Count() != ref.Count {
		t.Errorf("commit.size presence: count %d of %d rows", ref.Count, b.Rows)
	}
	if ref, _ := col("rare.score", vec.KindFloat); ref.MinFloat < 0 || ref.MaxFloat > 100 || ref.MinFloat > ref.MaxFloat {
		t.Errorf("rare.score stats %g..%g", ref.MinFloat, ref.MaxFloat)
	}
	id, _ := p.PathID("mixed")
	if kinds := b.Path(id); len(kinds) != 2 || kinds[0].Kind != vec.KindInt || kinds[1].Kind != vec.KindString {
		t.Errorf("mixed path has %d columns", len(kinds))
	}
}

func TestBlockFindsEachColumnWithoutReadingTheOthers(t *testing.T) {
	p := buildPart(t, sampleDocs(3000), 1000, Options{})
	var b, all Block
	for i := 0; i < p.NumBlocks(); i++ {
		if err := p.OpenBlock(i, &all); err != nil {
			t.Fatal(err)
		}
		refs := all.Columns()
		if len(refs) < 10 {
			t.Fatalf("block %d has %d columns", i, len(refs))
		}
		for j := range refs {
			// A fresh open leaves every other directory entry undecoded.
			if err := p.OpenBlock(i, &b); err != nil {
				t.Fatal(err)
			}
			want := &refs[j]
			got, ok := b.Column(want.PathID, want.Kind)
			if !ok || !reflect.DeepEqual(got, want) {
				t.Fatalf("block %d: Column(%d, %s) = %+v, %v, want %+v", i, want.PathID, want.Kind, got, ok, want)
			}
			a, c := new(vec.Column), new(vec.Column)
			if err := b.Decode(got, a); err != nil {
				t.Fatal(err)
			}
			if err := all.Decode(want, c); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(a, c) {
				t.Fatalf("block %d column %d decodes differently when looked up alone", i, j)
			}
			if _, ok := b.Column(want.PathID, vec.Kind(0)); ok {
				t.Fatalf("block %d: found a column of an unknown kind", i)
			}
			path := b.Path(want.PathID)
			found := false
			for k := range path {
				found = found || reflect.DeepEqual(&path[k], want)
			}
			if !found || len(path) == 0 || len(path) > 2 {
				t.Fatalf("block %d: Path(%d) returned %d columns", i, want.PathID, len(path))
			}
		}
		if _, ok := b.Column(uint32(len(p.Paths())), vec.KindString); ok {
			t.Fatal("found a column of a path the part does not have")
		}
	}
}

func TestDamagedFilterFailsOnlyTokenLookups(t *testing.T) {
	// Groups of three blocks in four stripes of several pages each, the
	// last group short.
	const group, stripes, blocks = 3, 4, 8
	var buf bytes.Buffer
	w := NewWriter(&buf, Options{})
	w.groupBlocks, w.fixedStripes, w.columnBytes = group, stripes, math.MaxInt
	batch := shred.NewBatch(shred.Options{})
	var pieces [blocks]int
	for i := 0; i < blocks; i++ {
		docs := sampleDocs(200)
		for k := 0; k < 9000+1500*i; k++ {
			docs = append(docs, fmt.Sprintf(`{"id":"request%dx%d"}`, i, k))
		}
		batch.Reset()
		for _, d := range docs {
			if err := batch.AddJSON([]byte(d), 5); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.WriteBatch(batch); err != nil {
			t.Fatal(err)
		}
		pieces[i] = len(tokenfilter.Append(nil, tokensOf(t, docs), stripes, tokenfilter.SharedBits)) / tokenfilter.BlockBytes / stripes
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	p, err := Open(data)
	if err != nil {
		t.Fatal(err)
	}

	// Where every lookup reads, worked out from the layout: the stripes of
	// a group follow its last block, from the next page of the file on when
	// the group is large, each the pieces of its blocks in order, in pages
	// of a checksum and pageBlocks bloom blocks.
	type place struct{ group, page int }
	type section struct{ off, size, stripe int }
	var sections []section
	var before [blocks]int
	for first := 0; first < blocks; first += group {
		last := min(first+group, blocks) - 1
		stripe := 0
		for i := first; i <= last; i++ {
			before[i] = stripe
			stripe += pieces[i]
		}
		e := p.index[last*indexEntrySize:]
		off := int(binary.LittleEndian.Uint64(e[20:])) + int(binary.LittleEndian.Uint32(e[28:]))
		pages := (stripes*stripe + pageBlocks - 1) / pageBlocks
		if stripe < 3*pageBlocks {
			t.Fatalf("a stripe of %d bloom blocks is too short to tell its pages apart", stripe)
		}
		size := (stripes*stripe + pages) * tokenfilter.BlockBytes
		if size >= filterAligned {
			off = (off + filterPage - 1) &^ (filterPage - 1)
		}
		sections = append(sections, section{off, size, stripe})
	}
	r := rand.New(rand.NewSource(6))
	hashes := make([]uint64, 400)
	for i := range hashes {
		hashes[i] = r.Uint64()
	}
	read := func(i int, h uint64) place {
		stripe := tokenfilter.Block(h, stripes)
		n := stripe*sections[i/group].stripe + before[i] + tokenfilter.Block(h, pieces[i]*stripes) - stripe*pieces[i]

		return place{i / group, n / pageBlocks}
	}
	// lookups fails the test unless exactly the lookups that read the
	// damaged place fail, and returns how many did.
	lookups := func(damaged place) (failed int) {
		var b Block
		for i := 0; i < blocks; i++ {
			if err := p.OpenBlock(i, &b); err != nil {
				t.Fatalf("OpenBlock read the filter: %v", err)
			}
			for _, h := range hashes {
				_, err := p.MayContainToken(i, h)
				if _, berr := b.MayContainToken(h); berr != err {
					t.Fatalf("block %d: the part and the block disagree: %v, %v", i, err, berr)
				}
				if want := read(i, h) == damaged; (err == ErrCorrupt) != want || (err != nil && err != ErrCorrupt) {
					t.Fatalf("block %d reads %+v, damage at %+v: %v", i, read(i, h), damaged, err)
				}
				if err != nil {
					failed++
				}
			}
		}

		return failed
	}
	if lookups(place{group: -1}) != 0 {
		t.Fatal("lookups of an intact part failed")
	}
	total := 0
	for trial := 0; trial < 150; trial++ {
		g := r.Intn(len(sections))
		sec := sections[g]
		at := r.Intn(sec.size)
		bit := byte(1) << r.Intn(8)
		// A page is checked when it is first read after the pages of the
		// part were let go of.
		data[sec.off+at] ^= bit
		p.ForgetReadAhead()
		total += lookups(place{g, at / filterPage})
		data[sec.off+at] ^= bit
		p.ForgetReadAhead()
	}
	if total == 0 {
		t.Fatal("no lookup read a damaged page")
	}
	// What fills the file up to the page a group starts on is zero.
	for g, sec := range sections {
		e := p.index[(min((g+1)*group, blocks)-1)*indexEntrySize:]
		end := int(binary.LittleEndian.Uint64(e[20:])) + int(binary.LittleEndian.Uint32(e[28:]))
		if pad := data[end:sec.off]; len(pad) >= filterPage || !bytes.Equal(pad, make([]byte, len(pad))) {
			t.Fatalf("group %d: %d bytes before its page, not all zero", g, len(pad))
		}
	}

	// Columns are read without their filters.
	for _, sec := range sections {
		data[sec.off+r.Intn(sec.size)] ^= 0x10
	}
	var b Block
	for i := 0; i < blocks; i++ {
		if err := p.OpenBlock(i, &b); err != nil {
			t.Fatal(err)
		}
		refs := b.Columns()
		for j := range refs {
			if err := b.Decode(&refs[j], new(vec.Column)); err != nil {
				t.Fatalf("block %d column %d: %v", i, j, err)
			}
		}
	}
}

func TestTokenFilterCoversEveryStringToken(t *testing.T) {
	docs := sampleDocs(2000)
	p := buildPart(t, docs, 500, Options{})
	var b Block
	row := 0
	for i := 0; i < p.NumBlocks(); i++ {
		if err := p.OpenBlock(i, &b); err != nil {
			t.Fatal(err)
		}
		for r := 0; r < b.Rows; r, row = r+1, row+1 {
			var v any
			if err := json.Unmarshal([]byte(docs[row]), &v); err != nil {
				t.Fatal(err)
			}
			for _, s := range stringsIn(v, nil) {
				for _, h := range tokenfilter.AppendHashes(nil, []byte(s)) {
					if may, err := b.MayContainToken(h); !may || err != nil {
						t.Fatalf("block %d misses a token of %q: %v", i, s, err)
					}
				}
			}
		}
		misses := 0
		for k := 0; k < 1000; k++ {
			if may, _ := b.MayContainToken(tokenfilter.AppendHashes(nil, []byte(fmt.Sprintf("absent%dtoken", k)))[0]); !may {
				misses++
			}
		}
		if misses < 950 {
			t.Fatalf("block %d: filter excluded only %d of 1000 absent tokens", i, misses)
		}
	}
}

func stringsIn(v any, dst []string) []string {
	switch x := v.(type) {
	case string:
		dst = append(dst, x)
	case []any:
		for _, e := range x {
			dst = stringsIn(e, dst)
		}
	case map[string]any:
		for _, e := range x {
			dst = stringsIn(e, dst)
		}
	}

	return dst
}

func TestCorruptPartsFailWithoutPanicking(t *testing.T) {
	for _, codec := range []Codec{CodecZstd, CodecLZ4, CodecNone} {
		corruptPartsFail(t, Options{Codec: codec})
	}
}

func corruptPartsFail(t *testing.T, opts Options) {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf, opts)
	// Two filter groups, the second short.
	w.groupBlocks, w.fixedStripes = 2, 8
	batch := shred.NewBatch(shred.Options{})
	for i, d := range sampleDocs(400) {
		if err := batch.AddJSON([]byte(d), 5); err != nil {
			t.Fatal(err)
		}
		if i%150 == 149 {
			if err := w.WriteBatch(batch); err != nil {
				t.Fatal(err)
			}
			batch.Reset()
		}
	}
	if err := w.WriteBatch(batch); err != nil || w.Close() != nil {
		t.Fatal("write failed")
	}
	good := buf.Bytes()

	decodeAll := func(data []byte) (failed bool) {
		p, err := Open(data)
		if err != nil {
			if err != ErrCorrupt {
				t.Fatalf("Open: %v", err)
			}

			return true
		}
		var b Block
		for i := 0; i < p.NumBlocks(); i++ {
			if err := p.OpenBlock(i, &b); err != nil {
				return true
			}
			// One lookup in every bloom block of the filter.
			n := uint64(filterSize(p, i))
			for k := uint64(0); k < n; k++ {
				if _, err := b.MayContainToken((k<<32 + n - 1) / n << 32); err == ErrCorrupt {
					failed = true
				} else if err != nil {
					t.Fatalf("block %d: %v", i, err)
				}
			}
			refs := b.Columns()
			for j := range refs {
				if err := b.Decode(&refs[j], new(vec.Column)); err != nil {
					failed = true
				}
			}
		}

		return failed
	}
	if decodeAll(good) {
		t.Fatal("intact part failed to decode")
	}
	for cut := 0; cut < len(good); cut += 1 + len(good)/200 {
		if !decodeAll(good[:cut]) {
			t.Fatalf("part truncated to %d bytes decoded cleanly", cut)
		}
	}
	r := rand.New(rand.NewSource(5))
	for trial := 0; trial < 3000; trial++ {
		bad := append([]byte(nil), good...)
		at := r.Intn(len(bad))
		bad[at] ^= byte(1 + r.Intn(255))
		if !decodeAll(bad) {
			t.Fatalf("flipping byte %d of %d went unnoticed", at, len(bad))
		}
	}

	// Garbage in what locates the filters, under a checksum that vouches
	// for it, fails Open or the lookups it misleads.
	p, err := Open(good)
	if err != nil || len(p.groups) != 2 {
		t.Fatalf("%d filter groups: %v", len(p.groups), err)
	}
	index := bytes.Index(good, p.index)
	footer := int(binary.LittleEndian.Uint64(good[len(good)-trailerSize:]))
	// The counts of filters and of groups take a byte each.
	table := index + len(p.index) + 2
	var places []int
	for i := 0; i < p.NumBlocks(); i++ {
		for at := 32; at < 40; at++ {
			places = append(places, index+i*indexEntrySize+at)
		}
	}
	for at := table - 2; at < table+len(p.filters)+len(p.groups)*groupEntrySize; at++ {
		places = append(places, at)
	}
	for trial := 0; trial < 3000; trial++ {
		bad := append([]byte(nil), good...)
		var changed []int
		for n := 1 + r.Intn(3); n > 0; n-- {
			at := places[r.Intn(len(places))]
			changed = append(changed, at-index)
			switch r.Intn(3) {
			case 0:
				bad[at] = byte(r.Intn(256))
			case 1:
				bad[at] ^= 1 << r.Intn(8)
			default:
				bad[at] = [...]byte{0, 1, 0x7f, 0x80, 0xff}[r.Intn(5)]
			}
		}
		sum := crc32.Checksum(bad[footer:len(bad)-trailerSize], castagnoli)
		binary.LittleEndian.PutUint32(bad[len(bad)-trailerSize+16:], sum)
		if !bytes.Equal(bad, good) && !decodeAll(bad) {
			t.Fatalf("trial %d: garbage at bytes %v of the index, which the filters follow at %d, went unnoticed", trial, changed, len(p.index)+2)
		}
	}
}

func TestCodecChoiceChangesOnlyTheSize(t *testing.T) {
	docs := sampleDocs(3000)
	sizes := map[Codec]int{}
	var want []string
	for _, codec := range []Codec{CodecZstd, CodecLZ4, CodecNone} {
		p := buildPart(t, docs, 1000, Options{Codec: codec})
		got, _ := readAll(t, p)
		if want == nil {
			want = got
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("codec %d reads back different rows", codec)
		}
		sizes[codec] = p.Size()
	}
	if sizes[CodecZstd] >= sizes[CodecLZ4] || sizes[CodecLZ4] >= sizes[CodecNone] {
		t.Fatalf("part sizes %v do not grow from zstd over lz4 to none", sizes)
	}
}

func TestWriterResetWritesTheSamePartAgain(t *testing.T) {
	docs := sampleDocs(1500)
	write := func(w *Writer, buf *bytes.Buffer, opts Options, docs []string) {
		t.Helper()
		batch := shred.NewBatch(shred.Options{})
		for i, d := range docs {
			if err := batch.AddJSON([]byte(d), int64(i)); err != nil {
				t.Fatal(err)
			}
			if batch.Rows == 400 || i == len(docs)-1 {
				if err := w.WriteBatch(batch); err != nil {
					t.Fatal(err)
				}
				batch.Reset()
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	var fresh, first, again bytes.Buffer
	write(NewWriter(&fresh, Options{}), &fresh, Options{}, docs)
	// A reused writer must carry nothing over from a part with other paths,
	// rows and options.
	w := NewWriter(&first, Options{Codec: CodecLZ4, NoTokenFilter: true})
	write(w, &first, Options{}, append([]string{`{"only_here":{"x":[1,2,3]},"n":-5}`}, docs[:700]...))
	w.Reset(&again, Options{})
	write(w, &again, Options{}, docs)
	if !bytes.Equal(again.Bytes(), fresh.Bytes()) {
		t.Fatalf("a reused writer wrote %d bytes that differ from the %d of a new one", again.Len(), fresh.Len())
	}
}

func TestDecodeValuesHoldsTheValuesOfMarkedRowsInOrder(t *testing.T) {
	p := buildPart(t, sampleDocs(700), 300, Options{})
	var b Block
	var byRow, byValue vec.Column
	sparse := 0
	for i := 0; i < p.NumBlocks(); i++ {
		if err := p.OpenBlock(i, &b); err != nil {
			t.Fatal(err)
		}
		refs := b.Columns()
		for j := range refs {
			ref := &refs[j]
			if err := b.Decode(ref, &byRow); err != nil {
				t.Fatal(err)
			}
			if err := b.DecodeValues(ref, &byValue); err != nil {
				t.Fatal(err)
			}
			if byValue.Rows != byRow.Rows || byValue.Layout != byRow.Layout || (byValue.Valid == nil) != (byRow.Valid == nil) {
				t.Fatalf("block %d column %d: by value %d rows, layout %d", i, j, byValue.Rows, byValue.Layout)
			}
			if byRow.Valid != nil {
				sparse++
			}
			slot := 0
			for row := 0; row < b.Rows; row++ {
				if !byRow.IsValid(row) {
					if byValue.IsValid(row) {
						t.Fatalf("block %d column %d: by value a mark on row %d", i, j, row)
					}

					continue
				}
				at := row
				if byRow.Valid != nil {
					at = slot
				}
				slot++
				name := p.Paths()[ref.PathID]
				switch ref.Kind {
				case vec.KindBool:
					if byValue.Bool(at) != byRow.Bool(row) {
						t.Fatalf("%s: row %d differs", name, row)
					}
				case vec.KindInt:
					if byValue.Int(at) != byRow.Int(row) {
						t.Fatalf("%s: row %d differs", name, row)
					}
				case vec.KindFloat:
					if math.Float64bits(byValue.Float(at)) != math.Float64bits(byRow.Float(row)) {
						t.Fatalf("%s: row %d differs", name, row)
					}
				case vec.KindString, vec.KindVariant:
					if !bytes.Equal(byValue.Bytes(at), byRow.Bytes(row)) {
						t.Fatalf("%s: row %d differs", name, row)
					}
				}
			}
			if slot != ref.Count {
				t.Fatalf("block %d column %d: %d values, want %d", i, j, slot, ref.Count)
			}
		}
	}
	if sparse == 0 {
		t.Fatal("no column has gaps")
	}
}

func TestWriterEncodesFlatValuesAtTheLevelThatWonTheLastTrial(t *testing.T) {
	r := rand.New(rand.NewSource(41))
	var hashes, text []byte
	words := strings.Fields("the quick brown fox jumps over a lazy dog while seven wizards quietly box")
	for i := 0; i < 4000; i++ {
		hashes = append(hashes, "bafyrei"...)
		for j := 0; j < 52; j++ {
			hashes = append(hashes, "abcdefghijklmnopqrstuvwxyz234567"[r.Intn(32)])
		}
		for j := 0; j < 12; j++ {
			text = append(append(text, words[r.Intn(len(words))]...), ' ')
		}
	}
	size := func(level Level, data []byte) int {
		bc := blobCodec{codec: CodecZstd, level: level}
		var scratch []byte

		return len(bc.appendBlob(nil, data, &scratch))
	}
	w := NewWriter(&bytes.Buffer{}, Options{})
	var decoded []byte
	for slot, data := range [][]byte{hashes, text} {
		want := min(size(LevelFast, data), size(LevelFastest, data))
		for i := 0; i < 2*trialEvery+3; i++ {
			start := len(w.block)
			w.appendValues(slot, data, 4000)
			if got := len(w.block) - start; got != want {
				t.Fatalf("column %d, block %d: section of %d bytes, the smaller level gives %d", slot, i, got, want)
			}
			payload, rest, _, err := readBlob(w.block[start:], &decoded)
			if err != nil || len(rest) != 0 || !bytes.Equal(payload, data) {
				t.Fatalf("column %d, block %d: section does not decode: %v", slot, i, err)
			}
		}
	}
	if w.trials[0].choice != valuesFastest {
		t.Error("values without repeats were not encoded at the fastest level")
	}

	// Another level is the caller's choice, and small values are not worth
	// a trial.
	compact := NewWriter(&bytes.Buffer{}, Options{Level: LevelCompact})
	// The block buffer comes from a pool and may hold a block already.
	start := len(compact.block)
	compact.appendValues(0, text, 4000)
	if got, want := len(compact.block)-start, size(LevelCompact, text); got != want || len(compact.trials) != 0 {
		t.Fatalf("compact level: section of %d bytes, want %d", got, want)
	}
	w.appendValues(7, text[:trialMinSize-1], 1)
	if len(w.trials) > 7 {
		t.Fatal("small values were tried at both levels")
	}
}

func TestWriterTakesTheFasterCodecForLongValuesWithinTheSlack(t *testing.T) {
	r := rand.New(rand.NewSource(43))
	// Prose with a wide vocabulary shrinks little under either codec; lines
	// that repeat a template shrink far more under the general one.
	vocabulary := make([][]byte, 4000)
	for i := range vocabulary {
		vocabulary[i] = make([]byte, 3+r.Intn(7))
		for k := range vocabulary[i] {
			vocabulary[i][k] = byte('a' + r.Intn(26))
		}
	}
	var prose, lines []byte
	for i := 0; i < 3000; i++ {
		for j := 0; j < 14; j++ {
			prose = append(append(prose, vocabulary[r.Intn(len(vocabulary))]...), ' ')
		}
		lines = append(lines, fmt.Sprintf("GET /api/v1/items/%d HTTP/1.1 200 %d bytes from 10.0.%d.%d in %dms ", r.Intn(50), 1000+r.Intn(9), r.Intn(4), r.Intn(250), r.Intn(90))...)
	}
	section := func(w *Writer, slot int, data []byte, count int) (kind uint64, size int) {
		start := len(w.block)
		w.appendValues(slot, data, count)
		head, _ := binary.Uvarint(w.block[start:])
		var decoded []byte
		payload, rest, _, err := readBlob(w.block[start:], &decoded)
		if err != nil || len(rest) != 0 || !bytes.Equal(payload, data) {
			t.Fatalf("section does not decode: %v", err)
		}

		return head & 3, len(w.block) - start
	}
	plain := NewWriter(&bytes.Buffer{}, Options{})
	loose := NewWriter(&bytes.Buffer{}, Options{ValueSlack: 50})
	for round := 0; round < trialEvery+2; round++ {
		_, zstdSize := section(plain, 0, prose, 3000)
		kind, size := section(loose, 0, prose, 3000)
		if kind != blobLZ4 || size*100 > zstdSize*150 {
			t.Fatalf("round %d: prose took codec %d at %d bytes against %d", round, kind, size, zstdSize)
		}
		if kind, _ := section(loose, 1, lines, 3000); kind != blobZstd {
			t.Fatalf("round %d: lines that shrink well took codec %d", round, kind)
		}
		// The same prose counted as short values stays with the general codec.
		if kind, _ := section(loose, 2, prose, len(prose)/8); kind != blobZstd {
			t.Fatalf("round %d: short values took codec %d", round, kind)
		}
		if kind, _ := section(plain, 1, prose, 3000); kind != blobZstd {
			t.Fatalf("round %d: without slack the values took codec %d", round, kind)
		}
	}
}

func TestDecodePresenceMarksTheRowsThatDecodeGivesValues(t *testing.T) {
	p := buildPart(t, sampleDocs(3000), 700, Options{})
	var b Block
	var full, presence vec.Column
	sparse := 0
	for i := 0; i < p.NumBlocks(); i++ {
		if err := p.OpenBlock(i, &b); err != nil {
			t.Fatal(err)
		}
		refs := b.Columns()
		for j := range refs {
			if err := b.Decode(&refs[j], &full); err != nil {
				t.Fatal(err)
			}
			if err := b.DecodePresence(&refs[j], &presence); err != nil {
				t.Fatal(err)
			}
			if presence.Rows != full.Rows || presence.Kind != full.Kind || (presence.Valid == nil) != (full.Valid == nil) {
				t.Fatalf("block %d column %d: presence of %d rows, kind %v, valid nil=%v against %d, %v, %v",
					i, j, presence.Rows, presence.Kind, presence.Valid == nil, full.Rows, full.Kind, full.Valid == nil)
			}
			count := 0
			for row := 0; row < full.Rows; row++ {
				if presence.IsValid(row) != full.IsValid(row) {
					t.Fatalf("block %d column %d row %d: presence says %v", i, j, row, presence.IsValid(row))
				}
				if presence.IsValid(row) {
					count++
				}
			}
			if count != refs[j].Count {
				t.Fatalf("block %d column %d: %d rows marked, the column holds %d values", i, j, count, refs[j].Count)
			}
			if presence.Valid != nil {
				sparse++
			}
		}
	}
	if sparse == 0 {
		t.Fatal("no column has gaps")
	}
}
