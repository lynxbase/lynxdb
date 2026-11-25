package part

import (
	"bytes"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// plannedPart is batchPart with a writer that may give its part
// dictionaries and is told of all its rows.
func plannedPart(t testing.TB, docs []string, blockRows int, opts Options) *Part {
	t.Helper()
	opts.Dictionary = true

	return batchPart(t, docs, blockRows, opts, true)
}

// batchPart shreds docs into one batch and writes them in blocks of
// blockRows rows; with tell set it tells the writer of all its rows first.
func batchPart(t testing.TB, docs []string, blockRows int, opts Options, tell bool) *Part {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf, opts)
	batch := shred.NewBatch(shred.Options{})
	for i, d := range docs {
		if err := batch.AddJSON([]byte(d), int64(1000+i)); err != nil {
			t.Fatalf("doc %d: %v", i, err)
		}
	}
	if len(opts.SortKey) > 0 {
		batch.SortBy(opts.SortKey)
	}
	if tell {
		w.Share([]shred.Run{{Batch: batch, From: 0, To: batch.Rows}})
	}
	block := shred.NewBatch(shred.Options{})
	for from := 0; from < batch.Rows; from += blockRows {
		block.Reset()
		batch.CopyRows(block, from, min(from+blockRows, batch.Rows))
		if err := w.WriteBatch(block); err != nil {
			t.Fatal(err)
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

const testBase32 = "abcdefghijklmnopqrstuvwxyz234567"

// account returns the identifier of account n, 32 bytes of which 24 differ.
func account(n int) string {
	r := rand.New(rand.NewSource(int64(n)))
	id := make([]byte, 24)
	for i := range id {
		id[i] = testBase32[r.Intn(32)]
	}

	return "did:plc:" + string(id)
}

// accountDocs returns n events of accounts drawn from users, one in odd of
// which has an identifier of another length and one in absent none.
func accountDocs(n, users, odd, absent int) []string {
	r := rand.New(rand.NewSource(7))
	docs := make([]string, n)
	for i := range docs {
		id := account(r.Intn(users))
		if odd > 0 && r.Intn(odd) == 0 {
			id = fmt.Sprintf("did:web:host%d.example", r.Intn(20))
		}
		var sb strings.Builder
		fmt.Fprintf(&sb, `{"seq":%d,"unique":"%s"`, i, account(1_000_000+i))
		if absent == 0 || r.Intn(absent) != 0 {
			fmt.Fprintf(&sb, `,"did":"%s"`, id)
		}
		sb.WriteString("}")
		docs[i] = sb.String()
	}

	return docs
}

// checkDocs fails unless the rows of p are docs in order.
func checkDocs(t *testing.T, p *Part, docs []string) {
	t.Helper()
	got, _ := readAll(t, p)
	if len(got) != len(docs) {
		t.Fatalf("part holds %d rows, want %d", len(got), len(docs))
	}
	for i := range docs {
		if !sameJSON(t, got[i], docs[i]) {
			t.Fatalf("row %d is %s, want %s", i, got[i], docs[i])
		}
	}
}

// sharedBlocks counts the blocks of p that store the string column at path
// as codes, and those that store it otherwise.
func sharedBlocks(t *testing.T, p *Part, path string) (shared, own int) {
	t.Helper()
	id, ok := p.PathID(path)
	if !ok {
		return 0, 0
	}
	var b Block
	for i := 0; i < p.NumBlocks(); i++ {
		if err := p.OpenBlock(i, &b); err != nil {
			t.Fatal(err)
		}
		if ref, ok := b.Column(id, vec.KindString); ok {
			if ref.Shared() {
				shared++
			} else {
				own++
			}
		}
	}

	return shared, own
}

func TestWriterSharesAColumnThatRepeatsAcrossBlocks(t *testing.T) {
	for _, tc := range []struct {
		name        string
		odd, absent int
	}{{"dense", 0, 0}, {"other lengths", 97, 0}, {"gaps", 0, 3}, {"both", 61, 5}} {
		t.Run(tc.name, func(t *testing.T) {
			docs := accountDocs(20000, 1500, tc.odd, tc.absent)
			p := plannedPart(t, docs, 2048, Options{})
			checkDocs(t, p, docs)
			if shared, own := sharedBlocks(t, p, "did"); shared != p.NumBlocks() || own != 0 {
				t.Fatalf("%d blocks store did as codes and %d otherwise, want all %d as codes", shared, own, p.NumBlocks())
			}
			if shared, _ := sharedBlocks(t, p, "unique"); shared != 0 {
				t.Fatalf("%d blocks store a column without repeats as codes", shared)
			}
			id, _ := p.PathID("did")
			d, ok := p.Dictionary(id)
			if !ok {
				t.Fatal("no dictionary")
			}
			if d.Len() != d.Entries() {
				t.Fatalf("a flushed dictionary has %d codes and %d strings", d.Len(), d.Entries())
			}
			// The strings are in order and found by value.
			var prev, cur []byte
			for code := 0; code < d.Len(); code++ {
				var err error
				if cur, err = d.Value(cur[:0], uint32(code)); err != nil {
					t.Fatal(err)
				}
				if code > 0 && bytes.Compare(prev, cur) >= 0 {
					t.Fatalf("string %d %q does not follow %q", code, cur, prev)
				}
				if got, ok, err := d.Find(cur); err != nil || !ok || got != uint32(code) {
					t.Fatalf("Find(%q) = %d, %v, %v, want %d", cur, got, ok, err, code)
				}
				prev = append(prev[:0], cur...)
			}
			for _, absent := range []string{"", "did:plc:", account(-1), "zzzz", "did:web:nowhere"} {
				if _, ok, err := d.Find([]byte(absent)); ok || err != nil {
					t.Fatalf("Find(%q) = %v, %v for a string the column lacks", absent, ok, err)
				}
			}
		})
	}
}

func TestWriterWithoutTheOptionWritesNoDictionary(t *testing.T) {
	docs := accountDocs(20000, 1500, 61, 5)
	told := batchPart(t, docs, 2048, Options{}, true)
	checkDocs(t, told, docs)
	if told.Shares() {
		t.Fatal("a writer whose options ask for no dictionary wrote one")
	}
	// What the writer is told of the rows changes nothing of such a part.
	if untold := batchPart(t, docs, 2048, Options{}, false); !bytes.Equal(told.data, untold.data) {
		t.Fatal("the part differs from the one of a writer that was not told of its rows")
	}
}

func TestSharedColumnIsNoLargerThanItsBlocksAlone(t *testing.T) {
	docs := accountDocs(40000, 3000, 0, 0)
	alone := buildPart(t, docs, 4096, Options{})
	shared := plannedPart(t, docs, 4096, Options{})
	if shared.Size() >= alone.Size() {
		t.Fatalf("the part with a dictionary takes %d bytes, the one without %d", shared.Size(), alone.Size())
	}
}

func TestSharedCodesAndRows(t *testing.T) {
	docs := accountDocs(12000, 900, 41, 4)
	p := plannedPart(t, docs, 2048, Options{})
	id, _ := p.PathID("did")
	d, ok := p.Dictionary(id)
	if !ok {
		t.Fatal("no dictionary")
	}
	var b Block
	var all, picked, presence vec.Column
	var codes []uint32
	var val []byte
	rng := rand.New(rand.NewSource(3))
	for i := 0; i < p.NumBlocks(); i++ {
		if err := p.OpenBlock(i, &b); err != nil {
			t.Fatal(err)
		}
		ref, ok := b.Column(id, vec.KindString)
		if !ok {
			t.Fatalf("block %d has no column", i)
		}
		if err := b.Decode(ref, &all); err != nil {
			t.Fatal(err)
		}
		var err error
		if codes, err = b.DecodeShared(ref, &presence, codes); err != nil {
			t.Fatal(err)
		}
		if len(codes) != ref.Count {
			t.Fatalf("block %d: %d codes for %d values", i, len(codes), ref.Count)
		}
		next := 0
		for row := 0; row < b.Rows; row++ {
			if presence.IsValid(row) != all.IsValid(row) {
				t.Fatalf("block %d row %d: presence differs", i, row)
			}
			if !all.IsValid(row) {
				continue
			}
			if val, err = d.Value(val[:0], codes[next]); err != nil || string(val) != string(all.Bytes(row)) {
				t.Fatalf("block %d row %d: code %d is %q, %v, the row holds %q", i, row, codes[next], val, err, all.Bytes(row))
			}
			next++
		}
		// A few rows read alone hold what the whole column holds.
		rows := []uint32{uint32(b.Rows - 1), 0, uint32(rng.Intn(b.Rows)), uint32(rng.Intn(b.Rows)), 0}
		if err := b.DecodeRows(ref, rows, &picked); err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			if picked.IsValid(int(row)) != all.IsValid(int(row)) ||
				(all.IsValid(int(row)) && string(picked.Bytes(int(row))) != string(all.Bytes(int(row)))) {
				t.Fatalf("block %d row %d read alone differs", i, row)
			}
		}
		// The rows that equal a value, found without decoding the strings.
		want := all.Bytes(int(rows[2]))
		if !all.IsValid(int(rows[2])) {
			want = []byte(account(0))
		}
		var expect []uint32
		for row := 0; row < b.Rows; row++ {
			if all.IsValid(row) && string(all.Bytes(row)) == string(want) {
				expect = append(expect, uint32(row))
			}
		}
		found, searched, err := b.FindEqual(ref, want, nil)
		if err != nil || !searched || !slices.Equal(found, expect) {
			t.Fatalf("block %d: FindEqual(%q) = %v, %v, %v, want %v", i, want, found, searched, err, expect)
		}
		if found, searched, err := b.FindEqual(ref, []byte("did:plc:nobody"), nil); err != nil || !searched || len(found) != 0 {
			t.Fatalf("block %d: FindEqual of an absent value = %v, %v, %v", i, found, searched, err)
		}
	}
}

func TestSharedColumnKeepsItsTokens(t *testing.T) {
	docs := accountDocs(9000, 700, 0, 0)
	alone := buildPart(t, docs, 2048, Options{})
	shared := plannedPart(t, docs, 2048, Options{})
	if s, _ := sharedBlocks(t, shared, "did"); s == 0 {
		t.Fatal("the column is not shared")
	}
	hashes := map[string]uint64{}
	for _, word := range []string{"did", "plc", account(3)[8:], account(699)[8:], "nobody"} {
		hashes[word] = tokenHash(word)
	}
	for i := 0; i < alone.NumBlocks(); i++ {
		for word, h := range hashes {
			a, errA := alone.MayContainToken(i, h)
			s, errS := shared.MayContainToken(i, h)
			if errA != nil || errS != nil {
				t.Fatal(errA, errS)
			}
			// A filter may answer yes for a token it lacks, never no for
			// one it has.
			if a && !s && word != "nobody" {
				t.Fatalf("block %d: the shared part lacks token %q", i, word)
			}
		}
	}
}

func TestSharedColumnOfASortKeyIsLeftAlone(t *testing.T) {
	docs := accountDocs(20000, 1500, 0, 0)
	p := plannedPart(t, docs, 2048, Options{Ordered: true, SortKey: []string{"did"}})
	if shared, _ := sharedBlocks(t, p, "did"); shared != 0 {
		t.Fatalf("%d blocks store a sort key column as codes", shared)
	}
	sorted := slices.Clone(docs)
	sort.SliceStable(sorted, func(a, b int) bool { return didOf(sorted[a]) < didOf(sorted[b]) })
	checkDocs(t, p, sorted)
}

// didOf returns the did member of a document of accountDocs.
func didOf(doc string) string {
	at := strings.Index(doc, `"did":"`)
	if at < 0 {
		return ""
	}
	rest := doc[at+7:]

	return rest[:strings.IndexByte(rest, '"')]
}

// tokenHash returns the hash of a word that is one token.
func tokenHash(word string) uint64 { return tokenfilter.AppendHashes(nil, []byte(word))[0] }

// TestSharedColumnHasAFilterOfItsOwn writes a column of identifiers as
// codes beside a small column of words: the tokens of the identifiers are
// in a filter of the column's own, as they are in a part without a
// dictionary, so that a word of the small column does not send a search to
// the strings of the dictionary.
func TestSharedColumnHasAFilterOfItsOwn(t *testing.T) {
	r := rand.New(rand.NewSource(11))
	docs := make([]string, 12000)
	for i := range docs {
		note := ""
		if i%40 == 0 {
			note = `,"note":"an earthquake"`
		}
		docs[i] = fmt.Sprintf(`{"seq":%d,"did":"%s"%s}`, i, account(r.Intn(5000)), note)
	}
	for name, p := range map[string]*Part{"without a dictionary": buildPart(t, docs, 2048, Options{}), "with one": plannedPart(t, docs, 2048, Options{})} {
		did, _ := p.PathID("did")
		note, _ := p.PathID("note")
		if shared, own := sharedBlocks(t, p, "did"); (name == "with one") != (shared > 0 && own == 0) {
			t.Fatalf("%s: %d blocks store codes and %d strings", name, shared, own)
		}
		word, held := []uint64{tokenHash("earthquake")}, []uint64{tokenHash(account(3)[8:])}
		var b Block
		var bits vec.Bitmap
		spared := 0
		for i := 0; i < p.NumBlocks(); i++ {
			if err := p.OpenBlock(i, &b); err != nil {
				t.Fatal(err)
			}
			if !b.OwnFilter(did, vec.KindString) || b.OwnFilter(note, vec.KindString) {
				t.Fatalf("%s, block %d: the identifiers have a filter of their own: %v, and the words: %v",
					name, i, b.OwnFilter(did, vec.KindString), b.OwnFilter(note, vec.KindString))
			}
			var err error
			if bits, err = b.ColumnsMayContain(bits, word); err != nil {
				t.Fatal(err)
			}
			for c, ref := range b.Columns() {
				switch {
				case ref.PathID == note && !bits.Get(c):
					t.Fatalf("%s, block %d: the filters rule the word out of the column that holds it", name, i)
				case ref.PathID == did && !bits.Get(c):
					spared++
				}
			}
			// A filter may answer yes for a token the column lacks, never
			// no for one it has.
			if may, err := b.MayContainTokensIn(did, vec.KindString, held); err != nil || (!may && strings.Contains(strings.Join(docs[i*2048:min((i+1)*2048, len(docs))], ""), account(3))) {
				t.Fatalf("%s, block %d: the filter of the identifiers lacks one: %v", name, i, err)
			}
		}
		if spared < p.NumBlocks()-1 {
			t.Fatalf("%s: a word of another column spares the identifiers in %d of %d blocks", name, spared, p.NumBlocks())
		}
	}
}
