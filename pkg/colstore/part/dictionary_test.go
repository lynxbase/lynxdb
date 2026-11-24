package part

import (
	"bytes"
	"errors"
	"math/bits"
	"math/rand"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

// decodeAll decodes the string column at path in every block of p.
func decodeAll(p *Part, path string) error {
	id, _ := p.PathID(path)
	var b Block
	var col vec.Column
	for i := 0; i < p.NumBlocks(); i++ {
		if err := p.OpenBlock(i, &b); err != nil {
			return err
		}
		if ref, ok := b.Column(id, vec.KindString); ok {
			if err := b.Decode(ref, &col); err != nil {
				return err
			}
		}
	}

	return nil
}

func TestDamageToADictionaryIsFound(t *testing.T) {
	docs := accountDocs(20000, 1500, 97, 4)
	whole := plannedPart(t, docs, 2048, Options{})
	id, _ := whole.PathID("did")
	d, ok := whole.Dictionary(id)
	if !ok || d.exceptions == 0 {
		t.Fatalf("no dictionary with strings kept as they are")
	}
	rng := rand.New(rand.NewSource(1))
	for try := 0; try < 200; try++ {
		data := bytes.Clone(whole.data)
		// Any bit of the sections: slots, strings kept as they are, and the
		// checksums of their pages.
		at := int(d.off) + rng.Intn(int(d.size))
		data[at] ^= 1 << rng.Intn(8)
		p, err := Open(data)
		if err != nil {
			t.Fatalf("a part with a damaged dictionary does not open: %v", err)
		}
		if err := decodeAll(p, "did"); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("a flipped bit at byte %d of the dictionary's %d: decoding returned %v", at-int(d.off), d.size, err)
		}
		// The column beside it reads as before.
		if err := decodeAll(p, "unique"); err != nil {
			t.Fatal(err)
		}
	}
	// A page is checked once, and again after Recheck.
	data := bytes.Clone(whole.data)
	p, err := Open(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := decodeAll(p, "did"); err != nil {
		t.Fatal(err)
	}
	data[int(d.off)+100] ^= 4
	if err := decodeAll(p, "did"); err != nil {
		t.Fatalf("a page seen to be sound was checked again: %v", err)
	}
	p.Recheck()
	if err := decodeAll(p, "did"); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("after Recheck decoding returned %v", err)
	}
}

func TestDictionaryOfStringsThatDoNotPack(t *testing.T) {
	// Strings of one length whose bytes come from more symbols than a
	// packed section takes stay in the forms of single blocks.
	r := rand.New(rand.NewSource(5))
	values := make([]string, 300)
	for i := range values {
		b := make([]byte, 16)
		for j := range b {
			b[j] = byte(33 + r.Intn(90))
		}
		values[i] = string(bytes.ReplaceAll(bytes.ReplaceAll(b, []byte(`"`), []byte("x")), []byte(`\`), []byte("y")))
	}
	docs := make([]string, 12000)
	for i := range docs {
		docs[i] = `{"v":"` + values[r.Intn(len(values))] + `"}`
	}
	p := plannedPart(t, docs, 2048, Options{})
	checkDocs(t, p, docs)
	if shared, _ := sharedBlocks(t, p, "v"); shared != 0 {
		t.Fatalf("%d blocks share a column whose strings do not pack", shared)
	}
}

func TestDamageToADictionaryWithGapsIsFound(t *testing.T) {
	opts := Options{Ordered: true, SortKey: sharedKey}
	var srcs []*Part
	for i := range 3 {
		srcs = append(srcs, plannedPart(t, keyedDocs(int64(i+1), 12000, 400, 1200), 2048, opts))
	}
	// A range leaves codes without strings when rows outside it, in the
	// blocks at its ends, hold strings that no row inside it does.
	var whole *Part
	var d *Dictionary
	cuts := [][]byte{nil, AppendKeyString(nil, []byte("key0100")), AppendKeyString(nil, []byte("key0250")), nil}
	for i := 0; i+1 < len(cuts) && (d == nil || d.liveLen == 0); i++ {
		whole = mergeKeyRange(t, srcs, opts, 4096, cuts[i], cuts[i+1])
		id, _ := whole.PathID("did")
		d, _ = whole.Dictionary(id)
	}
	if d == nil || d.liveLen == 0 {
		t.Fatal("no dictionary whose codes lack strings")
	}
	rng := rand.New(rand.NewSource(2))
	for try := 0; try < 200; try++ {
		data := bytes.Clone(whole.data)
		// Any bit of the bits that tell which codes have a string, or of
		// the numbers of strings before every run of them.
		at := int(d.off+d.slotsLen+d.exceptLen) + rng.Intn(int(d.liveLen))
		data[at] ^= 1 << rng.Intn(8)
		p, err := Open(data)
		if err != nil {
			t.Fatalf("a part with a damaged dictionary does not open: %v", err)
		}
		if err := decodeAll(p, "did"); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("a flipped bit at byte %d of %d: decoding returned %v", at-int(d.off+d.slotsLen+d.exceptLen), d.liveLen, err)
		}
		// A merge that reads the codes of the part finds it too.
		var buf bytes.Buffer
		w := NewWriter(&buf, opts)
		if err := Merge(w, []*Part{p, srcs[0]}, 4096); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("a merge of the damaged part returned %v", err)
		}
	}
}

// pagesRead counts the pages of d that were read, and so checked, since the
// last Recheck.
func pagesRead(d *Dictionary) int {
	n := 0
	for i := range d.sound {
		n += bits.OnesCount64(d.sound[i].Load())
	}

	return n
}

func TestALookupReadsFewPagesOfADictionary(t *testing.T) {
	docs := accountDocs(60000, 40000, 0, 0)
	p := plannedPart(t, docs, 8192, Options{})
	id, _ := p.PathID("did")
	d, ok := p.Dictionary(id)
	if !ok {
		t.Fatal("no dictionary")
	}
	pages := int((d.dataLen + dictPage - 1) / dictPage)
	if pages < 64 {
		t.Fatalf("a dictionary of %d pages is too small to tell a lookup from a scan", pages)
	}
	var b Block
	var col vec.Column
	if err := p.OpenBlock(3, &b); err != nil {
		t.Fatal(err)
	}
	ref, _ := b.Column(id, vec.KindString)
	if err := b.Decode(ref, &col); err != nil {
		t.Fatal(err)
	}
	want := bytes.Clone(col.Bytes(77))

	// The code of one string: a page for every halving of the dictionary
	// at most.
	p.Recheck()
	if _, found, err := d.Find(want); err != nil || !found {
		t.Fatalf("Find: %v, %v", found, err)
	}
	if got, most := pagesRead(d), bits.Len(uint(d.Entries()))+1; got > most {
		t.Fatalf("a search of %d strings read %d of %d pages, want at most %d", d.Entries(), got, pages, most)
	}
	// The rows of one block that hold the string, and the strings of a few
	// rows: the pages of those strings.
	p.Recheck()
	rows, searched, err := b.FindEqual(ref, want, nil)
	if err != nil || !searched || len(rows) == 0 {
		t.Fatalf("FindEqual: %v, %v, %v", rows, searched, err)
	}
	picked := []uint32{5, 77, 4000, 8191}
	if err := b.DecodeRows(ref, picked, &col); err != nil {
		t.Fatal(err)
	}
	if string(col.Bytes(77)) != string(want) {
		t.Fatalf("row 77 read alone is %q, want %q", col.Bytes(77), want)
	}
	if got, most := pagesRead(d), bits.Len(uint(d.Entries()))+1+2*len(picked); got > most {
		t.Fatalf("a search and %d rows read %d of %d pages, want at most %d", len(picked), got, pages, most)
	}
	// A whole block reads strings from all over the dictionary.
	p.Recheck()
	if err := b.Decode(ref, &col); err != nil {
		t.Fatal(err)
	}
	if got := pagesRead(d); got < pages/2 {
		t.Fatalf("a whole block read %d of %d pages; the test expects it to read most", got, pages)
	}
}
