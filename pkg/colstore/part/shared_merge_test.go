package part

import (
	"bytes"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

var sharedKey = []string{"k"}

// keyedDocs returns n events with a key of keys values, an account drawn
// from users and, in one event of three, a second account; seed tells the
// events of one call from those of another.
func keyedDocs(seed int64, n, keys, users int) []string {
	r := rand.New(rand.NewSource(seed))
	docs := make([]string, n)
	for i := range docs {
		var sb strings.Builder
		fmt.Fprintf(&sb, `{"k":"key%04d","did":"%s","seq":"%d-%d"`, r.Intn(keys), account(r.Intn(users)), seed, i)
		if r.Intn(3) == 0 {
			fmt.Fprintf(&sb, `,"other":"%s"`, account(r.Intn(users)))
		}
		sb.WriteString("}")
		docs[i] = sb.String()
	}

	return docs
}

// mergeKeyRange merges the rows of srcs between lo and hi into a part.
func mergeKeyRange(t testing.TB, srcs []*Part, opts Options, blockRows int, lo, hi []byte) *Part {
	t.Helper()
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

	return p
}

// holdRows fails unless the parts hold the rows of docs, in any order.
func holdRows(t *testing.T, parts []*Part, docs ...[]string) {
	t.Helper()
	var got, want []string
	for _, p := range parts {
		rows, _ := readAll(t, p)
		got = append(got, rows...)
	}
	for _, d := range docs {
		want = append(want, d...)
	}
	if len(got) != len(want) {
		t.Fatalf("the parts hold %d rows, want %d", len(got), len(want))
	}
	// A row is rendered with its members in the order of the paths, which
	// is not that of the document.
	norm := func(rows []string) []string {
		out := make([]string, len(rows))
		for i, row := range rows {
			members := strings.Split(strings.Trim(row, "{}"), ",")
			slices.Sort(members)
			out[i] = strings.Join(members, ",")
		}
		slices.Sort(out)

		return out
	}
	g, w := norm(got), norm(want)
	for i := range g {
		if g[i] != w[i] {
			t.Fatalf("row %d of the sorted rows is %s, want %s", i, g[i], w[i])
		}
	}
}

// checkDictionary fails unless every code of the blocks of p at path has a
// string and the dictionary holds no string that no block uses.
func checkDictionary(t *testing.T, p *Part, path string) *Dictionary {
	t.Helper()
	id, _ := p.PathID(path)
	d, ok := p.Dictionary(id)
	if !ok {
		t.Fatalf("no dictionary of %s", path)
	}
	used := map[uint32]bool{}
	var b Block
	var col vec.Column
	var codes []uint32
	for i := 0; i < p.NumBlocks(); i++ {
		if err := p.OpenBlock(i, &b); err != nil {
			t.Fatal(err)
		}
		ref, ok := b.Column(id, vec.KindString)
		if !ok || !ref.Shared() {
			continue
		}
		var err error
		if codes, err = b.DecodeShared(ref, &col, codes); err != nil {
			t.Fatal(err)
		}
		for _, c := range codes {
			used[c] = true
		}
	}
	if len(used) != d.Entries() {
		t.Fatalf("the blocks use %d codes of %s and its dictionary stores %d strings", len(used), path, d.Entries())
	}
	var prev, cur []byte
	first := true
	for code := 0; code < d.Len(); code++ {
		var err error
		cur, err = d.Value(cur[:0], uint32(code))
		if !used[uint32(code)] {
			if err == nil {
				t.Fatalf("code %d of %s, which no block uses, has a string", code, path)
			}

			continue
		}
		if err != nil {
			t.Fatalf("code %d of %s: %v", code, path, err)
		}
		if !first && bytes.Compare(prev, cur) >= 0 {
			t.Fatalf("string %q of code %d does not follow %q", cur, code, prev)
		}
		if got, ok, err := d.Find(cur); err != nil || !ok || got != uint32(code) {
			t.Fatalf("Find(%q) = %d, %v, %v, want %d", cur, got, ok, err, code)
		}
		prev, first = append(prev[:0], cur...), false
	}

	return d
}

func TestMergeMergesTheDictionariesOfItsParts(t *testing.T) {
	opts := Options{Ordered: true, SortKey: sharedKey}
	var srcs []*Part
	var docs [][]string
	for i := range 4 {
		d := keyedDocs(int64(i+1), 12000, 400, 1200)
		docs = append(docs, d)
		p := plannedPart(t, d, 2048, opts)
		if shared, own := sharedBlocks(t, p, "did"); shared == 0 || own != 0 {
			t.Fatalf("part %d stores did as codes in %d blocks and otherwise in %d", i, shared, own)
		}
		srcs = append(srcs, p)
	}
	merged := mergeKeyRange(t, srcs, opts, 4096, nil, nil)
	holdRows(t, []*Part{merged}, docs...)
	for _, path := range []string{"did", "other"} {
		if shared, own := sharedBlocks(t, merged, path); shared == 0 || own != 0 {
			t.Fatalf("the merged part stores %s as codes in %d blocks and otherwise in %d", path, shared, own)
		}
		if d := checkDictionary(t, merged, path); d.Len() != d.Entries() {
			t.Fatalf("a merge of whole parts left %d codes of %s without a string", d.Len()-d.Entries(), path)
		}
	}
	// A merge of the merged part with one more reads a dictionary that a
	// merge wrote.
	more := keyedDocs(9, 12000, 400, 1200)
	again := mergeKeyRange(t, []*Part{merged, plannedPart(t, more, 2048, opts)}, opts, 4096, nil, nil)
	holdRows(t, []*Part{again}, append(docs, more)...)
	checkDictionary(t, again, "did")
}

func TestMergeSharesAColumnThatItsPartsDoNot(t *testing.T) {
	opts := Options{Ordered: true, SortKey: sharedKey}
	var srcs []*Part
	var docs [][]string
	for i := range 6 {
		// Too few rows a part for its accounts to repeat, and enough in all.
		d := keyedDocs(int64(i+1), 5000, 300, 20000)
		docs = append(docs, d)
		p := plannedPart(t, d, 2048, opts)
		if shared, _ := sharedBlocks(t, p, "did"); shared != 0 {
			t.Fatalf("part %d shares a column whose values hardly repeat", i)
		}
		srcs = append(srcs, p)
	}
	merged := mergeKeyRange(t, srcs, opts, 4096, nil, nil)
	holdRows(t, []*Part{merged}, docs...)
	if shared, own := sharedBlocks(t, merged, "did"); shared == 0 || own != 0 {
		t.Fatalf("the merged part stores did as codes in %d blocks and otherwise in %d", shared, own)
	}
	checkDictionary(t, merged, "did")
	if shared, _ := sharedBlocks(t, merged, "seq"); shared != 0 {
		t.Fatal("the merged part shares a column without repeats")
	}
}

func TestMergeOfPartsWithAndWithoutADictionary(t *testing.T) {
	opts := Options{Ordered: true, SortKey: sharedKey}
	a, b := keyedDocs(1, 12000, 400, 1200), keyedDocs(2, 12000, 400, 1200)
	// A part of few rows has no dictionary, and one written without Share
	// has none either.
	c := keyedDocs(3, 900, 400, 1200)
	srcs := []*Part{plannedPart(t, a, 2048, opts), plannedPart(t, b, 2048, opts), plannedPart(t, c, 2048, opts)}
	if shared, _ := sharedBlocks(t, srcs[2], "did"); shared != 0 {
		t.Fatal("the small part has a dictionary")
	}
	merged := mergeKeyRange(t, srcs, opts, 4096, nil, nil)
	holdRows(t, []*Part{merged}, a, b, c)
	if shared, own := sharedBlocks(t, merged, "did"); shared == 0 || own != 0 {
		t.Fatalf("the merged part stores did as codes in %d blocks and otherwise in %d", shared, own)
	}
	checkDictionary(t, merged, "did")
}

func TestRangeMergesLeaveCodesWithoutAString(t *testing.T) {
	opts := Options{Ordered: true, SortKey: sharedKey}
	var srcs []*Part
	var docs [][]string
	for i := range 3 {
		d := keyedDocs(int64(i+1), 12000, 400, 1200)
		docs = append(docs, d)
		srcs = append(srcs, plannedPart(t, d, 2048, opts))
	}
	cuts := [][]byte{nil, AppendKeyString(nil, []byte("key0100")), AppendKeyString(nil, []byte("key0250")), nil}
	var parts []*Part
	holes := 0
	for i := 0; i+1 < len(cuts); i++ {
		p := mergeKeyRange(t, srcs, opts, 4096, cuts[i], cuts[i+1])
		if shared, own := sharedBlocks(t, p, "did"); shared == 0 || own != 0 {
			t.Fatalf("range %d stores did as codes in %d blocks and otherwise in %d", i, shared, own)
		}
		d := checkDictionary(t, p, "did")
		holes += d.Len() - d.Entries()
		parts = append(parts, p)
	}
	holdRows(t, parts, docs...)
	if holes == 0 {
		t.Fatal("no range left a code without a string, so the test does not read such dictionaries")
	}
	// A merge of the ranges reads dictionaries whose codes lack strings.
	whole := mergeKeyRange(t, parts, opts, 4096, nil, nil)
	holdRows(t, []*Part{whole}, docs...)
	if d := checkDictionary(t, whole, "did"); d.Len() != d.Entries() {
		t.Fatalf("the merge of whole parts left %d codes without a string", d.Len()-d.Entries())
	}
}

func TestMergeCopiesBlocksOfSharedColumns(t *testing.T) {
	opts := Options{Ordered: true, SortKey: sharedKey}
	// Parts whose keys follow one another: their blocks are copied. A
	// block is not when the merge shares a column that the block does not,
	// so the second account, which a part holds too seldom to share it, is
	// left out.
	var srcs []*Part
	var docs [][]string
	for i := range 3 {
		d := keyedDocs(int64(i+1), 12000, 400, 1200)
		for j := range d {
			d[j] = strings.Replace(d[j], `"k":"key`, fmt.Sprintf(`"k":"part%d-key`, i), 1)
			if at := strings.Index(d[j], `,"other"`); at >= 0 {
				d[j] = d[j][:at] + "}"
			}
		}
		docs = append(docs, d)
		srcs = append(srcs, plannedPart(t, d, 2048, opts))
	}
	var buf bytes.Buffer
	w := NewWriter(&buf, opts)
	if err := Merge(w, srcs, 2048); err != nil {
		t.Fatal(err)
	}
	copied := w.copied
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if copied == 0 {
		t.Fatal("no block was copied, so the test does not cover copies")
	}
	merged, err := Open(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	holdRows(t, []*Part{merged}, docs...)
	if shared, own := sharedBlocks(t, merged, "did"); shared == 0 || own != 0 {
		t.Fatalf("the merged part stores did as codes in %d blocks and otherwise in %d", shared, own)
	}
	checkDictionary(t, merged, "did")
}

func TestMergeIntoAWriterThatDoesNotPack(t *testing.T) {
	opts := Options{Ordered: true, SortKey: sharedKey}
	a, b := keyedDocs(1, 12000, 400, 1200), keyedDocs(2, 12000, 400, 1200)
	srcs := []*Part{plannedPart(t, a, 2048, opts), plannedPart(t, b, 2048, opts)}
	plain := opts
	plain.NoPacking = true
	merged := mergeKeyRange(t, srcs, plain, 4096, nil, nil)
	holdRows(t, []*Part{merged}, a, b)
	if shared, _ := sharedBlocks(t, merged, "did"); shared != 0 {
		t.Fatal("a writer that does not pack shares a column")
	}
}
