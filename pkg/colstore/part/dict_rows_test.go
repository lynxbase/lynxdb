package part

import (
	"math/rand"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

// asked records the bytes of a part that were asked for ahead.
type asked struct{ spans [][2]int }

func (a *asked) ask(off, n int) { a.spans = append(a.spans, [2]int{off, off + n}) }

func (a *asked) covers(off, n int) bool {
	for _, s := range a.spans {
		if s[0] <= off && off+n <= s[1] {
			return true
		}
	}

	return false
}

// checkRowsAskedTogether reads a few rows of every block of p alone and
// requires that every page of dictionary d read for them was asked for
// before, in few requests, and that rows whose pages were read are not
// asked for again.
func checkRowsAskedTogether(t *testing.T, p *Part, d *Dictionary, path string) {
	t.Helper()
	id, _ := p.PathID(path)
	var requests asked
	p.SetReadAhead(requests.ask)
	var b Block
	var all, picked vec.Column
	rng := rand.New(rand.NewSource(9))
	for i := 0; i < p.NumBlocks(); i++ {
		if err := p.OpenBlock(i, &b); err != nil {
			t.Fatal(err)
		}
		ref, ok := b.Column(id, vec.KindString)
		if !ok || !ref.Shared() {
			continue
		}
		if err := b.Decode(ref, &all); err != nil {
			t.Fatal(err)
		}
		rows := make([]uint32, 24)
		for k := range rows {
			rows[k] = uint32(rng.Intn(b.Rows))
		}
		p.ForgetReadAhead()
		requests.spans = requests.spans[:0]
		if err := b.DecodeRows(ref, rows, &picked); err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			if picked.IsValid(int(row)) != all.IsValid(int(row)) ||
				(all.IsValid(int(row)) && string(picked.Bytes(int(row))) != string(all.Bytes(int(row)))) {
				t.Fatalf("block %d row %d read alone differs", i, row)
			}
		}
		read := 0
		for pg := uint64(0); pg*dictPage < d.slotsLen; pg++ {
			if d.sound[pg>>6].Load()>>(pg&63)&1 == 0 {
				continue
			}
			read++
			if !requests.covers(int(d.off+pg*dictPage), int(min(dictPage, d.size-pg*dictPage))) {
				t.Fatalf("block %d: page %d of the dictionary was read without being asked for", i, pg)
			}
		}
		if read == 0 {
			t.Fatalf("block %d: no page of the strings was read", i)
		}
		// A request for each run of pages, the rows' and the bits' that
		// number them, at most.
		if most := 4 * len(rows); len(requests.spans) > most {
			t.Fatalf("block %d: %d requests for %d rows, want at most %d", i, len(requests.spans), len(rows), most)
		}
		requests.spans = requests.spans[:0]
		if err := b.DecodeRows(ref, rows, &picked); err != nil {
			t.Fatal(err)
		}
		if len(requests.spans) != 0 {
			t.Fatalf("block %d: %d requests for pages that were read", i, len(requests.spans))
		}
	}
}

func TestStringsOfPickedRowsAreAskedForTogether(t *testing.T) {
	p := plannedPart(t, accountDocs(60000, 40000, 0, 5), 8192, Options{})
	id, _ := p.PathID("did")
	d, ok := p.Dictionary(id)
	if !ok || d.dataLen < 64*dictPage {
		t.Fatal("no dictionary of many pages")
	}
	checkRowsAskedTogether(t, p, d, "did")
}

func TestStringsOfPickedRowsAreAskedForTogetherWhereCodesLackStrings(t *testing.T) {
	opts := Options{Ordered: true, SortKey: sharedKey}
	var srcs []*Part
	for i := range 3 {
		srcs = append(srcs, plannedPart(t, keyedDocs(int64(i+1), 12000, 400, 1200), 2048, opts))
	}
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
	checkRowsAskedTogether(t, whole, d, "did")
}
