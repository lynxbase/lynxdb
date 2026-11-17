package part

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// countedDocs returns n documents whose level, zone and svc hold few
// strings, zone one value over long stretches and svc in two rows of three;
// id holds a string of its own per document, mixed strings and numbers, and
// nullable a string or a null.
func countedDocs(rng *rand.Rand, n int) []string {
	docs := make([]string, n)
	for i := range docs {
		doc := fmt.Sprintf(`{"n":%d,"level":"%s"`, i, []string{"debug", "info", "info", "warn", "error"}[rng.IntN(5)])
		doc += fmt.Sprintf(`,"zone":"z%d"`, i/2500)
		if i%3 != 0 {
			doc += fmt.Sprintf(`,"svc":"svc-%02d"`, rng.IntN(1+rng.IntN(60)))
		}
		doc += fmt.Sprintf(`,"id":"id-%06d-%d"`, i, rng.IntN(1000))
		switch i % 4 {
		case 0:
			doc += `,"mixed":"text"`
		case 1:
			doc += fmt.Sprintf(`,"mixed":%d`, i)
		}
		if i%5 == 0 {
			doc += `,"nullable":"x"`
		} else if i%977 == 0 {
			doc += `,"nullable":null`
		}
		docs[i] = doc + "}"
	}

	return docs
}

// timedPart writes docs in blocks of blockRows rows to a part ordered by
// time, the first at time first and each a step after the one before.
func timedPart(t *testing.T, docs []string, first, step int64, blockRows int) *Part {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf, Options{Ordered: true})
	batch := shred.NewBatch(shred.Options{})
	for i, d := range docs {
		if err := batch.AddJSON([]byte(d), first+int64(i)*step); err != nil {
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
		t.Fatal(err)
	}

	return p
}

// requireCounts fails unless p knows the rows of every value of each path
// of counted as its decoded columns give them, and claims none for the
// paths of uncounted.
func requireCounts(t *testing.T, p *Part, counted, uncounted []string) {
	t.Helper()
	want := map[string]map[string]uint64{}
	var b Block
	var col vec.Column
	for i := 0; i < p.NumBlocks(); i++ {
		if err := p.OpenBlock(i, &b); err != nil {
			t.Fatal(err)
		}
		refs := b.Columns()
		for j := range refs {
			if refs[j].Kind != vec.KindString {
				continue
			}
			if err := b.Decode(&refs[j], &col); err != nil {
				t.Fatal(err)
			}
			name := p.Paths()[refs[j].PathID]
			if want[name] == nil {
				want[name] = map[string]uint64{}
			}
			for row := 0; row < col.Rows; row++ {
				if col.IsValid(row) {
					want[name][string(col.Bytes(row))]++
				}
			}
		}
	}
	for _, name := range counted {
		id, _ := p.PathID(name)
		c, ok := p.Counts(id)
		if !ok {
			t.Fatalf("%s: the part does not count its values", name)
		}
		if len(c.Values) != len(want[name]) {
			t.Fatalf("%s: %d values, want %d", name, len(c.Values), len(want[name]))
		}
		held := uint64(0)
		for i, v := range c.Values {
			if i > 0 && string(c.Values[i-1]) >= string(v) {
				t.Fatalf("%s: values out of order at %q", name, v)
			}
			if c.Rows[i] != want[name][string(v)] {
				t.Fatalf("%s: %q has %d rows, want %d", name, v, c.Rows[i], want[name][string(v)])
			}
			held += c.Rows[i]
		}
		if held != c.Held {
			t.Fatalf("%s: %d rows held, the values add up to %d", name, c.Held, held)
		}
	}
	for _, name := range uncounted {
		id, ok := p.PathID(name)
		if _, counted := p.Counts(id); ok && counted {
			t.Fatalf("%s: the part claims to count its values", name)
		}
	}
}

// TestPartCountsTheRowsOfTheValuesOfAPath requires a part to know the rows
// of every value of a path of few string values across its blocks, as many
// as its columns give when decoded, and not to claim it for a path of many
// values, of values of several kinds, or with a null.
func TestPartCountsTheRowsOfTheValuesOfAPath(t *testing.T) {
	p := buildPart(t, countedDocs(rand.New(rand.NewPCG(41, 42)), 9000), 700, Options{})
	requireCounts(t, p, []string{"level", "zone", "svc"}, []string{"id", "mixed", "nullable", "n", "no.such.path"})
}

// TestMergedPartCountsTheRowsOfTheValuesOfAPath requires a merge to keep
// the rows of the values of a path, where it copies blocks as they are
// stored and where it writes their rows anew, to count a path that one
// source lacks, and to stop counting one that a source does not count.
func TestMergedPartCountsTheRowsOfTheValuesOfAPath(t *testing.T) {
	rng := rand.New(rand.NewPCG(43, 44))
	const rows, blockRows = 6000, 500
	first := timedPart(t, countedDocs(rng, rows), 0, 10, blockRows)
	after := timedPart(t, countedDocs(rng, rows), 10*rows, 10, blockRows)
	among := timedPart(t, countedDocs(rng, rows), 5, 10, blockRows)
	bare := make([]string, rows)
	for i := range bare {
		bare[i] = fmt.Sprintf(`{"n":%d,"level":"info","svc":%d}`, i, i)
	}
	lacking := timedPart(t, bare, 20*rows, 10, blockRows)
	for name, c := range map[string]struct {
		srcs      []*Part
		copied    bool
		counted   []string
		uncounted []string
	}{
		"blocks copied":  {[]*Part{first, after}, true, []string{"level", "zone", "svc"}, []string{"id", "mixed", "nullable"}},
		"rows written":   {[]*Part{first, among}, false, []string{"level", "zone", "svc"}, []string{"id", "mixed", "nullable"}},
		"a path lacking": {[]*Part{first, lacking}, true, []string{"level", "zone"}, []string{"svc", "id"}},
	} {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf, Options{Ordered: true})
			if err := Merge(w, c.srcs, blockRows); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if (w.copied != 0) != c.copied {
				t.Fatalf("%d blocks copied", w.copied)
			}
			p, err := Open(buf.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			if p.Rows != 2*rows {
				t.Fatalf("%d rows", p.Rows)
			}
			requireCounts(t, p, c.counted, c.uncounted)
		})
	}
}

// TestPartCountsValuesThatABlockStoresOneByOne requires a part to count the
// values of a path through a block too short for a dictionary, which stores
// them one by one, and a merge that copies such a block to count them too.
func TestPartCountsValuesThatABlockStoresOneByOne(t *testing.T) {
	const blockRows = 700
	docs := countedDocs(rand.New(rand.NewPCG(45, 46)), 2*blockRows)
	for i, level := range []string{"debug", "info", "warn"} {
		docs = append(docs, fmt.Sprintf(`{"n":%d,"level":%q,"zone":"tail-%d"}`, i, level, i))
	}
	flat := func(p *Part) {
		t.Helper()
		var b Block
		if err := p.OpenBlock(p.NumBlocks()-1, &b); err != nil {
			t.Fatal(err)
		}
		id, _ := p.PathID("level")
		for _, ref := range b.Columns() {
			if ref.PathID == id && ref.enc != encFlat {
				t.Fatalf("the last block stores level with encoding %d; the test needs it value by value", ref.enc)
			}
		}
	}
	first := timedPart(t, docs, 0, 10, blockRows)
	flat(first)
	requireCounts(t, first, []string{"level", "zone", "svc"}, []string{"id"})

	after := timedPart(t, docs, int64(20*len(docs)), 10, blockRows)
	var buf bytes.Buffer
	w := NewWriter(&buf, Options{Ordered: true})
	if err := Merge(w, []*Part{first, after}, blockRows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if w.copied == 0 {
		t.Fatal("the merge copied no block")
	}
	merged, err := Open(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	requireCounts(t, merged, []string{"level", "zone", "svc"}, []string{"id"})
}
