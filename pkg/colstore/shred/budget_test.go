package shred

import (
	"fmt"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

func TestBatchSpendsItsColumnBudgetOnThePathsOfItsRows(t *testing.T) {
	// Every round of rows brings members no earlier round had; over all
	// rounds there are far more paths than the budget, in any one far fewer.
	b := NewBatch(Options{MaxColumns: 64})
	for round := range 40 {
		for row := range 50 {
			doc := fmt.Sprintf(`{"n":%d,"svc":"s%d","k%d_%d":"v","k%d_%d":%d}`, row, row%3, round, row%10, round, 10+row%10, row)
			if err := b.AddJSON([]byte(doc), 0); err != nil {
				t.Fatal(err)
			}
		}
		if rest := b.Lookup(RestPath, vec.KindVariant); rest != nil && rest.Count != 0 {
			t.Fatalf("round %d: %d rows kept members without a column of their own", round, rest.Count)
		}
		for i := range 20 {
			kind := vec.KindString
			if i >= 10 {
				kind = vec.KindInt
			}
			if c := b.Lookup(fmt.Sprintf("k%d_%d", round, i), kind); c == nil || c.Count != 5 {
				t.Fatalf("round %d: member %d has no column with its 5 values", round, i)
			}
		}
		b.Reset()
	}
}

func TestBatchKeepsItsPathsWhileItsRowsUseThem(t *testing.T) {
	b := NewBatch(Options{MaxColumns: 16})
	doc := []byte(`{"a":1,"b":2,"c":3,"d":4,"e":5,"f":6,"g":7,"h":8,"i":9,"j":10}`)
	if err := b.AddJSON(doc, 0); err != nil {
		t.Fatal(err)
	}
	first := b.Lookup("a", vec.KindInt)
	for range 5 {
		b.Reset()
		if err := b.AddJSON(doc, 0); err != nil {
			t.Fatal(err)
		}
		if b.Lookup("a", vec.KindInt) != first {
			t.Fatal("a batch whose rows use every path forgot its columns")
		}
	}
	// Rows of more paths than the budget still keep the rest apart.
	if err := b.AddJSON([]byte(`{"a":1,"l":1,"m":1,"n":1,"o":1,"p":1,"q":1,"r":1,"s":1,"t":1}`), 0); err != nil {
		t.Fatal(err)
	}
	if rest := b.Lookup(RestPath, vec.KindVariant); rest == nil || rest.Count != 1 {
		t.Fatal("members beyond the budget of one batch were not kept apart")
	}
}
