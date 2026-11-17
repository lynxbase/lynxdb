package part

import (
	"bytes"
	"errors"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// TestDamageIsFoundAtTheFirstReadAndAfterRecheck requires a column whose
// bytes do not match their checksum to fail the first read of it, and one
// damaged after it was read to fail the first read after Recheck.
func TestDamageIsFoundAtTheFirstReadAndAfterRecheck(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, Options{})
	batch := shred.NewBatch(shred.Options{})
	for _, d := range sampleDocs(2000) {
		if err := batch.AddJSON([]byte(d), 7); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.WriteBatch(batch); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	p, err := Open(data)
	if err != nil {
		t.Fatal(err)
	}
	var b Block
	if err := p.OpenBlock(0, &b); err != nil {
		t.Fatal(err)
	}
	refs := b.Columns()
	// The largest column, and a byte in the middle of it.
	at := 0
	for i := range refs {
		if refs[i].length > refs[at].length {
			at = i
		}
	}
	ref := &refs[at]
	if ref.length < 8 {
		t.Fatalf("the largest column holds %d bytes", ref.length)
	}
	damaged := &b.data[ref.off+ref.length/2]
	var col vec.Column
	read := func() error {
		var again Block
		if err := p.OpenBlock(0, &again); err != nil {
			t.Fatal(err)
		}

		return again.Decode(&again.Columns()[at], &col)
	}
	*damaged ^= 0x10
	if err := read(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("the first read of a damaged column: %v", err)
	}
	*damaged ^= 0x10
	if err := read(); err != nil {
		t.Fatalf("a read of the column put right: %v", err)
	}
	*damaged ^= 0x10
	p.Recheck()
	if err := read(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("the first read after Recheck of a column damaged since: %v", err)
	}
	*damaged ^= 0x10
	p.Recheck()
	if err := read(); err != nil {
		t.Fatalf("a read of the column put right again: %v", err)
	}
	for i := range refs {
		var other Block
		if err := p.OpenBlock(0, &other); err != nil {
			t.Fatal(err)
		}
		if err := other.Decode(&other.Columns()[i], &col); err != nil {
			t.Fatalf("column %d of the intact part: %v", i, err)
		}
	}
}
