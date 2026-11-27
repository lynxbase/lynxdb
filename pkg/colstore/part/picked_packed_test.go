package part

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

func packedPageColumn(t testing.TB) (*Block, *ColumnRef) {
	t.Helper()
	var lines, out bytes.Buffer
	r := rand.New(rand.NewSource(17))
	const alphabet = "abcdefghijklmnopqrstuvwxyz234567"
	for row := range 16384 {
		if row%7 == 0 {
			lines.WriteString("{}\n")
			continue
		}
		lines.WriteString(`{"id":"bafyrei`)
		for range 52 {
			lines.WriteByte(alphabet[r.Intn(len(alphabet))])
		}
		lines.WriteString("\"}\n")
	}
	w := NewWriter(&out, Options{})
	writeNDJSON(t, w, lines.Bytes(), 16384, "")
	p, err := Open(out.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	b := new(Block)
	if err := p.OpenBlock(0, b); err != nil {
		t.Fatal(err)
	}
	id, _ := p.PathID("id")
	ref, ok := b.Column(id, vec.KindString)
	if !ok || ref.enc != encFlat {
		t.Fatal("expected a flat string column")
	}
	if _, _, _, ok := packedBody(b.data[ref.off:ref.off+ref.length], true); !ok {
		t.Fatal("expected packed identifiers")
	}
	return b, ref
}

func TestPickedPackedIdentifiers(t *testing.T) {
	b, ref := packedPageColumn(t)
	rows := []uint32{16383, 7, 1, 16382, 1, 3, 899, 0}
	var all, picked vec.Column
	if err := b.Decode(ref, &all); err != nil {
		t.Fatal(err)
	}
	if err := b.DecodeRows(ref, rows, &picked); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		r := int(row)
		if picked.IsValid(r) != all.IsValid(r) || picked.IsValid(r) && !bytes.Equal(picked.Bytes(r), all.Bytes(r)) {
			t.Fatalf("different identifier at row %d", r)
		}
	}
}

func BenchmarkPickedPackedIdentifiers(b *testing.B) {
	blk, ref := packedPageColumn(b)
	var col vec.Column
	rows := make([]uint32, 20)
	for i := range rows {
		rows[i] = uint32(i * 817)
	}
	b.ReportAllocs()
	for b.Loop() {
		if err := blk.DecodeRows(ref, rows, &col); err != nil {
			b.Fatal(err)
		}
	}
}
