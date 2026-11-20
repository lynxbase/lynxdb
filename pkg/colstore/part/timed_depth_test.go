package part

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// Each column is made a little before the one ahead of it, so that it follows it.
func TestTimedColumnsFollowNoDeeperThanTheyAreRead(t *testing.T) {
	const cols = 24
	r := rand.New(rand.NewSource(3))
	batch := shred.NewBatch(shred.Options{})
	want := make([][]string, cols)
	ns := int64(1732206349000000000)
	for i := 0; i < 2000; i++ {
		ns += r.Int63n(1e9)
		at := ns
		var sb strings.Builder
		sb.WriteString(`{"n":1`)
		for k := range cols {
			at -= r.Int63n(1e8)
			key := timedKey(sortable32, 5, 13, uint64(at/1e3)<<10|uint64(r.Intn(1024)))
			want[k] = append(want[k], key)
			fmt.Fprintf(&sb, `,"c%02d":%q`, k, key)
		}
		if err := batch.AddJSON([]byte(sb.String()+"}"), ns); err != nil {
			t.Fatal(err)
		}
	}
	var buf bytes.Buffer
	w := NewWriter(&buf, Options{})
	if err := w.WriteBatch(batch); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	p, err := Open(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	var b Block
	for k := range cols {
		if err := p.OpenBlock(0, &b); err != nil {
			t.Fatal(err)
		}
		id, _ := p.PathID(fmt.Sprintf("c%02d", k))
		ref, _ := b.Column(id, vec.KindString)
		var col vec.Column
		if err := b.Decode(ref, &col); err != nil {
			t.Errorf("c%02d: %v", k, err)
			continue
		}
		for i := range want[k] {
			if string(col.Bytes(i)) != want[k][i] {
				t.Fatalf("c%02d row %d: %q, want %q", k, i, col.Bytes(i), want[k][i])
			}
		}
	}
}
