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

// A member at the time path must not stand in for the row times.
func TestTimedKeysFollowTheRowTimesBesideAMemberAtTheTimePath(t *testing.T) {
	const extra, before = 31, 6
	r := rand.New(rand.NewSource(1))
	batch := shred.NewBatch(shred.Options{TimeField: "ts"})
	ms := int64(1732206349000)
	var want []string
	for i := 0; i < 1000; i++ {
		ms += r.Int63n(1000)
		key := timedKey(sortable32, 5, 13, uint64(ms*1000)<<10|uint64(r.Intn(1024)))
		want = append(want, key)
		var sb strings.Builder
		fmt.Fprintf(&sb, `{"ts":%d,`, ms)
		for k := range extra {
			if k == before {
				fmt.Fprintf(&sb, `%q:%d,`, TimePath, i)
			}
			fmt.Fprintf(&sb, `"m%02d":%d,`, k, r.Intn(5))
		}
		fmt.Fprintf(&sb, `"key":%q}`, key)
		if err := batch.AddJSON([]byte(sb.String()), 0); err != nil {
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
	if err := p.OpenBlock(0, &b); err != nil {
		t.Fatal(err)
	}
	id, _ := p.PathID("key")
	ref, _ := b.Column(id, vec.KindString)
	var col vec.Column
	if err := b.Decode(ref, &col); err != nil {
		t.Fatal(err)
	}
	for i := range want {
		if string(col.Bytes(i)) != want[i] {
			t.Fatalf("row %d: %q, want %q", i, col.Bytes(i), want[i])
		}
	}
}
