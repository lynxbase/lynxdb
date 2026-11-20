package part

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// A row whose time is an integer member at the time path, wherever that
// member lies among many others, reads back the row times at that path, not
// the member that references them.
func TestRowTimesComeBeforeTheTimeMemberTheyWereTakenFrom(t *testing.T) {
	for members := 10; members <= 40; members++ {
		for at := 0; at < members; at++ {
			batch := shred.NewBatch(shred.Options{TimeFields: []string{"_time", "ts"}})
			for i := 0; i < 32; i++ {
				doc := "{"
				for k := 0; k < members; k++ {
					if k > 0 {
						doc += ","
					}
					if k == at {
						doc += fmt.Sprintf(`"_time":%d`, 1732206349000+int64(i)*1000)
					} else {
						doc += fmt.Sprintf(`"m%02d":%d`, k, (i+k)%7)
					}
				}
				if err := batch.AddJSON([]byte(doc+"}"), 0); err != nil {
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
			ref, _ := b.Column(0, vec.KindInt)
			var col vec.Column
			if err := b.Decode(ref, &col); err != nil {
				t.Fatalf("%d members, _time at %d: the row times: %v", members, at, err)
			}
			for i := 0; i < 32; i++ {
				if want := (1732206349000 + int64(i)*1000) * 1e6; col.Int(i) != want {
					t.Fatalf("%d members, _time at %d: row %d at %d, want %d", members, at, i, col.Int(i), want)
				}
			}
		}
	}
}
