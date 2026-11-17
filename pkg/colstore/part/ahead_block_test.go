package part

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

// TestAheadJoinsNearChunks requires Block.Ahead to ask for the chunks of the
// columns it is given in one request per run of near chunks, covering every
// byte of them, and a later read of those columns to ask for nothing.
func TestAheadJoinsNearChunks(t *testing.T) {
	p := buildPart(t, sampleDocs(6000), 3000, Options{})
	type span struct{ off, n int }
	var asked []span
	p.SetReadAhead(func(off, n int) { asked = append(asked, span{off, n}) })
	var b Block
	if err := p.OpenBlock(0, &b); err != nil {
		t.Fatal(err)
	}
	refs := b.Columns()
	var ids []uint32
	for i := range refs {
		if len(ids) == 0 || ids[len(ids)-1] != refs[i].PathID {
			ids = append(ids, refs[i].PathID)
		}
	}
	asked = asked[:0]
	b.Ahead(ids)
	if len(asked) == 0 || len(asked) >= len(refs) {
		t.Fatalf("the chunks of %d columns took %d requests", len(refs), len(asked))
	}
	for i := range refs {
		from, to := b.start+int(refs[i].off), b.start+int(refs[i].off+refs[i].length)
		covered := false
		for _, s := range asked {
			covered = covered || (s.off <= from && to <= s.off+s.n)
		}
		if refs[i].length > 0 && !covered {
			t.Fatalf("column %d, bytes %d to %d, was not asked for", i, from, to)
		}
	}
	asked = asked[:0]
	b.Ahead(ids)
	var col vec.Column
	for i := range refs {
		if err := b.Decode(&refs[i], &col); err != nil {
			t.Fatal(err)
		}
	}
	if len(asked) != 0 {
		t.Fatalf("columns asked for already took %d requests more", len(asked))
	}
}
