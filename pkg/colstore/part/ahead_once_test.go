package part

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// TestBytesAreAskedForOnce requires a part to announce the bytes of a
// column and the pages of its token filters ahead of the first read of
// them, and not again until its pages are let go of.
func TestBytesAreAskedForOnce(t *testing.T) {
	p := buildPart(t, sampleDocs(6000), 3000, Options{})
	asked := 0
	p.SetReadAhead(func(_, _ int) { asked++ })
	var b Block
	if err := p.OpenBlock(0, &b); err != nil {
		t.Fatal(err)
	}
	refs := b.Columns()
	at := 0
	for i := range refs {
		if refs[i].length > refs[at].length {
			at = i
		}
	}
	if refs[at].length < minReadAhead {
		t.Fatalf("the largest column holds %d bytes, too few to be announced", refs[at].length)
	}
	var col vec.Column
	reads := func() int {
		before := asked
		for range 3 {
			var again Block
			if err := p.OpenBlock(0, &again); err != nil {
				t.Fatal(err)
			}
			if err := again.Decode(&again.Columns()[at], &col); err != nil {
				t.Fatal(err)
			}
		}

		return asked - before
	}
	if n := reads(); n != 1 {
		t.Fatalf("three reads of a column asked for its bytes %d times", n)
	}
	hashes := tokenfilter.AppendHashes(nil, []byte("token"))
	lookups := func() int {
		before := asked
		for range 3 {
			for i := 0; i < p.NumBlocks(); i++ {
				if _, err := p.MayContainTokens(i, hashes); err != nil {
					t.Fatal(err)
				}
				if _, err := p.MayContainTokensIn(i, refs[at].PathID, refs[at].Kind, hashes); err != nil {
					t.Fatal(err)
				}
			}
		}

		return asked - before
	}
	first := lookups()
	if first == 0 {
		t.Fatal("the lookups of a token asked for no page")
	}
	if n := lookups(); n != 0 {
		t.Fatalf("the same lookups again asked for pages %d times", n)
	}
	p.ForgetReadAhead()
	if n := reads(); n != 1 {
		t.Fatalf("reads after the pages were let go of asked %d times", n)
	}
	if n := lookups(); n != first {
		t.Fatalf("lookups after the pages were let go of asked %d times, %d at first", n, first)
	}
}
