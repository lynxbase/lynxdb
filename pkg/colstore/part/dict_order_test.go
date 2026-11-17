package part

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

// TestDictionaryEntriesAreInByteOrder requires the entries of a dictionary
// to read back in the order of their bytes: values that begin alike, that
// are the beginning of one another, that differ only after many bytes or
// in their length, and that hold zero and high bytes.
func TestDictionaryEntriesAreInByteOrder(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	sets := map[string][]string{
		"one prefix":              nil,
		"prefixes of one another": {"", "a", "ab", "abc", "abcd", "abcdefgh", "abcdefghi", "abcdefghij", "abcdefgh\u0000", "abcdefgh\u0000\u0000", "b"},
		"late difference":         nil,
		"high and low bytes":      {"k\u0000", "k\u0001", "k\u007f", "kÿ", "kĀx", "k", "kk", "k\u0000\u0000", "é", "e"},
	}
	for i := 0; i < 300; i++ {
		sets["one prefix"] = append(sets["one prefix"], "did:plc:"+strconv.FormatUint(rng.Uint64(), 36))
		sets["late difference"] = append(sets["late difference"], fmt.Sprintf("https://example.org/a/long/shared/path/%04d/%d", rng.IntN(40), rng.IntN(9)))
	}
	for name, values := range sets {
		t.Run(name, func(t *testing.T) {
			const rows = 3000
			docs := make([]string, rows)
			for row := range docs {
				v, err := json.Marshal(values[rng.IntN(len(values))])
				if err != nil {
					t.Fatal(err)
				}
				docs[row] = `{"v":` + string(v) + `}`
			}
			p := buildPart(t, docs, rows, Options{})
			path, _ := p.PathID("v")
			var b Block
			if err := p.OpenBlock(0, &b); err != nil {
				t.Fatal(err)
			}
			ref, ok := b.Column(path, vec.KindString)
			if !ok || ref.enc != encDict {
				t.Fatalf("the column is not a dictionary: %v", ok)
			}
			var col vec.Column
			if err := b.Decode(ref, &col); err != nil {
				t.Fatal(err)
			}
			if col.DictLen() < 2 {
				t.Fatalf("%d entries", col.DictLen())
			}
			for e := 1; e < col.DictLen(); e++ {
				if bytes.Compare(col.DictValue(e-1), col.DictValue(e)) >= 0 {
					t.Fatalf("entry %d %q is not above entry %d %q", e, col.DictValue(e), e-1, col.DictValue(e-1))
				}
			}
		})
	}
}
