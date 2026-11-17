package part

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

// TestIdentifiersThatRepeatGoIntoADictionary requires identifiers of one
// length of which more than half are distinct to be stored as a dictionary
// when its entries pack, and as they are otherwise, and to read back the
// same either way.
func TestIdentifiersThatRepeatGoIntoADictionary(t *testing.T) {
	const rows = 4096
	base32 := func(rng *rand.Rand) string {
		const alphabet = "abcdefghijklmnopqrstuvwxyz234567"
		b := make([]byte, 24)
		for i := range b {
			b[i] = alphabet[rng.IntN(len(alphabet))]
		}

		return "did:plc:" + string(b)
	}
	anyByte := func(rng *rand.Rand) string {
		b := make([]byte, 32)
		for i := range b {
			b[i] = byte(0x30 + rng.IntN(0x4e))
		}

		return string(b)
	}
	for name, c := range map[string]struct {
		value    func(*rand.Rand) string
		distinct int
		enc      uint8
	}{
		"six in ten distinct":             {base32, rows * 6 / 10, encDict},
		"a third distinct":                {base32, rows / 3, encDict},
		"nearly all distinct":             {base32, rows - 8, encFlat},
		"nine in ten distinct":            {base32, rows * 9 / 10, encFlat},
		"six in ten distinct, no packing": {anyByte, rows * 6 / 10, encFlat},
	} {
		t.Run(name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(3, 4))
			values := make([]string, c.distinct)
			for i := range values {
				values[i] = c.value(rng)
			}
			docs, want := make([]string, rows), make([]string, rows)
			for row := range docs {
				// Every value appears, the rest of the rows repeat some.
				want[row] = values[row%c.distinct]
				if row >= c.distinct {
					want[row] = values[rng.IntN(c.distinct)]
				}
			}
			rng.Shuffle(rows, func(i, j int) { want[i], want[j] = want[j], want[i] })
			for row := range docs {
				docs[row] = fmt.Sprintf(`{"id":%q}`, want[row])
			}
			p := buildPart(t, docs, rows, Options{})
			path, _ := p.PathID("id")
			var b Block
			if err := p.OpenBlock(0, &b); err != nil {
				t.Fatal(err)
			}
			ref, ok := b.Column(path, vec.KindString)
			if !ok {
				t.Fatal("the block has no id column")
			}
			if ref.enc != c.enc {
				t.Fatalf("stored with encoding %d, want %d", ref.enc, c.enc)
			}
			var col vec.Column
			if err := b.Decode(ref, &col); err != nil {
				t.Fatal(err)
			}
			for row := range want {
				if got := string(col.Bytes(row)); got != want[row] {
					t.Fatalf("row %d reads %q, want %q", row, got, want[row])
				}
			}
		})
	}
}
