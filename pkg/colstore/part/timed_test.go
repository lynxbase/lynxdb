package part

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/internal/benchdata"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// timedKey spells v in base 2^radixBits of alphabet in digits digits.
func timedKey(alphabet string, radixBits uint, digits int, v uint64) string {
	out := make([]byte, digits)
	for i := digits - 1; i >= 0; i-- {
		out[i] = alphabet[v&(1<<radixBits-1)]
		v >>= radixBits
	}

	return string(out)
}

// keyShapes return, for row i at microsecond us, a key that holds the time
// in its leading bits as the families of time-ordered keys do.
var keyShapes = map[string]func(r *rand.Rand, i int, us int64) string{
	"sortable base32 of microseconds and a clock": func(r *rand.Rand, i int, us int64) string {
		return timedKey("234567abcdefghijklmnopqrstuvwxyz", 5, 13, uint64(us-r.Int63n(1e6))<<10|uint64(r.Intn(32)))
	},
	"crockford base32 of milliseconds and noise": func(r *rand.Rand, i int, us int64) string {
		const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
		return timedKey(crockford, 5, 10, uint64(us/1000)) + timedKey(crockford, 5, 12, r.Uint64()) + timedKey(crockford, 5, 4, r.Uint64())
	},
	"hex of milliseconds then random": func(r *rand.Rand, i int, us int64) string {
		var noise [10]byte
		r.Read(noise[:])
		return fmt.Sprintf("%012x", us/1000) + hex.EncodeToString(noise[:])
	},
	"decimal of milliseconds shifted over a sequence": func(r *rand.Rand, i int, us int64) string {
		return strconv.FormatUint(uint64(us/1000-1288834974657)<<22|uint64(i&4095), 10)
	},
	"prefixed milliseconds and a counter": func(r *rand.Rand, i int, us int64) string {
		return "req-" + timedKey("0123456789abcdef", 4, 11, uint64(us/1e3)) + timedKey("0123456789abcdef", 4, 3, uint64(i))
	},
}

// keyDocs returns rows documents some minutes apart whose key field takes
// the shape, a few of them far from their time, of other lengths or of
// other bytes, and whose sparse field holds the shape in one row of three.
func keyDocs(shape func(r *rand.Rand, i int, us int64) string, rows int, r *rand.Rand) []string {
	docs := make([]string, 0, rows)
	us := int64(1732206349000000)
	for i := 0; i < rows; i++ {
		us += r.Int63n(300000)
		key := shape(r, i, us)
		switch r.Intn(400) {
		case 0:
			key = "self"
		case 1:
			key = shape(r, i, us-r.Int63n(1e13))
		case 2:
			key = strings.Repeat("~", len(key))
		}
		doc := fmt.Sprintf(`{"us":%d,"key":%q`, us, key)
		if i%3 == 0 {
			doc += fmt.Sprintf(`,"sparse":%q`, shape(r, i, us))
		}
		docs = append(docs, doc+"}")
	}

	return docs
}

func TestTimedKeysRoundTripInTheirShapes(t *testing.T) {
	r := rand.New(rand.NewSource(5))
	for name, shape := range keyShapes {
		docs := keyDocs(shape, 9000, r)
		data := []byte(strings.Join(docs, "\n") + "\n")
		p, err := Open(encodeNDJSON(t, data, 4096, Options{}, "us"))
		if err != nil {
			t.Fatal(err)
		}
		got, _ := readAll(t, p)
		for i := range docs {
			if !sameJSON(t, got[i], docs[i]) {
				t.Fatalf("%s: row %d\n got  %s\n want %s", name, i, got[i], docs[i])
			}
		}
		var u Usage
		if err := p.AddUsage(&u, anyPath); err != nil {
			t.Fatal(err)
		}
		// A block takes the timed form when it is the smaller one.
		for _, path := range []string{"key", "sparse"} {
			c := u.Columns[ColumnKey{path, vec.KindString}]
			if c.Encodings["timed"] == nil {
				t.Errorf("%s: %s is timed in none of %d blocks", name, path, c.Blocks)
			}
			if c.Sections["unparsed"] != nil {
				t.Errorf("%s: %s has unparsed sections", name, path)
			}
		}
		// Values read by value and searched for equal the stored ones.
		checkTimedReads(t, p, docs)
	}
}

// checkTimedReads requires every value of the key columns of p, read by
// value and looked for, to be found in the rows that hold it.
func checkTimedReads(t *testing.T, p *Part, docs []string) {
	t.Helper()
	var b Block
	for i := 0; i < p.NumBlocks(); i++ {
		if err := p.OpenBlock(i, &b); err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{"sparse", "key"} {
			id, _ := p.PathID(path)
			ref, ok := b.Column(id, vec.KindString)
			if !ok {
				t.Fatalf("block %d has no %s", i, path)
			}
			var byValue, byRow vec.Column
			if err := b.DecodeValues(ref, &byValue); err != nil {
				t.Fatal(err)
			}
			if err := b.Decode(ref, &byRow); err != nil {
				t.Fatal(err)
			}
			if size := b.dataSize(ref); size != 0 && int(size) != len(byValue.Data) {
				t.Fatalf("block %d %s: %d bytes, the chunk says %d", i, path, len(byValue.Data), size)
			}
			value := 0
			for row := 0; row < b.Rows; row++ {
				if !byRow.IsValid(row) {
					continue
				}
				if !bytes.Equal(byRow.Bytes(row), byValue.Bytes(value)) {
					t.Fatalf("block %d %s row %d: %q by row, %q by value", i, path, row, byRow.Bytes(row), byValue.Bytes(value))
				}
				value++
			}
		}
	}
}

// canonical returns doc encoded with its members in order.
func canonical(t *testing.T, doc string) string {
	t.Helper()
	var v any
	if err := json.Unmarshal([]byte(doc), &v); err != nil {
		t.Fatal(err)
	}
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}

	return string(out)
}

// The token filters, which hold the strings' tokens, find the timed values.
func TestTimedKeysPassTheirTokenFilters(t *testing.T) {
	r := rand.New(rand.NewSource(6))
	docs := keyDocs(keyShapes["sortable base32 of microseconds and a clock"], 5000, r)
	p, err := Open(encodeNDJSON(t, []byte(strings.Join(docs, "\n")+"\n"), 4096, Options{}, "us"))
	if err != nil {
		t.Fatal(err)
	}
	columnAnswers(t, p, filterColumnBytes, r)
}

// Values with no time in them stay as they were.
func TestUntimedKeysKeepTheirEncoding(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	random := func(r *rand.Rand, i int, us int64) string {
		return timedKey("abcdefghijklmnopqrstuvwxyz234567", 5, 12, r.Uint64())
	}
	p, err := Open(encodeNDJSON(t, []byte(strings.Join(keyDocs(random, 5000, r), "\n")+"\n"), 4096, Options{}, "us"))
	if err != nil {
		t.Fatal(err)
	}
	var u Usage
	if err := p.AddUsage(&u, anyPath); err != nil {
		t.Fatal(err)
	}
	if timed := u.Columns[ColumnKey{"key", vec.KindString}].Encodings["timed"]; timed != nil {
		t.Fatalf("random keys were timed in %d blocks", timed.Blocks)
	}
}

// A timed column may follow another column of its block whatever form that
// one is stored in: the generator's record keys follow their revisions,
// which late deliveries leave far from the row times, and the times of
// creation follow the record keys.
func TestTimedColumnsFollowEachOther(t *testing.T) {
	data := benchdata.New(2, 3000).Append(nil, 30000)
	key := []string{"kind", "commit.operation", "commit.collection", "did"}
	batch := shred.NewBatch(shred.Options{TimeField: "time_us"})
	if errs, _ := batch.AddNDJSON(data, 0, shred.Intake{}, nil); len(errs) != 0 {
		t.Fatal(errs[0].Err)
	}
	batch.SortBy(key)
	var buf bytes.Buffer
	w := NewWriter(&buf, Options{Ordered: true, SortKey: key})
	block := shred.NewBatch(shred.Options{})
	for from := 0; from < batch.Rows; from += 8192 {
		block.Reset()
		batch.CopyRows(block, from, min(from+8192, batch.Rows))
		if err := w.WriteBatch(block); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	p, err := Open(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	got, _ := readAll(t, p)
	want := map[string]int{}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		want[canonical(t, line)]++
	}
	for _, doc := range got {
		if key := canonical(t, doc); want[key] > 0 {
			want[key]--
		} else {
			t.Fatalf("row %s is not one of the input", doc)
		}
	}
	var u Usage
	if err := p.AddUsage(&u, anyPath); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"commit.rkey", "commit.record.createdAt"} {
		c := u.Columns[ColumnKey{path, vec.KindString}]
		t.Logf("%s: %.2f bytes a value, encodings %v", path, float64(c.ChunkBytes)/float64(c.Values), c.Encodings["timed"])
		if c.Encodings["timed"] == nil {
			t.Errorf("%s is never timed", path)
		}
	}
}
