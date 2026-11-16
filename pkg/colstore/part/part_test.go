package part

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// buildPart shreds docs into blocks of blockRows rows and returns the part.
func buildPart(t testing.TB, docs []string, blockRows int, opts Options) *Part {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf, opts)
	batch := shred.NewBatch(shred.Options{})
	for i, d := range docs {
		if err := batch.AddJSON([]byte(d), int64(1000+i)); err != nil {
			t.Fatalf("doc %d: %v", i, err)
		}
		if batch.Rows == blockRows || i == len(docs)-1 {
			if err := w.WriteBatch(batch); err != nil {
				t.Fatal(err)
			}
			batch.Reset()
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	p, err := Open(buf.Bytes())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	return p
}

// readAll decodes every column of every block and renders each row.
func readAll(t testing.TB, p *Part) (docs []string, times []int64) {
	t.Helper()
	var b Block
	var r Renderer
	for i := 0; i < p.NumBlocks(); i++ {
		if err := p.OpenBlock(i, &b); err != nil {
			t.Fatalf("OpenBlock(%d): %v", i, err)
		}
		var paths []string
		var cols []*vec.Column
		refs := b.Columns()
		for j := range refs {
			col := new(vec.Column)
			if err := b.Decode(&refs[j], col); err != nil {
				t.Fatalf("block %d column %s/%s: %v", i, p.Paths()[refs[j].PathID], refs[j].Kind, err)
			}
			if refs[j].PathID == 0 {
				for row := 0; row < b.Rows; row++ {
					times = append(times, col.Int(row))
				}

				continue
			}
			paths = append(paths, p.Paths()[refs[j].PathID])
			cols = append(cols, col)
		}
		r.Reset(paths, cols)
		for row := 0; row < b.Rows; row++ {
			docs = append(docs, string(r.AppendRow(nil, row)))
		}
	}

	return docs, times
}

func sameJSON(t testing.TB, got, want string) bool {
	t.Helper()
	var g, w any
	if err := json.Unmarshal([]byte(got), &g); err != nil {
		t.Fatalf("rendered row is not JSON: %v\n%s", err, got)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatal(err)
	}

	return reflect.DeepEqual(g, w)
}

func sampleDocs(n int) []string {
	r := rand.New(rand.NewSource(11))
	kinds := []string{"commit", "identity", "account"}
	docs := []string{
		`{}`,
		`{"only":"here","deep":{"a":{"b":{"c":[1,2,{"x":null}]}}},"empty":{},"list":[]}`,
		`{"esc":"tab\t quote\" back\\ nl\n é 😀","neg":-9223372036854775808,"f":-0.5e-7,"t":true,"n":null}`,
	}
	for i := 0; i < n; i++ {
		var sb strings.Builder
		fmt.Fprintf(&sb, `{"did":"did:plc:%016x","seq":%d,"kind":"%s","region":"eu"`, r.Uint64(), 1_000_000+i*3, kinds[r.Intn(len(kinds))])
		// A few hundred values of varying length exercise wide dictionaries.
		fmt.Fprintf(&sb, `,"host":"node-%s","shard":"s%d"`, strings.Repeat("x", r.Intn(40)), r.Intn(400)*r.Intn(400))
		if r.Intn(4) != 0 {
			fmt.Fprintf(&sb, `,"commit":{"collection":"app.bsky.feed.%s","rev":"%x","size":%d,"ok":%v}`,
				[]string{"post", "like", "repost"}[r.Intn(3)], r.Uint32(), r.Intn(900), r.Intn(10) != 0)
		}
		if r.Intn(20) == 0 {
			fmt.Fprintf(&sb, `,"rare":{"note":"Hello World %d","score":%g,"tags":["a","B%d"]}`, i, r.Float64()*100, i%3)
		}
		if r.Intn(7) == 0 {
			sb.WriteString(`,"mixed":"text"`)
		} else if r.Intn(7) == 0 {
			fmt.Fprintf(&sb, `,"mixed":%d`, i)
		}
		sb.WriteString("}")
		docs = append(docs, sb.String())
	}

	return docs
}

func TestPartRoundTripsDocuments(t *testing.T) {
	docs := sampleDocs(3000)
	for _, opts := range []Options{{}, {Level: LevelCompact}, {NoTokenFilter: true}} {
		for _, blockRows := range []int{1, 7, 1000, 1 << 16} {
			if blockRows == 1 && opts.Level != LevelFast {
				continue
			}
			p := buildPart(t, docs, blockRows, opts)
			got, times := readAll(t, p)
			if len(got) != len(docs) || p.Rows != uint64(len(docs)) {
				t.Fatalf("rows: got %d, part says %d, want %d", len(got), p.Rows, len(docs))
			}
			for i := range docs {
				if !sameJSON(t, got[i], docs[i]) {
					t.Fatalf("opts %+v blockRows %d row %d\n got  %s\n want %s", opts, blockRows, i, got[i], docs[i])
				}
				if times[i] != int64(1000+i) {
					t.Fatalf("row %d time %d", i, times[i])
				}
			}
			if p.MinTime != 1000 || p.MaxTime != int64(1000+len(docs)-1) {
				t.Fatalf("time bounds %d..%d", p.MinTime, p.MaxTime)
			}
		}
	}
}

func TestBlockPicksEncodingsAndStatistics(t *testing.T) {
	p := buildPart(t, sampleDocs(3000)[3:], 1<<16, Options{})
	var b Block
	if err := p.OpenBlock(0, &b); err != nil {
		t.Fatal(err)
	}
	col := func(path string, kind vec.Kind) (*ColumnRef, *vec.Column) {
		t.Helper()
		id, ok := p.PathID(path)
		if !ok {
			t.Fatalf("no path %s", path)
		}
		ref, ok := b.Column(id, kind)
		if !ok {
			t.Fatalf("no %s column for %s", kind, path)
		}
		c := new(vec.Column)
		if err := b.Decode(ref, c); err != nil {
			t.Fatal(err)
		}

		return ref, c
	}

	if ref, c := col("region", vec.KindString); !ref.Const() || string(ref.ConstBytes()) != "eu" || c.Layout != vec.Const {
		t.Errorf("region is not a const column")
	}
	if _, c := col("kind", vec.KindString); c.Layout != vec.Dict || c.DictLen() != 3 || len(c.Codes8) != b.Rows ||
		string(c.DictValue(0)) != "account" || string(c.DictValue(2)) != "identity" {
		t.Errorf("kind is not a sorted 3-entry dictionary: layout %d, %d entries", c.Layout, c.DictLen())
	}
	if _, c := col("did", vec.KindString); c.Layout != vec.Flat {
		t.Errorf("unique did values were dictionary encoded")
	}
	if ref, c := col("seq", vec.KindInt); ref.MinInt != 1_000_000 || ref.MaxInt != int64(1_000_000+2999*3) || c.Valid != nil {
		t.Errorf("seq stats %d..%d", ref.MinInt, ref.MaxInt)
	}
	if ref, c := col("commit.size", vec.KindInt); ref.Count >= b.Rows || c.Valid == nil || c.Valid.Count() != ref.Count {
		t.Errorf("commit.size presence: count %d of %d rows", ref.Count, b.Rows)
	}
	if ref, _ := col("rare.score", vec.KindFloat); ref.MinFloat < 0 || ref.MaxFloat > 100 || ref.MinFloat > ref.MaxFloat {
		t.Errorf("rare.score stats %g..%g", ref.MinFloat, ref.MaxFloat)
	}
	id, _ := p.PathID("mixed")
	if kinds := b.Path(id); len(kinds) != 2 || kinds[0].Kind != vec.KindInt || kinds[1].Kind != vec.KindString {
		t.Errorf("mixed path has %d columns", len(kinds))
	}
}

func TestTokenFilterCoversEveryStringToken(t *testing.T) {
	docs := sampleDocs(2000)
	p := buildPart(t, docs, 500, Options{})
	var b Block
	row := 0
	for i := 0; i < p.NumBlocks(); i++ {
		if err := p.OpenBlock(i, &b); err != nil {
			t.Fatal(err)
		}
		for r := 0; r < b.Rows; r, row = r+1, row+1 {
			var v any
			if err := json.Unmarshal([]byte(docs[row]), &v); err != nil {
				t.Fatal(err)
			}
			for _, s := range stringsIn(v, nil) {
				for _, h := range tokenfilter.AppendHashes(nil, []byte(s)) {
					if !b.MayContainToken(h) {
						t.Fatalf("block %d misses a token of %q", i, s)
					}
				}
			}
		}
		misses := 0
		for k := 0; k < 1000; k++ {
			if !b.MayContainToken(tokenfilter.AppendHashes(nil, []byte(fmt.Sprintf("absent%dtoken", k)))[0]) {
				misses++
			}
		}
		if misses < 950 {
			t.Fatalf("block %d: filter excluded only %d of 1000 absent tokens", i, misses)
		}
	}
}

func stringsIn(v any, dst []string) []string {
	switch x := v.(type) {
	case string:
		dst = append(dst, x)
	case []any:
		for _, e := range x {
			dst = stringsIn(e, dst)
		}
	case map[string]any:
		for _, e := range x {
			dst = stringsIn(e, dst)
		}
	}

	return dst
}

func TestCorruptPartsFailWithoutPanicking(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf, Options{})
	batch := shred.NewBatch(shred.Options{})
	for _, d := range sampleDocs(400) {
		if err := batch.AddJSON([]byte(d), 5); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.WriteBatch(batch); err != nil || w.Close() != nil {
		t.Fatal("write failed")
	}
	good := buf.Bytes()

	decodeAll := func(data []byte) (failed bool) {
		p, err := Open(data)
		if err != nil {
			return true
		}
		var b Block
		for i := 0; i < p.NumBlocks(); i++ {
			if err := p.OpenBlock(i, &b); err != nil {
				return true
			}
			refs := b.Columns()
			for j := range refs {
				if err := b.Decode(&refs[j], new(vec.Column)); err != nil {
					failed = true
				}
			}
		}

		return failed
	}
	if decodeAll(good) {
		t.Fatal("intact part failed to decode")
	}
	for cut := 0; cut < len(good); cut += 1 + len(good)/200 {
		if !decodeAll(good[:cut]) {
			t.Fatalf("part truncated to %d bytes decoded cleanly", cut)
		}
	}
	r := rand.New(rand.NewSource(5))
	for trial := 0; trial < 3000; trial++ {
		bad := append([]byte(nil), good...)
		at := r.Intn(len(bad))
		bad[at] ^= byte(1 + r.Intn(255))
		if !decodeAll(bad) {
			t.Fatalf("flipping byte %d of %d went unnoticed", at, len(bad))
		}
	}
}
