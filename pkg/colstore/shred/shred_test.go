package shred

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/lynxbase/lynxdb/pkg/colstore/variant"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// rowsOf rebuilds every row of b as path -> value, with variants decoded.
func rowsOf(t testing.TB, b *Batch) []map[string]any {
	t.Helper()
	rows := make([]map[string]any, b.Rows)
	for i := range rows {
		rows[i] = map[string]any{}
	}
	for _, c := range b.Columns() {
		for i := 0; i < c.Count; i++ {
			var v any
			switch c.Kind {
			case vec.KindNull:
				v = nil
			case vec.KindBool:
				v = c.Bools[i] == 1
			case vec.KindInt:
				v = c.Ints[i]
			case vec.KindFloat:
				v = c.Floats[i]
			case vec.KindString:
				v = string(c.Bytes(i))
			case vec.KindVariant:
				text, err := variant.AppendJSON(nil, c.Bytes(i))
				if err != nil {
					t.Fatalf("column %s value %d: %v", c.Path, i, err)
				}
				v = decodeOracle(t, text)
			}
			row := rows[c.Row(i)]
			if _, dup := row[c.Path]; dup {
				t.Fatalf("row %d has two values for %s", c.Row(i), c.Path)
			}
			row[c.Path] = v
		}
	}

	return rows
}

func decodeOracle(t testing.TB, doc []byte) any {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(doc))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("oracle rejects %s: %v", doc, err)
	}

	return normalize(v)
}

// normalize maps json.Number to the int64 or float64 the shredder stores.
func normalize(v any) any {
	switch x := v.(type) {
	case json.Number:
		s := x.String()
		if !strings.ContainsAny(s, ".eE") {
			if i, err := strconv.ParseInt(s, 10, 64); err == nil {
				return i
			}
		}
		f, _ := strconv.ParseFloat(s, 64)

		return f
	case []any:
		for i := range x {
			x[i] = normalize(x[i])
		}
	case map[string]any:
		for k := range x {
			x[k] = normalize(x[k])
		}
	}

	return v
}

// flatten is the reference shredding of a decoded document.
func flatten(dst map[string]any, prefix string, v any, top bool) {
	obj, ok := v.(map[string]any)
	if !ok || (len(obj) == 0 && !top) {
		dst[prefix] = v

		return
	}
	for k, x := range obj {
		p := k
		if !top {
			p = prefix + "." + k
		}
		flatten(dst, p, x, false)
	}
}

func checkAgainstOracle(t *testing.T, docs ...string) {
	t.Helper()
	b := NewBatch(Options{})
	for _, d := range docs {
		if err := b.AddJSON([]byte(d), 0); err != nil {
			t.Fatalf("AddJSON(%s): %v", d, err)
		}
	}
	got := rowsOf(t, b)
	for i, d := range docs {
		want := map[string]any{}
		flatten(want, "", decodeOracle(t, []byte(d)), true)
		if !reflect.DeepEqual(got[i], want) {
			t.Fatalf("doc %d %s\n got  %#v\n want %#v", i, d, got[i], want)
		}
	}
}

func TestShredMatchesReferenceDecoder(t *testing.T) {
	checkAgainstOracle(t,
		`{}`,
		`{"a":1,"b":"x","c":true,"d":false,"e":null,"f":1.5}`,
		`{"a":{"b":{"c":"deep"}},"z":-0}`,
		`{"a":{},"b":[],"c":[1,"two",3.5,null,true,{"k":[{}]}]}`,
		` { "spaced" : [ 1 , 2 ] , "k" : { "x" : "y" } } `,
		`{"esc":"line\nbreak \"quoted\" back\\slash \/ é 😀 \ud800 tab\t"}`,
		`{"key\n":"escaped key"}`,
		`{"big":9223372036854775807,"small":-9223372036854775808,"over":9223372036854775808,"huge":1e400}`,
		`{"n":0.1,"m":-1E-3,"p":1e3,"q":12345678901234567890123}`,
		`{"dup":1,"dup":"last wins","dup":2}`,
		`{"long":"`+strings.Repeat("abcdefghij", 20)+`","longesc":"`+strings.Repeat("abcdefghij", 5)+`\n"}`,
		`{"a":1}`,
		`{"a":"now a string"}`,
		`{"a":{"nested":true}}`,
		`{"unicode":"привет мир ✓","empty":""}`,
	)
}

func TestShredSparseAndDenseColumns(t *testing.T) {
	b := NewBatch(Options{})
	docs := []string{
		`{"always":1,"late":null}`,
		`{"always":2}`,
		`{"always":3,"rare":"x"}`,
		`{"always":4,"late":"y"}`,
	}
	for _, d := range docs {
		if err := b.AddJSON([]byte(d), 0); err != nil {
			t.Fatal(err)
		}
	}
	byKey := map[string]*Column{}
	for _, c := range b.Columns() {
		byKey[c.Path+"/"+c.Kind.String()] = c
	}
	if c := byKey["always/int"]; c.Sparse || c.Count != 4 {
		t.Fatalf("always: sparse=%v count=%d", c.Sparse, c.Count)
	}
	if c := byKey["rare/string"]; !c.Sparse || c.Count != 1 || c.RowIDs[0] != 2 {
		t.Fatalf("rare: %+v", c)
	}
	if c := byKey["late/string"]; !c.Sparse || c.RowIDs[0] != 3 {
		t.Fatalf("late/string: %+v", c)
	}
	if c := byKey["late/null"]; c.Sparse || c.Count != 1 {
		t.Fatalf("late/null: %+v", c)
	}

	b.Reset()
	if err := b.AddJSON([]byte(`{"rare":"again"}`), 0); err != nil {
		t.Fatal(err)
	}
	if c := byKey["rare/string"]; c.Sparse || c.Count != 1 || string(c.Bytes(0)) != "again" {
		t.Fatalf("rare after reset: %+v", c)
	}
	if c := byKey["always/int"]; c.Count != 0 {
		t.Fatalf("always after reset: %+v", c)
	}
}

func TestShredRejectsMalformedAndLeavesBatchUnchanged(t *testing.T) {
	b := NewBatch(Options{})
	if err := b.AddJSON([]byte(`{"keep":1,"s":"v"}`), 0); err != nil {
		t.Fatal(err)
	}
	before := rowsOf(t, b)
	bad := []string{
		``, `[]`, `"str"`, `{`, `{"a"`, `{"a":`, `{"a":1`, `{"a":1,}`, `{"a":1 "b":2}`, `{a:1}`,
		`{"a":tru}`, `{"a":nul}`, `{"a":-}`, `{"a":1.2.3}`, `{"a":"unterminated}`, `{"a":"bad \x escape"}`,
		`{"a":"\u12"}`, `{"a":[1,2}`, `{"a":[1 2]}`, `{"a":{"b":1}} trailing`, `{"a":1}{"b":2}`,
		`{"ok":1,"then":{"deep":[1,{"x":}]}}`,
	}
	for _, d := range bad {
		err := b.AddJSON([]byte(d), 0)
		if err == nil {
			t.Fatalf("AddJSON(%q) succeeded", d)
		}
		if b.Rows != 1 {
			t.Fatalf("AddJSON(%q) changed the row count to %d", d, b.Rows)
		}
	}
	if after := rowsOf(t, b); !reflect.DeepEqual(before, after) {
		t.Fatalf("rejected documents left values behind:\n%v\n%v", before, after)
	}
	if err := b.AddJSON([]byte(`{"keep":2}`), 0); err != nil {
		t.Fatal(err)
	}
	if got := rowsOf(t, b)[1]; !reflect.DeepEqual(got, map[string]any{"keep": int64(2)}) {
		t.Fatalf("row after rejects = %v", got)
	}
}

func TestShredTimeField(t *testing.T) {
	b := NewBatch(Options{TimeField: "meta.ts"})
	docs := []string{
		`{"meta":{"ts":1732206349}}`,
		`{"meta":{"ts":1732206349000}}`,
		`{"meta":{"ts":1732206349000167}}`,
		`{"meta":{"ts":1732206349000167000}}`,
		`{"meta":{"ts":"2024-11-21T16:25:49.000167Z"}}`,
		`{"meta":{"ts":"not a time"}}`,
		`{"other":1}`,
	}
	for _, d := range docs {
		if err := b.AddJSON([]byte(d), 42); err != nil {
			t.Fatal(err)
		}
	}
	want := []int64{1732206349e9, 1732206349e9, 1732206349000167e3, 1732206349000167000, 1732206349000167000, 42, 42}
	if !reflect.DeepEqual(b.Times, want) {
		t.Fatalf("Times = %v\n want  %v", b.Times, want)
	}
}

func TestShredColumnCapKeepsOverflowInRest(t *testing.T) {
	b := NewBatch(Options{MaxColumns: 3})
	doc := `{"a":1,"b":{"c":2,"d":[1,2],"e":{"f":"g"}},"h":"i"}`
	if err := b.AddJSON([]byte(doc), 0); err != nil {
		t.Fatal(err)
	}
	got := rowsOf(t, b)[0]
	want := map[string]any{
		"a": int64(1), "b.c": int64(2),
		RestPath: map[string]any{"b.d": []any{int64(1), int64(2)}, "b.e": map[string]any{"f": "g"}, "h": "i"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got  %#v\nwant %#v", got, want)
	}
}

func TestShredTextAndDeepNesting(t *testing.T) {
	b := NewBatch(Options{})
	b.AddText([]byte("plain log line"), 7)
	deep := strings.Repeat(`{"n":`, maxDepth+5) + `1` + strings.Repeat(`}`, maxDepth+5)
	if err := b.AddJSON([]byte(deep), 0); err != nil {
		t.Fatal(err)
	}
	rows := rowsOf(t, b)
	if rows[0][RawPath] != "plain log line" || b.Times[0] != 7 {
		t.Fatalf("text row = %v", rows[0])
	}
	if len(rows[1]) != 1 {
		t.Fatalf("deep row = %v", rows[1])
	}
}

func TestPooledBatchIsEmpty(t *testing.T) {
	b := Get(Options{})
	if err := b.AddJSON([]byte(`{"a":1}`), 0); err != nil {
		t.Fatal(err)
	}
	Put(b)
	b = Get(Options{TimeField: "t"})
	defer Put(b)
	if b.Rows != 0 || len(b.Times) != 0 {
		t.Fatalf("pooled batch not empty: %d rows", b.Rows)
	}
	for _, c := range b.Columns() {
		if c.Count != 0 {
			t.Fatalf("pooled batch kept values in %s", c.Path)
		}
	}
}

func FuzzAddJSON(f *testing.F) {
	for _, s := range []string{
		`{"a":1,"b":{"c":[1,2,{"d":null}]},"e":"x\ny"}`, `{"a":1e5,"b":-0.0,"c":123456789012345678901}`,
		`{"":{"":1}}`, `{"a":{}}`, `{"k":"😀"}`, `{"a":[[[[]]]]}`,
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, doc string) {
		b := NewBatch(Options{})
		err := b.AddJSON([]byte(doc), 0)
		dec := json.NewDecoder(strings.NewReader(doc))
		dec.UseNumber()
		var ref any
		refErr := dec.Decode(&ref)
		if refErr == nil && !json.Valid([]byte(doc)) {
			refErr = fmt.Errorf("trailing data")
		}
		if _, isObj := ref.(map[string]any); refErr != nil || !isObj {
			if err == nil {
				// The shredder accepts a few inputs the reference rejects,
				// such as leading zeros; they must still produce valid rows.
				rowsOf(t, b)
			}

			return
		}
		if err != nil {
			t.Fatalf("rejected valid document %q: %v", doc, err)
		}
		// Invalid UTF-8 is stored as received; the reference replaces it.
		if hasAmbiguousPaths(ref, "", map[string]bool{}) || !utf8.ValidString(doc) {
			rowsOf(t, b)

			return
		}
		want := map[string]any{}
		flatten(want, "", normalize(ref), true)
		got := rowsOf(t, b)[0]
		if !equalValues(got, want) {
			t.Fatalf("doc %q\n got  %#v\n want %#v", doc, got, want)
		}
	})
}

// hasAmbiguousPaths reports documents where two members flatten to one path.
func hasAmbiguousPaths(v any, prefix string, seen map[string]bool) bool {
	obj, ok := v.(map[string]any)
	if !ok {
		return false
	}
	for k, x := range obj {
		p := k
		if prefix != "" || seen[""] {
			p = prefix + "." + k
		}
		if strings.Contains(k, ".") || k == "" || seen[p] {
			return true
		}
		seen[p] = true
		if hasAmbiguousPaths(x, p, seen) {
			return true
		}
	}

	return false
}

func equalValues(a, b any) bool {
	switch x := a.(type) {
	case float64:
		y, ok := b.(float64)

		return ok && (x == y || (math.IsNaN(x) && math.IsNaN(y)))
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for k, v := range x {
			w, ok := y[k]
			if !ok || !equalValues(v, w) {
				return false
			}
		}

		return true
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !equalValues(x[i], y[i]) {
				return false
			}
		}

		return true
	}

	return reflect.DeepEqual(a, b)
}
