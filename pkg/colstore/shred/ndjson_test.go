package shred

import (
	"reflect"
	"strings"
	"testing"
)

var ndjsonDocs = []string{
	`{"a":1,"b":"x","c":true,"d":false,"e":null,"f":1.5}`,
	`{"a":{"b":{"c":"deep"}},"n":-12}`,
	`{"a":{},"b":[],"c":[1,"two",3.5,null,true,{"k":[{}]}]}`,
	`{"esc":"line\nbreak \"quoted\" back\\slash é 😀","key":"escaped key"}`,
	`{"big":9223372036854775807,"small":-9223372036854775808,"over":9223372036854775808}`,
	`{"n":0.1,"m":-1E-3,"p":1e3}`,
	`{"dup":1,"dup":"last wins","dup":2}`,
	`{"a":"scalar here"}`,
	`{"a":{"nested":true},"unicode":"привет ✓","empty":""}`,
	`{"long":"` + strings.Repeat("abcdefghij", 30) + `"}`,
}

func TestAddNDJSONMatchesPerDocumentShredding(t *testing.T) {
	want := NewBatch(Options{TimeField: "n"})
	for _, d := range ndjsonDocs {
		if err := want.AddJSON([]byte(d), 5); err != nil {
			t.Fatal(err)
		}
	}
	chunk := []byte(strings.Join(ndjsonDocs, "\n") + "\n\n")

	got := NewBatch(Options{TimeField: "n"})
	if errs, _ := got.AddNDJSON(chunk, 5, Intake{}, nil); len(errs) != 0 {
		t.Fatalf("AddNDJSON: %v", errs)
	}
	if !reflect.DeepEqual(rowsOf(t, got), rowsOf(t, want)) || !reflect.DeepEqual(got.Times, want.Times) {
		t.Fatal("AddNDJSON differs from AddJSON")
	}
}

func TestAddNDJSONReportsBadLinesAndKeepsGoodOnes(t *testing.T) {
	chunk := []byte("{\"a\":1}\n{\"a\":\n\n[1,2]\n{\"a\":4}")
	b := NewBatch(Options{})
	errs, _ := b.AddNDJSON(chunk, 0, Intake{}, nil)
	if len(errs) != 2 || errs[0].Line != 1 || errs[1].Line != 2 {
		t.Fatalf("errs = %+v", errs)
	}
	want := []map[string]any{{"a": int64(1)}, {"a": int64(4)}}
	if got := rowsOf(t, b); !reflect.DeepEqual(got, want) {
		t.Fatalf("rows = %v", got)
	}
}

func TestAddNDJSONKeepsLinesThatAreNotObjectsAsText(t *testing.T) {
	chunk := []byte("{\"a\":1}\nMar  2 06:00:01 node-1 sshd[411]: Failed password\r\n{\"a\":\n\n[1,2]\n{\"a\":4}")
	b := NewBatch(Options{})
	if errs, _ := b.AddNDJSON(chunk, 42, Intake{Text: true}, nil); len(errs) != 0 {
		t.Fatalf("errs = %+v", errs)
	}
	want := []map[string]any{
		{"a": int64(1)},
		{RawPath: "Mar  2 06:00:01 node-1 sshd[411]: Failed password"},
		{RawPath: `{"a":`},
		{RawPath: "[1,2]"},
		{"a": int64(4)},
	}
	if got := rowsOf(t, b); !reflect.DeepEqual(got, want) {
		t.Fatalf("rows = %v", got)
	}
	for row, at := range b.Times {
		if at != 42 {
			t.Fatalf("row %d has the time %d", row, at)
		}
	}
}

func TestSplitLines(t *testing.T) {
	data := []byte("a\nb\nc")
	chunk, rest := SplitLines(data, 2)
	if string(chunk) != "a\nb\n" || string(rest) != "c" {
		t.Fatalf("chunk=%q rest=%q", chunk, rest)
	}
	chunk, rest = SplitLines(rest, 2)
	if string(chunk) != "c" || rest != nil {
		t.Fatalf("chunk=%q rest=%q", chunk, rest)
	}
}
