package part

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
)

var mergeKey = []string{"svc", "level"}

// orderedPart shreds docs, sorts them by mergeKey and writes one block.
func orderedPart(t testing.TB, docs []string, times []int64) *Part {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf, Options{Ordered: true, SortKey: mergeKey})
	batch := shred.NewBatch(shred.Options{})
	for i, d := range docs {
		if err := batch.AddJSON([]byte(d), times[i]); err != nil {
			t.Fatal(err)
		}
	}
	batch.SortBy(mergeKey)
	if err := w.WriteBatch(batch); err != nil || w.Close() != nil {
		t.Fatal("write failed")
	}
	p, err := Open(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	return p
}

func mergeParts(t testing.TB, srcs []*Part, blockRows int) *Part {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf, Options{Ordered: true, SortKey: mergeKey})
	if err := Merge(w, srcs, blockRows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	p, err := Open(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	return p
}

type mergedRow struct {
	Svc   *string `json:"svc"`
	Level *int64  `json:"level"`
	Src   int     `json:"src"`
	Seq   int     `json:"seq"`
	time  int64
	text  string
}

func decodeRows(t testing.TB, p *Part) []mergedRow {
	t.Helper()
	docs, times := readAll(t, p)
	rows := make([]mergedRow, len(docs))
	for i, d := range docs {
		if err := json.Unmarshal([]byte(d), &rows[i]); err != nil {
			t.Fatal(err)
		}
		rows[i].time, rows[i].text = times[i], d
	}

	return rows
}

// ordered reports whether a comes no later than b: by svc, level, time, and
// for full ties by source and position within it.
func ordered(a, b mergedRow) bool {
	cmpPtr := func(x, y *string) int {
		switch {
		case x == nil && y == nil:
			return 0
		case x == nil:
			return -1
		case y == nil:
			return 1
		case *x < *y:
			return -1
		case *x > *y:
			return 1
		}

		return 0
	}
	if d := cmpPtr(a.Svc, b.Svc); d != 0 {
		return d < 0
	}
	switch {
	case a.Level == nil && b.Level != nil:
		return true
	case a.Level != nil && b.Level == nil:
		return false
	case a.Level != nil && *a.Level != *b.Level:
		return *a.Level < *b.Level
	}
	if a.time != b.time {
		return a.time < b.time
	}
	if a.Src != b.Src {
		return a.Src < b.Src
	}

	return a.Seq <= b.Seq
}

func TestMergeOrdersRowsAndKeepsEveryValue(t *testing.T) {
	r := rand.New(rand.NewSource(9))
	services := []string{"api", "billing", "ingest"}
	var srcs []*Part
	var all []string
	for src := 0; src < 5; src++ {
		var docs []string
		var times []int64
		for seq := 0; seq < 2500; seq++ {
			doc := fmt.Sprintf(`{"src":%d,"seq":%d,"msg":"m%d"`, src, seq, r.Intn(50))
			if r.Intn(8) != 0 {
				doc += fmt.Sprintf(`,"svc":"%s"`, services[r.Intn(len(services))])
			}
			if r.Intn(5) != 0 {
				doc += fmt.Sprintf(`,"level":%d`, r.Intn(4))
			}
			if src%2 == 0 && r.Intn(30) == 0 {
				doc += fmt.Sprintf(`,"only_even":{"tags":["a",%d],"f":%g}`, seq, r.Float64())
			}
			if src == 3 {
				doc += `,"const_here":"same"`
			}
			docs = append(docs, doc+"}")
			times = append(times, int64(r.Intn(20)))
		}
		srcs = append(srcs, orderedPart(t, docs, times))
		all = append(all, docs...)
	}

	check := func(p *Part, wantBlocks int) {
		t.Helper()
		if !p.Ordered() || len(p.SortKey()) != 2 || p.SortKey()[0] != "svc" {
			t.Fatalf("merged part lost its sort key: %v %v", p.Ordered(), p.SortKey())
		}
		if p.NumBlocks() != wantBlocks || p.Rows != uint64(len(all)) {
			t.Fatalf("%d blocks, %d rows; want %d blocks, %d rows", p.NumBlocks(), p.Rows, wantBlocks, len(all))
		}
		rows := decodeRows(t, p)
		for i := 1; i < len(rows); i++ {
			if !ordered(rows[i-1], rows[i]) {
				t.Fatalf("rows %d and %d are out of order:\n%s\n%s", i-1, i, rows[i-1].text, rows[i].text)
			}
		}
		got := make([]string, len(rows))
		for i, row := range rows {
			got[i] = row.text
		}
		want := append([]string(nil), all...)
		sort.Slice(got, func(i, j int) bool { return rows[i].Src*1e6+rows[i].Seq < rows[j].Src*1e6+rows[j].Seq })
		sort.Slice(rows, func(i, j int) bool { return rows[i].Src*1e6+rows[i].Seq < rows[j].Src*1e6+rows[j].Seq })
		for i := range want {
			if !sameJSON(t, rows[i].text, want[i]) {
				t.Fatalf("row src=%d seq=%d changed:\n got  %s\n want %s", rows[i].Src, rows[i].Seq, rows[i].text, want[i])
			}
		}
	}

	merged := mergeParts(t, srcs, 1000)
	check(merged, 13)

	// Parts with several blocks merge the same way as one-block parts.
	left := mergeParts(t, srcs[:2], 700)
	right := mergeParts(t, srcs[2:], 4096)
	check(mergeParts(t, []*Part{left, right}, 5000), 3)
	check(mergeParts(t, []*Part{merged}, 0), 1)
}

func TestMergeRejectsDifferentlyOrderedSources(t *testing.T) {
	ordered := orderedPart(t, []string{`{"svc":"a"}`}, []int64{1})
	unordered := buildPart(t, []string{`{"svc":"a"}`}, 10, Options{})
	other := func() *Part {
		var buf bytes.Buffer
		w := NewWriter(&buf, Options{Ordered: true, SortKey: []string{"level"}})
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		p, err := Open(buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}

		return p
	}()
	for name, srcs := range map[string][]*Part{"unordered": {ordered, unordered}, "other key": {ordered, other}} {
		w := NewWriter(&bytes.Buffer{}, Options{Ordered: true, SortKey: mergeKey})
		if err := Merge(w, srcs, 0); !errors.Is(err, ErrSortKey) {
			t.Errorf("%s source: %v", name, err)
		}
	}
	if err := Merge(NewWriter(&bytes.Buffer{}, Options{}), []*Part{ordered}, 0); !errors.Is(err, ErrSortKey) {
		t.Errorf("unordered destination: %v", err)
	}
}
