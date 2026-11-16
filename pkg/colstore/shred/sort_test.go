package shred

import (
	"cmp"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

// keyOrder mirrors the ordering SortBy documents for one decoded value.
func keyOrder(v any, present bool) (kind int, val any) {
	switch x := v.(type) {
	case nil:
		if !present {
			return 0, nil
		}

		return 1, nil
	case bool:
		return 2, x
	case int64:
		return 3, x
	case float64:
		return 4, x
	case string:
		return 5, x
	}

	return 6, nil
}

func compareKey(a, b map[string]any, path string) int {
	av, aok := a[path]
	bv, bok := b[path]
	ak, ax := keyOrder(av, aok)
	bk, bx := keyOrder(bv, bok)
	if ak != bk {
		return cmp.Compare(ak, bk)
	}
	switch ak {
	case 2:
		if ax.(bool) == bx.(bool) {
			return 0
		}
		if bx.(bool) {
			return -1
		}

		return 1
	case 3:
		return cmp.Compare(ax.(int64), bx.(int64))
	case 4:
		return cmp.Compare(ax.(float64), bx.(float64))
	case 5:
		return cmp.Compare(ax.(string), bx.(string))
	}

	return 0
}

func TestSortByMatchesReferenceOrder(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	b := NewBatch(Options{})
	services := []string{"api", "billing", "ingest", "query-frontend", "Zeta"}
	for i := 0; i < 5000; i++ {
		doc := fmt.Sprintf(`{"seq":%d,"payload":"p%d"`, i, r.Intn(1000))
		if r.Intn(10) != 0 {
			doc += fmt.Sprintf(`,"svc":"%s"`, services[r.Intn(len(services))])
		}
		switch r.Intn(6) {
		case 0:
			doc += fmt.Sprintf(`,"mixed":%d`, r.Intn(20)-10)
		case 1:
			doc += fmt.Sprintf(`,"mixed":"s%d"`, r.Intn(5))
		case 2:
			doc += fmt.Sprintf(`,"mixed":%v`, r.Intn(2) == 0)
		case 3:
			doc += `,"mixed":null`
		case 4:
			doc += fmt.Sprintf(`,"mixed":%g`, float64(r.Intn(7))+0.5)
		}
		if r.Intn(50) == 0 {
			doc += fmt.Sprintf(`,"rare":{"n":%d}`, i)
		}
		doc += "}"
		if err := b.AddJSON([]byte(doc), int64(r.Intn(40))); err != nil {
			t.Fatal(err)
		}
	}
	keys := []string{"svc", "mixed"}
	want := rowsOf(t, b)
	times := append([]int64(nil), b.Times...)
	index := make([]int, len(want))
	for i := range index {
		index[i] = i
	}
	sort.SliceStable(index, func(x, y int) bool {
		a, c := index[x], index[y]
		for _, k := range keys {
			if d := compareKey(want[a], want[c], k); d != 0 {
				return d < 0
			}
		}

		return times[a] < times[c]
	})

	b.SortBy(keys)
	got := rowsOf(t, b)
	for at, from := range index {
		if !reflect.DeepEqual(got[at], want[from]) || b.Times[at] != times[from] {
			t.Fatalf("row %d after sorting\n got  %v @%d\n want %v @%d", at, got[at], b.Times[at], want[from], times[from])
		}
	}

	again := rowsOf(t, b)
	b.SortBy(keys)
	if !reflect.DeepEqual(rowsOf(t, b), again) {
		t.Fatal("sorting a sorted batch changed it")
	}
	b.SortBy([]string{"no.such.path"})
	b.SortBy(nil)
}

func TestSortByKeepsVariantAndSparseValuesWithTheirRows(t *testing.T) {
	b := NewBatch(Options{})
	docs := []string{
		`{"k":"b","tags":["x",1],"only":1}`,
		`{"k":"a","tags":["y"]}`,
		`{"k":"c"}`,
		`{"k":"a","tags":[],"deep":{"v":true}}`,
		`{"tags":["z"]}`,
	}
	for i, d := range docs {
		if err := b.AddJSON([]byte(d), int64(100-i)); err != nil {
			t.Fatal(err)
		}
	}
	before := rowsOf(t, b)
	b.SortBy([]string{"k"})
	after := rowsOf(t, b)
	// Missing k first, then a (by time), b, c.
	for at, from := range []int{4, 3, 1, 0, 2} {
		if !reflect.DeepEqual(after[at], before[from]) {
			t.Fatalf("row %d = %v, want %v", at, after[at], before[from])
		}
	}
}
