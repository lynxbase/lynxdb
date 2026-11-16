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

func TestSortByTimeBucketsGroupsRowsByBucketThenKey(t *testing.T) {
	const hour = int64(3600e9)
	r := rand.New(rand.NewSource(41))
	b := NewBatch(Options{})
	type row struct {
		bucket int64
		svc    string
		ts     int64
		seq    int
	}
	var want []row
	for i := 0; i < 5000; i++ {
		// Timestamps on both sides of zero, so that rounding down matters.
		ts := int64(r.Intn(6)-3)*hour + int64(r.Intn(int(hour)))
		svc := fmt.Sprintf("s%02d", r.Intn(17))
		if err := b.AddJSON([]byte(fmt.Sprintf(`{"svc":"%s","seq":%d}`, svc, i)), ts); err != nil {
			t.Fatal(err)
		}
		want = append(want, row{floorDiv(ts, hour), svc, ts, i})
	}
	sort.SliceStable(want, func(i, j int) bool {
		a, c := want[i], want[j]
		if a.bucket != c.bucket {
			return a.bucket < c.bucket
		}
		if a.svc != c.svc {
			return a.svc < c.svc
		}

		return a.ts < c.ts
	})
	buckets := b.SortByTimeBuckets(nil, hour, []string{"svc"})
	if len(buckets) != 6 {
		t.Fatalf("%d buckets: %v", len(buckets), buckets)
	}
	rows := rowsOf(t, b)
	at := 0
	for i, bucket := range buckets {
		if i > 0 && buckets[i-1].ID >= bucket.ID {
			t.Fatalf("buckets out of order: %v", buckets)
		}
		for n := 0; n < bucket.Rows; n, at = n+1, at+1 {
			w := want[at]
			if w.bucket != bucket.ID || b.Times[at] != w.ts || rows[at]["seq"] != int64(w.seq) || rows[at]["svc"] != w.svc {
				t.Fatalf("row %d in bucket %d: %v @%d, want %+v", at, bucket.ID, rows[at], b.Times[at], w)
			}
		}
	}
	if at != len(want) {
		t.Fatalf("buckets cover %d of %d rows", at, len(want))
	}

	one := NewBatch(Options{})
	for i := 0; i < 10; i++ {
		if err := one.AddJSON([]byte(fmt.Sprintf(`{"svc":"s%d"}`, 9-i)), -5); err != nil {
			t.Fatal(err)
		}
	}
	if got := one.SortByTimeBuckets(nil, hour, []string{"svc"}); len(got) != 1 || got[0] != (TimeBucket{ID: -1, Rows: 10}) {
		t.Fatalf("one bucket: %v", got)
	}
	if first := rowsOf(t, one)[0]["svc"]; first != "s0" {
		t.Fatalf("rows of a single bucket are not sorted: first is %v", first)
	}
	if got := NewBatch(Options{}).SortByTimeBuckets(nil, hour, nil); got != nil {
		t.Fatalf("an empty batch has buckets %v", got)
	}
}
