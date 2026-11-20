package shred

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

// randomBatch returns a batch of rows of sparse and mixed columns.
func randomBatch(t *testing.T, seed int64, n int) *Batch {
	t.Helper()
	r := rand.New(rand.NewSource(seed))
	b := NewBatch(Options{})
	for i := 0; i < n; i++ {
		doc := fmt.Sprintf(`{"seq":%d,"src":%d`, i, seed)
		if r.Intn(8) != 0 {
			doc += fmt.Sprintf(`,"svc":"s%d"`, r.Intn(6))
		}
		switch r.Intn(5) {
		case 0:
			doc += fmt.Sprintf(`,"mixed":%d`, r.Intn(9)-4)
		case 1:
			doc += fmt.Sprintf(`,"mixed":"m%d"`, r.Intn(4))
		case 2:
			doc += `,"mixed":null`
		case 3:
			doc += fmt.Sprintf(`,"mixed":%v,"flag":%v`, r.Intn(2) == 0, r.Intn(3) == 0)
		}
		if r.Intn(30) == 0 {
			doc += fmt.Sprintf(`,"rare":{"f":%g,"tags":["a",%d]}`, float64(i)/7, i)
		}
		if err := b.AddJSON([]byte(doc+"}"), int64(r.Intn(1000))); err != nil {
			t.Fatal(err)
		}
	}

	return b
}

func TestInterleaveTakesTheRowsOfRunsInTheOrderPicked(t *testing.T) {
	only := NewBatch(Options{})
	for i := range 40 {
		if err := only.AddJSON([]byte(fmt.Sprintf(`{"only":"here","svc":%d}`, i)), 5); err != nil {
			t.Fatal(err)
		}
	}
	batches := []*Batch{randomBatch(t, 1, 900), randomBatch(t, 2, 1300), only}
	runs := []Run{{batches[0], 100, 900}, {batches[1], 0, 1250}, {batches[2], 3, 40}}
	all := make([][]map[string]any, len(batches))
	for i, b := range batches {
		all[i] = rowsOf(t, b)
	}
	r := rand.New(rand.NewSource(9))
	next := []int{runs[0].From, runs[1].From, runs[2].From}
	var picks []uint32
	var want []map[string]any
	var times []int64
	for len(picks) < 2000 {
		src := r.Intn(len(runs))
		if next[src] == runs[src].To {
			continue
		}
		picks = append(picks, uint32(src))
		want = append(want, all[src][next[src]])
		times = append(times, batches[src].Times[next[src]])
		next[src]++
	}
	out := NewBatch(Options{})
	out.Interleave(runs, picks[:700])
	out.Interleave(runs, picks[700:1999])
	out.Interleave(runs, picks[1999:])
	if got := rowsOf(t, out); !reflect.DeepEqual(got, want) || !reflect.DeepEqual(out.Times, times) {
		t.Fatal("the rows interleaved differ from the rows picked")
	}
	for i := range runs {
		if runs[i].From != next[i] {
			t.Fatalf("run %d is left at row %d, want %d", i, runs[i].From, next[i])
		}
	}
}
