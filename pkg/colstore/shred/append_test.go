package shred

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

func TestProgrammaticAppendBuildsTheSameColumns(t *testing.T) {
	parsed := NewBatch(Options{})
	for i, d := range []string{`{"n":1,"s":"a","f":0.5,"b":true}`, `{"s":"bb","z":null}`, `{"n":3,"s":"","b":false}`} {
		if err := parsed.AddJSON([]byte(d), int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	built := NewBatch(Options{})
	built.Col("n", vec.KindInt).AppendInt(0, 1)
	built.Col("s", vec.KindString).AppendBytes(0, []byte("a"))
	built.Col("f", vec.KindFloat).AppendFloat(0, 0.5)
	built.Col("b", vec.KindBool).AppendBool(0, true)
	built.Col("s", vec.KindString).AppendByteRun(1, []byte("xxbb"), []uint32{2, 4, 4})
	built.Col("z", vec.KindNull).AppendNull(1)
	built.Col("n", vec.KindInt).AppendInts(2, []int64{3})
	built.Col("b", vec.KindBool).AppendBool(2, false)
	built.AppendRows([]int64{0, 1, 2}, 0)
	if !reflect.DeepEqual(rowsOf(t, built), rowsOf(t, parsed)) {
		t.Fatalf("built %v\nparsed %v", rowsOf(t, built), rowsOf(t, parsed))
	}
	if !reflect.DeepEqual(built.Times, parsed.Times) || built.Rows != 3 {
		t.Fatalf("times %v rows %d", built.Times, built.Rows)
	}
}

func TestCopyRowsSlicesEveryColumnShape(t *testing.T) {
	src := NewBatch(Options{})
	for i := 0; i < 500; i++ {
		doc := fmt.Sprintf(`{"dense":%d,"s":"v%d"`, i, i%7)
		if i%3 == 0 {
			doc += fmt.Sprintf(`,"sparse":%g,"flag":%v`, float64(i)/2, i%2 == 0)
		}
		if i < 40 {
			doc += `,"early":"x"`
		}
		if i == 250 {
			doc += `,"once":[1,"two"],"nothing":null`
		}
		if err := src.AddJSON([]byte(doc+"}"), int64(1000+i)); err != nil {
			t.Fatal(err)
		}
	}
	want := rowsOf(t, src)
	for _, cut := range [][]int{{0, 500}, {0, 1}, {30, 50}, {249, 252}, {251, 500}, {100, 100}, {37, 463}} {
		dst := NewBatch(Options{})
		// Rows already in the destination must keep their values.
		if err := dst.AddJSON([]byte(`{"dense":-1,"own":true}`), 5); err != nil {
			t.Fatal(err)
		}
		src.CopyRows(dst, cut[0], cut[1])
		src.CopyRows(dst, cut[0], cut[1])
		got := rowsOf(t, dst)
		n := cut[1] - cut[0]
		if len(got) != 1+2*n || !reflect.DeepEqual(got[0], map[string]any{"dense": int64(-1), "own": true}) {
			t.Fatalf("cut %v: %d rows, first %v", cut, len(got), got[0])
		}
		for i := 0; i < 2*n; i++ {
			if !reflect.DeepEqual(got[1+i], want[cut[0]+i%n]) || dst.Times[1+i] != int64(1000+cut[0]+i%n) {
				t.Fatalf("cut %v: copied row %d = %v @%d, want %v", cut, i, got[1+i], dst.Times[1+i], want[cut[0]+i%n])
			}
		}
	}
}
