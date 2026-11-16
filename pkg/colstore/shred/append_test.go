package shred

import (
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
