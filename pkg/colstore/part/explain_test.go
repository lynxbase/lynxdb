package part

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/lynxbase/lynxdb/internal/benchdata"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
)

func TestExplainColumnReadsBlocksOfEveryLayout(t *testing.T) {
	// Five blocks, s in them the same in every row, absent, few values in
	// half the rows, distinct values in half the rows and few values in
	// every row; the generator's timed columns are sparse where its commits
	// are.
	const blockRows = 2048
	lines := bytes.Split(bytes.TrimSpace(benchdata.New(5, 2000).Append(nil, 5*blockRows)), []byte("\n"))
	var out bytes.Buffer
	w := NewWriter(&out, Options{})
	want := 0
	for k := 0; k < 5; k++ {
		var nd []byte
		for i, line := range lines[k*blockRows : (k+1)*blockRows] {
			var s string
			switch {
			case k == 0:
				s = "same"
			case k == 2 && i%2 == 0:
				s = fmt.Sprintf("few%d", i%7)
			case k == 3 && i%2 == 0:
				s = fmt.Sprintf("distinct%06d", i*7919)
			case k == 4:
				s = fmt.Sprintf("few%d", i%3)
			}
			if s != "" {
				line = append(line[:len(line)-1:len(line)-1], `,"s":"`+s+`"}`...)
				want++
			}
			nd = append(append(nd, line...), '\n')
		}
		batch := shred.NewBatch(shred.Options{TimeField: "time_us"})
		if errs, _ := batch.AddNDJSON(nd, 0, shred.Intake{}, nil); len(errs) != 0 {
			t.Fatal(errs[0].Err)
		}
		if err := w.WriteBatch(batch); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	p, err := Open(out.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range []string{"s", "commit.rev", "commit.rkey", "commit.record.createdAt"} {
		e := ColumnExplain{Path: path}
		if err := p.ExplainColumn(&e, func() bool { return true }); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if e.Blocks == 0 {
			t.Fatalf("%s: no blocks explained", path)
		}
		if path != "s" {
			continue
		}
		if e.Blocks != 4 || e.Values != uint64(want) {
			t.Fatalf("s: %d blocks and %d values, want 4 and %d", e.Blocks, e.Values, want)
		}
		for _, enc := range []string{"const", "dict"} {
			if e.Stored[enc] == nil {
				t.Fatalf("s: no block stored as %s: %v", enc, e.Stored)
			}
		}
	}
}
