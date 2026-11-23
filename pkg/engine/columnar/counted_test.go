package columnar

import (
	"context"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/ppl"
)

// TestCountByAFieldReadsNoBlockOfAPartThatKnowsIt requires a count of the
// events by a field of few values to come from what the parts know of the
// field, with and without a sort and a range of time that holds them whole.
func TestCountByAFieldReadsNoBlockOfAPartThatKnowsIt(t *testing.T) {
	db, err := Open(Config{Dir: t.TempDir(), Store: colstore.StoreOptions{TimeField: "ts", Partition: time.Hour, FlushInterval: -1}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := appendTo(db, "web", lines("web", 0, 900)); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	for text, rows := range map[string]string{
		`source=web | stats count() as n by host | sort host`:                                "host=h0 n=300; host=h1 n=300; host=h2 n=300",
		`source=web | stats count() as n by host | fields host, n | sort - n, host | head 2`: "host=h0 n=300; host=h1 n=300",
		`source=web earliest=-30d | stats count() by host | sort host`:                       "count()=300 host=h0; count()=300 host=h1; count()=300 host=h2",
	} {
		compiled, err := ppl.Compile(text)
		if err != nil {
			t.Fatalf("%s: %v", text, err)
		}
		res, err := db.Query(context.Background(), compiled.Plan, Options{Now: testNow})
		if err != nil {
			t.Fatalf("%s: %v", text, err)
		}
		if got := render(res.Maps()); got != rows {
			t.Errorf("%s\n got  %s\n want %s", text, got, rows)
		}
		if res.table == nil || res.table.Stats.CountedByIndex == 0 || res.table.Stats.Blocks != 0 {
			t.Errorf("%s: read blocks: %+v", text, res.table)
		}
	}
}
