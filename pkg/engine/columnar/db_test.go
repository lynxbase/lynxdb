package columnar

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/ppl"
	"github.com/lynxbase/lynxdb/pkg/query"
)

func testConfig(dir string) Config {
	return Config{Dir: dir, Store: colstore.StoreOptions{TimeField: "ts", SortKey: []string{"host"}, Partition: time.Hour, FlushInterval: -1}}
}

func ask(t *testing.T, db *DB, text string) string {
	t.Helper()
	compiled, err := ppl.Compile(text)
	if err != nil {
		t.Fatalf("%s: %v", text, err)
	}
	res, err := db.Query(context.Background(), compiled.Plan, Options{Now: testNow})
	if err != nil {
		t.Fatalf("%s: %v", text, err)
	}

	return render(res.Maps())
}

func lines(index string, from, n int) []byte {
	var b strings.Builder
	for i := from; i < from+n; i++ {
		at := testNow.Add(-time.Duration(i) * time.Minute).UnixMicro()
		fmt.Fprintf(&b, `{"ts":%d,"host":"h%d","n":%d,"index":%q}`+"\n", at, i%3, i, index)
	}

	return []byte(b.String())
}

func TestDatabaseKeepsIndexesAcrossOpens(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	for index, n := range map[string]int{"web": 300, "db": 200} {
		if rejected, err := appendTo(db, index, append(lines(index, 0, n), "not json\n"...)); err != nil || rejected != 1 {
			t.Fatalf("append to %s: %d rejected, %v", index, rejected, err)
		}
	}
	if _, err := appendTo(db, "../escape", lines("x", 0, 1)); !errors.Is(err, ErrIndexName) {
		t.Fatalf("an index name that is a path: %v", err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(db.Names()); got != "[db web]" {
		t.Fatalf("indexes %s", got)
	}
	want := map[string]string{
		`source=web | stats count() as n by host | sort host`: "host=h0 n=100; host=h1 n=100; host=h2 n=100",
		"source=`*` | stats count() as n, max(n) as top":      "n=500 top=299",
		`source=db | where n < 2 | fields n, index | sort n`:  "index=db n=0; index=db n=1",
		`source=main | stats count() as n`:                    "n=0",
	}
	for text, rows := range want {
		if got := ask(t, db, text); got != rows {
			t.Errorf("%s\n got  %s\n want %s", text, got, rows)
		}
	}
	stats := db.Stats()
	if len(stats) != 2 || stats[0].Name != "db" || stats[0].Rows != 200 || stats[1].Rows != 300 || stats[1].Bytes == 0 ||
		!stats[1].Last.Equal(testNow) || !stats[1].First.Equal(testNow.Add(-299*time.Minute)) {
		t.Fatalf("stats %+v", stats)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := appendTo(db, "web", lines("web", 0, 1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("append after close: %v", err)
	}

	db, err = Open(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for text, rows := range want {
		if got := ask(t, db, text); got != rows {
			t.Errorf("after reopening: %s\n got  %s\n want %s", text, got, rows)
		}
	}
	// The partitions of an hour that end three hours back or earlier go.
	removed, err := db.DropBefore(testNow.Add(-3 * time.Hour))
	if err != nil || removed == 0 {
		t.Fatalf("dropped %d events, %v", removed, err)
	}
	if got := ask(t, db, "source=`*` | stats count() as n, max(n) as top"); got != fmt.Sprintf("n=%d top=%d", 500-removed, (500-int(removed))/2-1) {
		t.Errorf("after dropping %d events: %s", removed, got)
	}
	if dropped, err := db.Drop("db"); err != nil || !dropped {
		t.Fatalf("drop: %v, %v", dropped, err)
	}
	if dropped, err := db.Drop("db"); err != nil || dropped {
		t.Fatalf("drop of an index that is gone: %v, %v", dropped, err)
	}
	if got := fmt.Sprint(db.Names()); got != "[web]" {
		t.Fatalf("indexes after a drop: %s", got)
	}
	compiled, err := query.Compile(`source=db | stats count() as n`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query(context.Background(), compiled.Plan, Options{Now: testNow}); !errors.Is(err, ErrNoIndex) {
		t.Errorf("a query of a dropped index: %v", err)
	}
}

func TestDatabaseTakesAppendsAndQueriesAtOnce(t *testing.T) {
	db, err := Open(testConfig(""))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if _, err := appendTo(db, fmt.Sprintf("i%d", w%3), lines("x", w*1000+i*10, 10)); err != nil {
					t.Error(err)
				}
				if i%5 == 0 {
					if err := db.Sync(); err != nil {
						t.Error(err)
					}
					ask(t, db, "source=`i*` | stats count() as n")
				}
			}
		}(w)
	}
	wg.Wait()
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := ask(t, db, "source=`i*` | stats count() as n"); got != "n=1200" {
		t.Fatalf("after the appends: %s", got)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

// appendTo appends lines of JSON objects to an index and returns the number
// of lines rejected.
func appendTo(db *DB, index string, lines []byte) (int64, error) {
	done, err := db.AppendRows(index, lines, colstore.Intake{})

	return done.Rejected, err
}

func TestQueryReadsBufferedIndexes(t *testing.T) {
	db, err := Open(testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, index := range []string{"web", "db", "web-eu"} {
		if _, err := appendTo(db, index, lines(index, 0, 40)); err != nil {
			t.Fatal(err)
		}
	}
	parts := func(index string) int {
		st, _ := db.Index(index)

		return st.Parts
	}
	if got := ask(t, db, "source=web | stats count() as n"); got != "n=40" {
		t.Fatalf("a query after an append saw %s", got)
	}
	if parts("web") != 0 || parts("db") != 0 || parts("web-eu") != 0 {
		t.Fatalf("parts after a query of web: web %d, db %d, web-eu %d", parts("web"), parts("db"), parts("web-eu"))
	}
	if got := ask(t, db, "source=`web*` | where host in [ source=db | fields host ] | stats count() as n"); got != "n=80" {
		t.Fatalf("a query with a subquery saw %s", got)
	}
	if parts("db") != 0 || parts("web-eu") != 0 {
		t.Fatalf("parts after a query of all three: db %d, web-eu %d", parts("db"), parts("web-eu"))
	}
}
