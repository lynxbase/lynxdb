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
		if rejected, err := db.Append(index, append(lines(index, 0, n), "not json\n"...)); err != nil || rejected != 1 {
			t.Fatalf("append to %s: %d rejected, %v", index, rejected, err)
		}
	}
	if _, err := db.Append("../escape", lines("x", 0, 1)); !errors.Is(err, ErrIndexName) {
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
		`source=nope | stats count() as n`:                    "n=0",
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
	if _, err := db.Append("web", lines("web", 0, 1)); !errors.Is(err, ErrClosed) {
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
	if got := ask(t, db, `source=db | stats count() as n`); got != "n=0" {
		t.Errorf("a dropped index holds %s", got)
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
				if _, err := db.Append(fmt.Sprintf("i%d", w%3), lines("x", w*1000+i*10, 10)); err != nil {
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
