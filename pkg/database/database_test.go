package database

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/query"
)

func openTest(t *testing.T, cfg Config) *DB {
	t.Helper()
	db, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return db
}

func run(t *testing.T, db *DB, src string, opts QueryOptions) []string {
	t.Helper()
	c, err := query.Compile(src)
	if err != nil {
		t.Fatal(err)
	}
	res, err := db.Query(context.Background(), c, opts)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, res.Len())
	for i := range out {
		out[i] = string(res.AppendJSON(nil, i))
	}

	return out
}

func TestAppendQuery(t *testing.T) {
	db := openTest(t, Config{})
	lines := `{"_time":"2026-01-01T00:00:00Z","level":"error","n":1}
{"timestamp":"2026-01-01T01:00:00Z","level":"info","n":2}
plain text line
`
	rejected, err := db.Append("", []byte(lines))
	if err != nil || rejected != 0 {
		t.Fatalf("append: %d %v", rejected, err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := run(t, db, "source=main | stats count()", QueryOptions{}); len(got) != 1 || got[0] != `{"count()":3}` {
		t.Fatalf("count: %v", got)
	}
	got := run(t, db, "source=main | where level = 'error' | fields _time, n", QueryOptions{})
	if len(got) != 1 || got[0] != `{"_time":"2026-01-01T00:00:00Z","n":1}` {
		t.Fatalf("time field: %v", got)
	}
	got = run(t, db, "source=main | where isnotnull(_raw) | fields _raw", QueryOptions{})
	if len(got) != 1 || !strings.Contains(got[0], "plain text line") {
		t.Fatalf("text line: %v", got)
	}
	fields, ok := db.Fields("main")
	if !ok || !strings.Contains(strings.Join(fields, ","), "level") {
		t.Fatalf("fields: %v %v", fields, ok)
	}
	if st := db.Stats(); len(st) != 1 || st[0].Name != "main" || st[0].Rows != 3 {
		t.Fatalf("stats: %+v", st)
	}
}

func TestQueryTimeRange(t *testing.T) {
	db := openTest(t, Config{})
	lines := `{"_time":"2026-01-01T00:00:00Z","n":1}
{"_time":"2026-01-02T00:00:00Z","n":2}
{"_time":"2026-01-03T00:00:00Z","n":3}
`
	if _, err := db.Append("web", []byte(lines)); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	from := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)
	got := run(t, db, "source=web | stats count()", QueryOptions{From: from, To: to})
	if len(got) != 1 || got[0] != `{"count()":1}` {
		t.Fatalf("range: %v", got)
	}
}

func TestQueryTimeRangeSubquery(t *testing.T) {
	db := openTest(t, Config{})
	web := `{"_time":"2026-01-01T00:00:00Z","n":1}
{"_time":"2026-01-02T00:00:00Z","n":2}
{"_time":"2026-01-03T00:00:00Z","n":3}
`
	ids := `{"_time":"2026-01-02T01:00:00Z","n":1}
{"_time":"2026-01-02T02:00:00Z","n":2}
{"_time":"2026-01-02T03:00:00Z","n":3}
`
	if _, err := db.Append("web", []byte(web)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Append("ids", []byte(ids)); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	from := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)
	q := "source=ids | where n in [ source=web | fields n ] | stats count()"
	if got := run(t, db, q, QueryOptions{}); got[0] != `{"count()":3}` {
		t.Fatalf("whole range: %v", got)
	}
	if got := run(t, db, q, QueryOptions{From: from, To: to}); got[0] != `{"count()":1}` {
		t.Fatalf("subquery not narrowed: %v", got)
	}
}

func TestRetention(t *testing.T) {
	var logs bytes.Buffer
	db := openTest(t, Config{Retention: 48 * time.Hour, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	now := time.Now().UTC()
	lines := `{"_time":"` + now.Add(-10*24*time.Hour).Format(time.RFC3339) + `"}
{"_time":"` + now.Format(time.RFC3339) + `"}
`
	if _, err := db.Append("main", []byte(lines)); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	db.expire(now)
	if got := run(t, db, "source=main | stats count()", QueryOptions{}); got[0] != `{"count()":1}` {
		t.Fatalf("after retention: %v", got)
	}
	cutoff := "cutoff=" + now.Add(-48*time.Hour).UTC().Format(time.RFC3339)
	for _, want := range []string{"level=WARN", "index=main", "events=1", cutoff} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("retention log lacks %s: %s", want, logs.String())
		}
	}
}

func TestJobs(t *testing.T) {
	db := openTest(t, Config{MaxConcurrent: 1})
	if _, err := db.Append("main", []byte(`{"a":1}`+"\n")); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	c, err := query.Compile("source=main | stats count()")
	if err != nil {
		t.Fatal(err)
	}
	j, err := db.Submit(c, "source=main | stats count()", QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	<-j.Done()
	st := j.Status()
	if st.State != JobDone || st.Rows != 1 {
		t.Fatalf("status: %+v", st)
	}
	if got, ok := db.Job(j.ID); !ok || got != j {
		t.Fatal("job not found")
	}
	if len(db.Jobs()) != 1 {
		t.Fatal("jobs list")
	}
	db.jobs.expire(time.Now().Add(time.Second))
	if _, ok := db.Job(j.ID); ok {
		t.Fatal("finished job kept past its time")
	}
}

func TestTooManyQueries(t *testing.T) {
	db := openTest(t, Config{MaxConcurrent: 1})
	release, err := db.acquire()
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	c, _ := query.Compile("source=main | head 1")
	if _, err := db.Query(context.Background(), c, QueryOptions{}); !errors.Is(err, ErrTooManyQueries) {
		t.Fatalf("got %v", err)
	}
}
