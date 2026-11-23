package main

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTemp(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	return path
}

func TestDetectIngestFormat(t *testing.T) {
	cases := []struct {
		path, body, want string
	}{
		{"a.ndjson", "plain text\n", ingestFormatNDJSON},
		{"a.jsonl", "", ingestFormatNDJSON},
		{"a.csv", `{"a":1}` + "\n", ingestFormatCSV},
		{"A.TSV", "x\ty\n", ingestFormatCSV},
		{"a.json", "\n\n" + `{"index":{"_index":"web"}}` + "\n" + `{"a":1}` + "\n", ingestFormatESBulk},
		{"a.json", `{"create":{}}` + "\n", ingestFormatESBulk},
		{"a.log", `{"index":"web","msg":"ok"}` + "\n", ingestFormatNDJSON},
		{"", `{"status":503}` + "\n", ingestFormatNDJSON},
		{"", "2026-01-01T00:00:00Z level=info msg=ok\n", ingestFormatText},
		{"", "{not json\n", ingestFormatText},
		{"", "", ingestFormatText},
	}
	for _, c := range cases {
		got, r, err := detectIngestFormat(c.path, strings.NewReader(c.body))
		if err != nil {
			t.Fatalf("%s %q: %v", c.path, c.body, err)
		}
		if got != c.want {
			t.Errorf("%s %q: format %s, want %s", c.path, c.body, got, c.want)
		}
		rest, _ := io.ReadAll(r)
		if string(rest) != c.body {
			t.Errorf("%s %q: the reader yields %q", c.path, c.body, rest)
		}
	}
}

func TestCSVObject(t *testing.T) {
	header := []string{"status", "zip", "ratio", "big", "note", "empty", "signed", "source"}
	record := []string{"503", "007", "-1.5", "1e3", "+1", "", "-0", "csv"}
	got, err := json.Marshal(csvObject(header, record, "flag", "export"))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"big":1e3,"note":"+1","ratio":-1.5,"signed":-0,"source":"csv","sourcetype":"export","status":503,"zip":"007"}`
	if string(got) != want {
		t.Errorf("got %s\nwant %s", got, want)
	}
}

func TestWithOrigin(t *testing.T) {
	cases := []struct {
		line, want string
	}{
		{`{"msg":"ok","n":1.50}`, `{"msg":"ok","n":1.50,"source":"web","sourcetype":"nginx"}`},
		{`{"source":"own"}`, `{"source":"own","sourcetype":"nginx"}`},
		{`not json`, `not json`},
		{`[1,2]`, `[1,2]`},
		{`null`, `null`},
	}
	for _, c := range cases {
		if got := string(withOrigin([]byte(c.line), "web", "nginx")); got != c.want {
			t.Errorf("%s: got %s, want %s", c.line, got, c.want)
		}
	}
	if got := string(withOrigin([]byte(`{"b":1, "a":2}`), "", "")); got != `{"b":1, "a":2}` {
		t.Errorf("without origin flags the line changed: %s", got)
	}
}

func TestIngestRejectsFlagsTheFormatIgnores(t *testing.T) {
	baseURL := newTestServer(t)
	text := writeTemp(t, "app.log", "level=info msg=ok\n")
	bulk := writeTemp(t, "dump.json", `{"index":{}}`+"\n"+`{"msg":"ok"}`+"\n")
	csvPath := writeTemp(t, "a.csv", "a\n1\n")

	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"ingest", text, "--delimiter", ";"}, "--delimiter applies to CSV input"},
		{[]string{"ingest", bulk, "--source", "web"}, "--source and --sourcetype do not apply"},
		{[]string{"ingest", csvPath, "--delimiter", "ab"}, "--delimiter must be one character"},
		{[]string{"ingest", text, "--format", "xml"}, `unknown format "xml"`},
	} {
		_, _, err := runCmd(t, append([]string{"--server", baseURL}, c.args...)...)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%v: got %v, want an error containing %q", c.args, err, c.want)
		}
	}
}

func TestIngestFormats(t *testing.T) {
	baseURL := newTestServer(t)

	ndjson := writeTemp(t, "events.ndjson",
		`{"timestamp":"2026-02-14T12:00:00Z","host":"web-01","status":200}`+"\n\n"+
			`{"timestamp":"2026-02-14T12:01:00Z","host":"web-02","status":503,"source":"own"}`+"\n")
	csvPath := writeTemp(t, "orders.csv", "\xef\xbb\xbfid,amount,status,note\n1,10.5,200,\n2,7,503,\"late, again\"\n")
	tsv := writeTemp(t, "orders.tsv", "id\tamount\n1\t3\n")
	bulk := writeTemp(t, "dump.json",
		`{"index":{}}`+"\n"+`{"host":"web-03","status":500}`+"\n"+
			`{"create":{"_index":"bulk_named"}}`+"\n"+`{"host":"web-04","status":200}`+"\n")

	for _, args := range [][]string{
		{"ingest", ndjson, "--index", "nd", "--source", "flag"},
		{"ingest", csvPath, "--index", "csv", "--sourcetype", "orders"},
		{"ingest", tsv, "--index", "tsv"},
		{"ingest", bulk, "--index", "bulk"},
	} {
		if _, stderr, err := runCmd(t, append([]string{"--server", baseURL}, args...)...); err != nil {
			t.Fatalf("%v: %v\n%s", args, err, stderr)
		}
	}

	for q, want := range map[string]string{
		"source=nd | stats count() as n by source | sort source":        `{"n":1,"source":"flag"}` + "\n" + `{"n":1,"source":"own"}`,
		"source=nd | where status > 500 | fields host":                  `{"host":"web-02"}`,
		"source=csv | where status > 500 | fields id, amount, note":     `{"amount":7,"id":2,"note":"late, again"}`,
		"source=csv | stats sum(amount) as total, count(note) as notes": `{"notes":1,"total":17.5}`,
		"source=csv | stats count() as n by sourcetype":                 `{"n":2,"sourcetype":"orders"}`,
		"source=tsv | fields id, amount":                                `{"amount":3,"id":1}`,
		"source=bulk | fields host, status":                             `{"host":"web-03","status":500}`,
		"source=bulk_named | fields host, status":                       `{"host":"web-04","status":200}`,
	} {
		stdout, _, err := runCmd(t, "--server", baseURL, "query", "--format", "json", q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if got := strings.TrimSpace(stdout); got != want {
			t.Errorf("%s:\ngot  %s\nwant %s", q, got, want)
		}
	}
}
