package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// respondJSON serves body as the JSON answer of every request.
func respondJSON(t *testing.T, body map[string]any) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(body)
	}))
	t.Cleanup(srv.Close)

	return srv.URL
}

func TestQueryPrintsServerWarnings(t *testing.T) {
	for _, rows := range [][]map[string]any{{{"n": 1}}, {}} {
		url := respondJSON(t, map[string]any{
			"data": map[string]any{"type": "events", "events": rows, "total": len(rows)},
			"meta": map[string]any{"warnings": []string{"field stauts is in no index", "second"}},
		})

		stdout, stderr, err := runCmd(t, "--server", url, "query", "--format", "json", "source=main | where stauts = 1")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if strings.Contains(stdout, "stauts") {
			t.Errorf("a warning went to stdout: %q", stdout)
		}
		for _, w := range []string{"field stauts is in no index", "second"} {
			if !strings.Contains(stderr, w) {
				t.Errorf("%d rows: stderr lacks the warning %q:\n%s", len(rows), w, stderr)
			}
		}

		_, stderr, err = runCmd(t, "--server", url, "--quiet", "query", "--format", "json", "source=main | where stauts = 1")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if strings.Contains(stderr, "stauts") {
			t.Errorf("--quiet printed a warning:\n%s", stderr)
		}
	}
}

func TestIngestWarnsOfFailedAndExpiredEvents(t *testing.T) {
	path := writeTemp(t, "app.log", "one\ntwo\nthree\nfour\n")
	cases := []struct {
		data map[string]any
		want string
	}{
		{map[string]any{"accepted": 1, "failed": 1, "expired": 2}, "3 of 4 events were not stored: 1 failed, 2 expired (older than the server's retention)."},
		{map[string]any{"accepted": 4, "failed": 0, "expired": 0}, ""},
	}
	for _, c := range cases {
		url := respondJSON(t, map[string]any{"data": c.data})

		_, stderr, err := runCmd(t, "--server", url, "ingest", path)
		if err != nil {
			t.Fatalf("ingest failed: %v", err)
		}
		if c.want == "" && strings.Contains(stderr, "not stored") {
			t.Errorf("%v: unexpected warning:\n%s", c.data, stderr)
		}
		if c.want != "" && !strings.Contains(stderr, c.want) {
			t.Errorf("%v: stderr lacks %q:\n%s", c.data, c.want, stderr)
		}

		_, stderr, err = runCmd(t, "--server", url, "--quiet", "ingest", path)
		if err != nil {
			t.Fatalf("ingest failed: %v", err)
		}
		if strings.Contains(stderr, "not stored") {
			t.Errorf("--quiet printed a warning:\n%s", stderr)
		}
	}
}
