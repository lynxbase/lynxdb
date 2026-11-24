package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestFields_AfterIngest_ContainsKnownFields(t *testing.T) {
	baseURL := setupServerWithData(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "fields", "--format", "json")
	if err != nil {
		t.Fatalf("fields command failed: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	if len(lines) == 0 {
		t.Fatal("expected at least one field in output")
	}

	fieldNames := make(map[string]bool)

	for _, line := range lines {
		var entry map[string]interface{}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("parse field JSON line: %v\nline: %q", err, line)
		}

		name, _ := entry["name"].(string)
		if name != "" {
			fieldNames[name] = true
		}
	}

	// The access.log has a "level" field that the field catalog should discover.
	if !fieldNames["level"] {
		t.Errorf("expected 'level' field in catalog, got fields: %v", fieldNames)
	}
}

// fieldValuesServer starts a server that answers every request for the
// values of a field with none, and returns its URL and a func that returns
// the since, from and to of each request, empty where it sent none.
func fieldValuesServer(t *testing.T) (string, func() [][3]string) {
	t.Helper()
	var (
		mu     sync.Mutex
		bounds [][3]string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		mu.Lock()
		bounds = append(bounds, [3]string{q.Get("since"), q.Get("from"), q.Get("to")})
		mu.Unlock()

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{"field": "status", "values": []interface{}{}},
		})
	}))
	t.Cleanup(srv.Close)

	return srv.URL, func() [][3]string {
		mu.Lock()
		defer mu.Unlock()
		return bounds
	}
}

// TestFieldValuesTimeFlags pins the from and to that the time flags of
// fields --values send, as text the server resolves.
func TestFieldValuesTimeFlags(t *testing.T) {
	for _, c := range []struct {
		args     []string
		from, to string
	}{
		{nil, "", ""},
		{[]string{"--since", "1h"}, "-1h", "now"},
		{[]string{"--since", "@d"}, "@d", "now"},
		{[]string{"--from", "2025-01-15T08:00:00Z"}, "2025-01-15T08:00:00Z", "now"},
		{[]string{"--to", "@d"}, "", "@d"},
		{[]string{"--from", "-1d@d", "--to", "@d"}, "-1d@d", "@d"},
	} {
		url, bounds := fieldValuesServer(t)
		args := append([]string{"--server", url, "--format", "json", "fields", "status", "--values"}, c.args...)
		if _, _, err := runCmd(t, args...); err != nil {
			t.Fatalf("%q: %v", c.args, err)
		}
		if got := bounds(); len(got) != 1 || got[0] != [3]string{"", c.from, c.to} {
			t.Errorf("%q: sent %v, want one request with from %q and to %q", c.args, got, c.from, c.to)
		}
	}
}

// TestFieldValuesRefusedTimeFlagsSendNothing pins that time flags of fields
// --values that conflict or hold a value that is not valid fail as usage
// errors before a request.
func TestFieldValuesRefusedTimeFlagsSendNothing(t *testing.T) {
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"--since", "1h", "--from", "-1d"}, "--since and --from cannot be used together"},
		{[]string{"--since", "1h", "--to", "now"}, "--since and --to cannot be used together"},
		{[]string{"--since", "yesterday"}, `invalid --since "yesterday": `},
		{[]string{"--from", "banana"}, `invalid --from "banana": `},
		{[]string{"--to", "-1h@z"}, `invalid --to "-1h@z": `},
		{[]string{"--from", "now", "--to", "-1h"}, `--from "now" is after --to "-1h"`},
	} {
		url, bounds := fieldValuesServer(t)
		args := append([]string{"--server", url, "fields", "status", "--values"}, c.args...)
		_, _, err := runCmd(t, args...)
		var usage usageError
		if !errors.As(err, &usage) || !strings.HasPrefix(err.Error(), c.want) {
			t.Errorf("%q: got %v, want a usage error that starts with %q", c.args, err, c.want)
		}
		if got := bounds(); len(got) != 0 {
			t.Errorf("%q: sent %v, want no request", c.args, got)
		}
	}
}

func TestFields_EmptyServer_NoError(t *testing.T) {
	baseURL := newTestServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "fields", "--format", "json")
	if err != nil {
		t.Fatalf("fields command failed on empty server: %v", err)
	}

	// Empty server should return no fields — output should be empty or blank.
	if trimmed := strings.TrimSpace(stdout); trimmed != "" {
		// If there's output, each line should still be valid JSON.
		for _, line := range strings.Split(trimmed, "\n") {
			var entry map[string]interface{}
			if err := json.Unmarshal([]byte(line), &entry); err != nil {
				t.Errorf("unexpected non-JSON output on empty server: %q", line)
			}
		}
	}
}
