package rest

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
)

// getExplain calls the explain endpoint and returns the data object.
func getExplain(t *testing.T, srv *Server, query string) map[string]interface{} {
	t.Helper()

	u := fmt.Sprintf("http://%s/api/v1/query/explain?q=%s", srv.Addr(), url.QueryEscape(query))
	resp, err := http.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: %d, body: %s", resp.StatusCode, body)
	}

	var envelope map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode: %v", err)
	}
	data, ok := envelope["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing data: %#v", envelope)
	}

	return data
}

func explainParsed(t *testing.T, data map[string]interface{}) map[string]interface{} {
	t.Helper()
	if data["is_valid"] != true {
		t.Fatalf("expected is_valid=true, got: %#v", data)
	}
	parsed, ok := data["parsed"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing parsed: %#v", data)
	}

	return parsed
}

func TestQueryExplain_Valid(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	parsed := explainParsed(t, getExplain(t, srv, `search source=main error | head 10`))

	for _, key := range []string{"logical_plan", "optimized_plan"} {
		plan, _ := parsed[key].(string)
		if !strings.Contains(plan, "Scan(main)") {
			t.Fatalf("%s missing Scan(main): %s", key, plan)
		}
		if !strings.Contains(plan, "Limit(10)") {
			t.Fatalf("%s missing Limit(10): %s", key, plan)
		}
	}
	terms, _ := parsed["search_terms"].([]interface{})
	if !slices.Contains(terms, interface{}("error")) {
		t.Fatalf("search_terms = %v, want error", terms)
	}
	if _, ok := parsed["optimizer_rules"].([]interface{}); !ok {
		t.Fatalf("missing optimizer_rules: %#v", parsed)
	}
	if parsed["result_type"] != "events" {
		t.Fatalf("result_type = %v, want events", parsed["result_type"])
	}
}

func TestQueryExplain_MissingSource(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	data := getExplain(t, srv, "stats count()")
	if data["is_valid"] != false {
		t.Fatalf("expected is_valid=false, got: %#v", data)
	}
	errs, _ := data["errors"].([]interface{})
	if len(errs) != 1 {
		t.Fatalf("errors = %#v, want one", data["errors"])
	}
	first, _ := errs[0].(map[string]interface{})
	if first["kind"] != "syntax" {
		t.Fatalf("kind = %v, want syntax", first["kind"])
	}
	if msg, _ := first["message"].(string); msg == "" {
		t.Fatalf("missing message: %#v", first)
	}
	pos, _ := first["position"].(map[string]interface{})
	if pos["line"] != float64(1) {
		t.Fatalf("position = %v, want line 1", pos)
	}
}

func TestQueryExplain_InvalidQuery(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	data := getExplain(t, srv, "INVALID @@@")
	if data["is_valid"] != false {
		t.Fatal("expected is_valid=false")
	}
	errs := data["errors"].([]interface{})
	if len(errs) == 0 {
		t.Fatal("expected errors array")
	}
}

func TestQueryExplain_MissingQuery(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/query/explain", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 400 {
		t.Fatalf("status: %d, want 400", resp.StatusCode)
	}
}

func TestQueryExplain_StatsQuery(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	parsed := explainParsed(t, getExplain(t, srv, "source=main | stats count() by host"))

	plan, _ := parsed["optimized_plan"].(string)
	if !strings.Contains(plan, "Aggregate") {
		t.Fatalf("plan missing Aggregate node: %s", plan)
	}
	if parsed["result_type"] != "aggregate" {
		t.Fatalf("result_type = %v, want aggregate", parsed["result_type"])
	}
	fieldsRead, _ := parsed["fields_read"].([]interface{})
	if !slices.Contains(fieldsRead, interface{}("host")) {
		t.Fatalf("fields_read = %v, want host", fieldsRead)
	}
}
