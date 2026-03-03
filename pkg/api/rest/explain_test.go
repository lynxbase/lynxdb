package rest

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
)

func TestQueryExplain_Valid(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	u := fmt.Sprintf("http://%s/api/v1/query/explain?q=%s", srv.Addr(),
		url.QueryEscape(`FROM main | search "error" | head 10`))
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
	json.NewDecoder(resp.Body).Decode(&envelope)
	data := envelope["data"].(map[string]interface{})

	if data["is_valid"] != true {
		t.Fatal("expected is_valid=true")
	}
	parsed := data["parsed"].(map[string]interface{})
	if parsed["result_type"] != "events" {
		t.Fatalf("result_type: %v", parsed["result_type"])
	}
	if parsed["pipeline"] == nil {
		t.Fatal("missing pipeline")
	}
}

func TestQueryExplain_InvalidQuery(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	u := fmt.Sprintf("http://%s/api/v1/query/explain?q=%s", srv.Addr(),
		url.QueryEscape("INVALID @@@"))
	resp, err := http.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status: %d (explain should return 200 even for invalid queries)", resp.StatusCode)
	}

	var envelope map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&envelope)
	data := envelope["data"].(map[string]interface{})

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

	u := fmt.Sprintf("http://%s/api/v1/query/explain?q=%s", srv.Addr(),
		url.QueryEscape("FROM main | stats count by host"))
	resp, err := http.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var envelope map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&envelope)
	data := envelope["data"].(map[string]interface{})
	parsed := data["parsed"].(map[string]interface{})
	if parsed["result_type"] != "aggregate" {
		t.Fatalf("result_type: %v", parsed["result_type"])
	}
}
