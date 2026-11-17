package rest

import (
	"fmt"
	"testing"
)

func TestFieldValues_Basic(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 30, 3)

	data := getData(t, fmt.Sprintf("http://%s/api/v1/fields/host/values", srv.Addr()))

	if data["field"] != "host" {
		t.Fatalf("field: %v", data["field"])
	}
	values := data["values"].([]interface{})
	if len(values) != 3 {
		t.Fatalf("values = %v, want 3 hosts", values)
	}
	for _, v := range values {
		entry := v.(map[string]interface{})
		if entry["count"] != float64(10) {
			t.Errorf("value %v count = %v, want 10", entry["value"], entry["count"])
		}
	}
}

func TestFieldValues_Limit(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 50, 10)

	data := getData(t, fmt.Sprintf("http://%s/api/v1/fields/host/values?limit=3", srv.Addr()))
	values := data["values"].([]interface{})
	if len(values) != 3 {
		t.Fatalf("values: got %d, want 3", len(values))
	}
}

func TestFieldValues_NonExistent(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 10, 2)

	data := getData(t, fmt.Sprintf("http://%s/api/v1/fields/nonexistent/values", srv.Addr()))
	if values := data["values"].([]interface{}); len(values) != 0 {
		t.Fatalf("values = %v, want none", values)
	}
}

func TestFields_List(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestNDJSON(t, srv.Addr(), "main", "{\"host\":\"a\",\"level\":\"info\"}\n")
	ingestNDJSON(t, srv.Addr(), "web", "{\"host\":\"b\",\"path\":\"/\"}\n")

	indexesOf := func(query string) map[string][]interface{} {
		t.Helper()
		data := getData(t, fmt.Sprintf("http://%s/api/v1/fields%s", srv.Addr(), query))
		out := map[string][]interface{}{}
		for _, f := range data["fields"].([]interface{}) {
			entry := f.(map[string]interface{})
			out[entry["name"].(string)] = entry["indexes"].([]interface{})
		}

		return out
	}

	all := indexesOf("")
	if got := fmt.Sprint(all["host"]); got != "[main web]" {
		t.Errorf("host indexes = %s, want [main web]", got)
	}
	if got := fmt.Sprint(all["level"]); got != "[main]" {
		t.Errorf("level indexes = %s, want [main]", got)
	}
	if got := fmt.Sprint(all["path"]); got != "[web]" {
		t.Errorf("path indexes = %s, want [web]", got)
	}

	web := indexesOf("?index=web")
	if _, ok := web["level"]; ok {
		t.Errorf("index=web lists level: %v", web)
	}
	if _, ok := web["path"]; !ok {
		t.Errorf("index=web misses path: %v", web)
	}

	prefixed := indexesOf("?prefix=pa")
	if len(prefixed) != 1 || prefixed["path"] == nil {
		t.Errorf("prefix=pa = %v, want only path", prefixed)
	}
}

func TestSources_Basic(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	ingestTestEvents(t, srv.Addr(), 20, 4)

	data := getData(t, fmt.Sprintf("http://%s/api/v1/sources", srv.Addr()))
	sources := data["sources"].([]interface{})
	if len(sources) != 1 {
		t.Fatalf("sources = %v, want main", sources)
	}
	src := sources[0].(map[string]interface{})
	if src["name"] != "main" {
		t.Fatalf("source name: %v", src["name"])
	}
	if int(src["event_count"].(float64)) != 20 {
		t.Fatalf("event_count: %v", src["event_count"])
	}
	if src["first_event"] == nil || src["last_event"] == nil {
		t.Fatalf("source = %v, want first_event and last_event", src)
	}
}

func TestSources_Empty(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	data := getData(t, fmt.Sprintf("http://%s/api/v1/sources", srv.Addr()))
	sources := data["sources"].([]interface{})
	if len(sources) != 0 {
		t.Fatalf("expected 0 sources, got %d", len(sources))
	}
}
