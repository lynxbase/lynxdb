package rest

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestPartitionRequestsOnRunningServer(t *testing.T) {
	cfg := storageConfig(nil, 90*time.Minute)
	cfg.DataDir = t.TempDir()
	srv, stop := startTestServerWithConfig(t, cfg)
	defer stop()
	base := "http://" + srv.Addr() + "/api/v1/indexes/"
	request := func(method, path, body string, want int) any {
		t.Helper()
		code, reply := call(t, method, base+path, body)
		if code != want {
			t.Fatalf("%s %s: got %d, want %d: %v", method, path, code, want, reply)
		}
		return reply["data"]
	}
	archive := filepath.Join(t.TempDir(), "morning")
	body := func(field, path string) string {
		data, err := json.Marshal(map[string]string{field: path})
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	const partitions = "web/partitions"
	const first = partitions + "/2026-10-01"
	const second = partitions + "/2026-10-01T01:30:00Z"
	request(http.MethodGet, partitions, "", 404)
	ingestNDJSON(t, srv.Addr(), "web", "{\"ts\":\"2026-10-01T02:00:00Z\"}\n{\"ts\":\"2026-10-01T01:00:00Z\"}")
	list := request(http.MethodGet, partitions, "", 200).(map[string]any)["partitions"].([]any)
	if len(list) != 2 {
		t.Fatalf("partitions = %v", list)
	}
	morning := list[0].(map[string]any)
	if morning["start"] != "2026-10-01T00:00:00Z" || morning["end"] != "2026-10-01T01:30:00Z" ||
		morning["events"] != float64(1) || morning["parts"] != float64(1) || morning["bytes"].(float64) <= 0 {
		t.Fatalf("first partition = %v", morning)
	}

	for _, start := range []string{"not-a-date", "2026-10-01T01:00:00Z", "1500-01-01"} {
		request(http.MethodDelete, partitions+"/"+start, "", 400)
	}
	for _, invalid := range []string{`{"from":"wrong field"}`, `{"to":"relative"}`, body("to", archive) + " {}", body("to", filepath.Join(cfg.DataDir, "indexes", "foreign")), body("to", filepath.Join(cfg.DataDir, "auth"))} {
		request(http.MethodPost, first+"/detach", invalid, 400)
	}

	got := request(http.MethodPost, first+"/detach", body("to", archive), 200)
	if !reflect.DeepEqual(got, morning) {
		t.Fatalf("detach reported %v, want %v", got, morning)
	}
	request(http.MethodDelete, first, "", 404)
	request(http.MethodPost, "missing/partitions/attach", body("from", archive), 404)
	request(http.MethodPut, "hourly", `{"partition":"1h"}`, 201)
	request(http.MethodPost, "hourly/partitions/attach", body("from", archive), 409)
	// A late event recreates the interval. Attaching over it must preserve it.
	ingestNDJSON(t, srv.Addr(), "web", `{"ts":"2026-10-01T00:55:00Z"}`)
	request(http.MethodPost, partitions+"/attach", body("from", archive), 409)
	request(http.MethodPost, first+"/detach", body("to", archive), 409)
	request(http.MethodDelete, first, "", 200)
	if got := request(http.MethodPost, partitions+"/attach", body("from", archive), 200); !reflect.DeepEqual(got, morning) {
		t.Fatalf("attach reported %v, want %v", got, morning)
	}
	if got := request(http.MethodDelete, second, "", 200); !reflect.DeepEqual(got, list[1]) {
		t.Fatalf("drop reported %v, want %v", got, list[1])
	}

	stop()
	srv, stop = startTestServerWithConfig(t, cfg)
	defer stop()
	base = "http://" + srv.Addr() + "/api/v1/indexes/"
	got = request(http.MethodGet, partitions, "", 200).(map[string]any)["partitions"]
	if !reflect.DeepEqual(got, []any{morning}) {
		t.Fatalf("after reopen: partitions = %v", got)
	}
	request(http.MethodDelete, first, "", 200)
	got = request(http.MethodGet, partitions, "", 200).(map[string]any)["partitions"]
	if !reflect.DeepEqual(got, []any{}) {
		t.Fatalf("empty index: partitions = %v", got)
	}
}
