package rest

import (
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/config"
)

// call sends a request with a JSON body, when there is one, and returns the
// status and the decoded reply.
func call(t *testing.T, method, url, body string) (int, map[string]interface{}) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var reply map[string]interface{}
	if err := json.Unmarshal(raw, &reply); err != nil {
		t.Fatalf("%s %s: %v: %s", method, url, err, raw)
	}

	return resp.StatusCode, reply
}

// settingsOf returns the settings in an index entry.
func settingsOf(entry interface{}) map[string]interface{} {
	e, _ := entry.(map[string]interface{})

	return map[string]interface{}{"sort_key": e["sort_key"], "time_fields": e["time_fields"], "partition": e["partition"]}
}

func settings(sortKey, timeFields []interface{}, partition string) map[string]interface{} {
	return map[string]interface{}{"sort_key": sortKey, "time_fields": timeFields, "partition": partition}
}

func storageConfig(sortKey []string, partition time.Duration) Config {
	var cfg Config
	cfg.Storage.SortKey, cfg.Storage.TimeFields, cfg.Storage.Partition = sortKey, []string{"ts"}, config.Duration(partition)

	return cfg
}

func TestIndexSettingsAreCreatedRepeatedRefusedAndKept(t *testing.T) {
	cfg := storageConfig([]string{"host"}, time.Hour)
	cfg.DataDir = t.TempDir()
	srv, stop := startTestServerWithConfig(t, cfg)
	base := "http://" + srv.Addr() + "/api/v1/indexes/"
	bench := settings([]interface{}{"kind", "did"}, []interface{}{"ts"}, "1d")

	code, reply := call(t, http.MethodPut, base+"bench", `{"sort_key":["kind","did"],"partition":"24h"}`)
	if code != http.StatusCreated || !reflect.DeepEqual(settingsOf(reply["data"]), bench) {
		t.Fatalf("create: %d %v", code, reply)
	}
	for _, again := range []string{`{"sort_key":["kind","did"],"partition":"1d"}`, `{}`, ``, `{"time_fields":["ts"]}`} {
		if code, reply := call(t, http.MethodPut, base+"bench", again); code != http.StatusOK || !reflect.DeepEqual(settingsOf(reply["data"]), bench) {
			t.Fatalf("create %s again: %d %v", again, code, reply)
		}
	}
	for _, other := range []string{`{"sort_key":["host"]}`, `{"sort_key":[]}`, `{"partition":"2h"}`, `{"time_fields":["time"]}`} {
		code, reply := call(t, http.MethodPut, base+"bench", other)
		if e, _ := reply["error"].(map[string]interface{}); code != http.StatusConflict || e["code"] != "CONFLICT" {
			t.Fatalf("create %s over other settings: %d %v", other, code, reply)
		}
	}
	for name, bad := range map[string]string{
		"bench2": `{"sort_key":["a..b"]}`,
		"bench3": `{"partition":"10s"}`,
		"bench4": `{"partition":"soon"}`,
		"bench5": `{"time_fields":["t","t"]}`,
		".dot":   `{}`,
	} {
		if code, reply := call(t, http.MethodPut, base+name, bad); code != http.StatusBadRequest {
			t.Fatalf("create %s with %s: %d %v", name, bad, code, reply)
		}
	}
	if code, reply := call(t, http.MethodPut, base+"bench6", `{"sortkey":["a"]}`); code != http.StatusBadRequest {
		t.Fatalf("a field that is not a setting: %d %v", code, reply)
	}
	if code, reply := call(t, http.MethodGet, base+"bench2", ""); code != http.StatusNotFound {
		t.Fatalf("an index a refused create named: %d %v", code, reply)
	}

	ingestNDJSON(t, srv.Addr(), "web", `{"ts":1700000000,"host":"a"}`)
	web := settings([]interface{}{"host"}, []interface{}{"ts"}, "1h0m0s")
	if code, reply := call(t, http.MethodGet, base+"web", ""); code != http.StatusOK || !reflect.DeepEqual(settingsOf(reply["data"]), web) {
		t.Fatalf("an index made by ingest: %d %v", code, reply)
	}
	_, list := call(t, http.MethodGet, "http://"+srv.Addr()+"/api/v1/indexes", "")
	_, stats := call(t, http.MethodGet, "http://"+srv.Addr()+"/api/v1/stats", "")
	for what, reply := range map[string]map[string]interface{}{"indexes": list, "stats": stats} {
		indexes := reply["data"].(map[string]interface{})["indexes"].([]interface{})
		if len(indexes) != 2 || !reflect.DeepEqual(settingsOf(indexes[0]), bench) || !reflect.DeepEqual(settingsOf(indexes[1]), web) {
			t.Fatalf("the indexes of %s: %v", what, indexes)
		}
	}
	stop()

	// A server with other storage settings reads the indexes as they were
	// made and gives its own only to new ones.
	cfg2 := storageConfig(nil, 2*time.Hour)
	cfg2.DataDir = cfg.DataDir
	srv, stop = startTestServerWithConfig(t, cfg2)
	defer stop()
	base = "http://" + srv.Addr() + "/api/v1/indexes/"
	for name, want := range map[string]map[string]interface{}{"bench": bench, "web": web} {
		if code, reply := call(t, http.MethodGet, base+name, ""); code != http.StatusOK || !reflect.DeepEqual(settingsOf(reply["data"]), want) {
			t.Fatalf("%s after a restart: %d %v", name, code, reply)
		}
	}
	if code, reply := call(t, http.MethodGet, base+"web", ""); reply["data"].(map[string]interface{})["events"] != float64(1) {
		t.Fatalf("web after a restart: %d %v", code, reply)
	}
	if code, reply := call(t, http.MethodPut, base+"fresh", `{}`); code != http.StatusCreated ||
		!reflect.DeepEqual(settingsOf(reply["data"]), settings([]interface{}{}, []interface{}{"ts"}, "2h0m0s")) {
		t.Fatalf("an index created under the new settings: %d %v", code, reply)
	}
}
