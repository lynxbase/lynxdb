package rest

import (
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/internal/benchdata"
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

	return map[string]interface{}{
		"sort_key": e["sort_key"], "time_fields": e["time_fields"], "partition": e["partition"], "part_dictionary": e["part_dictionary"],
	}
}

// settings returns the settings of an index without part dictionaries as
// an index entry shows them.
func settings(sortKey, timeFields []interface{}, partition string) map[string]interface{} {
	return map[string]interface{}{"sort_key": sortKey, "time_fields": timeFields, "partition": partition, "part_dictionary": false}
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
	for _, again := range []string{`{"sort_key":["kind","did"],"partition":"1d"}`, `{}`, ``, `{"time_fields":["ts"]}`, `{"part_dictionary":false}`} {
		if code, reply := call(t, http.MethodPut, base+"bench", again); code != http.StatusOK || !reflect.DeepEqual(settingsOf(reply["data"]), bench) {
			t.Fatalf("create %s again: %d %v", again, code, reply)
		}
	}
	for _, other := range []string{`{"sort_key":["host"]}`, `{"sort_key":[]}`, `{"partition":"2h"}`, `{"time_fields":["time"]}`, `{"part_dictionary":true}`} {
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
		"bench7": `{"part_dictionary":"yes"}`,
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

func TestPartDictionaryIsSetWithAnIndexShownAndKept(t *testing.T) {
	cfg := storageConfig(nil, time.Hour)
	cfg.DataDir = t.TempDir()
	srv, stop := startTestServerWithConfig(t, cfg)
	base := "http://" + srv.Addr() + "/api/v1/indexes/"
	shown := func(reply map[string]interface{}) interface{} {
		entry, _ := reply["data"].(map[string]interface{})

		return entry["part_dictionary"]
	}

	if code, reply := call(t, http.MethodPut, base+"coded", `{"part_dictionary":true}`); code != http.StatusCreated || shown(reply) != true {
		t.Fatalf("create with part dictionaries: %d %v", code, reply)
	}
	if code, reply := call(t, http.MethodPut, base+"plain", `{}`); code != http.StatusCreated || shown(reply) != false {
		t.Fatalf("create without the setting: %d %v", code, reply)
	}
	for name, has := range map[string]bool{"coded": true, "plain": false} {
		again, other := `{"part_dictionary":true}`, `{"part_dictionary":false}`
		if !has {
			again, other = other, again
		}
		if code, reply := call(t, http.MethodPut, base+name, again); code != http.StatusOK || shown(reply) != has {
			t.Fatalf("create %s again with %s: %d %v", name, again, code, reply)
		}
		code, reply := call(t, http.MethodPut, base+name, other)
		if e, _ := reply["error"].(map[string]interface{}); code != http.StatusConflict || e["code"] != "CONFLICT" ||
			!strings.Contains(e["message"].(string), "part_dictionary") {
			t.Fatalf("create %s over its setting with %s: %d %v", name, other, code, reply)
		}
		if code, reply := call(t, http.MethodGet, base+name, ""); code != http.StatusOK || shown(reply) != has {
			t.Fatalf("%s: %d %v", name, code, reply)
		}
	}
	ingestNDJSON(t, srv.Addr(), "web", `{"ts":1700000000,"host":"a"}`)
	if code, reply := call(t, http.MethodGet, base+"web", ""); code != http.StatusOK || shown(reply) != false {
		t.Fatalf("an index made by ingest: %d %v", code, reply)
	}
	stop()

	// A server whose storage settings give new indexes part dictionaries
	// leaves those that exist as they were made.
	cfg.Storage.PartDictionary = true
	srv, stop = startTestServerWithConfig(t, cfg)
	defer stop()
	base = "http://" + srv.Addr() + "/api/v1/indexes/"
	ingestNDJSON(t, srv.Addr(), "late", `{"ts":1700000000,"host":"a"}`)
	if code, reply := call(t, http.MethodPut, base+"fresh", `{}`); code != http.StatusCreated || shown(reply) != true {
		t.Fatalf("an index created under the new settings: %d %v", code, reply)
	}
	if code, reply := call(t, http.MethodPut, base+"spared", `{"part_dictionary":false}`); code != http.StatusCreated || shown(reply) != false {
		t.Fatalf("an index created without part dictionaries under the new settings: %d %v", code, reply)
	}
	_, list := call(t, http.MethodGet, "http://"+srv.Addr()+"/api/v1/indexes", "")
	got := map[string]interface{}{}
	for _, entry := range list["data"].(map[string]interface{})["indexes"].([]interface{}) {
		e := entry.(map[string]interface{})
		got[e["name"].(string)] = e["part_dictionary"]
	}
	want := map[string]interface{}{"coded": true, "plain": false, "web": false, "late": true, "fresh": true, "spared": false}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("the part dictionaries of the indexes: %v, want %v", got, want)
	}
}

func TestAnIndexWithPartDictionariesStoresAndAnswersByCodes(t *testing.T) {
	srv, stop := startTestServerWithConfig(t, Config{})
	defer stop()
	root := "http://" + srv.Addr() + "/api/v1/"
	// The accounts of the events repeat across the blocks of a part.
	events := string(benchdata.New(1, 2000).Append(nil, 50000))
	for index, body := range map[string]string{
		"coded": `{"time_fields":["time_us"],"part_dictionary":true}`,
		"plain": `{"time_fields":["time_us"]}`,
	} {
		if code, reply := call(t, http.MethodPut, root+"indexes/"+index, body); code != http.StatusCreated {
			t.Fatalf("create %s: %d %v", index, code, reply)
		}
		ingestNDJSON(t, srv.Addr(), index, events)
		if code, reply := call(t, http.MethodPost, root+"indexes/"+index+"/compact", ""); code != http.StatusOK {
			t.Fatalf("compact %s: %d %v", index, code, reply)
		}
	}
	dictionaries := func(index string) (bytes int64) {
		for _, c := range getColumns(t, root+"indexes/"+index+"/columns").Columns {
			bytes += c.DictionaryBytes
		}

		return bytes
	}
	if coded, plain := dictionaries("coded"), dictionaries("plain"); coded == 0 || plain != 0 {
		t.Fatalf("the part dictionaries of coded take %d bytes and those of plain %d", coded, plain)
	}
	for _, q := range []string{
		`stats count() as n by did | sort - n, did | head 5`,
		`stats distinct_count(did) as accounts`,
		`where like(did, 'did:plc:a%') | stats count() as n`,
	} {
		var rows [2]interface{}
		var byCode [2]float64
		for i, index := range []string{"coded", "plain"} {
			raw, _ := json.Marshal(map[string]string{"q": "source=" + index + " | " + q})
			code, reply := call(t, http.MethodPost, root+"query", string(raw))
			if code != http.StatusOK {
				t.Fatalf("%s over %s: %d %v", q, index, code, reply)
			}
			rows[i] = reply["data"].(map[string]interface{})["rows"]
			stats, _ := reply["meta"].(map[string]interface{})["stats"].(map[string]interface{})
			aggregated, _ := stats["aggregated_by_code"].(float64)
			filtered, _ := stats["columns_filtered_by_code"].(float64)
			byCode[i] = aggregated + filtered
		}
		if rows[0] == nil || !reflect.DeepEqual(rows[0], rows[1]) {
			t.Errorf("%s\n coded %v\n plain %v", q, rows[0], rows[1])
		}
		if byCode[0] == 0 || byCode[1] != 0 {
			t.Errorf("%s: %v blocks and columns read by code in coded, %v in plain", q, byCode[0], byCode[1])
		}
	}
}
