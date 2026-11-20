package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/lynxbase/lynxdb/internal/benchdata"
	"github.com/lynxbase/lynxdb/pkg/config"
)

func TestExplainColumnComparesTheForms(t *testing.T) {
	srv, cleanup := startTestServerWithConfig(t, Config{Storage: config.StorageConfig{
		SortKey:    []string{"kind", "commit.operation", "commit.collection", "did"},
		TimeFields: []string{"time_us"},
	}})
	defer cleanup()
	ingestNDJSON(t, srv.Addr(), "bsky", string(benchdata.New(1, 2000).Append(nil, 20000)))

	for _, path := range []string{"commit.rkey", "commit.record.createdAt"} {
		resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/indexes/bsky/columns?explain=%s&blocks=50", srv.Addr(), path))
		if err != nil {
			t.Fatal(err)
		}
		var body struct{ Data columnExplain }
		err = json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: status %d, decode: %v", path, resp.StatusCode, err)
		}
		e := body.Data
		if e.Blocks == 0 || len(e.Stored) == 0 || len(e.Forms) < 2 {
			t.Fatalf("%s: %+v", path, e)
		}
		smallest := 0
		for _, f := range e.Forms {
			smallest += f.Smallest
		}
		if smallest != e.Blocks {
			t.Fatalf("%s: %d blocks, %d smallest forms", path, e.Blocks, smallest)
		}
		if out := os.Getenv("EXPLAIN_OUT"); out != "" {
			raw, _ := json.MarshalIndent(e, "", " ")
			if err := os.WriteFile(out+"."+path+".json", raw, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
}
