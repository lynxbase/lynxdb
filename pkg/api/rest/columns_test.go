package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/lynxbase/lynxdb/internal/benchdata"
	"github.com/lynxbase/lynxdb/pkg/config"
)

func TestIndexColumnsAddUpToTheIndexSize(t *testing.T) {
	srv, cleanup := startTestServerWithConfig(t, Config{Storage: config.StorageConfig{
		SortKey:    []string{"kind", "commit.operation", "commit.collection", "did"},
		TimeFields: []string{"time_us"},
	}})
	defer cleanup()
	ingestNDJSON(t, srv.Addr(), "bsky", string(benchdata.New(1, 2000).Append(nil, 20000)))

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/indexes/bsky/columns", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct{ Data indexUsage }
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, decode: %v", resp.StatusCode, err)
	}
	u := body.Data
	if u.Rows != 20000 || u.Bytes <= 0 || len(u.Columns) == 0 {
		t.Fatalf("rows %d, bytes %d, columns %d", u.Rows, u.Bytes, len(u.Columns))
	}
	for name, list := range map[string][]usageShare{"summary": u.Summary, "breakdown": u.Breakdown} {
		sum := int64(0)
		for _, s := range list {
			sum += s.Bytes
		}
		if sum != u.Bytes {
			t.Errorf("%s adds up to %d of %d bytes", name, sum, u.Bytes)
		}
	}
	for i, c := range u.Columns {
		if i > 0 && c.Bytes > u.Columns[i-1].Bytes {
			t.Fatalf("column %s follows a smaller one", c.Path)
		}
		sum := int64(0)
		for _, s := range c.Sections {
			sum += s.Bytes
		}
		if sum != c.ChunkBytes {
			t.Errorf("%s %s: sections hold %d of %d chunk bytes", c.Path, c.Kind, sum, c.ChunkBytes)
		}
	}

	missing, err := http.Get(fmt.Sprintf("http://%s/api/v1/indexes/nothing/columns", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	missing.Body.Close()
	if missing.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown index: status %d", missing.StatusCode)
	}
}
