package rest

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/ppl"
)

// The catalog is built once from the PPL catalogs, which are fixed at
// compile time, so it carries a strong ETag.
var catalogCache struct {
	once sync.Once
	body []byte
	etag string
}

func (s *Server) handleCatalog(w http.ResponseWriter, r *http.Request) {
	catalogCache.once.Do(func() {
		catalogCache.body, catalogCache.etag = buildCatalogResponse()
	})

	if match := r.Header.Get("If-None-Match"); match == catalogCache.etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("ETag", catalogCache.etag)
	w.Header().Set("Cache-Control", "public, max-age=86400")
	_, _ = w.Write(catalogCache.body)
}

type catalogCommand struct {
	Name    string `json:"name"`
	Support string `json:"support"`
	Notes   string `json:"notes,omitempty"`
}

type catalogFunction struct {
	Name        string `json:"name"`
	Kind        string `json:"kind"`
	Category    string `json:"category,omitempty"`
	Support     string `json:"support"`
	Lowering    string `json:"lowering,omitempty"`
	Notes       string `json:"notes,omitempty"`
	Doc         string `json:"doc,omitempty"`
	Unsupported string `json:"unsupported,omitempty"`
}

type catalog struct {
	Language  string            `json:"language"`
	Commands  []catalogCommand  `json:"commands"`
	Functions []catalogFunction `json:"functions"`
}

func buildCatalogResponse() ([]byte, string) {
	c := catalog{Language: "ppl"}
	for _, cmd := range ppl.Commands() {
		c.Commands = append(c.Commands, catalogCommand{Name: cmd.Name, Support: string(cmd.Support), Notes: cmd.Notes})
	}
	for _, f := range ppl.ScalarFunctions() {
		cf := catalogFunction{Name: f.Name, Kind: "scalar", Lowering: f.Lowering, Unsupported: f.Unsupported, Support: "full"}
		if f.Unsupported != "" {
			cf.Support = "unsupported"
		}
		if fn, ok := ir.LookupFunc(f.IR); ok {
			cf.Category, cf.Doc = fn.Category, fn.Doc
		}
		c.Functions = append(c.Functions, cf)
	}
	for _, a := range ppl.AggregateFunctions() {
		cf := catalogFunction{Name: a.Name, Kind: "aggregate", Notes: a.Note, Unsupported: a.Unsupported, Support: "full"}
		if a.Unsupported != "" {
			cf.Support = "unsupported"
		}
		if fn, ok := ir.LookupFunc(a.IR); ok {
			cf.Category, cf.Doc = fn.Category, fn.Doc
		}
		c.Functions = append(c.Functions, cf)
	}
	body, err := json.Marshal(map[string]interface{}{"data": c})
	if err != nil {
		body = []byte(`{"data":{}}`)
	}
	return body, fmt.Sprintf(`"%x"`, sha256.Sum256(body))
}
