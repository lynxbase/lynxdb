//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/client"
)

func TestE2E_Fields_ReturnsFieldCatalog(t *testing.T) {
	h := NewHarness(t)
	h.IngestFile("idx_ssh", "testdata/logs/OpenSSH_2k.log")

	ctx := context.Background()
	fields, err := h.Client().Fields(ctx)
	if err != nil {
		t.Fatalf("Fields: %v", err)
	}

	byName := make(map[string][]string, len(fields))
	for _, f := range fields {
		byName[f.Name] = f.Indexes
	}
	// Text lines hold _raw, _time, source and sourcetype; key=value pairs
	// such as "uid=0" in the pam_unix lines become fields too.
	for _, name := range []string{"_raw", "_time", "source", "sourcetype", "uid", "rhost"} {
		indexes, ok := byName[name]
		if !ok {
			t.Errorf("field %q missing from catalog %v", name, fields)
			continue
		}
		if strings.Join(indexes, ",") != "idx_ssh" {
			t.Errorf("field %q: expected indexes [idx_ssh], got %v", name, indexes)
		}
	}
}

func TestE2E_FieldValues_ReturnsTopValues(t *testing.T) {
	h := NewHarness(t)

	ctx := context.Background()
	events := []client.IngestEvent{
		{Event: `{"host":"web-01","status":200}`, Host: "web-01", Fields: map[string]interface{}{"status": 200}},
		{Event: `{"host":"web-02","status":404}`, Host: "web-02", Fields: map[string]interface{}{"status": 404}},
		{Event: `{"host":"web-01","status":200}`, Host: "web-01", Fields: map[string]interface{}{"status": 200}},
		{Event: `{"host":"web-03","status":500}`, Host: "web-03", Fields: map[string]interface{}{"status": 500}},
	}
	_, err := h.Client().IngestEvents(ctx, events)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	result, err := h.Client().FieldValues(ctx, "host", 10)
	if err != nil {
		t.Fatalf("FieldValues: %v", err)
	}
	if result.Field != "host" {
		t.Errorf("expected field=host, got %s", result.Field)
	}
	if len(result.Values) != 3 {
		t.Fatalf("expected 3 values, got %d: %+v", len(result.Values), result.Values)
	}
	if fmt.Sprint(result.Values[0].Value) != "web-01" || result.Values[0].Count != 2 {
		t.Errorf("expected top value web-01 with count 2, got %+v", result.Values[0])
	}
	rest := map[string]int64{}
	for _, v := range result.Values[1:] {
		rest[fmt.Sprint(v.Value)] = v.Count
	}
	if rest["web-02"] != 1 || rest["web-03"] != 1 {
		t.Errorf("expected web-02 and web-03 with count 1, got %+v", result.Values[1:])
	}
}

func TestE2E_Sources_ReturnsIngestedSources(t *testing.T) {
	h := NewHarness(t)
	h.IngestFile("idx_ssh", "testdata/logs/OpenSSH_2k.log")

	ctx := context.Background()
	sources, err := h.Client().Sources(ctx)
	if err != nil {
		t.Fatalf("Sources: %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("expected 1 source, got %d: %+v", len(sources), sources)
	}
	src := sources[0]
	if src.Name != "idx_ssh" || src.EventCount != 2000 {
		t.Errorf("expected idx_ssh with 2000 events, got %+v", src)
	}
	if src.FirstEvent == "" || src.LastEvent == "" || src.FirstEvent > src.LastEvent {
		t.Errorf("expected first_event <= last_event, got first=%q last=%q", src.FirstEvent, src.LastEvent)
	}
}
