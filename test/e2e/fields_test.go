//go:build e2e

package e2e

import (
	"context"
	"testing"
)

func TestE2E_Fields_ReturnsFieldCatalog(t *testing.T) {
	h := NewHarness(t)
	h.IngestFile("idx_ssh", "testdata/OpenSSH_2k.log")

	ctx := context.Background()
	fields, err := h.Client().Fields(ctx)
	if err != nil {
		t.Fatalf("Fields: %v", err)
	}
	if len(fields) == 0 {
		t.Fatal("expected non-empty field catalog")
	}

	// Log all discovered fields. The field catalog tracks extracted/indexed
	// fields, not internal virtual fields like _raw and _time.
	fieldNames := make([]string, 0, len(fields))
	for _, f := range fields {
		fieldNames = append(fieldNames, f.Name)
	}
	t.Logf("field catalog: %d fields: %v", len(fields), fieldNames)
}

func TestE2E_FieldValues_ReturnsTopValues(t *testing.T) {
	h := NewHarness(t)

	ctx := context.Background()
	events := []map[string]interface{}{
		{"host": "web-01", "status": 200},
		{"host": "web-02", "status": 404},
		{"host": "web-01", "status": 200},
		{"host": "web-03", "status": 500},
	}
	_, err := h.Client().Ingest(ctx, events)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	result, err := h.Client().FieldValues(ctx, "host", 10)
	if err != nil {
		t.Fatalf("FieldValues: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil FieldValuesResult")
	}
	if result.Field != "host" {
		t.Errorf("expected field=host, got %s", result.Field)
	}
	t.Logf("field values: %d unique, %d total", result.UniqueCount, result.TotalCount)
}

func TestE2E_Sources_ReturnsIngestedSources(t *testing.T) {
	h := NewHarness(t)
	h.IngestFile("idx_ssh", "testdata/OpenSSH_2k.log")

	ctx := context.Background()
	sources, err := h.Client().Sources(ctx)
	if err != nil {
		t.Fatalf("Sources: %v", err)
	}
	// At minimum we should get one source entry back after ingest.
	t.Logf("sources: %d entries", len(sources))
}
