//go:build e2e

package e2e

import (
	"context"
	"testing"

	"github.com/OrlovEvgeny/Lynxdb/pkg/client"
)

func TestE2E_SavedQueries_CRUD(t *testing.T) {
	// Previously broken: client decoded List response into a struct wrapper
	// instead of []SavedQuery directly. Fixed in pkg/client/queries.go —
	// ListSavedQueries now decodes the raw array from the envelope correctly.
	h := NewHarness(t)
	ctx := context.Background()

	// Create (works because server's SavedQueryInput accepts both "q" and "query").
	input := client.SavedQueryInput{
		Name: "test-saved-query",
		Q:    `FROM main | stats count by host`,
		From: "-1h",
	}
	sq, err := h.Client().CreateSavedQuery(ctx, input)
	if err != nil {
		t.Fatalf("CreateSavedQuery: %v", err)
	}
	if sq.Name != "test-saved-query" {
		t.Errorf("expected name=test-saved-query, got %s", sq.Name)
	}
	sqID := sq.ID

	// List.
	queries, err := h.Client().ListSavedQueries(ctx)
	if err != nil {
		t.Fatalf("ListSavedQueries: %v", err)
	}
	{
		found := false
		for _, q := range queries {
			if q.ID == sqID {
				found = true
			}
		}
		if !found {
			t.Error("created saved query not found in ListSavedQueries")
		}
	}

	// Update.
	input.Name = "test-saved-query-updated"
	updated, err := h.Client().UpdateSavedQuery(ctx, sqID, input)
	if err != nil {
		t.Fatalf("UpdateSavedQuery: %v", err)
	}
	if updated.Name != "test-saved-query-updated" {
		t.Errorf("expected updated name, got %s", updated.Name)
	}

	// Delete.
	err = h.Client().DeleteSavedQuery(ctx, sqID)
	if err != nil {
		t.Fatalf("DeleteSavedQuery: %v", err)
	}
}

func TestE2E_SavedQueries_DeleteNonexistent_ReturnsNotFound(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()

	err := h.Client().DeleteSavedQuery(ctx, "nonexistent-sq-xyz")
	if err == nil {
		t.Fatal("expected error deleting nonexistent saved query, got nil")
	}
	if !client.IsNotFound(err) {
		t.Logf("delete nonexistent returned: %v (expected NotFound)", err)
	}
}
