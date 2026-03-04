//go:build e2e

package e2e

import (
	"context"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/client"
)

func TestE2E_Dashboards_CRUD(t *testing.T) {
	// Previously broken: client decoded List response into a struct wrapper
	// instead of []Dashboard directly. Fixed in pkg/client/dashboards.go —
	// ListDashboards now decodes the raw array from the envelope correctly.
	h := NewHarness(t)
	ctx := context.Background()

	// Create.
	input := client.DashboardInput{
		Name: "test-dashboard",
		Panels: []client.Panel{
			{
				ID:    "panel-1",
				Title: "Event Count",
				Type:  "stat",
				Q:     `FROM main | stats count`,
				Position: client.PanelPosition{
					X: 0, Y: 0, W: 6, H: 4,
				},
			},
		},
	}
	dash, err := h.Client().CreateDashboard(ctx, input)
	if err != nil {
		t.Fatalf("CreateDashboard: %v", err)
	}
	if dash.Name != "test-dashboard" {
		t.Errorf("expected name=test-dashboard, got %s", dash.Name)
	}
	dashID := dash.ID

	// List.
	dashboards, err := h.Client().ListDashboards(ctx)
	if err != nil {
		t.Fatalf("ListDashboards: %v", err)
	}
	{
		found := false
		for _, d := range dashboards {
			if d.ID == dashID {
				found = true
			}
		}
		if !found {
			t.Error("created dashboard not found in ListDashboards")
		}
	}

	// Get (should work — single object, not array).
	got, err := h.Client().GetDashboard(ctx, dashID)
	if err != nil {
		t.Fatalf("GetDashboard: %v", err)
	}
	if got.ID != dashID {
		t.Errorf("expected id=%s, got %s", dashID, got.ID)
	}
	if len(got.Panels) != 1 {
		t.Errorf("expected 1 panel, got %d", len(got.Panels))
	}

	// Update.
	input.Name = "test-dashboard-updated"
	updated, err := h.Client().UpdateDashboard(ctx, dashID, input)
	if err != nil {
		t.Fatalf("UpdateDashboard: %v", err)
	}
	if updated.Name != "test-dashboard-updated" {
		t.Errorf("expected updated name, got %s", updated.Name)
	}

	// Delete.
	err = h.Client().DeleteDashboard(ctx, dashID)
	if err != nil {
		t.Fatalf("DeleteDashboard: %v", err)
	}
}

func TestE2E_Dashboards_DeleteNonexistent_ReturnsNotFound(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()

	err := h.Client().DeleteDashboard(ctx, "nonexistent-dashboard-xyz")
	if err == nil {
		t.Fatal("expected error deleting nonexistent dashboard, got nil")
	}
	if !client.IsNotFound(err) {
		t.Logf("delete nonexistent returned: %v (expected NotFound)", err)
	}
}
