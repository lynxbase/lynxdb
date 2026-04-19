//go:build e2e

package e2e

import (
	"context"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/client"
)

func TestE2E_Alerts_CRUD(t *testing.T) {
	// Previously broken: client AlertInput serialized query as json:"q" but
	// server expected json:"query". Fixed in pkg/client/types.go — AlertInput.Q
	// now uses json:"query".
	h := NewHarness(t)
	ctx := context.Background()

	// Create.
	input := client.AlertInput{
		Name:     "test-alert",
		Q:        `FROM main | stats count | WHERE count > 100`,
		Interval: "5m",
		Channels: []client.NotificationChannel{
			{Type: "webhook", Config: map[string]interface{}{"url": "http://localhost:9999/hook"}},
		},
	}
	alert, err := h.Client().CreateAlert(ctx, input)
	if err != nil {
		t.Fatalf("CreateAlert: %v", err)
	}
	if alert.Name != "test-alert" {
		t.Errorf("expected name=test-alert, got %s", alert.Name)
	}
	if alert.Q != input.Q {
		t.Errorf("expected query=%q, got %q", input.Q, alert.Q)
	}
	alertID := alert.ID

	// List.
	alerts, err := h.Client().ListAlerts(ctx)
	if err != nil {
		t.Fatalf("ListAlerts: %v", err)
	}
	found := false
	for _, a := range alerts {
		if a.ID == alertID {
			found = true
			if a.Q != input.Q {
				t.Errorf("list query: got %q, want %q", a.Q, input.Q)
			}
		}
	}
	if !found {
		t.Error("created alert not found in ListAlerts")
	}

	// Get.
	got, err := h.Client().GetAlert(ctx, alertID)
	if err != nil {
		t.Fatalf("GetAlert: %v", err)
	}
	if got.ID != alertID {
		t.Errorf("expected id=%s, got %s", alertID, got.ID)
	}
	if got.Q != input.Q {
		t.Errorf("get query: got %q, want %q", got.Q, input.Q)
	}

	// Update.
	input.Name = "test-alert-updated"
	input.Q = `FROM main | stats count | WHERE count > 200`
	updated, err := h.Client().UpdateAlert(ctx, alertID, input)
	if err != nil {
		t.Fatalf("UpdateAlert: %v", err)
	}
	if updated.Name != "test-alert-updated" {
		t.Errorf("expected updated name=test-alert-updated, got %s", updated.Name)
	}
	if updated.Q != input.Q {
		t.Errorf("update query: got %q, want %q", updated.Q, input.Q)
	}

	disabled := false
	patched, err := h.Client().PatchAlert(ctx, alertID, client.AlertPatchInput{Enabled: &disabled})
	if err != nil {
		t.Fatalf("PatchAlert: %v", err)
	}
	if patched.Enabled {
		t.Error("expected patched alert to be disabled")
	}

	// Delete.
	err = h.Client().DeleteAlert(ctx, alertID)
	if err != nil {
		t.Fatalf("DeleteAlert: %v", err)
	}
}

func TestE2E_Alerts_TestDryRun(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()

	alert, err := h.Client().CreateAlert(ctx, client.AlertInput{
		Name:     "dryrun-alert",
		Q:        `FROM main | stats count`,
		Interval: "5m",
		Channels: []client.NotificationChannel{
			{Type: "webhook", Config: map[string]interface{}{"url": "http://localhost:9999/hook"}},
		},
	})
	if err != nil {
		t.Fatalf("CreateAlert: %v", err)
	}
	t.Cleanup(func() {
		_ = h.Client().DeleteAlert(ctx, alert.ID)
	})

	result, err := h.Client().TestAlert(ctx, alert.ID)
	if err != nil {
		t.Fatalf("TestAlert: %v", err)
	}
	// WouldTrigger can be true or false — we just verify the field is populated.
	t.Logf("TestAlert: would_trigger=%v, message=%s", result.WouldTrigger, result.Message)
}

func TestE2E_Alerts_DeleteNonexistent_ReturnsNotFound(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()

	err := h.Client().DeleteAlert(ctx, "nonexistent-alert-id-xyz")
	if err == nil {
		t.Fatal("expected error deleting nonexistent alert, got nil")
	}
	if !client.IsNotFound(err) {
		t.Errorf("delete nonexistent: expected NotFound, got %v", err)
	}
}
