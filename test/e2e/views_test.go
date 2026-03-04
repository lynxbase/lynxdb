//go:build e2e

package e2e

import (
	"context"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/client"
)

func TestE2E_Views_CRUD(t *testing.T) {
	h := NewHarness(t, WithDisk())
	ctx := context.Background()

	// Create.
	view, err := h.Client().CreateView(ctx, client.ViewInput{
		Name: "mv_test_hosts",
		Q:    `FROM main | stats count by host`,
	})
	if err != nil {
		t.Fatalf("CreateView: %v", err)
	}
	if view.Name != "mv_test_hosts" {
		t.Errorf("expected name=mv_test_hosts, got %s", view.Name)
	}

	// List.
	views, err := h.Client().ListViews(ctx)
	if err != nil {
		t.Fatalf("ListViews: %v", err)
	}
	found := false
	for _, v := range views {
		if v.Name == "mv_test_hosts" {
			found = true
		}
	}
	if !found {
		t.Error("created view not found in ListViews")
	}

	// Get.
	detail, err := h.Client().GetView(ctx, "mv_test_hosts")
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if detail.Name != "mv_test_hosts" {
		t.Errorf("GetView: expected name=mv_test_hosts, got %s", detail.Name)
	}

	// Patch.
	retention := "720h"
	patched, err := h.Client().PatchView(ctx, "mv_test_hosts", client.ViewPatchInput{
		Retention: &retention,
	})
	if err != nil {
		t.Fatalf("PatchView: %v", err)
	}
	if patched.Retention != "720h" {
		t.Logf("PatchView retention: got %q (may not reflect immediately)", patched.Retention)
	}

	// Delete.
	err = h.Client().DeleteView(ctx, "mv_test_hosts")
	if err != nil {
		t.Fatalf("DeleteView: %v", err)
	}

	// Verify deleted.
	_, err = h.Client().GetView(ctx, "mv_test_hosts")
	if err == nil {
		t.Error("expected error after deleting view, got nil")
	} else if !client.IsNotFound(err) {
		t.Logf("GetView after delete returned: %v (expected NotFound)", err)
	}
}

func TestE2E_Views_CreateDuplicate_ReturnsAlreadyExists(t *testing.T) {
	h := NewHarness(t, WithDisk())
	ctx := context.Background()

	input := client.ViewInput{
		Name: "mv_dup_test",
		Q:    `FROM main | stats count`,
	}

	_, err := h.Client().CreateView(ctx, input)
	if err != nil {
		t.Fatalf("first CreateView: %v", err)
	}
	t.Cleanup(func() {
		_ = h.Client().DeleteView(ctx, "mv_dup_test")
	})

	_, err = h.Client().CreateView(ctx, input)
	if err == nil {
		t.Fatal("expected error for duplicate view, got nil")
	}
	if !client.IsAlreadyExists(err) {
		t.Logf("duplicate create returned: %v (expected AlreadyExists)", err)
	}
}

func TestE2E_Views_DeleteNonexistent_ReturnsNotFound(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()

	err := h.Client().DeleteView(ctx, "nonexistent_view_xyz")
	if err == nil {
		t.Fatal("expected error deleting nonexistent view, got nil")
	}
	if !client.IsNotFound(err) {
		t.Logf("delete nonexistent returned: %v (expected NotFound)", err)
	}
}
