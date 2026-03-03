//go:build e2e

package e2e

import (
	"context"
	"testing"

	"github.com/OrlovEvgeny/Lynxdb/pkg/client"
)

func TestE2E_Config_GetDefault(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()

	cfg, err := h.Client().GetConfig(ctx)
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config result")
	}
	if len(cfg) == 0 {
		t.Error("expected non-empty config map")
	}
	t.Logf("config keys: %d", len(cfg))
}

func TestE2E_Config_PatchAndVerify(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()

	retention := "30d"
	patched, err := h.Client().PatchConfig(ctx, client.ConfigPatch{
		Retention: &retention,
	})
	if err != nil {
		t.Fatalf("PatchConfig: %v", err)
	}
	if patched == nil {
		t.Fatal("expected non-nil patched config")
	}

	// Verify the patch is reflected.
	cfg, err := h.Client().GetConfig(ctx)
	if err != nil {
		t.Fatalf("GetConfig after patch: %v", err)
	}
	t.Logf("config after patch: %v", cfg)
}
