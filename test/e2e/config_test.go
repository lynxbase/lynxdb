//go:build e2e

package e2e

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/client"
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

	req, err := http.NewRequest(http.MethodPatch, h.BaseURL()+"/api/v1/config", strings.NewReader(`{"log_level":"debug"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PATCH config: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PATCH log_level: status %d", resp.StatusCode)
	}

	// Retention is read when the database opens, so a patch of it is refused.
	retention := "30d"
	if _, err := h.Client().PatchConfig(ctx, client.ConfigPatch{Retention: &retention}); !client.IsValidationError(err) {
		t.Fatalf("PatchConfig retention: err = %v, want a validation error", err)
	}

	cfg, err := h.Client().GetConfig(ctx)
	if err != nil {
		t.Fatalf("GetConfig after patch: %v", err)
	}
	if cfg["log_level"] != "debug" {
		t.Errorf("expected log_level=debug after patch, got %v", cfg["log_level"])
	}
	if cfg["retention"] == retention {
		t.Errorf("retention = %v after a refused patch", cfg["retention"])
	}
}
