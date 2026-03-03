//go:build e2e

package e2e

import (
	"context"
	"testing"
)

func TestE2E_Health_ReturnsHealthy(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()

	result, err := h.Client().Health(ctx)
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	// Previously broken: Client.Health() did not unwrap the {"data": ...}
	// envelope. Fixed in pkg/client/status.go — Health() now correctly
	// decodes via the envelope struct.
	if result.Status != "healthy" {
		t.Errorf("expected status=healthy, got %q", result.Status)
	}
}

func TestE2E_Status_ReturnsServerInfo(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()

	status, err := h.Client().Status(ctx)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.Version == "" {
		t.Error("expected non-empty version")
	}
	if status.Health != "healthy" {
		t.Errorf("expected health=healthy, got %q", status.Health)
	}
}

func TestE2E_Stats_ReturnsNonNil(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()

	stats, err := h.Client().Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats == nil {
		t.Fatal("expected non-nil stats result")
	}
}

func TestE2E_Metrics_ReturnsNonNil(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()

	metrics, err := h.Client().Metrics(ctx)
	if err != nil {
		t.Fatalf("Metrics: %v", err)
	}
	if metrics == nil {
		t.Fatal("expected non-nil metrics result")
	}
}
