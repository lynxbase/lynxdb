package model

import (
	"testing"
)

func TestDefaultIndexConfig(t *testing.T) {
	cfg := DefaultIndexConfig("main")
	if cfg.Name != "main" {
		t.Fatalf("expected name 'main', got %q", cfg.Name)
	}
}
