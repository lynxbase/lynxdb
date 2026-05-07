package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSigmaCompatCheck_PrintsSummary(t *testing.T) {
	stdout, _, err := runCmd(t, "sigma", "compat-check")
	if err != nil {
		t.Fatalf("compat-check failed: %v", err)
	}
	for _, want := range []string{"rsigma compatibility: 0.9.0", "fixtures:"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("compat-check output missing %q:\n%s", want, stdout)
		}
	}
}

func TestSigmaCompatCheck_RsigmaVersion(t *testing.T) {
	stdout, _, err := runCmd(t, "sigma", "compat-check", "--rsigma-version", "v0.9.0")
	if err != nil {
		t.Fatalf("compat-check version failed: %v", err)
	}
	if !strings.Contains(stdout, "compatible with rsigma v0.9.0") {
		t.Fatalf("unexpected compat output:\n%s", stdout)
	}

	_, _, err = runCmd(t, "sigma", "compat-check", "--rsigma-version", "0.10.0")
	if err == nil {
		t.Fatal("expected incompatible rsigma version to fail")
	}
}

func TestSigmaCompatCheck_JSON(t *testing.T) {
	stdout, _, err := runCmd(t, "sigma", "compat-check", "--json")
	if err != nil {
		t.Fatalf("compat-check json failed: %v", err)
	}
	var manifest map[string]interface{}
	if err := json.Unmarshal([]byte(stdout), &manifest); err != nil {
		t.Fatalf("parse compat manifest JSON: %v\n%s", err, stdout)
	}
	if manifest["rsigma_version"] != "0.9.0" {
		t.Fatalf("rsigma_version = %v", manifest["rsigma_version"])
	}
}
