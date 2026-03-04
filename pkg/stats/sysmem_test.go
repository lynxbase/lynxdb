package stats

import (
	"runtime"
	"testing"
)

func TestTotalSystemMemory_Sanity(t *testing.T) {
	mem := TotalSystemMemory()

	switch runtime.GOOS {
	case "linux", "darwin":
		if mem <= 0 {
			t.Fatalf("expected positive memory on %s, got %d", runtime.GOOS, mem)
		}
		// Sanity: at least 128MB and less than 1PB.
		if mem < 128*1024*1024 {
			t.Fatalf("memory too low to be real: %d bytes", mem)
		}
		if mem > 1<<50 {
			t.Fatalf("memory suspiciously high: %d bytes", mem)
		}
		t.Logf("TotalSystemMemory() = %d bytes (%.1f GB)", mem, float64(mem)/(1<<30))
	case "windows":
		if mem <= 0 {
			t.Fatalf("expected positive memory on windows, got %d", mem)
		}
		if mem < 128*1024*1024 {
			t.Fatalf("memory too low to be real: %d bytes", mem)
		}
		if mem > 1<<50 {
			t.Fatalf("memory suspiciously high: %d bytes", mem)
		}
		t.Logf("TotalSystemMemory() = %d bytes (%.1f GB)", mem, float64(mem)/(1<<30))
	}
}

func TestEphemeralMemoryLimit_HalfOfTotal(t *testing.T) {
	total := TotalSystemMemory()
	limit := EphemeralMemoryLimit()

	switch runtime.GOOS {
	case "linux", "darwin":
		if total <= 0 {
			t.Fatal("system memory detection failed — expected positive total memory")
		}
		expected := total / 2
		if limit != expected {
			t.Fatalf("EphemeralMemoryLimit() = %d, want %d (half of %d)", limit, expected, total)
		}
	case "windows":
		if total <= 0 {
			t.Fatal("system memory detection failed — expected positive total memory")
		}
		expected := total / 2
		if limit != expected {
			t.Fatalf("EphemeralMemoryLimit() = %d, want %d (half of %d)", limit, expected, total)
		}
	}
}
