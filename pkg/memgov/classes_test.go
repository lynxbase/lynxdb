package memgov

import "testing"

func TestUnit_MemoryClass_String_AllClasses(t *testing.T) {
	classes := []struct {
		class MemoryClass
		want  string
	}{
		{ClassNonRevocable, "non-revocable"},
		{ClassRevocable, "revocable"},
		{ClassSpillable, "spillable"},
		{ClassPageCache, "page-cache"},
		{ClassMetadata, "metadata"},
		{ClassTempIO, "temp-io"},
	}

	for _, tc := range classes {
		got := tc.class.String()
		if got != tc.want {
			t.Errorf("MemoryClass(%d).String() = %q, want %q", tc.class, got, tc.want)
		}
		if got == "unknown" {
			t.Errorf("MemoryClass(%d).String() = %q, valid classes must not be unknown", tc.class, got)
		}
	}
}

func TestUnit_MemoryClass_String_OutOfRange_ReturnsUnknown(t *testing.T) {
	invalid := MemoryClass(99)
	got := invalid.String()
	if got != "unknown" {
		t.Errorf("MemoryClass(99).String() = %q, want %q", got, "unknown")
	}

	negative := MemoryClass(-1)
	got = negative.String()
	if got != "unknown" {
		t.Errorf("MemoryClass(-1).String() = %q, want %q", got, "unknown")
	}
}

func TestUnit_MemoryClass_NumClasses_Is6(t *testing.T) {
	// Guard against accidentally adding/removing a class without updating tests.
	if numClasses != 6 {
		t.Errorf("numClasses = %d, want 6 -- if you added a new class, update tests", numClasses)
	}
}

func TestUnit_ClassStats_ZeroValue(t *testing.T) {
	var cs ClassStats
	if cs.Allocated != 0 || cs.Peak != 0 || cs.Limit != 0 {
		t.Errorf("zero-value ClassStats should have all fields zero, got %+v", cs)
	}
}

func TestUnit_TotalStats_ZeroValue(t *testing.T) {
	var ts TotalStats
	if ts.Allocated != 0 || ts.Peak != 0 || ts.Limit != 0 {
		t.Errorf("zero-value TotalStats should have all fields zero, got %+v", ts)
	}
	for i := MemoryClass(0); i < numClasses; i++ {
		if ts.ByClass[i] != (ClassStats{}) {
			t.Errorf("zero-value TotalStats.ByClass[%d] should be zero, got %+v", i, ts.ByClass[i])
		}
	}
}
