package segment

import (
	"bytes"
	"testing"
)

func BenchmarkWriter_BSI_Disabled(b *testing.B) {
	events := generateTestEvents(10000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		w := NewWriter(&buf)
		w.SetIndexConfig(IndexConfig{DisableBSI: true})
		if _, err := w.Write(events); err != nil {
			b.Fatalf("Write: %v", err)
		}
	}
}

func BenchmarkWriter_BSI_EnabledWideNumeric(b *testing.B) {
	events := generateTestEvents(10000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		w := NewWriter(&buf)
		if _, err := w.Write(events); err != nil {
			b.Fatalf("Write: %v", err)
		}
	}
}
