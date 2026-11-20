package tokenfilter

import (
	"math/rand"
	"testing"
)

// BenchmarkFilter builds and probes the filter of a block's column of 16384
// distinct tokens, per token.
func BenchmarkFilter(b *testing.B) {
	r := rand.New(rand.NewSource(1))
	keys := make([]uint64, 16384)
	for i := range keys {
		keys[i] = r.Uint64()
	}
	cells := Cells(len(keys), 128)
	per := Blocks(len(keys), cells, ColumnFalse)
	var s Scratch
	var f []byte
	b.Run("build", func(b *testing.B) {
		for b.Loop() {
			f = AppendFilter(f[:0], keys, cells, per, &s)
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(keys)), "ns/token")
	})
	f = AppendFilter(f[:0], keys, cells, per, &s)
	probes := make([]uint64, 4096)
	for i := range probes {
		probes[i] = r.Uint64()
	}
	b.Run("probe", func(b *testing.B) {
		i := 0
		for b.Loop() {
			MayContain(f, probes[i&(len(probes)-1)], cells)
			i++
		}
	})
}
