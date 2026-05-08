package segment

import (
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
)

type footerV2RoundTripCase struct {
	Footer *Footer
}

func (footerV2RoundTripCase) Generate(r *rand.Rand, _ int) reflect.Value {
	rgCount := r.Intn(33)
	catCount := r.Intn(33)

	f := Footer{
		EventCount:         int64(r.Intn(10000)),
		RowGroups:          make([]RowGroupMeta, rgCount),
		InvertedOffset:     int64(r.Intn(1 << 16)),
		InvertedLength:     int64(r.Intn(1 << 12)),
		PrimaryIndexOffset: int64(r.Intn(1 << 16)),
		PrimaryIndexLength: int64(r.Intn(1 << 12)),
		Catalog:            make([]CatalogEntry, catCount),
	}

	for i := range f.RowGroups {
		colCount := r.Intn(5)
		constCount := r.Intn(3)
		rg := RowGroupMeta{
			RowCount:             r.Intn(1000),
			ColumnPresenceBits:   r.Uint64(),
			PerColumnBloomOffset: int64(r.Intn(1 << 16)),
			PerColumnBloomLength: int64(r.Intn(1 << 12)),
			Columns:              make([]ColumnChunkMeta, colCount),
			ConstColumns:         make([]ConstColumnEntry, constCount),
		}
		if r.Intn(4) == 0 {
			rg.RequiredCapabilities = CapBit_ColumnZSTD
		}
		if r.Intn(3) == 0 {
			rg.PerColumnRangeOffset = int64(4096 + r.Intn(1<<16))
			rg.PerColumnRangeLength = int64(1 + r.Intn(4096))
		}
		for j := range rg.Columns {
			rg.Columns[j] = ColumnChunkMeta{
				Name:         shortName("col", i, j),
				EncodingType: uint8(r.Intn(4)),
				Compression:  CompressionType(r.Intn(3)),
				Offset:       int64(r.Intn(1 << 16)),
				Length:       int64(r.Intn(1 << 12)),
				RawSize:      int64(r.Intn(1 << 12)),
				CRC32:        r.Uint32(),
				MinValue:     shortName("min", i, j),
				MaxValue:     shortName("max", i, j),
				Count:        int64(r.Intn(1000)),
				NullCount:    int64(r.Intn(100)),
			}
		}
		for j := range rg.ConstColumns {
			rg.ConstColumns[j] = ConstColumnEntry{
				Name:         shortName("const", i, j),
				EncodingType: uint8(r.Intn(4)),
				Value:        shortName("value", i, j),
			}
		}
		f.RowGroups[i] = rg
	}
	for i := range f.Catalog {
		f.Catalog[i] = CatalogEntry{
			Name:         shortName("catalog", i, r.Intn(1000)),
			DominantType: uint8(r.Intn(4)),
			IndexProfile: IndexProfile(r.Intn(4)),
		}
	}
	f.RequiredCaps, f.OptionalCaps = aggregateCapabilities(f.RowGroups)

	return reflect.ValueOf(footerV2RoundTripCase{Footer: &f})
}

func TestProperty_FooterV2_RandomCatalogAndRowGroups_RoundTrip(t *testing.T) {
	cfg := &quick.Config{MaxCount: 200}

	err := quick.Check(func(tc footerV2RoundTripCase) bool {
		got, err := decodeFooterV2(encodeFooterV2(tc.Footer))
		if err != nil {
			t.Logf("decodeFooterV2: %v", err)
			return false
		}
		assertFooterEqual(t, tc.Footer, got)
		return true
	}, cfg)
	if err != nil {
		t.Fatal(err)
	}
}

func shortName(prefix string, a, b int) string {
	return prefix + "_" + string(rune('a'+a%26)) + "_" + string(rune('a'+b%26))
}
