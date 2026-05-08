package segment

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func FuzzWriter_BSISection_NumericInputs_DoNotPanicAndCRCValid(f *testing.F) {
	f.Add(int64(0), uint64(0), int64(1))
	f.Add(int64(math.MinInt64), uint64(0x7ff8000000000001), int64(math.MaxInt64))
	f.Add(int64(-1000), uint64(0xfff0000000000000), int64(1000))
	f.Add(int64(42), uint64(0x7ff0000000000000), int64(42))

	f.Fuzz(func(t *testing.T, baseInt int64, floatBits uint64, delta int64) {
		base := time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)
		floatValue := math.Float64frombits(floatBits)
		events := make([]*event.Event, 64)
		for i := range events {
			intValue := int64(uint64(baseInt) + uint64(delta)*uint64(i+1))
			e := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("value=%d row=%d", intValue, i))
			e.Host = "fuzz-host"
			e.Source = "/tmp/fuzz.log"
			e.SourceType = "json"
			e.Index = "main"
			e.SetField("value", event.IntValue(intValue))
			e.SetField("ratio", event.FloatValue(floatValue+float64(i%3)))
			events[i] = e
		}

		data := writeRangeBSISegment(t, events, func(w *Writer) {
			w.SetIndexConfig(IndexConfig{
				ProfileOverrides: map[string]IndexProfile{
					"_time": IndexProfileRangeBSI,
					"value": IndexProfileRangeBSI,
					"ratio": IndexProfileRangeBSI,
				},
				BSIMaxBitCount: 64,
			})
		})
		_, sections := rangeSectionsFromFooter(t, data)
		for _, sectionBytes := range sections {
			if len(sectionBytes) == 0 {
				continue
			}
			section := parseRangeSectionSegmentTest(t, sectionBytes)
			for _, entry := range section.Entries {
				assertRangeEntryCRCValidSegmentTest(t, entry)
				_ = decodeRangeBSIEntrySegmentTest(t, entry)
			}
		}
	})
}
