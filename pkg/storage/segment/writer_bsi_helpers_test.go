package segment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"testing"
	"time"

	bsi "github.com/RoaringBitmap/roaring/BitSliceIndexing"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/storage/segment/index"
)

type parsedRangeSectionSegmentTest struct {
	Count   uint16
	Entries []parsedRangeEntrySegmentTest
}

type parsedRangeEntrySegmentTest struct {
	Name       string
	Layout     uint8
	BitCount   uint8
	MinValue   int64
	MaxValue   int64
	ValueKind  uint8
	Payload    []byte
	CRC        uint32
	crcPayload []byte
}

func makeRangeBSIEvents(t *testing.T, n int) []*event.Event {
	t.Helper()
	base := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	events := make([]*event.Event, n)
	for i := 0; i < n; i++ {
		duration := int64((i*7919)%200000 + i/7)
		latency := math.Log1p(float64(i+1)) * 37.25
		ts := base.Add(time.Duration(i*91+17) * time.Millisecond)
		e := event.NewEvent(ts, fmt.Sprintf("duration_ms=%d latency=%.6f row=%d", duration, latency, i))
		e.Host = fmt.Sprintf("host-%02d", i%17)
		e.Source = "/var/log/range-bsi.log"
		e.SourceType = "json"
		e.Index = "main"
		e.SetField("duration_ms", event.IntValue(duration))
		e.SetField("latency", event.FloatValue(latency))
		e.SetField("level", event.StringValue(fmt.Sprintf("level-%d", i%5)))
		events[i] = e
	}
	return events
}

func writeRangeBSISegment(t *testing.T, events []*event.Event, configure func(*Writer)) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.SetRowGroupSize(512)
	if configure != nil {
		configure(w)
	}
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}
	return append([]byte(nil), buf.Bytes()...)
}

func rangeSectionsFromFooter(t *testing.T, data []byte) (*Footer, [][]byte) {
	t.Helper()
	footer, err := DecodeFooter(data)
	if err != nil {
		t.Fatalf("DecodeFooter: %v", err)
	}
	sections := make([][]byte, len(footer.RowGroups))
	for i, rg := range footer.RowGroups {
		if rg.PerColumnRangeLength == 0 {
			continue
		}
		start := int(rg.PerColumnRangeOffset)
		end := start + int(rg.PerColumnRangeLength)
		if start < 0 || end < start || end > len(data) {
			t.Fatalf("row group %d range section [%d,%d) outside segment length %d", i, start, end, len(data))
		}
		sections[i] = append([]byte(nil), data[start:end]...)
	}
	return footer, sections
}

func parseRangeSectionSegmentTest(t *testing.T, data []byte) parsedRangeSectionSegmentTest {
	t.Helper()
	if len(data) < index.RangeSectionHeaderSize {
		t.Fatalf("range section length = %d, want at least %d", len(data), index.RangeSectionHeaderSize)
	}
	if string(data[:4]) != index.RangeBitmapMagic {
		t.Fatalf("range section magic = %q, want %q", data[:4], index.RangeBitmapMagic)
	}
	if !bytes.Equal(data[4:8], []byte{0, 0, 0, 0}) {
		t.Fatalf("range section reserved = %x, want zeroes", data[4:8])
	}
	count := binary.LittleEndian.Uint16(data[8:10])
	if !bytes.Equal(data[10:index.RangeSectionHeaderSize], []byte{0, 0, 0, 0, 0, 0}) {
		t.Fatalf("range section padding = %x, want zeroes", data[10:index.RangeSectionHeaderSize])
	}

	pos := index.RangeSectionHeaderSize
	entries := make([]parsedRangeEntrySegmentTest, 0, count)
	for i := uint16(0); i < count; i++ {
		entryStart := pos
		if pos+2 > len(data) {
			t.Fatalf("entry %d truncated before name length", i)
		}
		nameLen := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if pos+nameLen+1+1+8+8+1+4 > len(data) {
			t.Fatalf("entry %d truncated before payload", i)
		}
		name := string(data[pos : pos+nameLen])
		pos += nameLen
		layout := data[pos]
		pos++
		bitCount := data[pos]
		pos++
		minValue := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
		pos += 8
		maxValue := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
		pos += 8
		valueKind := data[pos]
		pos++
		payloadLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if pos+payloadLen+4 > len(data) {
			t.Fatalf("entry %d payload length %d exceeds section", i, payloadLen)
		}
		payload := append([]byte(nil), data[pos:pos+payloadLen]...)
		pos += payloadLen
		crcPayload := append([]byte(nil), data[entryStart:pos]...)
		crc := binary.LittleEndian.Uint32(data[pos : pos+4])
		pos += 4

		entries = append(entries, parsedRangeEntrySegmentTest{
			Name:       name,
			Layout:     layout,
			BitCount:   bitCount,
			MinValue:   minValue,
			MaxValue:   maxValue,
			ValueKind:  valueKind,
			Payload:    payload,
			CRC:        crc,
			crcPayload: crcPayload,
		})
	}
	if pos != len(data) {
		t.Fatalf("range section has %d trailing bytes", len(data)-pos)
	}
	return parsedRangeSectionSegmentTest{Count: count, Entries: entries}
}

func assertRangeEntryCRCValidSegmentTest(t *testing.T, entry parsedRangeEntrySegmentTest) {
	t.Helper()
	if got := crc32.ChecksumIEEE(entry.crcPayload); got != entry.CRC {
		t.Fatalf("entry %q CRC = %#x, want %#x", entry.Name, got, entry.CRC)
	}
}

func decodeRangeBSIEntrySegmentTest(t *testing.T, entry parsedRangeEntrySegmentTest) *bsi.BSI {
	t.Helper()
	pos := 0
	var frames [][]byte
	for pos < len(entry.Payload) {
		if pos+4 > len(entry.Payload) {
			t.Fatalf("entry %q payload truncated before frame length", entry.Name)
		}
		frameLen := int(binary.LittleEndian.Uint32(entry.Payload[pos : pos+4]))
		pos += 4
		if pos+frameLen > len(entry.Payload) {
			t.Fatalf("entry %q payload frame length %d exceeds payload", entry.Name, frameLen)
		}
		frames = append(frames, append([]byte(nil), entry.Payload[pos:pos+frameLen]...))
		pos += frameLen
	}
	decoded := bsi.NewDefaultBSI()
	if err := decoded.UnmarshalBinary(frames); err != nil {
		t.Fatalf("entry %q UnmarshalBinary: %v", entry.Name, err)
	}
	return decoded
}

func rangeEntryByNameSegmentTest(t *testing.T, section parsedRangeSectionSegmentTest, name string) parsedRangeEntrySegmentTest {
	t.Helper()
	for _, entry := range section.Entries {
		if entry.Name == name {
			return entry
		}
	}
	t.Fatalf("range entry %q not found in section %+v", name, section.Entries)
	return parsedRangeEntrySegmentTest{}
}

func catalogEntryByNameSegmentTest(t *testing.T, footer *Footer, name string) CatalogEntry {
	t.Helper()
	for _, cat := range footer.Catalog {
		if cat.Name == name {
			return cat
		}
	}
	t.Fatalf("catalog entry %q not found", name)
	return CatalogEntry{}
}
