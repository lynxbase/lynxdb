package part

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
	"github.com/lynxbase/lynxdb/pkg/storage/segment/index"
)

func TestIntegration_PartStreamWriter_FinalizeVerifiesAndPromotesV2WithBSI(t *testing.T) {
	layout := NewLayout(t.TempDir())
	writer, err := NewPartStreamWriter(layout, "main", 1, WithFSync(false))
	if err != nil {
		t.Fatalf("NewPartStreamWriter: %v", err)
	}
	writer.SetRowGroupSize(512)

	events := makePartRangeBSIEvents(t, 1024, 100)
	if err := writer.WriteRowGroup(context.Background(), events); err != nil {
		t.Fatalf("WriteRowGroup: %v", err)
	}

	meta, err := writer.Finalize(context.Background())
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if meta.FormatMajor != segment.LSG_FORMAT_MAJOR_V2 {
		t.Fatalf("FormatMajor = %d, want %d", meta.FormatMajor, segment.LSG_FORMAT_MAJOR_V2)
	}
	if meta.BSIColumns == 0 {
		t.Fatal("BSIColumns = 0, want at least one range BSI column")
	}
	if meta.BSISectionBytes == 0 {
		t.Fatal("BSISectionBytes = 0, want non-empty range BSI sections")
	}
	assertNoTempPartFiles(t, filepath.Dir(meta.Path))

	ms, err := segment.OpenSegmentFile(meta.Path)
	if err != nil {
		t.Fatalf("OpenSegmentFile: %v", err)
	}
	defer ms.Close()
	reader := ms.Reader()
	if !reader.HasRangeBSI() {
		t.Fatal("HasRangeBSI() = false, want true")
	}
	if err := reader.VerifyAllRangeBSIs(); err != nil {
		t.Fatalf("VerifyAllRangeBSIs: %v", err)
	}
}

func TestIntegration_PartStreamWriter_DisableBSI_EmitsV2WithoutRangeSections(t *testing.T) {
	layout := NewLayout(t.TempDir())
	writer, err := NewPartStreamWriter(layout, "main", 1, WithFSync(false), WithDisableBSI(true))
	if err != nil {
		t.Fatalf("NewPartStreamWriter: %v", err)
	}

	if err := writer.WriteRowGroup(context.Background(), makePartRangeBSIEvents(t, 1024, 100)); err != nil {
		t.Fatalf("WriteRowGroup: %v", err)
	}
	meta, err := writer.Finalize(context.Background())
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if meta.FormatMajor != segment.LSG_FORMAT_MAJOR_V2 {
		t.Fatalf("FormatMajor = %d, want %d", meta.FormatMajor, segment.LSG_FORMAT_MAJOR_V2)
	}
	if meta.BSIColumns != 0 {
		t.Fatalf("BSIColumns = %d, want 0 when BSI is disabled", meta.BSIColumns)
	}
	if meta.BSISectionBytes != 0 {
		t.Fatalf("BSISectionBytes = %d, want 0 when BSI is disabled", meta.BSISectionBytes)
	}

	data, err := os.ReadFile(meta.Path)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", meta.Path, err)
	}
	reader, err := segment.OpenSegment(data)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	if reader.HasRangeBSI() {
		t.Fatal("HasRangeBSI() = true, want false when BSI is disabled")
	}
	footer, err := segment.DecodeFooter(data)
	if err != nil {
		t.Fatalf("DecodeFooter: %v", err)
	}
	for i, rg := range footer.RowGroups {
		if rg.PerColumnRangeLength != 0 {
			t.Fatalf("RowGroups[%d].PerColumnRangeLength = %d, want 0", i, rg.PerColumnRangeLength)
		}
	}
}

func TestUnit_VerifySegmentBeforePromote_CorruptRangeBSI_ReturnsVerificationError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tmp_corrupt.lsg")
	data := writePartRangeBSISegment(t, makePartRangeBSIEvents(t, 1024, 100))
	mutateFirstPartRangeBSIPayloadByte(t, data)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, _, _, err := verifySegmentBeforePromote(path)
	if err == nil {
		t.Fatal("verifySegmentBeforePromote returned nil, want verification error")
	}
	if !strings.Contains(err.Error(), "verify range BSI") {
		t.Fatalf("error = %v, want message to mention range BSI verification", err)
	}
	if !errors.Is(err, index.ErrRangeSectionCorrupt) {
		t.Fatalf("error = %v, want ErrRangeSectionCorrupt", err)
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("corrupt temp segment was removed by verifier; stat error: %v", statErr)
	}
}

func makePartRangeBSIEvents(t *testing.T, n int, statusBase int64) []*event.Event {
	t.Helper()
	base := time.Date(2030, 1, 2, 8, 0, 0, 0, time.UTC)
	events := make([]*event.Event, n)
	for i := 0; i < n; i++ {
		status := statusBase + int64(i)
		e := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("status=%d bytes=%d", status, i*64))
		e.Host = "part-bsi-host"
		e.Source = "/var/log/part-bsi.log"
		e.SourceType = "json"
		e.Index = "main"
		e.SetField("status", event.IntValue(status))
		e.SetField("bytes", event.IntValue(int64(i*64)))
		events[i] = e
	}
	return events
}

func writePartRangeBSISegment(t *testing.T, events []*event.Event) []byte {
	t.Helper()
	var buf bytes.Buffer
	writer := segment.NewWriter(&buf)
	writer.SetRowGroupSize(512)
	if _, err := writer.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}
	return append([]byte(nil), buf.Bytes()...)
}

func mutateFirstPartRangeBSIPayloadByte(t *testing.T, data []byte) {
	t.Helper()
	footer, err := segment.DecodeFooter(data)
	if err != nil {
		t.Fatalf("DecodeFooter: %v", err)
	}
	for rgIdx, rg := range footer.RowGroups {
		if rg.PerColumnRangeLength <= index.RangeSectionHeaderSize {
			continue
		}
		start := int(rg.PerColumnRangeOffset)
		end := start + int(rg.PerColumnRangeLength)
		if start < 0 || end > len(data) {
			t.Fatalf("row group %d range section [%d,%d) outside segment length %d", rgIdx, start, end, len(data))
		}
		payloadOffset, payloadLength := firstPartRangeEntryPayload(t, data[start:end])
		if payloadLength == 0 {
			t.Fatalf("row group %d first range BSI payload is empty", rgIdx)
		}
		data[start+payloadOffset+payloadLength/2] ^= 0xff
		return
	}
	t.Fatal("no non-empty range BSI section found")
}

func firstPartRangeEntryPayload(t *testing.T, section []byte) (offset int, length int) {
	t.Helper()
	if len(section) < index.RangeSectionHeaderSize {
		t.Fatalf("range section length = %d, want at least %d", len(section), index.RangeSectionHeaderSize)
	}
	pos := index.RangeSectionHeaderSize
	if pos+2 > len(section) {
		t.Fatal("range entry truncated before name length")
	}
	nameLen := int(binary.LittleEndian.Uint16(section[pos : pos+2]))
	pos += 2
	const fixedAfterNameBeforePayloadLen = 1 + 1 + 8 + 8 + 1
	if pos+nameLen+fixedAfterNameBeforePayloadLen+4 > len(section) {
		t.Fatal("range entry truncated before payload length")
	}
	pos += nameLen + fixedAfterNameBeforePayloadLen
	payloadLen := int(binary.LittleEndian.Uint32(section[pos : pos+4]))
	pos += 4
	if pos+payloadLen+4 > len(section) {
		t.Fatal("range entry payload exceeds section")
	}
	return pos, payloadLen
}

func assertNoTempPartFiles(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", dir, err)
	}
	for _, entry := range entries {
		if IsTempFile(entry.Name()) {
			t.Fatalf("found leftover temp part file %s", entry.Name())
		}
	}
}
