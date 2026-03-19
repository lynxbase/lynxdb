package bufmgr

import (
	"testing"
	"unsafe"
)

// allocTestFrame creates a Frame backed by a Go heap slice for testing.
func allocTestFrame(size int) *Frame {
	buf := make([]byte, size)
	f := &Frame{
		ID:       FrameID(1),
		data:     unsafe.Pointer(&buf[0]),
		size:     size,
		PageSize: size,
	}
	f.State.Store(int32(StateClean))
	return f
}

func TestUnit_Frame_WriteAt_ReadAt_RoundTrip(t *testing.T) {
	f := allocTestFrame(256)

	payload := []byte("test payload data 12345")
	err := f.WriteAt(payload, 10)
	if err != nil {
		t.Fatalf("WriteAt: %v", err)
	}

	readBuf := make([]byte, len(payload))
	err = f.ReadAt(readBuf, 10)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}

	if string(readBuf) != string(payload) {
		t.Fatalf("ReadAt returned %q; want %q", readBuf, payload)
	}
}

func TestUnit_Frame_WriteAt_OutOfBounds_ReturnsError(t *testing.T) {
	f := allocTestFrame(64)

	tests := []struct {
		name   string
		offset int
		data   []byte
	}{
		{"negative offset", -1, []byte("x")},
		{"offset past end", 65, []byte("x")},
		{"data overflows", 60, make([]byte, 10)},
		{"exactly at boundary", 64, []byte("x")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := f.WriteAt(tc.data, tc.offset)
			if err == nil {
				t.Fatalf("WriteAt(offset=%d, len=%d) should return error for frame size %d",
					tc.offset, len(tc.data), f.size)
			}
		})
	}
}

func TestUnit_Frame_ReadAt_OutOfBounds_ReturnsError(t *testing.T) {
	f := allocTestFrame(64)

	tests := []struct {
		name   string
		offset int
		dstLen int
	}{
		{"negative offset", -1, 1},
		{"offset past end", 65, 1},
		{"read overflows", 60, 10},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dst := make([]byte, tc.dstLen)
			err := f.ReadAt(dst, tc.offset)
			if err == nil {
				t.Fatalf("ReadAt(offset=%d, len=%d) should return error for frame size %d",
					tc.offset, tc.dstLen, f.size)
			}
		})
	}
}

func TestUnit_Frame_WriteAt_MarksDirty(t *testing.T) {
	f := allocTestFrame(64)
	f.State.Store(int32(StateClean))

	err := f.WriteAt([]byte("x"), 0)
	if err != nil {
		t.Fatalf("WriteAt: %v", err)
	}

	if !f.IsDirty() {
		t.Fatal("WriteAt did not mark frame as dirty")
	}
}

func TestUnit_Frame_PinUnpin_CountTracking(t *testing.T) {
	f := allocTestFrame(64)

	if f.IsPinned() {
		t.Fatal("fresh frame should not be pinned")
	}

	f.Pin()
	f.Pin()
	f.Pin()

	if f.PinCount.Load() != 3 {
		t.Fatalf("PinCount after 3 Pins = %d; want 3", f.PinCount.Load())
	}
	if !f.IsPinned() {
		t.Fatal("frame should be pinned after Pin()")
	}

	f.Unpin()
	f.Unpin()

	if f.PinCount.Load() != 1 {
		t.Fatalf("PinCount after 2 Unpins = %d; want 1", f.PinCount.Load())
	}
	if !f.IsPinned() {
		t.Fatal("frame should still be pinned with PinCount=1")
	}

	f.Unpin()
	if f.IsPinned() {
		t.Fatal("frame should not be pinned after all Unpins")
	}
}

func TestUnit_Frame_Unpin_BelowZero_ClampsToZero(t *testing.T) {
	f := allocTestFrame(64)

	// Unpin without prior Pin should clamp to 0.
	f.Unpin()
	if f.PinCount.Load() != 0 {
		t.Fatalf("PinCount after Unpin below zero = %d; want 0", f.PinCount.Load())
	}

	// Multiple unpins below zero should all clamp.
	f.Unpin()
	f.Unpin()
	if f.PinCount.Load() != 0 {
		t.Fatalf("PinCount after multiple sub-zero Unpins = %d; want 0", f.PinCount.Load())
	}
}

func TestUnit_Frame_Reset_ClearsAllState(t *testing.T) {
	f := allocTestFrame(64)

	// Set up a frame with non-default state.
	f.Pin()
	f.Pin()
	f.State.Store(int32(StateDirty))
	f.RefBit.Store(true)
	f.Owner = OwnerQuery
	f.Tag = "some-tag"
	f.SetMeta("some-meta")

	f.reset()

	if f.PinCount.Load() != 0 {
		t.Errorf("PinCount after reset = %d; want 0", f.PinCount.Load())
	}
	if FrameState(f.State.Load()) != StateFree {
		t.Errorf("State after reset = %s; want free", FrameState(f.State.Load()))
	}
	if f.RefBit.Load() {
		t.Error("RefBit after reset is true; want false")
	}
	if f.Owner != OwnerFree {
		t.Errorf("Owner after reset = %s; want free", f.Owner)
	}
	if f.Tag != "" {
		t.Errorf("Tag after reset = %q; want empty", f.Tag)
	}
	if f.Meta() != nil {
		t.Errorf("Meta after reset = %v; want nil", f.Meta())
	}
}

func TestUnit_Frame_DataSlice_NilData_ReturnsNil(t *testing.T) {
	f := &Frame{
		ID:   FrameID(1),
		data: nil,
		size: 64,
	}

	got := f.DataSlice()
	if got != nil {
		t.Fatalf("DataSlice with nil data returned non-nil slice of len %d", len(got))
	}
}

func TestUnit_Frame_Size_ReturnsConfiguredSize(t *testing.T) {
	f := allocTestFrame(1024)

	if f.Size() != 1024 {
		t.Fatalf("Size() = %d; want 1024", f.Size())
	}
}

func TestUnit_Frame_SetMeta_GetMeta_RoundTrip(t *testing.T) {
	f := allocTestFrame(64)

	type customMeta struct {
		SegmentID string
		Offset    int64
	}

	meta := &customMeta{SegmentID: "seg-42", Offset: 8192}
	f.SetMeta(meta)

	got, ok := f.Meta().(*customMeta)
	if !ok {
		t.Fatal("Meta() did not return *customMeta")
	}
	if got.SegmentID != "seg-42" || got.Offset != 8192 {
		t.Fatalf("Meta() = %+v; want {SegmentID:seg-42, Offset:8192}", got)
	}
}

func TestUnit_Frame_IsDirty_OnlyTrueForDirtyState(t *testing.T) {
	f := allocTestFrame(64)

	states := []struct {
		state FrameState
		dirty bool
	}{
		{StateFree, false},
		{StateLoading, false},
		{StateClean, false},
		{StateDirty, true},
		{StateWriteback, false},
		{StateEvicting, false},
	}

	for _, tc := range states {
		f.State.Store(int32(tc.state))
		if f.IsDirty() != tc.dirty {
			t.Errorf("IsDirty() with state %s = %v; want %v", tc.state, f.IsDirty(), tc.dirty)
		}
	}
}

func TestUnit_Frame_Pin_SetsRefBit(t *testing.T) {
	f := allocTestFrame(64)
	f.RefBit.Store(false)

	f.Pin()

	if !f.RefBit.Load() {
		t.Fatal("Pin() should set RefBit to true for clock second-chance")
	}
}

func TestUnit_FrameState_String_AllStates(t *testing.T) {
	tests := []struct {
		state FrameState
		want  string
	}{
		{StateFree, "free"},
		{StateLoading, "loading"},
		{StateClean, "clean"},
		{StateDirty, "dirty"},
		{StateWriteback, "writeback"},
		{StateEvicting, "evicting"},
		{FrameState(99), "unknown"},
	}

	for _, tc := range tests {
		got := tc.state.String()
		if got != tc.want {
			t.Errorf("FrameState(%d).String() = %q; want %q", tc.state, got, tc.want)
		}
	}
}

func TestUnit_FrameOwner_String_AllOwners(t *testing.T) {
	tests := []struct {
		owner FrameOwner
		want  string
	}{
		{OwnerFree, "free"},
		{OwnerSegCache, "seg-cache"},
		{OwnerQuery, "query"},
		{OwnerMemtable, "memtable"},
		{OwnerCompaction, "compaction"},
		{FrameOwner(99), "unknown"},
	}

	for _, tc := range tests {
		got := tc.owner.String()
		if got != tc.want {
			t.Errorf("FrameOwner(%d).String() = %q; want %q", tc.owner, got, tc.want)
		}
	}
}

func TestUnit_FrameRef_IsValid(t *testing.T) {
	ref := FrameRef{FrameID: 1, Offset: 0, Length: 100}
	if !ref.IsValid() {
		t.Fatal("FrameRef with Length>0 should be valid")
	}

	zeroRef := FrameRef{}
	if zeroRef.IsValid() {
		t.Fatal("zero-value FrameRef should not be valid")
	}
}

func TestUnit_Frame_WriteAt_ZeroLength_Succeeds(t *testing.T) {
	f := allocTestFrame(64)

	// Writing zero bytes at any valid offset should succeed.
	err := f.WriteAt([]byte{}, 0)
	if err != nil {
		t.Fatalf("WriteAt with empty data: %v", err)
	}

	err = f.WriteAt([]byte{}, 63)
	if err != nil {
		t.Fatalf("WriteAt with empty data at offset 63: %v", err)
	}
}

func TestUnit_Frame_ReadAt_ZeroLength_Succeeds(t *testing.T) {
	f := allocTestFrame(64)

	err := f.ReadAt([]byte{}, 0)
	if err != nil {
		t.Fatalf("ReadAt with empty buffer: %v", err)
	}
}

func TestUnit_Frame_WriteAt_FullFrame(t *testing.T) {
	f := allocTestFrame(64)

	data := make([]byte, 64)
	for i := range data {
		data[i] = byte(i)
	}

	err := f.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("WriteAt full frame: %v", err)
	}

	readBuf := make([]byte, 64)
	err = f.ReadAt(readBuf, 0)
	if err != nil {
		t.Fatalf("ReadAt full frame: %v", err)
	}

	for i := range readBuf {
		if readBuf[i] != byte(i) {
			t.Fatalf("byte at offset %d = %d; want %d", i, readBuf[i], i)
		}
	}
}
