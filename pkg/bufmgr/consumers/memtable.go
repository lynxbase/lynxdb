package consumers

import (
	"fmt"
	"sync"

	"github.com/lynxbase/lynxdb/pkg/bufmgr"
)

// MemtableFrameWriter writes incoming events into buffer manager frames.
// Memtable frames are always marked dirty (they contain unflushed data).
//
// Thread-safe. Multiple ingest goroutines may call Append concurrently.
type MemtableFrameWriter struct {
	mu      sync.Mutex
	mgr     bufmgr.Manager
	frames  []*bufmgr.Frame
	current *bufmgr.Frame
	offset  int
}

// NewMemtableFrameWriter creates a writer backed by the buffer manager.
func NewMemtableFrameWriter(mgr bufmgr.Manager) *MemtableFrameWriter {
	return &MemtableFrameWriter{
		mgr: mgr,
	}
}

// Append writes data into the memtable frame buffer.
func (mw *MemtableFrameWriter) Append(data []byte) (bufmgr.FrameRef, error) {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	dataLen := len(data)
	if dataLen == 0 {
		return bufmgr.FrameRef{}, nil
	}

	if mw.current == nil || mw.offset+dataLen > mw.current.Size() {
		if err := mw.allocNewFrame(); err != nil {
			return bufmgr.FrameRef{}, err
		}
	}

	if dataLen > mw.current.Size() {
		return bufmgr.FrameRef{}, fmt.Errorf(
			"bufmgr.MemtableFrameWriter.Append: data size %d exceeds frame size %d",
			dataLen, mw.current.Size())
	}

	if err := mw.current.WriteAt(data, mw.offset); err != nil {
		return bufmgr.FrameRef{}, fmt.Errorf("bufmgr.MemtableFrameWriter.Append: %w", err)
	}

	ref := bufmgr.FrameRef{
		FrameID: mw.current.ID,
		Offset:  mw.offset,
		Length:  dataLen,
	}
	mw.offset += dataLen

	return ref, nil
}

// FrameCount returns the number of frames holding memtable data.
func (mw *MemtableFrameWriter) FrameCount() int {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	return len(mw.frames)
}

// BytesWritten returns the total bytes written across all frames.
func (mw *MemtableFrameWriter) BytesWritten() int64 {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	if len(mw.frames) == 0 {
		return 0
	}

	frameSize := mw.mgr.FrameSize()

	return int64(len(mw.frames)-1)*int64(frameSize) + int64(mw.offset)
}

// ReleaseAll frees all memtable frames back to the manager.
func (mw *MemtableFrameWriter) ReleaseAll() {
	mw.mu.Lock()
	frames := mw.frames
	mw.frames = nil
	mw.current = nil
	mw.offset = 0
	mw.mu.Unlock()

	for _, f := range frames {
		for f.PinCount.Load() > 0 {
			f.Unpin()
		}
	}
}

func (mw *MemtableFrameWriter) allocNewFrame() error {
	if mw.current != nil {
		mw.current.Unpin()
	}

	f, err := mw.mgr.AllocFrame(bufmgr.OwnerMemtable, "memtable")
	if err != nil {
		return fmt.Errorf("bufmgr.MemtableFrameWriter.allocNewFrame: %w", err)
	}

	// Mark dirty immediately — unflushed data.
	f.State.Store(int32(bufmgr.StateDirty))

	mw.current = f
	mw.offset = 0
	mw.frames = append(mw.frames, f)

	return nil
}
