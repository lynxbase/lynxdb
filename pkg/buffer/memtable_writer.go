package buffer

import (
	"fmt"
	"sync"
)

// MemtablePageWriter writes incoming events into buffer pool pages.
// Memtable pages are always marked dirty (they contain unflushed data) and have
// elevated eviction priority — the buffer pool avoids evicting dirty memtable
// pages because losing them means re-reading the WAL (expensive). Eviction order:
// unpinned cache pages first, then unpinned query pages, then memtable pages.
//
// Thread-safe. Multiple ingest goroutines may call Append concurrently.
type MemtablePageWriter struct {
	mu      sync.Mutex
	pool    *Pool
	pages   []*Page // all pages holding memtable data
	current *Page   // current page being written to
	offset  int     // write position within current page
}

// NewMemtablePageWriter creates a writer backed by the buffer pool.
func NewMemtablePageWriter(pool *Pool) *MemtablePageWriter {
	return &MemtablePageWriter{
		pool: pool,
	}
}

// Append writes data into the memtable page buffer. The data is appended to
// the current page. When the current page is full, a new page is allocated.
// All memtable pages are pinned during active writes and marked dirty.
//
// Returns the PageRef for the written data, which can be used to read it back
// via Pool.Resolve().
func (mw *MemtablePageWriter) Append(data []byte) (PageRef, error) {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	dataLen := len(data)
	if dataLen == 0 {
		return PageRef{}, nil
	}

	// Allocate a new page if the current one has insufficient space.
	if mw.current == nil || mw.offset+dataLen > mw.current.Size() {
		if err := mw.allocNewPage(); err != nil {
			return PageRef{}, err
		}
	}

	// If a single write exceeds the page size, return an error.
	// Callers should chunk large writes.
	if dataLen > mw.current.Size() {
		return PageRef{}, fmt.Errorf("buffer.MemtablePageWriter.Append: data size %d exceeds page size %d", dataLen, mw.current.Size())
	}

	// Write data at current offset.
	if err := mw.current.WriteAt(data, mw.offset); err != nil {
		return PageRef{}, fmt.Errorf("buffer.MemtablePageWriter.Append: %w", err)
	}

	ref := PageRef{
		PageID: mw.current.ID(),
		Offset: mw.offset,
		Length: dataLen,
	}
	mw.offset += dataLen

	return ref, nil
}

// PageCount returns the number of pages holding memtable data.
func (mw *MemtablePageWriter) PageCount() int {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	return len(mw.pages)
}

// BytesWritten returns the total bytes written across all pages.
func (mw *MemtablePageWriter) BytesWritten() int64 {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	if len(mw.pages) == 0 {
		return 0
	}

	pageSize := mw.pool.PageSize()

	// Full pages + partial current page.
	return int64(len(mw.pages)-1)*int64(pageSize) + int64(mw.offset)
}

// ReleaseAll frees all memtable pages back to the pool. Called after the
// memtable is flushed to segment files. After ReleaseAll, the writer can
// be reused for a new memtable generation.
func (mw *MemtablePageWriter) ReleaseAll() {
	mw.mu.Lock()
	pages := mw.pages
	mw.pages = nil
	mw.current = nil
	mw.offset = 0
	mw.mu.Unlock()

	for _, p := range pages {
		// Ensure unpinned before freeing.
		for p.PinCount() > 0 {
			p.Unpin()
		}
		mw.pool.FreePage(p)
	}
}

// allocNewPage allocates a new memtable page. Must be called with mw.mu held.
func (mw *MemtablePageWriter) allocNewPage() error {
	// Unpin the current page if it exists (it's full now, but stays dirty
	// and in the pool as an eviction candidate).
	if mw.current != nil {
		mw.current.Unpin()
	}

	p, err := mw.pool.AllocPage(OwnerMemtable, "memtable")
	if err != nil {
		return fmt.Errorf("buffer.MemtablePageWriter.allocNewPage: %w", err)
	}

	// Memtable pages are always dirty (unflushed data).
	p.MarkDirty()

	mw.current = p
	mw.offset = 0
	mw.pages = append(mw.pages, p)

	return nil
}
