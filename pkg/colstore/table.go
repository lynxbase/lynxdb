// Package colstore keeps a table as a set of immutable columnar parts and
// lets readers scan a consistent snapshot of it while writers replace parts.
package colstore

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
)

// Handle is a part that belongs to a table.
type Handle struct {
	*part.Part
	// PartMeta is zero for a part that no directory manages.
	PartMeta

	refs    atomic.Int64
	release func()
}

// NewHandle wraps p; release runs once, after the last version and snapshot
// referencing the part are gone, and is where a mapping is unmapped or a file
// deleted.
func NewHandle(p *part.Part, release func()) *Handle {
	return &Handle{Part: p, release: release}
}

func (h *Handle) unref() {
	if h.refs.Add(-1) == 0 && h.release != nil {
		h.release()
	}
}

// version is one immutable state of the part set, kept alive by the table's
// own pointer and by every snapshot taken from it.
type version struct {
	refs  atomic.Int64
	parts []*Handle
}

// tryRef takes a reference unless the version has already been released.
func (v *version) tryRef() bool {
	for {
		n := v.refs.Load()
		if n == 0 {
			return false
		}
		if v.refs.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

func (v *version) unref() {
	if v.refs.Add(-1) == 0 {
		for _, h := range v.parts {
			h.unref()
		}
	}
}

// Table is the current set of parts; readers and writers never block each
// other, and writers are serialized among themselves.
type Table struct {
	mu      sync.Mutex
	current atomic.Pointer[version]
}

// NewTable returns an empty table.
func NewTable() *Table {
	t := &Table{}
	v := &version{}
	v.refs.Store(1)
	t.current.Store(v)

	return t
}

// ErrUnknownPart reports an attempt to remove a part the table does not hold.
var ErrUnknownPart = errors.New("colstore: part is not in the table")

// Apply removes and adds parts atomically: every snapshot sees either all
// of the change or none of it.
func (t *Table) Apply(add, remove []*Handle) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	old := t.current.Load()
	next := &version{parts: make([]*Handle, 0, len(old.parts)+len(add))}
	removed := 0
	for _, h := range old.parts {
		drop := false
		for _, r := range remove {
			if r == h {
				drop = true

				break
			}
		}
		if drop {
			removed++

			continue
		}
		next.parts = append(next.parts, h)
	}
	if removed != len(remove) {
		return ErrUnknownPart
	}
	next.parts = append(next.parts, add...)
	for _, h := range next.parts {
		h.refs.Add(1)
	}
	next.refs.Store(1)
	t.current.Store(next)
	old.unref()

	return nil
}

// Close drops every part, which stays usable by snapshots taken earlier.
func (t *Table) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	empty := &version{}
	empty.refs.Store(1)
	t.current.Swap(empty).unref()
}

// Snapshot is a stable view of the table that must be released when done.
type Snapshot struct {
	v   *version
	key KeyFilter
}

// Narrow returns a snapshot of the same state whose scans apply k; it is
// released on its own.
func (s *Snapshot) Narrow(k KeyFilter) *Snapshot {
	s.v.refs.Add(1)

	return &Snapshot{v: s.v, key: k}
}

// Snapshot returns the current state of the table.
func (t *Table) Snapshot() *Snapshot {
	for {
		// A version loaded here may be released before the reference is
		// taken; tryRef then fails and the newer version is loaded.
		if v := t.current.Load(); v.tryRef() {
			return &Snapshot{v: v}
		}
	}
}

// Union returns a snapshot of the rows of all of snaps, which stay the
// caller's to release; the union is released on its own.
func Union(snaps ...*Snapshot) *Snapshot {
	v := &version{}
	for _, s := range snaps {
		for _, h := range s.v.parts {
			h.refs.Add(1)
			v.parts = append(v.parts, h)
		}
	}
	v.refs.Store(1)

	return &Snapshot{v: v}
}

// Parts returns the parts visible to the snapshot.
func (s *Snapshot) Parts() []*Handle { return s.v.parts }

// Release ends the snapshot, after which its parts must not be used.
func (s *Snapshot) Release() {
	if s.v != nil {
		s.v.unref()
		s.v = nil
	}
}
