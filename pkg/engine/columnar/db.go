package columnar

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/ir"
)

// Config sets up a database.
type Config struct {
	// Dir holds one directory per index; an empty Dir keeps the database in
	// a temporary directory that Close removes.
	Dir string
	// Store configures the store of every index.
	Store colstore.StoreOptions
}

// ErrClosed reports the use of a database after Close.
var ErrClosed = errors.New("columnar: database is closed")

// ErrIndexName reports an index name that cannot name a directory.
var ErrIndexName = errors.New("columnar: index names hold letters, digits, dots, dashes and underscores and do not start with a dot")

// DB is a database: indexes that each keep their events in a store of
// their own under one directory. An index comes into being with its first
// events.
type DB struct {
	cfg       Config
	temporary bool

	mu     sync.RWMutex
	stores map[string]*colstore.Store
	closed bool
}

// Open opens the database in cfg.Dir with the indexes it holds.
func Open(cfg Config) (*DB, error) {
	db := &DB{cfg: cfg, stores: map[string]*colstore.Store{}}
	if cfg.Dir == "" {
		dir, err := os.MkdirTemp("", "lynxdb-")
		if err != nil {
			return nil, err
		}
		db.cfg.Dir, db.temporary = dir, true
	}
	if err := os.MkdirAll(db.cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(db.cfg.Dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if !e.IsDir() || !validIndex(e.Name()) {
			continue
		}
		if _, err := db.open(e.Name()); err != nil {
			return nil, errors.Join(err, db.Close())
		}
	}

	return db, nil
}

func validIndex(name string) bool {
	if name == "" || len(name) > 255 || name[0] == '.' {
		return false
	}
	for i := 0; i < len(name); i++ {
		switch c := name[i]; {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '.', c == '-', c == '_':
		default:
			return false
		}
	}

	return true
}

// open opens the store of an index; the caller holds the lock for writing
// or is the only user of the database.
func (db *DB) open(index string) (*colstore.Store, error) {
	s, err := colstore.OpenStore(filepath.Join(db.cfg.Dir, index), db.cfg.Store)
	if err != nil {
		return nil, fmt.Errorf("columnar: index %s: %w", index, err)
	}
	db.stores[index] = s

	return s, nil
}

// store returns the store of an index, which it creates when create is set
// and there is none.
func (db *DB) store(index string, create bool) (*colstore.Store, error) {
	db.mu.RLock()
	s, closed := db.stores[index], db.closed
	db.mu.RUnlock()
	switch {
	case closed:
		return nil, ErrClosed
	case s != nil || !create:
		return s, nil
	case !validIndex(index):
		return nil, fmt.Errorf("%w: %q", ErrIndexName, index)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, ErrClosed
	}
	if s := db.stores[index]; s != nil {
		return s, nil
	}

	return db.open(index)
}

// Append adds the events of lines, one JSON object per line, to an index
// and returns the number of lines it rejected. The events are durable after
// Sync or the flush interval of the store.
func (db *DB) Append(index string, lines []byte) (rejected int64, err error) {
	s, err := db.store(index, true)
	if err != nil {
		return 0, err
	}

	return s.Append(lines)
}

// Sync makes the events appended so far durable.
func (db *DB) Sync() error {
	var errs []error
	for _, s := range db.all() {
		errs = append(errs, s.Sync())
	}

	return errors.Join(errs...)
}

// all returns the stores of the indexes.
func (db *DB) all() []*colstore.Store {
	db.mu.RLock()
	defer db.mu.RUnlock()
	out := make([]*colstore.Store, 0, len(db.stores))
	for _, s := range db.stores {
		out = append(out, s)
	}

	return out
}

// Snapshot returns the events that an index holds now, for the caller to
// release.
func (db *DB) Snapshot(name string) (*colstore.Snapshot, bool) {
	s, err := db.store(name, false)
	if err != nil || s == nil {
		return nil, false
	}

	return s.Table().Snapshot(), true
}

// Names lists the indexes in the order of their names.
func (db *DB) Names() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	names := make([]string, 0, len(db.stores))
	for name := range db.stores {
		names = append(names, name)
	}
	sort.Strings(names)

	return names
}

// Query runs a plan over the indexes.
func (db *DB) Query(ctx context.Context, plan *ir.Plan, opts Options) (*Result, error) {
	return Run(ctx, plan, db, opts)
}

// IndexStats describes what an index holds.
type IndexStats struct {
	Name  string
	Rows  uint64
	Bytes int64
	Parts int
	// First and Last are the times of the earliest and the latest event;
	// both are zero for an index without events.
	First, Last time.Time
}

// Stats describes every index, in the order of their names.
func (db *DB) Stats() []IndexStats {
	names := db.Names()
	out := make([]IndexStats, 0, len(names))
	for _, name := range names {
		snap, ok := db.Snapshot(name)
		if !ok {
			continue
		}
		st := IndexStats{Name: name}
		first, last := int64(0), int64(0)
		for i, h := range snap.Parts() {
			st.Rows += h.Rows
			st.Bytes += int64(h.Part.Size())
			st.Parts++
			if i == 0 || h.MinTime < first {
				first = h.MinTime
			}
			if i == 0 || h.MaxTime > last {
				last = h.MaxTime
			}
		}
		if st.Parts != 0 {
			st.First, st.Last = time.Unix(0, first).UTC(), time.Unix(0, last).UTC()
		}
		snap.Release()
		out = append(out, st)
	}

	return out
}

// DropBefore removes from every index the partitions that end at or before
// t and returns the number of events removed. Indexes that are not
// partitioned by time keep everything.
func (db *DB) DropBefore(t time.Time) (uint64, error) {
	var rows uint64
	var errs []error
	for _, s := range db.all() {
		n, err := s.DropBefore(t.UnixNano())
		rows += n
		errs = append(errs, err)
	}

	return rows, errors.Join(errs...)
}

// Drop removes an index and its events; it reports whether there was one.
func (db *DB) Drop(index string) (bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return false, ErrClosed
	}
	s := db.stores[index]
	if s == nil {
		return false, nil
	}
	delete(db.stores, index)

	return true, errors.Join(s.Close(), os.RemoveAll(filepath.Join(db.cfg.Dir, index)))
}

// Close makes the appended events durable and closes every index.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	var errs []error
	for _, s := range db.stores {
		errs = append(errs, s.Close())
	}
	db.stores = nil
	if db.temporary {
		errs = append(errs, os.RemoveAll(db.cfg.Dir))
	}

	return errors.Join(errs...)
}
