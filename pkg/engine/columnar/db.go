package columnar

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/ir"
)

// Config sets up a database.
type Config struct {
	// Dir holds one directory per index; an empty Dir keeps the database in
	// a temporary directory that Close removes.
	Dir string
	// Store configures the store of every index; its sort key, time fields
	// and partition span are the settings of an index created without its
	// own.
	Store colstore.StoreOptions
}

// ErrClosed reports the use of a database after Close.
var ErrClosed = errors.New("columnar: database is closed")

// ErrIndexName reports an index name that cannot name a directory.
var ErrIndexName = errors.New("columnar: index names hold letters, digits, dots, dashes and underscores and do not start with a dot")

// DB is a database: indexes that each keep their events in a store of
// their own under one directory. An index comes into being with its first
// events or with Create; its directory holds its settings.
type DB struct {
	cfg       Config
	temporary bool

	mu       sync.RWMutex
	stores   map[string]*colstore.Store
	settings map[string]Settings
	closed   bool
}

// Open opens the database in cfg.Dir with the indexes it holds.
func Open(cfg Config) (*DB, error) {
	db := &DB{cfg: cfg, stores: map[string]*colstore.Store{}, settings: map[string]Settings{}}
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
		name := e.Name()
		if !e.IsDir() {
			continue
		}
		if hidden, ok := strings.CutPrefix(name, "."); ok {
			// An index whose creation did not finish.
			if index, ok := strings.CutSuffix(hidden, newSuffix); ok && validIndex(index) {
				if err := os.RemoveAll(filepath.Join(db.cfg.Dir, name)); err != nil {
					return nil, errors.Join(err, db.Close())
				}
			}

			continue
		}
		if !validIndex(name) {
			continue
		}
		if _, err := db.open(name); err != nil {
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

// open opens the store of an index with the settings its directory holds;
// a directory without them is not an index. The caller holds the lock for
// writing or is the only user of the database.
func (db *DB) open(index string) (*colstore.Store, error) {
	dir := filepath.Join(db.cfg.Dir, index)
	settings, err := readSettings(dir)
	if err != nil {
		return nil, fmt.Errorf("columnar: index %s: %w", index, err)
	}
	s, err := colstore.OpenStore(dir, settings.options(db.cfg.Store))
	if err != nil {
		return nil, fmt.Errorf("columnar: index %s: %w", index, err)
	}
	db.stores[index], db.settings[index] = s, settings

	return s, nil
}

// create makes an index with the settings of want, those that want leaves
// unset taken from the store options, and opens it: its directory comes
// into being with the settings in it, made durable, or not at all. The
// caller holds the lock for writing.
func (db *DB) create(index string, want Settings) (*colstore.Store, error) {
	if err := makeIndexDir(db.cfg.Dir, index, want.fill(storeSettings(db.cfg.Store))); err != nil {
		return nil, fmt.Errorf("columnar: index %s: %w", index, err)
	}

	return db.open(index)
}

// Create creates an index with the settings of want, those that want
// leaves unset taken from the store options, and returns its settings and
// whether it created it. An index that exists keeps its settings: Create
// returns them when they agree with every setting that want sets, else an
// error wrapping ErrSettingsConflict. Settings that an index cannot take
// return an error wrapping ErrSettings.
func (db *DB) Create(index string, want Settings) (Settings, bool, error) {
	if err := want.Validate(); err != nil {
		return Settings{}, false, err
	}
	if !validIndex(index) {
		return Settings{}, false, fmt.Errorf("%w: %q", ErrIndexName, index)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return Settings{}, false, ErrClosed
	}
	if db.stores[index] != nil {
		has := db.settings[index]
		if c := want.conflict(has); c != "" {
			return has, false, fmt.Errorf("%w: index %s has %s", ErrSettingsConflict, index, c)
		}

		return has, false, nil
	}
	if _, err := db.create(index, want); err != nil {
		return Settings{}, false, err
	}

	return db.settings[index], true, nil
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

	return db.create(index, Settings{})
}

// AppendRows adds the events of lines, one per line, to an index, which it
// creates when there is none, and returns what became of the lines; in says
// which lines are kept. An event that carries no time gets the time of the
// call. The events are durable after Sync or the flush interval of the store.
func (db *DB) AppendRows(index string, lines []byte, in colstore.Intake) (colstore.Appended, error) {
	s, err := db.store(index, true)
	if err != nil {
		return colstore.Appended{}, err
	}

	return s.AppendRows(lines, time.Now().UnixNano(), in)
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

// Query runs a plan over the indexes. It first writes what was appended to
// the indexes the plan reads, so that the query sees every event taken
// before it; an index whose events could not be written is read as it is on
// disk, and the result says so in a warning.
func (db *DB) Query(ctx context.Context, plan *ir.Plan, opts Options) (*Result, error) {
	behind := db.syncSources(plan, opts.DefaultSource)
	res, err := Run(ctx, plan, db, opts)
	if err != nil {
		return nil, err
	}
	res.Warnings = append(behind, res.Warnings...)

	return res, nil
}

// syncSources syncs the indexes that plan reads, those of its subqueries
// included, and returns a warning for every one that failed.
func (db *DB) syncSources(plan *ir.Plan, defaultSource string) []string {
	var warnings []string
	var done []string
	sync := func(sources []ir.Source) {
		names, _ := sourceNames(sources, defaultSource, db.Names)
		for _, name := range names {
			if slices.Contains(done, name) {
				continue
			}
			done = append(done, name)
			s, err := db.store(name, false)
			if err != nil || s == nil {
				continue
			}
			if err := s.Sync(); err != nil {
				warnings = append(warnings, fmt.Sprintf("index %s: its latest events are not searchable yet: %v", name, err))
			}
		}
	}
	ir.WalkPlan(plan.Root, func(n ir.Node) bool {
		switch x := n.(type) {
		case *ir.Scan:
			sync(x.Sources)
		case *ir.SourceSchema:
			sync([]ir.Source{x.Source})
		}

		return true
	})

	return warnings
}

// IndexStats describes what an index holds.
type IndexStats struct {
	Name  string
	Rows  uint64
	Bytes int64
	// FilterBytes is the part of Bytes that the token filters take.
	FilterBytes int64
	Parts       int
	// First and Last are the times of the earliest and the latest event;
	// both are zero for an index without events.
	First, Last time.Time
	// Merges is the number of merges under way and RunsToMerge the number
	// of sorted runs still to be merged; the index is quiet when both are
	// zero.
	Merges, RunsToMerge int
	Settings            Settings
}

// Stats describes every index, in the order of their names.
func (db *DB) Stats() []IndexStats {
	names := db.Names()
	out := make([]IndexStats, 0, len(names))
	for _, name := range names {
		if st, ok := db.Index(name); ok {
			out = append(out, st)
		}
	}

	return out
}

// Index describes an index; it reports false when there is no such index.
func (db *DB) Index(name string) (IndexStats, bool) {
	db.mu.RLock()
	s, settings, closed := db.stores[name], db.settings[name], db.closed
	db.mu.RUnlock()
	if closed || s == nil {
		return IndexStats{}, false
	}
	snap := s.Table().Snapshot()
	defer snap.Release()
	st := IndexStats{Name: name, Settings: settings}
	st.Merges, st.RunsToMerge = s.MergeState()
	first, last := int64(0), int64(0)
	for i, h := range snap.Parts() {
		st.Rows += h.Rows
		st.Bytes += int64(h.Size())
		st.FilterBytes += h.FilterBytes()
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

	return st, true
}

// IndexUsage tells where the bytes of an index go.
type IndexUsage struct {
	part.Usage
	// Tiers holds by merge tier the parts, rows and bytes; tier 0 holds the
	// runs as flushed.
	Tiers map[int]*TierUsage
}

// TierUsage sums the parts of one merge tier.
type TierUsage struct {
	Parts int
	Rows  uint64
	Bytes int64
}

// Usage tells where the bytes of the parts of an index go, see
// part.Part.AddUsage for sections; ok is false when there is no such index.
func (db *DB) Usage(index string, sections func(path string) bool) (u IndexUsage, ok bool, err error) {
	snap, ok := db.Snapshot(index)
	if !ok {
		return u, false, nil
	}
	defer snap.Release()
	u.Tiers = map[int]*TierUsage{}
	for _, h := range snap.Parts() {
		if err := h.AddUsage(&u.Usage, sections); err != nil {
			return u, true, fmt.Errorf("columnar: index %s: %w", index, err)
		}
		t := u.Tiers[h.Tier]
		if t == nil {
			t = &TierUsage{}
			u.Tiers[h.Tier] = t
		}
		t.Parts++
		t.Rows += h.Rows
		t.Bytes += int64(h.Size())
	}

	return u, true, nil
}

// Compaction describes what Compact did to an index.
type Compaction struct {
	PartsBefore, Parts int
	BytesBefore, Bytes int64
	Took               time.Duration
}

// Compact makes the events appended to an index durable and merges each of
// its partitions into one sorted run, and returns once that is done; it
// reports false when there is no such index.
func (db *DB) Compact(index string) (Compaction, bool, error) {
	s, err := db.store(index, false)
	if err != nil || s == nil {
		return Compaction{}, false, err
	}
	begin := time.Now()
	if err := s.Sync(); err != nil {
		return Compaction{}, true, err
	}
	var c Compaction
	c.PartsBefore, c.BytesBefore = partsAndBytes(s)
	if err := s.Compact(); err != nil {
		return Compaction{}, true, err
	}
	c.Parts, c.Bytes = partsAndBytes(s)
	c.Took = time.Since(begin)

	return c, true, nil
}

func partsAndBytes(s *colstore.Store) (parts int, bytes int64) {
	snap := s.Table().Snapshot()
	defer snap.Release()
	for _, h := range snap.Parts() {
		parts++
		bytes += int64(h.Size())
	}

	return parts, bytes
}

// ExplainColumn tells, for the string column of path, what an even sample
// of at most blocks blocks of an index takes in every form the writer has
// and why the writer chose as it did, see part.Part.ExplainColumn; ok is
// false when there is no such index.
func (db *DB) ExplainColumn(index, path string, blocks int) (e part.ColumnExplain, ok bool, err error) {
	snap, ok := db.Snapshot(index)
	if !ok {
		return e, false, nil
	}
	defer snap.Release()
	total := 0
	for _, h := range snap.Parts() {
		total += h.NumBlocks()
	}
	every := max((total+blocks-1)/max(blocks, 1), 1)
	seen := 0
	pick := func() bool {
		seen++

		return (seen-1)%every == 0
	}
	e.Path = path
	for _, h := range snap.Parts() {
		if err := h.ExplainColumn(&e, pick); err != nil {
			return e, true, fmt.Errorf("columnar: index %s: %w", index, err)
		}
	}

	return e, true, nil
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
	delete(db.settings, index)

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
	db.stores, db.settings = nil, nil
	if db.temporary {
		errs = append(errs, os.RemoveAll(db.cfg.Dir))
	}

	return errors.Join(errs...)
}
