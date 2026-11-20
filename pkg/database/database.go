// Package database is the database of a LynxDB server: indexes kept by the
// columnar core, PPL queries run as jobs, and retention.
package database

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/engine/columnar"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/query"
)

// DefaultSource is the index that a query without a source reads and that
// ingest without an index writes.
const DefaultSource = "main"

// DefaultTimeFields are the fields whose value becomes the time of an event,
// tried in their order.
var DefaultTimeFields = []string{"_time", "timestamp", "@timestamp", "time", "ts"}

// DefaultPartition is the span of time whose events are kept apart, so that
// retention drops whole spans.
const DefaultPartition = 24 * time.Hour

// Config sets up a database.
type Config struct {
	// Dir holds the indexes; an empty Dir keeps them in a temporary
	// directory that Close removes.
	Dir string
	// Retention is how long events are kept; zero keeps them all.
	Retention time.Duration
	// Partition is the span of time whose events are kept apart; zero
	// means DefaultPartition.
	Partition time.Duration
	// FlushInterval is the longest that appended events wait before they
	// are written; zero means the store's own default.
	FlushInterval time.Duration
	// TimeFields name the fields whose value becomes the time of an event;
	// nil means DefaultTimeFields.
	TimeFields []string
	// SortKey names the fields that order the rows of the parts written
	// from then on; empty keeps the order of time.
	SortKey []string
	// MaxConcurrent bounds the queries that run at once; zero means no
	// bound.
	MaxConcurrent int
	// JobTTL is how long a finished job stays; zero means five minutes.
	JobTTL time.Duration
	Logger *slog.Logger
}

// ErrTooManyQueries reports a query refused because MaxConcurrent queries
// run.
var ErrTooManyQueries = errors.New("database: too many concurrent queries")

// DB is an open database.
type DB struct {
	cfg  Config
	core *columnar.DB
	log  *slog.Logger

	slots chan struct{}
	jobs  jobs

	stop chan struct{}
	wg   sync.WaitGroup
	once sync.Once
}

// Open opens the database in cfg.Dir and starts its retention.
func Open(cfg Config) (*DB, error) {
	if cfg.Partition <= 0 {
		cfg.Partition = DefaultPartition
	}
	if cfg.TimeFields == nil {
		cfg.TimeFields = DefaultTimeFields
	}
	if cfg.JobTTL <= 0 {
		cfg.JobTTL = 5 * time.Minute
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	core, err := columnar.Open(columnar.Config{Dir: cfg.Dir, Store: colstore.StoreOptions{
		TimeFields:    cfg.TimeFields,
		SortKey:       cfg.SortKey,
		KeepText:      true,
		Partition:     cfg.Partition,
		FlushInterval: cfg.FlushInterval,
	}})
	if err != nil {
		return nil, err
	}
	db := &DB{cfg: cfg, core: core, log: cfg.Logger, stop: make(chan struct{})}
	db.jobs.byID = map[string]*Job{}
	if cfg.MaxConcurrent > 0 {
		db.slots = make(chan struct{}, cfg.MaxConcurrent)
	}
	db.wg.Add(1)
	go db.maintain()

	return db, nil
}

// Append adds the events of ndjson, one JSON object per line, to an index;
// a line that is not a JSON object becomes an event holding the text in
// _raw. It returns the number of lines rejected.
func (db *DB) Append(index string, ndjson []byte) (int64, error) {
	if index == "" {
		index = DefaultSource
	}

	return db.core.Append(index, ndjson)
}

// Sync makes the events appended so far durable and visible.
func (db *DB) Sync() error { return db.core.Sync() }

// Stats describes every index in the order of their names.
func (db *DB) Stats() []columnar.IndexStats { return db.core.Stats() }

// Usage is columnar.DB.Usage.
func (db *DB) Usage(index string, sections bool) (columnar.IndexUsage, bool, error) {
	return db.core.Usage(index, sections)
}

// Names lists the indexes in the order of their names.
func (db *DB) Names() []string { return db.core.Names() }

// Fields lists the paths of the fields that an index holds, sorted and
// without repeats; it reports false when there is no such index.
func (db *DB) Fields(index string) ([]string, bool) {
	snap, ok := db.core.Snapshot(index)
	if !ok {
		return nil, false
	}
	defer snap.Release()
	seen := map[string]bool{}
	var out []string
	for _, h := range snap.Parts() {
		for _, p := range h.Paths() {
			if !seen[p] {
				seen[p] = true
				out = append(out, p)
			}
		}
	}
	sort.Strings(out)

	return out, true
}

// Snapshot returns the parts that an index holds now, for the caller to
// release; it reports false when there is no such index.
func (db *DB) Snapshot(index string) (*colstore.Snapshot, bool) { return db.core.Snapshot(index) }

// Drop removes an index and its events; it reports whether there was one.
func (db *DB) Drop(index string) (bool, error) { return db.core.Drop(index) }

// Compact makes the events appended to an index durable and merges each of
// its partitions into one sorted run, and returns once that is done; it
// reports false when there is no such index.
func (db *DB) Compact(index string) (columnar.Compaction, bool, error) {
	if index == "" {
		index = DefaultSource
	}

	return db.core.Compact(index)
}

// QueryOptions sets how a query runs.
type QueryOptions struct {
	// From and To narrow every scan of the query to [From, To); a zero
	// time leaves that end as the query sets it.
	From, To time.Time
	// Now is the time that relative times count from; zero means the time
	// of the call.
	Now time.Time
}

// Query runs a compiled query and returns its rows.
func (db *DB) Query(ctx context.Context, c *query.Compiled, opts QueryOptions) (*columnar.Result, error) {
	release, err := db.acquire()
	if err != nil {
		return nil, err
	}
	defer release()

	return db.query(ctx, c, opts)
}

// acquire takes a query slot and returns its release.
func (db *DB) acquire() (func(), error) {
	if db.slots == nil {
		return func() {}, nil
	}
	select {
	case db.slots <- struct{}{}:
		return func() { <-db.slots }, nil
	default:
		return nil, ErrTooManyQueries
	}
}

func (db *DB) query(ctx context.Context, c *query.Compiled, opts QueryOptions) (*columnar.Result, error) {
	if opts.Now.IsZero() {
		opts.Now = time.Now()
	}
	plan := c.Plan
	if !opts.From.IsZero() || !opts.To.IsZero() {
		plan = &ir.Plan{Root: narrow(plan.Root, opts.From, opts.To, opts.Now)}
	}

	return db.core.Query(ctx, plan, columnar.Options{DefaultSource: DefaultSource, Now: opts.Now})
}

// narrow returns the plan under n with every scan restricted to [from, to),
// those of the subqueries in its filters and computed columns included.
func narrow(n ir.Node, from, to, now time.Time) ir.Node {
	switch x := n.(type) {
	case *ir.Filter:
		c := *x
		c.Cond = narrowSubqueries(x.Cond, from, to, now)
		n = &c
	case *ir.Compute:
		c := *x
		c.Assigns = append([]ir.Assign(nil), x.Assigns...)
		for i := range c.Assigns {
			c.Assigns[i].Expr = narrowSubqueries(c.Assigns[i].Expr, from, to, now)
		}
		n = &c
	}
	if s, ok := n.(*ir.Scan); ok {
		earliest, latest := s.Time.Resolve(now)
		if !from.IsZero() && (earliest.IsZero() || from.After(earliest)) {
			earliest = from
		}
		if !to.IsZero() && (latest.IsZero() || to.Before(latest)) {
			latest = to
		}
		c := s.WithInputs(nil).(*ir.Scan)
		c.Time = &ir.TimeRange{}
		if !earliest.IsZero() {
			c.Time.Earliest = ir.AbsoluteBound(earliest)
		}
		if !latest.IsZero() {
			c.Time.Latest = ir.AbsoluteBound(latest)
		}

		return c
	}
	ins := n.Inputs()
	if len(ins) == 0 {
		return n
	}
	outs := make([]ir.Node, len(ins))
	for i, in := range ins {
		outs[i] = narrow(in, from, to, now)
	}

	return n.WithInputs(outs)
}

func narrowSubqueries(e ir.Expr, from, to, now time.Time) ir.Expr {
	return ir.RewriteExpr(e, func(x ir.Expr) ir.Expr {
		sq, ok := x.(*ir.Subquery)
		if !ok {
			return x
		}
		c := *sq
		c.Plan = narrow(sq.Plan, from, to, now)

		return &c
	})
}

// maintain drops expired events and finished jobs until Close.
func (db *DB) maintain() {
	defer db.wg.Done()
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	for {
		db.expire(time.Now())
		select {
		case <-db.stop:
			return
		case <-tick.C:
		}
	}
}

// expire drops the events older than the retention and the jobs that
// finished longer than JobTTL before now.
func (db *DB) expire(now time.Time) {
	if db.cfg.Retention > 0 {
		cutoff := now.Add(-db.cfg.Retention)
		for _, index := range db.core.Names() {
			n, err := db.core.DropIndexBefore(index, cutoff)
			if err != nil {
				db.log.Warn("retention failed", "index", index, "error", err)
			}
			if n > 0 {
				db.log.Warn("retention removed events older than the cutoff",
					"index", index, "events", n, "cutoff", cutoff.UTC().Format(time.RFC3339))
			}
		}
	}
	db.jobs.expire(now.Add(-db.cfg.JobTTL))
}

// Close cancels the running jobs, makes the appended events durable and
// closes the indexes.
func (db *DB) Close() error {
	var err error
	db.once.Do(func() {
		close(db.stop)
		db.wg.Wait()
		db.jobs.cancelAll()
		err = db.core.Close()
	})

	return err
}
