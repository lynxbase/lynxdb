package exec_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	osexec "os/exec"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blevesearch/mmap-go"
	"github.com/klauspost/compress/zstd"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec/flow"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
)

// The logs benchmark: a generated stream of application logs, loaded through
// a store in several row orders and asked the queries of a log search.

func logsEnvInt(t testing.TB, name string, def int) int {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		t.Fatalf("%s=%q is not a positive number", name, v)
	}

	return n
}

// logsOptions reads the size of the stream: LYNXDB_LOGSBENCH_EVENTS lines
// (2,000,000), LYNXDB_LOGSBENCH_SEED, the time LYNXDB_LOGSBENCH_SPAN they
// arrive over (6h) and the share LYNXDB_LOGSBENCH_TEXT of plain text (0.2).
func logsOptions(t testing.TB) logGenOptions {
	t.Helper()
	opts := logGenOptions{
		Seed:   uint64(logsEnvInt(t, "LYNXDB_LOGSBENCH_SEED", 1)),
		Events: logsEnvInt(t, "LYNXDB_LOGSBENCH_EVENTS", 2_000_000),
		Span:   6 * time.Hour,
		Text:   0.2,
	}
	if v := os.Getenv("LYNXDB_LOGSBENCH_SPAN"); v != "" {
		span, err := time.ParseDuration(v)
		if err != nil || span <= 0 {
			t.Fatalf("LYNXDB_LOGSBENCH_SPAN=%q is not a positive duration", v)
		}
		opts.Span = span
	}
	if v := os.Getenv("LYNXDB_LOGSBENCH_TEXT"); v != "" {
		share, err := strconv.ParseFloat(v, 64)
		if err != nil || share < 0 || share > 1 {
			t.Fatalf("LYNXDB_LOGSBENCH_TEXT=%q is not a share", v)
		}
		opts.Text = share
	}

	return opts
}

// logsFiles returns the stream of opts as two mapped files: the lines as
// services and daemons write them, in the file named by
// LYNXDB_LOGSBENCH_FILE or a temporary one, and beside it, with .wrapped
// before the extension, the same stream with every text line wrapped in an
// object, which is what a store that takes only JSON is given. A file that
// does not exist is written first.
func logsFiles(t testing.TB, opts logGenOptions) (path string, plain, wrapped []byte) {
	t.Helper()
	path = os.Getenv("LYNXDB_LOGSBENCH_FILE")
	if path == "" {
		path = filepath.Join(t.TempDir(), "logs.ndjson")
	}
	plain = logsFile(t, path, opts)
	opts.WrapText = true
	ext := filepath.Ext(path)

	return path, plain, logsFile(t, strings.TrimSuffix(path, ext)+".wrapped"+ext, opts)
}

func logsFile(t testing.TB, path string, opts logGenOptions) []byte {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		begin := time.Now()
		f, err := os.Create(path + ".partial")
		if err != nil {
			t.Fatal(err)
		}
		size, err := newLogGen(opts).write(f)
		if err == nil {
			err = f.Close()
		}
		if err == nil {
			err = os.Rename(path+".partial", path)
		}
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %d lines, %.1f MB, to %s in %.1fs", opts.Events, float64(size)/1e6, path, time.Since(begin).Seconds())
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	mapped, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mapped.Unmap() })

	return mapped
}

type countingWriter struct{ n int64 }

func (c *countingWriter) Write(p []byte) (int, error) {
	c.n += int64(len(p))

	return len(p), nil
}

// zstdSize returns the size of the file at zstd level 3, from the zstd
// command when there is one, and says which compressor measured it.
func zstdSize(t testing.TB, path string, data []byte) (int64, string) {
	t.Helper()
	// The size is kept beside the file, which takes seconds to compress.
	kept := fmt.Sprintf("%s.%d.zstd3", path, len(data))
	if b, err := os.ReadFile(kept); err == nil {
		if n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64); err == nil {
			return n, "zstd -3"
		}
	}
	var out countingWriter
	if bin, err := osexec.LookPath("zstd"); err == nil {
		cmd := osexec.Command(bin, "-3", "-T1", "-c", path)
		cmd.Stdout = &out
		if err := cmd.Run(); err == nil {
			_ = os.WriteFile(kept, []byte(strconv.FormatInt(out.n, 10)), 0o644)

			return out.n, "zstd -3"
		}
		out.n = 0
	}
	enc, err := zstd.NewWriter(&out, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(enc, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	return out.n, "the Go zstd at its default level"
}

// cutRequests cuts a stream into inserts of docs lines.
func cutRequests(stream []byte, docs int) (reqs [][]byte, lines int) {
	for len(stream) > 0 {
		var chunk []byte
		chunk, stream = shred.SplitLines(stream, docs)
		lines += bytes.Count(chunk, []byte{'\n'})
		reqs = append(reqs, chunk)
	}

	return reqs, lines
}

// logsIngest is what loading a stream into a store took.
type logsIngest struct {
	durable, durableCPU, settled, settledCPU time.Duration
	// liveHeap is the most the heap held after a collection, liveBefore
	// what it held when the load began.
	liveHeap, liveBefore uint64
	rejected             int64
	// written is the size of every part file the load wrote, those that
	// merges replaced included, and files their number.
	written int64
	files   int
}

// partSizes records in sizes the largest size seen of every part file of
// dir, written or still being written, by the number in its name.
func partSizes(dir string, sizes map[string]int64) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		id, _, isPart := strings.Cut(e.Name(), ".part")
		if !isPart {
			continue
		}
		if info, err := e.Info(); err == nil {
			sizes[id] = max(sizes[id], info.Size())
		}
	}
}

// ingestLogs sends the requests to the store in dir from several streams
// that take them in turn, as the handlers of concurrent inserts do, and
// waits for the merges to settle.
func ingestLogs(t testing.TB, s *colstore.Store, dir string, reqs [][]byte, streams int) logsIngest {
	t.Helper()
	var in logsIngest
	var liveMax atomic.Uint64
	sampling, sampled := make(chan struct{}), make(chan struct{})
	// Twice, since a pool gives up its buffers a collection late.
	runtime.GC()
	runtime.GC()
	sample := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(sample)
	in.liveBefore = sample[0].Value.Uint64()
	sizes := map[string]int64{}
	go func() {
		defer close(sampled)
		for {
			select {
			case <-sampling:
				return
			// Often enough to meet a part between the merge that writes it
			// and the merge that replaces it.
			case <-time.After(5 * time.Millisecond):
				metrics.Read(sample)
				if v := sample[0].Value.Uint64(); v > liveMax.Load() {
					liveMax.Store(v)
				}
				partSizes(dir, sizes)
			}
		}
	}()
	cpuStart, start := cpuTime(), time.Now()
	var next, rejected atomic.Int64
	var wg sync.WaitGroup
	for range streams {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= len(reqs) {
					return
				}
				n, err := s.Append(reqs[i])
				if err != nil {
					t.Error(err)

					return
				}
				rejected.Add(n)
			}
		}()
	}
	wg.Wait()
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	in.durable, in.durableCPU = time.Since(start), cpuTime()-cpuStart
	if err := s.Settle(); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("LYNXDB_LOGSBENCH_COMPACT") != "" {
		if err := s.Compact(); err != nil {
			t.Fatal(err)
		}
	}
	in.settled, in.settledCPU = time.Since(start), cpuTime()-cpuStart
	close(sampling)
	<-sampled
	in.liveHeap, in.rejected = liveMax.Load(), rejected.Load()
	partSizes(dir, sizes)
	for _, size := range sizes {
		in.written += size
	}
	in.files = len(sizes)

	return in
}

// partsInMemory returns a table of the part files of dir read into memory,
// in the order and with the run of the parts of snap, the store's own view
// of the directory. Queries then read the parts as the tables of the other
// benchmarks are read, without the advice a directory gives the kernel
// about its mapped files.
func partsInMemory(t testing.TB, dir string, snap *colstore.Snapshot) *colstore.Table {
	t.Helper()
	type shape struct {
		size       int
		rows       uint64
		first, end int64
	}
	shapeOf := func(p *part.Part) shape { return shape{p.Size(), p.Rows, p.MinTime, p.MaxTime} }
	files, err := filepath.Glob(filepath.Join(dir, "*.part"))
	if err != nil {
		t.Fatal(err)
	}
	loaded := map[shape]*part.Part{}
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		p, err := part.Open(data)
		if err != nil {
			t.Fatal(err)
		}
		if _, dup := loaded[shapeOf(p)]; dup {
			t.Fatalf("two parts of %s have one size, row count and time range", dir)
		}
		loaded[shapeOf(p)] = p
	}
	var handles []*colstore.Handle
	for _, h := range snap.Parts() {
		p := loaded[shapeOf(h.Part)]
		if p == nil {
			t.Fatalf("%s has no file for a part of %d rows", dir, h.Rows)
		}
		copied := colstore.NewHandle(p, nil)
		copied.PartMeta = h.PartMeta
		handles = append(handles, copied)
	}
	tbl := colstore.NewTable()
	if err := tbl.Apply(handles, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tbl.Close)

	return tbl
}

// logsShape describes the parts of a snapshot.
type logsShape struct {
	rows                                 uint64
	size                                 int64
	parts, blocks, runs                  int
	paths, columns, widest, rest, spread int
}

func shapeOf(t testing.TB, snap *colstore.Snapshot) logsShape {
	t.Helper()
	var s logsShape
	paths := map[string]struct{}{}
	runs := map[[2]uint64]struct{}{}
	var b part.Block
	for _, h := range snap.Parts() {
		s.rows += h.Rows
		s.size += int64(h.Size())
		s.parts++
		runs[[2]uint64{uint64(h.Tier), h.Run}] = struct{}{}
		for _, p := range h.Paths() {
			paths[p] = struct{}{}
		}
		restID, hasRest := h.PathID(shred.RestPath)
		for i := 0; i < h.NumBlocks(); i++ {
			if err := h.OpenBlock(i, &b); err != nil {
				t.Fatal(err)
			}
			s.blocks++
			n := len(b.Columns())
			s.columns += n
			s.widest = max(s.widest, n)
			if hasRest && len(b.Path(restID)) > 0 {
				s.rest++
			}
			if b.MaxTime-b.MinTime > int64(5*time.Minute) {
				s.spread++
			}
		}
	}
	s.paths, s.runs = len(paths), len(runs)

	return s
}

// logTiming is the measurement of one query on one layout.
type logTiming struct {
	best, median, cpu time.Duration
	stats             exec.Stats
	mallocs, bytes    uint64
	rows              int
	head              string
	digest            uint64
}

func timeLogQuery(t testing.TB, snap *colstore.Snapshot, plan exec.Node, ordered bool, runs int, profile string) logTiming {
	t.Helper()
	cores := runtime.GOMAXPROCS(0)
	var res *exec.Result
	run := func() (err error) {
		res, err = exec.Run(context.Background(), snap, plan, exec.Options{Workers: cores})

		return err
	}
	if profile != "" {
		f, err := os.Create(profile)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			t.Fatal(err)
		}
	}
	took, cpu, err := timeRuns(runs, run)
	if profile != "" {
		pprof.StopCPUProfile()
	}
	if err != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if err := run(); err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&after)
	out := logTiming{best: took[0], median: took[runs/2], cpu: cpu, stats: res.Stats, rows: res.Rows,
		mallocs: after.Mallocs - before.Mallocs, bytes: after.TotalAlloc - before.TotalAlloc}
	rows := resultRows(res)
	if !ordered {
		sort.Strings(rows)
	}
	h := fnv.New64a()
	for _, row := range rows {
		_, _ = io.WriteString(h, row)
		_, _ = h.Write([]byte{'\n'})
	}
	out.digest = h.Sum64()
	out.head = strings.Join(rows[:min(len(rows), 3)], "; ")
	if len(out.head) > 220 {
		out.head = out.head[:220] + "..."
	}

	return out
}

// logsLayouts returns the row orders to load: arrival order and the sort
// keys of LYNXDB_LOGSBENCH_KEYS, which separates keys by semicolons and the
// paths of one by commas.
func logsLayouts() [][]string {
	keys := "service,level;level,service;service,host"
	if v := os.Getenv("LYNXDB_LOGSBENCH_KEYS"); v != "" {
		keys = v
	}
	layouts := [][]string{nil}
	for _, key := range strings.Split(keys, ";") {
		if key != "arrival" && key != "" {
			layouts = append(layouts, strings.Split(key, ","))
		}
	}

	return layouts
}

func layoutName(key []string) string {
	if len(key) == 0 {
		return "arrival"
	}

	return strings.Join(key, ",")
}

// TestLogsBenchFile writes the stream of the logs benchmark to the file
// named by LYNXDB_LOGSBENCH_FILE, for loading into other systems, and
// reports its size and what zstd makes of it.
func TestLogsBenchFile(t *testing.T) {
	if os.Getenv("LYNXDB_LOGSBENCH_FILE") == "" || os.Getenv("LYNXDB_LOGSBENCH") != "" {
		t.Skip("set LYNXDB_LOGSBENCH_FILE alone to write the stream of the logs benchmark")
	}
	path, data, wrapped := logsFiles(t, logsOptions(t))
	packed, by := zstdSize(t, path, data)
	t.Logf("%s: %.1f MB, %.1f MB with %s (%.2fx); %.1f MB with the text lines wrapped in objects",
		path, float64(len(data))/1e6, float64(packed)/1e6, by, float64(len(data))/float64(packed), float64(len(wrapped))/1e6)
}

// TestLogsBench loads the generated logs through a store, once per row
// order, and times the log queries on every core. LYNXDB_LOGSBENCH turns it
// on. LYNXDB_LOGSBENCH_DIR names a directory that keeps the stores, and a
// store found there is queried as it is; LYNXDB_LOGSBENCH_STREAMS is the
// number of concurrent inserts (8) and LYNXDB_LOGSBENCH_REQUEST_DOCS the
// lines of one (4000); LYNXDB_LOGSBENCH_COMPACT merges every store into one
// run. A buffer of the store is written when it holds LYNXDB_LOGSBENCH_RUN_MB
// of input (128) and, with LYNXDB_LOGSBENCH_TIMED_FLUSH, also a second after
// its first row arrived. The queries read the parts from memory unless
// LYNXDB_LOGSBENCH_MAPPED is set, and then as the store maps them, with its
// advice to the kernel. LYNXDB_LOGSBENCH_LAYOUT and LYNXDB_LOGSBENCH_QUERY keep the layouts
// and queries whose name contains them, LYNXDB_LOGSBENCH_RUNS is the number
// of timed runs (7) and LYNXDB_LOGSBENCH_PROFILE names a directory that
// receives a CPU profile per query and layout.
func TestLogsBench(t *testing.T) {
	if os.Getenv("LYNXDB_LOGSBENCH") == "" {
		t.Skip("set LYNXDB_LOGSBENCH to run the logs benchmark")
	}
	opts := logsOptions(t)
	path, data, wrapped := logsFiles(t, opts)
	gen := newLogGen(opts)
	consts := logConstantsOf(gen, data)
	reqs, lines := cutRequests(wrapped, logsEnvInt(t, "LYNXDB_LOGSBENCH_REQUEST_DOCS", 4000))
	text := 0
	for line := range bytes.Lines(data) {
		if line[0] != '{' {
			text++
		}
	}
	packed, by := zstdSize(t, path, data)
	megabytes := float64(len(data)) / 1e6
	t.Logf("%d lines, %d of them text, %.1f MB (%.0f bytes a line), %.1f MB with %s (%.2fx); %d inserts",
		lines, text, megabytes, float64(len(data))/float64(lines), float64(packed)/1e6, by, float64(len(data))/float64(packed), len(reqs))
	t.Logf("the busiest service is %s, the trace %s, the rare member %s (in %d events), the hour from %s",
		consts.service, consts.trace, consts.rare, bytes.Count(data, []byte(`"`+consts.rare[strings.LastIndexByte(consts.rare, '.')+1:]+`":`)), consts.hourLo.Format(time.RFC3339))

	streams := logsEnvInt(t, "LYNXDB_LOGSBENCH_STREAMS", 8)
	runs := logsEnvInt(t, "LYNXDB_LOGSBENCH_RUNS", 7)
	base := os.Getenv("LYNXDB_LOGSBENCH_DIR")
	if base == "" {
		base = t.TempDir()
	}
	onlyLayout, only, profileDir := os.Getenv("LYNXDB_LOGSBENCH_LAYOUT"), os.Getenv("LYNXDB_LOGSBENCH_QUERY"), os.Getenv("LYNXDB_LOGSBENCH_PROFILE")
	queries := logQueries(consts)
	type cell struct {
		name string
		logTiming
	}
	var names []string
	table := map[string][]cell{}
	digests := map[string]uint64{}
	refused := map[string]string{}
	for _, key := range logsLayouts() {
		name := layoutName(key)
		if !strings.Contains(name, onlyLayout) {
			continue
		}
		names = append(names, name)
		dir := filepath.Join(base, strings.ReplaceAll(name, ",", "+"))
		_, statErr := os.Stat(dir)
		storeOpts := colstore.StoreOptions{TimeField: logsTimeField, SortKey: key, FlushInterval: -1}
		if os.Getenv("LYNXDB_LOGSBENCH_RUN_MB") != "" {
			storeOpts.FlushBytes = logsEnvInt(t, "LYNXDB_LOGSBENCH_RUN_MB", 0) << 20
		}
		if os.Getenv("LYNXDB_LOGSBENCH_TIMED_FLUSH") != "" {
			storeOpts.FlushInterval = 0
		}
		store, err := colstore.OpenStore(dir, storeOpts)
		if err != nil {
			t.Fatal(err)
		}
		var in logsIngest
		if statErr != nil {
			in = ingestLogs(t, store, dir, reqs, streams)
			t.Logf("%s: durable in %.2fs wall and %.2fs CPU, settled in %.2fs wall and %.2fs CPU on %d streams: %.0fk docs/s, %.0fk docs/s per core, %.0f MB/s per core; "+
				"live heap up to %.0f MB, %.0f MB of it there before; %d lines rejected",
				name, in.durable.Seconds(), in.durableCPU.Seconds(), in.settled.Seconds(), in.settledCPU.Seconds(), streams,
				float64(lines)/in.settled.Seconds()/1e3, float64(lines)/in.settledCPU.Seconds()/1e3, megabytes/in.settledCPU.Seconds(),
				float64(in.liveHeap)/1e6, float64(in.liveBefore)/1e6, in.rejected)
		}
		snap := store.Table().Snapshot()
		if os.Getenv("LYNXDB_LOGSBENCH_MAPPED") == "" {
			loaded := partsInMemory(t, dir, snap).Snapshot()
			snap.Release()
			snap = loaded
		}
		shape := shapeOf(t, snap)
		t.Logf("%s: %d rows in %d parts of %d runs, %d blocks; %.1f MB on disk, %.2fx of the input and %.2fx of zstd; "+
			"%d paths, %.0f columns a block and at most %d, %d blocks with members without a column, %d blocks spanning over 5 minutes",
			name, shape.rows, shape.parts, shape.runs, shape.blocks, float64(shape.size)/1e6, megabytes*1e6/float64(shape.size), float64(packed)/float64(shape.size),
			shape.paths, float64(shape.columns)/float64(max(shape.blocks, 1)), shape.widest, shape.rest, shape.spread)
		if shape.rows != uint64(lines) {
			t.Errorf("%s holds %d rows of %d lines", name, shape.rows, lines)
		}
		if in.files > 0 {
			t.Logf("%s: wrote %.1f MB in %d part files until settled, %.2f times the size it settled at",
				name, float64(in.written)/1e6, in.files, float64(in.written)/float64(shape.size))
		}
		million := float64(shape.rows) / 1e6
		for _, q := range queries {
			if !strings.Contains(q.name, only) {
				continue
			}
			type variant struct {
				cell
				plan exec.Node
			}
			var variants []variant
			stated, err := flow.Compile(q.text, flow.Options{Now: logsEpoch.Add(opts.Span)})
			switch {
			case err == nil:
				variants = append(variants, variant{cell{name: q.name}, stated})
			case errors.Is(err, flow.ErrUnsupported) || q.restated != "":
				refused[q.name] = logRefusal(err)
			default:
				t.Errorf("%s: %v", q.name, err)
			}
			if err != nil && q.restated != "" {
				restated, err := flow.Compile(q.restated, flow.Options{Now: logsEpoch.Add(opts.Span)})
				if err != nil {
					t.Fatalf("%s: %v", q.restated, err)
				}
				variants = append(variants, variant{cell{name: q.name + ", restated"}, restated})
			}
			if q.plan != nil {
				variants = append(variants, variant{cell{name: q.name + ", by hand"}, q.plan})
			}
			for i := range variants {
				v := &variants[i]
				profile := ""
				if profileDir != "" {
					profile = filepath.Join(profileDir, strings.ReplaceAll(strings.Fields(v.name)[0]+"_"+name+strings.Repeat("_other", i), ",", "+")+".prof")
				}
				v.logTiming = timeLogQuery(t, snap, v.plan, q.ordered, runs, profile)
				s := v.stats
				t.Logf("%-13s %-36s %8.2f ms (median %.2f, cpu %.1f; a million rows %.2f ms, cpu %.1f), holds %.1f MB, %d allocations (%.2f MB); "+
					"blocks: %d opened, %d from the index, skipped %d by key, %d by tokens, %d by time, %d by statistics; %d rows pass, %d rows out",
					name, v.name, millis(v.best), millis(v.median), millis(v.cpu), millis(v.best)/million, millis(v.cpu)/million,
					float64(s.HeldBytes)/1e6, v.mallocs, float64(v.bytes)/1e6,
					s.Blocks, s.CountedByIndex, s.SkippedByKey, s.SkippedByToken, s.SkippedByTime, s.SkippedByStats, s.RowsMatched, v.rows)
				if prev, ok := digests[v.name]; ok && prev != v.digest {
					t.Errorf("%s: %s answers %s, another layout something else", name, v.name, v.head)
				} else if !ok {
					t.Logf("%-13s %-36s answer: %s", "", v.name, v.head)
				}
				digests[v.name] = v.digest
				table[v.name] = append(table[v.name], v.cell)
			}
		}
		snap.Release()
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	}

	var out strings.Builder
	fmt.Fprintf(&out, "best ms and CPU ms per query and layout\n%-44s", "")
	for _, name := range names {
		fmt.Fprintf(&out, " %20s", name)
	}
	for _, q := range queries {
		for _, name := range []string{q.name, q.name + ", restated", q.name + ", by hand"} {
			cells := table[name]
			if len(cells) == 0 {
				continue
			}
			fmt.Fprintf(&out, "\n%-44s", name)
			for _, c := range cells {
				fmt.Fprintf(&out, " %11.2f %8.1f", millis(c.best), millis(c.cpu))
			}
		}
	}
	t.Log(out.String())
	for _, q := range queries {
		if why, ok := refused[q.name]; ok {
			t.Logf("refused: %s: %s", q.name, why)
		}
	}
}
