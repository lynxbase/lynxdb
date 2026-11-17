package colstore

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

const (
	// killChildEnv carries the killConfig of a child process.
	killChildEnv = "LYNXDB_STORE_KILL_CHILD"
	// killAppenders write through Append and Sync; the one writer behind
	// them flushes a Writer of its own, whose flushes have known borders.
	killAppenders = 3
	killWriters   = killAppenders + 1
	// killPerHour is how many documents of a writer start in one partition.
	killPerHour = 40
	// killKept is how many partitions behind the slowest writer are kept.
	killKept = 2
)

// killConfig tells a child where to write and with which numbers.
type killConfig struct {
	Dir  string
	Seed int64
	// Next holds, per writer, the first document number it may use.
	Next         []int64
	RewriteAfter int
	// NoSync leaves out the syncs, so that far more commits meet the kill.
	NoSync bool
}

func killStoreOptions() StoreOptions {
	return StoreOptions{
		SortKey: []string{"k"}, TimeField: "ts", BlockRows: 64, FlushBytes: 4 << 10,
		MergeFanIn: 2, FlushInterval: 3 * time.Millisecond, Partition: time.Hour,
	}
}

type killDocID struct{ w, n int64 }

// hour is the partition of the document. Numbers that follow each other are
// spread over neighboring partitions, so that one flush writes several, and
// no later number falls into a partition before n/killPerHour.
func (id killDocID) hour() int64 {
	return firstTimedHour + id.n/killPerHour + id.n%(1+(id.w+2)%3)
}

func (id killDocID) time() int64 {
	return (id.hour()*3600e6 + id.n%3000) * 1000
}

// check is a value that only this document holds.
func (id killDocID) check() int64 {
	x := uint64(id.w)<<44 ^ uint64(id.n)
	x = (x ^ x>>30) * 0xbf58476d1ce4e5b9
	x = (x ^ x>>27) * 0x94d049bb133111eb

	return int64((x ^ x>>31) >> 12)
}

func (id killDocID) key() string { return fmt.Sprintf("k%02d", id.check()%29) }

func (id killDocID) pad() string { return strconv.FormatInt(id.check(), 36) }

func (id killDocID) appendJSON(dst []byte) []byte {
	return fmt.Appendf(dst, `{"w":%d,"n":%d,"ts":%d,"k":"%s","v":%d,"pad":"%s"}`+"\n",
		id.w, id.n, id.time()/1000, id.key(), id.check(), id.pad())
}

// say writes one line with one write, so that a kill leaves whole lines.
func say(format string, args ...any) {
	_, _ = os.Stdout.WriteString(fmt.Sprintf(format, args...) + "\n")
}

// TestStoreKillChild is the body of the processes TestStoreSurvivesKills
// starts and kills; it writes until then.
func TestStoreKillChild(t *testing.T) {
	raw := os.Getenv(killChildEnv)
	if raw == "" {
		t.Skip("runs as a child process of TestStoreSurvivesKills")
	}
	var cfg killConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatal(err)
	}
	say("start")
	manifestRewriteAfter = cfg.RewriteAfter
	if cfg.NoSync {
		syncFile = func(*os.File) error { return nil }
	}
	s, err := OpenStore(cfg.Dir, killStoreOptions())
	if err != nil {
		say("error open: %v", err)
		os.Exit(3)
	}
	say("open")
	// acked is set behind the line that reports it, so that the parent has
	// read every acknowledgment a drop was computed from.
	acked := make([]atomic.Int64, killWriters)
	for w := range acked {
		acked[w].Store(cfg.Next[w] - 1)
	}
	for w := int64(0); w < killAppenders; w++ {
		go killAppend(s, w, cfg, &acked[w])
	}
	go killWrite(s, killAppenders, cfg, &acked[killAppenders])
	go killDrop(s, cfg, acked)
	go killCompact(s, cfg)
	go killRead(s, cfg)
	select {}
}

func killAppend(s *Store, w int64, cfg killConfig, acked *atomic.Int64) {
	rng := rand.New(rand.NewSource(cfg.Seed + w))
	n := cfg.Next[w]
	var buf []byte
	for {
		buf = buf[:0]
		for i := 1 + rng.Intn(30); i > 0; i-- {
			buf = killDocID{w, n}.appendJSON(buf)
			n++
		}
		if rejected, err := s.Append(buf); err != nil || rejected != 0 {
			say("error append: %d lines rejected, %v", rejected, err)

			return
		}
		if rng.Intn(4) == 0 {
			sent := n - 1
			if err := s.Sync(); err != nil {
				say("error sync: %v", err)

				return
			}
			say("ack %d %d", w, sent)
			acked.Store(sent)
		}
		time.Sleep(time.Duration(rng.Intn(1500)) * time.Microsecond)
	}
}

func killWrite(s *Store, w int64, cfg killConfig, acked *atomic.Int64) {
	rng := rand.New(rand.NewSource(cfg.Seed + w))
	n := cfg.Next[w]
	wr := s.NewWriter()
	var buf []byte
	for {
		// Less than the flush size, so that the writer flushes only when
		// told to.
		last := n + 4 + int64(rng.Intn(20))
		say("intent %d %d", w, last)
		buf = buf[:0]
		for ; n <= last; n++ {
			buf = killDocID{w, n}.appendJSON(buf)
		}
		err := wr.AddNDJSON(buf)
		if err == nil {
			err = wr.Flush()
		}
		if err != nil || wr.Rejected != 0 {
			say("error flush: %d lines rejected, %v", wr.Rejected, err)

			return
		}
		say("ack %d %d", w, last)
		acked.Store(last)
		time.Sleep(time.Duration(rng.Intn(3000)) * time.Microsecond)
	}
}

func killDrop(s *Store, cfg killConfig, acked []atomic.Int64) {
	rng := rand.New(rand.NewSource(cfg.Seed + 100))
	var done int64
	for {
		time.Sleep(time.Duration(50+rng.Intn(200)) * time.Millisecond)
		// Every document of a partition below this hour is acknowledged.
		hour := int64(math.MaxInt64)
		for w := range acked {
			hour = min(hour, firstTimedHour+(acked[w].Load()+1)/killPerHour)
		}
		if hour -= killKept; hour <= done {
			continue
		}
		say("drop %d", hour)
		if _, err := s.DropBefore(hour * int64(time.Hour)); err != nil {
			say("error drop: %v", err)

			return
		}
		say("dropped %d", hour)
		done = hour
	}
}

func killCompact(s *Store, cfg killConfig) {
	rng := rand.New(rand.NewSource(cfg.Seed + 101))
	for {
		time.Sleep(time.Duration(100+rng.Intn(300)) * time.Millisecond)
		if err := s.Compact(); err != nil {
			say("error compact: %v", err)

			return
		}
	}
}

// killRead scans and holds snapshots, which keeps the files of merged and
// dropped parts in the directory for the kill to find.
func killRead(s *Store, cfg killConfig) {
	rng := rand.New(rand.NewSource(cfg.Seed + 102))
	var cols [2]vec.Column
	for {
		snap := s.Table().Snapshot()
		_, err := snap.Scan(context.Background(), ScanOptions{Workers: 2}, func(w int, p *part.Part, b *part.Block) error {
			_, err := column(p, b, "n", vec.KindInt, &cols[w])

			return err
		})
		if err != nil {
			say("error scan: %v", err)

			return
		}
		time.Sleep(time.Duration(rng.Intn(30)) * time.Millisecond)
		snap.Release()
	}
}

// killReport is what a child said before it was killed.
type killReport struct {
	// acked holds, per writer, the last number it reported durable, -1
	// without a report.
	acked []int64
	// intent is the last number of the flush the Writer announced last.
	intent int64
	// drop and dropped are the hours of the last drop announced and of the
	// last one reported done.
	drop, dropped   int64
	started, opened bool
}

func parseKillReport(t *testing.T, lines []string) killReport {
	t.Helper()
	r := killReport{acked: make([]int64, killWriters), intent: -1}
	for w := range r.acked {
		r.acked[w] = -1
	}
	for _, line := range lines {
		var w, n int64
		switch {
		case line == "start":
			r.started = true
		case line == "open":
			r.opened = true
		case scan(line, "ack %d %d", &w, &n):
			r.acked[w] = n
		case scan(line, "intent %d %d", &w, &n):
			r.intent = n
		case scan(line, "dropped %d", &n):
			r.dropped = n
		case scan(line, "drop %d", &n):
			r.drop = n
		default:
			t.Fatalf("the child said %q", line)
		}
	}

	return r
}

func scan(line, format string, args ...any) bool {
	n, err := fmt.Sscanf(line, format, args...)

	return err == nil && n == len(args)
}

// killStats counts what the cycles of one store met.
type killStats struct {
	Cycles, KilledStarting, KilledOpening, KilledRunning int
	// TornRecords, Leftovers and Rewrites count what the kills left in the
	// directory: manifests with a cut record, files no record names, and
	// manifests that were rewritten since the kill before.
	TornRecords, Leftovers, Rewrites int
	Acked, Unacked                   int
	DropsDone, DropsCut, DropsKept   int
	FlushesCut, FlushesKept          int
	MergedParts, Compactions         int
}

func (s *killStats) add(o killStats) {
	s.Cycles += o.Cycles
	s.KilledStarting += o.KilledStarting
	s.KilledOpening += o.KilledOpening
	s.KilledRunning += o.KilledRunning
	s.TornRecords += o.TornRecords
	s.Leftovers += o.Leftovers
	s.Rewrites += o.Rewrites
	s.Acked += o.Acked
	s.Unacked += o.Unacked
	s.DropsDone += o.DropsDone
	s.DropsCut += o.DropsCut
	s.DropsKept += o.DropsKept
	s.FlushesCut += o.FlushesCut
	s.FlushesKept += o.FlushesKept
	s.MergedParts += o.MergedParts
	s.Compactions += o.Compactions
}

// runAndKill starts a child on the store and kills it at a moment drawn
// from rng: while the process starts, while it opens the store, or while it
// writes.
func runAndKill(t *testing.T, cfg killConfig, rng *rand.Rand, stats *killStats) killReport {
	t.Helper()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestStoreKillChild$", "-test.timeout=2m")
	cmd.Env = append(os.Environ(), killChildEnv+"="+string(raw))
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	started, opened, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		defer close(done)
		in := bufio.NewScanner(stdout)
		for in.Scan() {
			line := in.Text()
			lines = append(lines, line)
			switch line {
			case "start":
				close(started)
			case "open":
				close(opened)
			}
		}
	}()
	var reached chan struct{}
	var delay time.Duration
	switch mode := rng.Intn(100); {
	case mode < 15:
		delay = time.Duration(rng.Intn(20000)) * time.Microsecond
	case mode < 40:
		reached, delay = started, time.Duration(rng.Intn(6000))*time.Microsecond
	default:
		reached, delay = opened, time.Duration(rng.Intn(400000))*time.Microsecond
	}
	if reached != nil {
		select {
		case <-reached:
		case <-done:
		case <-time.After(time.Minute):
			_ = cmd.Process.Kill()
			<-done
			t.Fatalf("the child did not get to work within a minute; it said %q and wrote %s", lines, stderr.String())
		}
	}
	select {
	case <-time.After(delay):
	case <-done:
	}
	_ = cmd.Process.Kill()
	<-done
	err = cmd.Wait()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Fatalf("the child ended by itself: %v; it said %q and wrote %s", err, lines, stderr.String())
	}
	r := parseKillReport(t, lines)
	switch {
	case !r.started:
		stats.KilledStarting++
	case !r.opened:
		stats.KilledOpening++
	default:
		stats.KilledRunning++
	}

	return r
}

// scanKillDocs decodes every column of every block of every part, checks
// the rows against what their numbers say and the statistics of blocks and
// parts against the rows, and returns how often each document is there.
func scanKillDocs(snap *Snapshot) (map[killDocID]int, error) {
	counts := map[killDocID]int{}
	var b part.Block
	var scratch, ws, ns, vs, ks, pads, times vec.Column
	for _, h := range snap.Parts() {
		if key := h.SortKey(); !h.Ordered() || len(key) != 1 || key[0] != "k" {
			return nil, fmt.Errorf("part of run %d is ordered by %v", h.Run, key)
		}
		var rows uint64
		minTime, maxTime := int64(math.MaxInt64), int64(math.MinInt64)
		var key, prev []byte
		first, last := h.KeyRange()
		for i := 0; i < h.NumBlocks(); i++ {
			if err := h.OpenBlock(i, &b); err != nil {
				return nil, fmt.Errorf("block %d of a part of run %d: %v", i, h.Run, err)
			}
			cols := b.Columns()
			for c := range cols {
				if err := b.Decode(&cols[c], &scratch); err != nil {
					return nil, fmt.Errorf("column %s of block %d of a part of run %d: %v", h.Paths()[cols[c].PathID], i, h.Run, err)
				}
			}
			if _, err := h.MayContainToken(i, uint64(i)*0x9e3779b97f4a7c15); err != nil {
				return nil, fmt.Errorf("token filter of block %d of a part of run %d: %v", i, h.Run, err)
			}
			for _, c := range []struct {
				path string
				kind vec.Kind
				dst  *vec.Column
			}{
				{"w", vec.KindInt, &ws}, {"n", vec.KindInt, &ns}, {"v", vec.KindInt, &vs},
				{"k", vec.KindString, &ks}, {"pad", vec.KindString, &pads}, {part.TimePath, vec.KindInt, &times},
			} {
				ref, err := column(h.Part, &b, c.path, c.kind, c.dst)
				if err != nil || ref == nil || ref.Count != b.Rows {
					return nil, fmt.Errorf("column %s of block %d of a part of run %d does not hold every row: %v", c.path, i, h.Run, err)
				}
			}
			blockRows, blockMin, blockMax := h.BlockStats(i)
			lo, hi := int64(math.MaxInt64), int64(math.MinInt64)
			for row := 0; row < b.Rows; row++ {
				id := killDocID{ws.Int(row), ns.Int(row)}
				if vs.Int(row) != id.check() || string(ks.Bytes(row)) != id.key() || string(pads.Bytes(row)) != id.pad() || times.Int(row) != id.time() {
					return nil, fmt.Errorf("document %v holds v=%d k=%s pad=%s time=%d", id, vs.Int(row), ks.Bytes(row), pads.Bytes(row), times.Int(row))
				}
				if id.hour() != h.Partition {
					return nil, fmt.Errorf("document %v of hour %d is in partition %d", id, id.hour(), h.Partition)
				}
				counts[id]++
				lo, hi = min(lo, id.time()), max(hi, id.time())
				key = part.AppendKeyString(key[:0], ks.Bytes(row))
				if bytes.Compare(key, prev) < 0 {
					return nil, fmt.Errorf("key %x follows %x in a part of run %d", key, prev, h.Run)
				}
				if row == 0 && !bytes.Equal(key, h.FirstKey(i)) {
					return nil, fmt.Errorf("block %d of a part of run %d starts with key %x, its statistics say %x", i, h.Run, key, h.FirstKey(i))
				}
				prev = append(prev[:0], key...)
			}
			if blockRows != b.Rows || blockMin != lo || blockMax != hi {
				return nil, fmt.Errorf("block %d of a part of run %d holds %d rows of times %d..%d, its statistics say %d rows of %d..%d",
					i, h.Run, b.Rows, lo, hi, blockRows, blockMin, blockMax)
			}
			rows += uint64(b.Rows)
			minTime, maxTime = min(minTime, lo), max(maxTime, hi)
		}
		if rows != h.Rows || rows == 0 || minTime != h.MinTime || maxTime != h.MaxTime {
			return nil, fmt.Errorf("a part of run %d holds %d rows of times %d..%d, its footer says %d rows of %d..%d",
				h.Run, rows, minTime, maxTime, h.Rows, h.MinTime, h.MaxTime)
		}
		if !bytes.Equal(first, h.FirstKey(0)) || !bytes.Equal(last, prev) {
			return nil, fmt.Errorf("a part of run %d has key range %x..%x, its rows %x..%x", h.Run, first, last, h.FirstKey(0), prev)
		}
	}

	return counts, nil
}

func readKillDocs(t *testing.T, snap *Snapshot) map[killDocID]int {
	t.Helper()
	counts, err := scanKillDocs(snap)
	if err != nil {
		t.Fatal(err)
	}

	return counts
}

// killModel is what the parent knows about the store between two children.
type killModel struct {
	// present holds the documents of the last check. They were committed
	// then, so they stay until their partition is dropped.
	present map[killDocID]struct{}
	// next holds, per writer, the first number no child has used.
	next []int64
	// dropped is the hour below which every partition is dropped.
	dropped int64
	// first tells a rewritten manifest from the one before.
	first uint32
}

// judge holds the documents of the store against the contract: what a
// child reported durable, and what was there before, is there once; a
// dropped partition is empty; a drop or a flush that the kill met is there
// as a whole or not at all; nothing else is there.
func (m *killModel) judge(t *testing.T, counts map[killDocID]int, r killReport, stats *killStats) {
	t.Helper()
	done := max(m.dropped, r.dropped)
	underWay := max(done, r.drop)
	writer := int64(killAppenders)
	flushFrom := max(r.acked[writer], m.next[writer]-1)
	var belowDrop, flushed, unacked int
	top := append([]int64(nil), m.next...)
	for id, count := range counts {
		if count != 1 {
			t.Fatalf("document %v is there %d times", id, count)
		}
		top[id.w] = max(top[id.w], id.n+1)
		switch hour := id.hour(); {
		case hour < done:
			t.Fatalf("document %v is in partition %d, below the dropped hour %d", id, hour, done)
		case hour < underWay:
			belowDrop++
		}
		if _, was := m.present[id]; was || id.n <= r.acked[id.w] {
			continue
		}
		if id.n < m.next[id.w] {
			t.Fatalf("document %v is there and was not at the last check", id)
		}
		if id.w == writer {
			if id.n > r.intent {
				t.Fatalf("document %v lies behind the flush %d..%d that was under way", id, flushFrom+1, r.intent)
			}
			flushed++
		}
		unacked++
	}
	// Every document below a drop under way was durable before it began.
	wantedBelowDrop, keptBelowDrop, acked := 0, 0, 0
	must := func(id killDocID) {
		switch hour := id.hour(); {
		case hour < done:
		case hour < underWay:
			wantedBelowDrop++
			keptBelowDrop += counts[id]
		case counts[id] != 1:
			t.Fatalf("document %v was durable and is gone", id)
		}
	}
	for id := range m.present {
		must(id)
	}
	for w := range r.acked {
		for n := m.next[w]; n <= r.acked[w]; n++ {
			if id := (killDocID{int64(w), n}); !has(m.present, id) {
				must(id)
				acked++
			}
		}
		top[w] = max(top[w], r.acked[w]+1)
	}
	top[writer] = max(top[writer], r.intent+1)
	if r.dropped > m.dropped {
		stats.DropsDone++
	}
	switch {
	case underWay == done:
	case belowDrop == 0:
		done = underWay
		stats.DropsCut++
	case belowDrop != wantedBelowDrop || keptBelowDrop != wantedBelowDrop:
		t.Fatalf("the drop below hour %d that was under way left %d of %d documents", underWay, belowDrop, wantedBelowDrop)
	default:
		stats.DropsKept++
	}
	switch want := int(r.intent - flushFrom); {
	case flushed == 0 && want > 0:
		stats.FlushesCut++
	case flushed != 0 && flushed != want:
		t.Fatalf("the flush %d..%d that was under way left %d of %d documents", flushFrom+1, r.intent, flushed, want)
	case flushed != 0:
		stats.FlushesKept++
	}
	stats.Acked += acked
	stats.Unacked += unacked
	m.present = make(map[killDocID]struct{}, len(counts))
	for id := range counts {
		m.present[id] = struct{}{}
	}
	m.next, m.dropped = top, done
}

func has(set map[killDocID]struct{}, id killDocID) bool {
	_, ok := set[id]

	return ok
}

// check opens the store a kill left and holds it against the contract.
func (m *killModel) check(t *testing.T, dir string, r killReport, rng *rand.Rand, stats *killStats) {
	t.Helper()
	log, err := os.ReadFile(filepath.Join(dir, manifestName))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	named, _, valid, err := parseManifest(log)
	if err != nil {
		t.Fatalf("the manifest a kill left: %v", err)
	}
	if valid < len(log) {
		stats.TornRecords++
	}
	if valid > 0 {
		if first := binary.LittleEndian.Uint32(log[4:]); first != m.first {
			if m.first != 0 {
				stats.Rewrites++
			}
			m.first = first
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if _, ok := named[e.Name()]; !ok && (strings.HasSuffix(e.Name(), partSuffix) || strings.HasSuffix(e.Name(), tmpSuffix)) {
			stats.Leftovers++
		}
	}

	opts := killStoreOptions()
	opts.MergeFanIn, opts.FlushInterval = -1, -1
	s, err := OpenStore(dir, opts)
	if err != nil {
		t.Fatalf("opening the store a kill left: %v", err)
	}
	defer s.Close()
	if lost := s.Lost(); len(lost) != 0 {
		t.Fatalf("the open set parts %v aside", lost)
	}
	checkDirMatchesManifest(t, s.dir, "after a kill")
	snap := s.Table().Snapshot()
	counts := readKillDocs(t, snap)
	for _, shape := range runShapes(t, snap) {
		if shape.tier > 0 {
			stats.MergedParts += shape.parts
		}
	}
	snap.Release()
	m.judge(t, counts, r, stats)
	if rng.Intn(4) == 0 {
		if err := s.Compact(); err != nil {
			t.Fatalf("compacting the store a kill left: %v", err)
		}
		snap := s.Table().Snapshot()
		merged := readKillDocs(t, snap)
		snap.Release()
		if !maps.Equal(counts, merged) {
			t.Fatalf("a compaction turned %d documents into %d", len(counts), len(merged))
		}
		checkDirMatchesManifest(t, s.dir, "after a compaction")
		stats.Compactions++
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// runKillCycles kills children on one store until the time is up, each
// starting on what the one before left.
func runKillCycles(t *testing.T, seed int64, budget time.Duration) killStats {
	t.Helper()
	dir, err := os.MkdirTemp("", "lynxdb-store-kill-")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if t.Failed() {
			t.Logf("seed %d; the store is kept in %s", seed, dir)

			return
		}
		os.RemoveAll(dir)
	}()
	rng := rand.New(rand.NewSource(seed))
	m := &killModel{present: map[killDocID]struct{}{}, next: make([]int64, killWriters)}
	var stats killStats
	for deadline := time.Now().Add(budget); time.Now().Before(deadline); stats.Cycles++ {
		cfg := killConfig{Dir: dir, Seed: seed*1000 + int64(stats.Cycles), Next: m.next, RewriteAfter: 1 + rng.Intn(12), NoSync: rng.Intn(2) == 0}
		r := runAndKill(t, cfg, rng, &stats)
		m.check(t, dir, r, rng, &stats)
	}

	return stats
}

func TestStoreSurvivesKills(t *testing.T) {
	if testing.Short() {
		t.Skip("kills child processes for some seconds")
	}
	if runtime.GOOS == "windows" {
		t.Skip("needs a signal that cannot be caught")
	}
	// The parent kills nothing of its own.
	skipSyncs(t)
	budget, soak := 3*time.Second, false
	if v := os.Getenv("LYNXDB_STORE_SOAK"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("LYNXDB_STORE_SOAK: %v", err)
		}
		budget, soak = d, true
	}
	seed := time.Now().UnixNano() % 1e9
	if v := os.Getenv("LYNXDB_STORE_SEED"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatalf("LYNXDB_STORE_SEED: %v", err)
		}
		seed = n
	}
	// Two stores side by side double the kills of a run.
	const stores = 2
	var total killStats
	var mu sync.Mutex
	t.Run("stores", func(t *testing.T) {
		for i := int64(0); i < stores; i++ {
			t.Run(strconv.FormatInt(seed+i, 10), func(t *testing.T) {
				t.Parallel()
				stats := runKillCycles(t, seed+i, budget)
				mu.Lock()
				total.add(stats)
				mu.Unlock()
			})
		}
	})
	t.Logf("seed %d: %+v", seed, total)
	if t.Failed() {
		return
	}
	if total.KilledRunning == 0 || total.Acked == 0 {
		t.Fatalf("no child was killed at work: %+v", total)
	}
	if soak && (total.KilledStarting == 0 || total.KilledOpening == 0 || total.Leftovers == 0 ||
		total.Rewrites == 0 || total.MergedParts == 0 || total.DropsDone == 0 || total.FlushesCut == 0 || total.FlushesKept == 0) {
		t.Fatalf("a soak did not meet every kind of kill: %+v", total)
	}
}
