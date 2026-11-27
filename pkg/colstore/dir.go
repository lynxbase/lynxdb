package colstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/blevesearch/mmap-go"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
)

const (
	manifestName = "MANIFEST"
	// endName is the file of marks that say how far the manifest reached.
	endName = manifestName + ".end"
	// lostName is the directory for parts that no record names and that
	// no mark allows to be removed.
	lostName   = "lost+found"
	partSuffix = ".part"
	tmpSuffix  = ".tmp"
)

// readAdvice is cleared by benchmarks that measure reads without advice to
// the kernel.
var readAdvice = true

// manifestRewriteAfter is the number of manifest records after which the
// manifest is replaced by a single record listing the live parts.
var manifestRewriteAfter = 1024

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// syncFile makes the bytes of a file or the entries of a directory durable;
// tests replace it to fail or to hold a write.
var syncFile = (*os.File).Sync

// ErrManifest reports a manifest that is damaged or names a part the
// directory cannot provide.
var ErrManifest = errors.New("colstore: manifest is damaged or names a missing or damaged part")

// ErrClosed reports the use of a store or a directory after its Close.
var ErrClosed = errors.New("colstore: closed")

// ErrFailed reports a directory that takes no further changes because its
// table and its manifest may have come apart: a record could be neither
// made durable nor taken back.
var ErrFailed = errors.New("colstore: directory takes no changes after a failed manifest write")

// Dir keeps a table's parts as files in one directory. A manifest, an
// append-only log of part set changes, names the live files; a change is
// visible to readers only after its record is on disk.
type Dir struct {
	path    string
	table   *Table
	nextID  atomic.Uint64
	nextRun atomic.Uint64
	// flushed and merged count the bytes of the part files committed
	// without and with parts they replace.
	flushed, merged atomic.Int64

	mu      sync.Mutex
	log     *os.File
	records int
	// size is the length of the records in the log and first the checksum
	// of the first of them, zero for an empty log.
	size  int64
	first uint32
	// end holds the marks; slot is the one of the log in use.
	end  *os.File
	slot int
	lost []string
	live map[*Handle]*partFileRef
	// Retired parts can outlive the directory through a snapshot. Close
	// unlinks those still pinned before a new directory can reuse their names.
	retiredMu sync.Mutex
	retired   map[string]struct{}
	// failed, once set, is what every further change returns.
	failed error
	closed bool
}

// partFileRef ties a live handle to its file.
type partFileRef struct {
	name string
	meta PartMeta
	// data maps the part file, and blocks maps it once more for the
	// column chunks when they have advice of their own.
	data   []byte
	blocks []byte
}

// PartMeta is what the manifest records about a part besides its name. Parts
// that share a Run hold one sorted sequence of rows cut by key range; Tier
// counts the merges that produced it; Partition numbers the span of time the
// rows belong to, zero for a table that is not partitioned.
type PartMeta struct {
	Run       uint64
	Tier      int
	Partition int64
}

// OpenDir opens the table stored in path, creating the directory when it
// does not exist and removing files no manifest record names. An open that
// fails with ErrManifest leaves every file as it found it. A part file is
// removed only under a mark that says the manifest has lost no record;
// without one it is moved to the lost+found directory, see Lost.
func OpenDir(path string) (*Dir, error) {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, err
	}
	d := &Dir{path: path, table: NewTable(), live: map[*Handle]*partFileRef{}, retired: map[string]struct{}{}}
	// Run 0 is left to parts without a run.
	d.nextRun.Store(1)
	file := filepath.Join(path, manifestName)
	log, err := os.ReadFile(file)
	missing := errors.Is(err, os.ErrNotExist)
	if err != nil && !missing {
		return nil, err
	}
	names, records, valid, err := parseManifest(log)
	if err != nil {
		return nil, err
	}
	d.records, d.size = records, int64(valid)
	if valid > 0 {
		d.first = binary.LittleEndian.Uint32(log[4:])
	}
	slot, err := readEnd(path, endMark{size: d.size, first: d.first})
	if err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	// The names of parts set aside are not given to parts again.
	kept, _ := os.ReadDir(filepath.Join(path, lostName))
	for _, e := range kept {
		d.skipID(e.Name())
	}
	var unnamed []string
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, partSuffix) && !strings.HasSuffix(name, tmpSuffix) {
			continue
		}
		d.skipID(name)
		if _, ok := names[name]; !ok {
			// A finished part is written after the manifest exists, so one
			// without a manifest lost it and may be live.
			if missing && strings.HasSuffix(name, partSuffix) {
				return nil, fmt.Errorf("%w: part %s has no manifest", ErrManifest, name)
			}
			unnamed = append(unnamed, name)
		}
	}

	handles := make([]*Handle, 0, len(names))
	for name, meta := range names {
		if meta.Run >= d.nextRun.Load() {
			d.nextRun.Store(meta.Run + 1)
		}
		h, ref, err := d.open(name, meta)
		if err != nil {
			for _, opened := range handles {
				opened.release()
			}

			return nil, fmt.Errorf("%w: %s: %v", ErrManifest, name, err)
		}
		handles = append(handles, h)
		d.live[h] = ref
	}
	if err := d.table.Apply(handles, nil); err != nil {
		return nil, err
	}
	// Files are cut and removed only once every live part has opened: when
	// one is missing, an unnamed file may hold the only copy of its rows.
	if d.lost, err = tidy(path, unnamed, valid, len(log), slot >= 0); err != nil {
		d.table.Close()

		return nil, err
	}
	if d.log, err = os.OpenFile(file, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644); err != nil {
		d.table.Close()

		return nil, err
	}
	if d.end, err = os.OpenFile(filepath.Join(path, endName), os.O_RDWR|os.O_CREATE, 0o644); err != nil {
		d.log.Close()
		d.table.Close()

		return nil, err
	}
	d.slot = max(slot, 0)
	d.mark(d.slot, endMark{size: d.size, first: d.first})
	d.trim()

	return d, nil
}

// tidy cuts the manifest to the size of its whole records and clears away
// the files it does not name. When no mark says the manifest is whole, a
// finished part may be one that a lost record named: it is moved aside and
// returned, not removed.
func tidy(path string, unnamed []string, valid, size int, whole bool) (lost []string, err error) {
	if valid < size {
		if err := os.Truncate(filepath.Join(path, manifestName), int64(valid)); err != nil {
			return nil, err
		}
	}
	for _, name := range unnamed {
		file := filepath.Join(path, name)
		if whole || !strings.HasSuffix(name, partSuffix) {
			if err := os.Remove(file); err != nil {
				return lost, err
			}

			continue
		}
		if err := os.MkdirAll(filepath.Join(path, lostName), 0o755); err != nil {
			return lost, err
		}
		if err := os.Rename(file, filepath.Join(path, lostName, name)); err != nil {
			return lost, err
		}
		lost = append(lost, name)
	}

	return lost, nil
}

// Lost returns the names of the part files that the open moved into the
// lost+found directory of the table.
func (d *Dir) Lost() []string { return d.lost }

// endMark says how far a manifest reached: the size of its records, and the
// checksum of the first of them, which tells a rewritten manifest from the
// one it replaced. A manifest shorter than its mark has lost records.
type endMark struct {
	size  int64
	first uint32
}

const endMarkSize = 16

func (m endMark) bytes() []byte {
	b := make([]byte, endMarkSize)
	binary.LittleEndian.PutUint64(b, uint64(m.size))
	binary.LittleEndian.PutUint32(b[8:], m.first)
	binary.LittleEndian.PutUint32(b[12:], crc32.Checksum(b[:12], castagnoli))

	return b
}

// readEnd compares the manifest that log describes with the marks of the
// directory. It returns the slot of a mark that says the manifest is whole,
// -1 when no mark speaks of it, and ErrManifest when it is shorter than a
// mark of it.
func readEnd(path string, log endMark) (slot int, err error) {
	data, err := os.ReadFile(filepath.Join(path, endName))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	slot = -1
	for i := 0; len(data) >= (i+1)*endMarkSize; i++ {
		b := data[i*endMarkSize:]
		if crc32.Checksum(b[:12], castagnoli) != binary.LittleEndian.Uint32(b[12:]) {
			continue
		}
		size, first := int64(binary.LittleEndian.Uint64(b)), binary.LittleEndian.Uint32(b[8:])
		switch {
		// A manifest never goes back to no records, whatever replaced it.
		case size > log.size && (first == log.first || log.size == 0):
			return 0, fmt.Errorf("%w: %d bytes of records are left of %d", ErrManifest, log.size, size)
		// The mark of an empty manifest stays until the first record is
		// marked, which a crash can come before.
		case first == log.first || size == 0:
			slot = i
		}
	}

	return slot, nil
}

// mark notes in a slot of the end file how far a manifest has reached. The
// file is not synced: a mark that is behind its manifest claims less. A mark
// follows the sync of its record, so a crash between the two that is followed
// by the loss of exactly that record goes unseen.
func (d *Dir) mark(slot int, m endMark) {
	_, _ = d.end.WriteAt(m.bytes(), int64(slot)*endMarkSize)
}

func (d *Dir) skipID(name string) {
	if id, ok := partID(name); ok && id >= d.nextID.Load() {
		d.nextID.Store(id + 1)
	}
}

func partID(name string) (uint64, bool) {
	name = strings.TrimSuffix(strings.TrimSuffix(name, tmpSuffix), partSuffix)
	id, err := strconv.ParseUint(name, 16, 64)

	return id, err == nil
}

// parseManifest returns the live part names the records of data describe,
// the number of records and their size; bytes behind them are a torn append.
func parseManifest(data []byte) (names map[string]PartMeta, records, valid int, err error) {
	names = map[string]PartMeta{}
	for valid < len(data) {
		payload, ok := wholeRecord(data[valid:])
		if !ok {
			if !tornAppend(data[valid:]) {
				return nil, 0, 0, fmt.Errorf("%w: record %d is damaged", ErrManifest, records)
			}

			break
		}
		if !applyRecord(names, payload) {
			return nil, 0, 0, fmt.Errorf("%w: record %d is malformed", ErrManifest, records)
		}
		valid += 8 + len(payload)
		records++
	}

	return names, records, valid, nil
}

// wholeRecord returns the payload of the record that data starts with when
// all of it is there and matches its checksum.
func wholeRecord(data []byte) ([]byte, bool) {
	if len(data) < 8 {
		return nil, false
	}
	size := uint64(binary.LittleEndian.Uint32(data))
	// Zeros pass for an empty record, which no change writes.
	if size == 0 || size > uint64(len(data)-8) || crc32.Checksum(data[8:8+size], castagnoli) != binary.LittleEndian.Uint32(data[4:]) {
		return nil, false
	}

	return data[8 : 8+size], true
}

// tornAppend reports whether tail, which does not start with a whole record,
// is what a crash during an append leaves: zeros, or the first bytes of one
// record. Anything else is damage, and cutting it off would drop the records
// behind it.
func tornAppend(tail []byte) bool {
	if len(bytes.TrimLeft(tail, "\x00")) == 0 || len(tail) < 8 {
		return true
	}
	size := uint64(binary.LittleEndian.Uint32(tail))
	if size <= uint64(len(tail)-8) {
		return false
	}
	// A record that is whole under another size has a damaged size.
	if crc32.Checksum(tail[8:], castagnoli) == binary.LittleEndian.Uint32(tail[4:]) {
		return false
	}
	for at := 1; at <= len(tail)-8; at++ {
		if _, ok := wholeRecord(tail[at:]); ok {
			return false
		}
	}

	return true
}

// A manifest record lists the parts a change adds, each with its run, tier
// and partition, and then the parts it removes.
func applyRecord(names map[string]PartMeta, payload []byte) bool {
	for pass := 0; pass < 2; pass++ {
		n, used := binary.Uvarint(payload)
		if used <= 0 {
			return false
		}
		payload = payload[used:]
		for ; n > 0; n-- {
			size, used := binary.Uvarint(payload)
			if used <= 0 || size > uint64(len(payload)-used) {
				return false
			}
			name := string(payload[used : used+int(size)])
			payload = payload[used+int(size):]
			if pass == 1 {
				delete(names, name)

				continue
			}
			run, used := binary.Uvarint(payload)
			if used <= 0 {
				return false
			}
			tier, used2 := binary.Uvarint(payload[used:])
			if used2 <= 0 {
				return false
			}
			partition, used3 := binary.Varint(payload[used+used2:])
			if used3 <= 0 {
				return false
			}
			payload = payload[used+used2+used3:]
			names[name] = PartMeta{Run: run, Tier: int(tier), Partition: partition}
		}
	}

	return len(payload) == 0
}

func appendRecord(dst []byte, add []string, meta []PartMeta, remove []string) []byte {
	start := len(dst)
	dst = append(dst, 0, 0, 0, 0, 0, 0, 0, 0)
	dst = binary.AppendUvarint(dst, uint64(len(add)))
	for i, name := range add {
		dst = binary.AppendUvarint(dst, uint64(len(name)))
		dst = append(dst, name...)
		dst = binary.AppendUvarint(dst, meta[i].Run)
		dst = binary.AppendUvarint(dst, uint64(meta[i].Tier))
		dst = binary.AppendVarint(dst, meta[i].Partition)
	}
	dst = binary.AppendUvarint(dst, uint64(len(remove)))
	for _, name := range remove {
		dst = binary.AppendUvarint(dst, uint64(len(name)))
		dst = append(dst, name...)
	}
	payload := dst[start+8:]
	binary.LittleEndian.PutUint32(dst[start:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(dst[start+4:], crc32.Checksum(payload, castagnoli))

	return dst
}

// open maps a part file and wraps it in a handle that unmaps it, and
// deletes it once dead, when its last reader is gone. The part is live only
// once a manifest record names it.
func (d *Dir) open(name string, meta PartMeta) (*Handle, *partFileRef, error) {
	file := filepath.Join(d.path, name)
	f, err := os.Open(file)
	if err != nil {
		return nil, nil, err
	}
	data, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		f.Close()

		return nil, nil, err
	}
	p, err := part.Open(data)
	if err != nil {
		_ = data.Unmap()
		f.Close()

		return nil, nil, err
	}
	var blocks mmap.MMap
	if readAdvice {
		// Column chunks and the pages of token filters are read one here
		// and one there, each asked for in one piece, through a mapping of
		// their own. The directories and the footer are read side by side
		// and keep the kernel's own read-ahead.
		if blocks, err = mmap.Map(f, mmap.RDONLY, 0); err != nil {
			_ = data.Unmap()
			f.Close()

			return nil, nil, err
		}
		adviseRandom(blocks)
		p.Detach()
		p.SetBlockData(blocks)
		p.SetReadAhead(func(off, n int) { adviseWillNeed(blocks, off, n) })
	}
	ref := &partFileRef{name: name, meta: meta, data: data, blocks: blocks}
	h := NewHandle(p, func() {
		_ = data.Unmap()
		if blocks != nil {
			_ = blocks.Unmap()
		}
		f.Close()
		d.retiredMu.Lock()
		if _, dead := d.retired[name]; dead {
			os.Remove(file)
			delete(d.retired, name)
		}
		d.retiredMu.Unlock()
	})
	h.PartMeta = meta

	return h, ref, nil
}

// NewRun returns a run number no part of the table has.
func (d *Dir) NewRun() uint64 { return d.nextRun.Add(1) - 1 }

// Table returns the table of live parts.
func (d *Dir) Table() *Table { return d.table }

// ReleasePages lets go of the pages of the live part files that this process
// holds in memory through their mappings; reads bring them back from the
// page cache or the disk. The kernel does not drop cached pages that a
// process still maps, so a cold read is measured after this call.
func (d *Dir) ReleasePages() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for h, ref := range d.live {
		adviseDontNeed(ref.data)
		adviseDontNeed(ref.blocks)
		h.ForgetReadAhead()
	}
}

// PartFile is a part being written; it becomes visible through Commit, which
// records Meta with it.
type PartFile struct {
	Meta PartMeta

	f    *os.File
	name string
	tmp  string
	size int64
	// begun is how much of the file the device was asked to start on.
	begun int64
	// before, when set, runs ahead of every write, which fails with what it
	// returns.
	before func() error
}

// writebackStep is the bytes written to a part file between requests to
// the device to start on them.
const writebackStep = 1 << 20

// Create starts a part file that is a run of its own. Any number of them can
// be written at once.
func (d *Dir) Create() (*PartFile, error) {
	name := fmt.Sprintf("%016x%s", d.nextID.Add(1)-1, partSuffix)
	tmp := filepath.Join(d.path, name+tmpSuffix)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}

	return &PartFile{Meta: PartMeta{Run: d.NewRun()}, f: f, name: name, tmp: tmp}, nil
}

// Write appends part bytes to the file.
func (pf *PartFile) Write(p []byte) (int, error) {
	if pf.before != nil {
		if err := pf.before(); err != nil {
			return 0, err
		}
	}
	n, err := pf.f.Write(p)
	pf.size += int64(n)
	if pf.size-pf.begun >= writebackStep {
		startWriteback(pf.f, pf.begun, pf.size-pf.begun)
		pf.begun = pf.size
	}

	return n, err
}

// BeforeWrite makes fn run ahead of every write to the file, which lets a
// writer of less urgent parts wait its turn at the disk or give up: a write
// fails with the error fn returns.
func (pf *PartFile) BeforeWrite(fn func() error) { pf.before = fn }

// Abort discards the file.
func (pf *PartFile) Abort() {
	pf.f.Close()
	os.Remove(pf.tmp)
}

var _ io.Writer = (*PartFile)(nil)

// Commit makes the written parts durable and swaps them for the removed
// ones in one manifest record, so a crash leaves either all of the change
// or none of it. It returns the handles of the added parts; when it fails,
// the table is unchanged and the files of the added parts are gone.
func (d *Dir) Commit(add []*PartFile, remove []*Handle) ([]*Handle, error) {
	return d.commit(add, remove, d.table.Apply)
}

func (d *Dir) commit(add []*PartFile, remove []*Handle, publish func(add, remove []*Handle) error) (handles []*Handle, err error) {
	// Files a record may name stay for the next open to judge.
	recorded := false
	defer func() {
		if err == nil {
			return
		}
		for _, h := range handles {
			h.release()
		}
		handles = nil
		for _, pf := range add {
			pf.f.Close()
			if !recorded {
				os.Remove(pf.tmp)
				os.Remove(filepath.Join(d.path, pf.name))
			}
		}
	}()
	names := make([]string, len(add))
	metas := make([]PartMeta, len(add))
	sealed := make([]error, len(add))
	var wg sync.WaitGroup
	for i, pf := range add {
		names[i], metas[i] = pf.name, pf.Meta
		wg.Add(1)
		go func() {
			defer wg.Done()
			sealed[i] = pf.seal(filepath.Join(d.path, pf.name))
		}()
	}
	wg.Wait()
	if err := errors.Join(sealed...); err != nil {
		return nil, err
	}
	if err := syncDir(d.path); err != nil {
		return nil, err
	}
	refs := make([]*partFileRef, 0, len(add))
	for i, name := range names {
		h, ref, err := d.open(name, metas[i])
		if err != nil {
			return handles, err
		}
		handles, refs = append(handles, h), append(refs, ref)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failed != nil {
		return handles, d.failed
	}
	gone := make([]string, 0, len(remove))
	for i, h := range remove {
		ref := d.live[h]
		if ref == nil || slices.Contains(remove[:i], h) {
			return handles, ErrUnknownPart
		}
		gone = append(gone, ref.name)
	}
	recorded = true
	if err := d.appendLog(appendRecord(nil, names, metas, gone)); err != nil {
		recorded = d.failed != nil

		return handles, err
	}
	for i, h := range handles {
		d.live[h] = refs[i]
	}
	d.retiredMu.Lock()
	for _, h := range remove {
		d.retired[d.live[h].name] = struct{}{}
		delete(d.live, h)
	}
	d.retiredMu.Unlock()
	if err := publish(handles, remove); err != nil {
		// The record stands, so the table must follow it or stop.
		d.failed = fmt.Errorf("%w: %w", ErrFailed, err)

		return nil, d.failed
	}
	d.trim()
	written := &d.flushed
	if len(remove) > 0 {
		written = &d.merged
	}
	for _, pf := range add {
		written.Add(pf.size)
	}

	return handles, nil
}

// Written returns the bytes of the part files committed so far: those that
// added rows and those that replaced other parts.
func (d *Dir) Written() (flushed, merged int64) { return d.flushed.Load(), d.merged.Load() }

// appendLog makes record the last of the manifest and durable. When it
// cannot, it cuts the log back to the records before; when that fails as
// well, the manifest may or may not hold the record after a restart, and the
// directory takes no further changes.
func (d *Dir) appendLog(record []byte) error {
	_, err := d.log.Write(record)
	if err == nil {
		err = syncFile(d.log)
	}
	if err == nil {
		if d.size == 0 {
			d.first = binary.LittleEndian.Uint32(record[4:])
		}
		d.records++
		d.size += int64(len(record))
		d.mark(d.slot, endMark{size: d.size, first: d.first})

		return nil
	}
	undo := d.log.Truncate(d.size)
	if undo == nil {
		undo = syncFile(d.log)
	}
	if undo != nil {
		d.failed = fmt.Errorf("%w: %w", ErrFailed, err)
	}

	return err
}

// seal makes the file durable under its final name.
func (pf *PartFile) seal(name string) error {
	if err := syncFile(pf.f); err != nil {
		return err
	}
	if err := pf.f.Close(); err != nil {
		return err
	}

	return os.Rename(pf.tmp, name)
}

// trim rewrites a manifest that has grown long. A rewrite that fails leaves
// a log that works, and the next change tries again.
func (d *Dir) trim() {
	if d.records > manifestRewriteAfter {
		_ = d.rewrite()
	}
}

// rewrite replaces the manifest by one record listing the live parts.
func (d *Dir) rewrite() error {
	names := make([]string, 0, len(d.live))
	metas := make([]PartMeta, 0, len(d.live))
	for _, ref := range d.live {
		names = append(names, ref.name)
		metas = append(metas, ref.meta)
	}
	file := filepath.Join(d.path, manifestName)
	tmp := file + tmpSuffix
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	record := appendRecord(nil, names, metas, nil)
	next := endMark{size: int64(len(record)), first: binary.LittleEndian.Uint32(record[4:])}
	if _, err = f.Write(record); err == nil {
		err = syncFile(f)
	}
	if err == nil {
		// The mark of the rewritten manifest is there before the manifest
		// is, in the slot that the manifest in use leaves alone.
		d.mark(1-d.slot, next)
		// A manifest that begins like the one in use cannot be told from
		// it, so no mark of that beginning may claim more than it holds.
		if next.first == d.first {
			d.mark(d.slot, endMark{size: min(next.size, d.size), first: d.first})
		}
		err = os.Rename(tmp, file)
	}
	if err != nil {
		f.Close()
		os.Remove(tmp)

		return err
	}
	// The renamed file is the log from here on, also when the directory
	// cannot be synced: records appended to the file it replaced would be
	// lost. The next change syncs the directory before it adds a record.
	d.log.Close()
	d.log, d.records, d.size = f, 1, next.size
	d.slot, d.first = 1-d.slot, next.first
	// Errors of the log then carry its name and not the temporary one.
	if named, err := os.OpenFile(file, os.O_WRONLY|os.O_APPEND, 0o644); err == nil {
		f.Close()
		d.log = named
	}

	return syncDir(d.path)
}

// Close releases the table's parts and the manifest; parts held by
// snapshots stay readable until those are released. A closed directory
// returns ErrClosed for every change.
func (d *Dir) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	if d.failed == nil {
		d.failed = ErrClosed
	}
	d.table.Close()
	d.retiredMu.Lock()
	for name := range d.retired {
		os.Remove(filepath.Join(d.path, name))
	}
	clear(d.retired)
	d.retiredMu.Unlock()
	d.end.Close()

	return d.log.Close()
}

// syncDir makes the directory's entries durable.
func syncDir(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	err = syncFile(f)
	if cerr := f.Close(); err == nil {
		err = cerr
	}

	return err
}
