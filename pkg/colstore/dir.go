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
	partSuffix   = ".part"
	tmpSuffix    = ".tmp"
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

// Dir keeps a table's parts as files in one directory. A manifest, an
// append-only log of part set changes, names the live files; a change is
// visible to readers only after its record is on disk.
type Dir struct {
	path    string
	table   *Table
	nextID  atomic.Uint64
	nextRun atomic.Uint64

	mu      sync.Mutex
	log     *os.File
	records int
	live    map[*Handle]*partFileRef
}

// partFileRef ties a handle to its file; dead marks a part the manifest no
// longer names, whose file goes away with its last reader.
type partFileRef struct {
	name string
	meta PartMeta
	data []byte // the mapping of the part file
	dead atomic.Bool
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
// fails with ErrManifest leaves every file as it found it.
func OpenDir(path string) (*Dir, error) {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, err
	}
	d := &Dir{path: path, table: NewTable(), live: map[*Handle]*partFileRef{}}
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
	d.records = records

	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var unnamed []string
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, partSuffix) && !strings.HasSuffix(name, tmpSuffix) {
			continue
		}
		if id, ok := partID(name); ok && id >= d.nextID.Load() {
			d.nextID.Store(id + 1)
		}
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
	if err := tidy(path, unnamed, valid, len(log)); err != nil {
		d.table.Close()

		return nil, err
	}
	if d.log, err = os.OpenFile(file, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644); err != nil {
		d.table.Close()

		return nil, err
	}
	if d.records > manifestRewriteAfter {
		if err := d.rewrite(); err != nil {
			d.table.Close()

			return nil, err
		}
	}

	return d, nil
}

// tidy cuts the manifest to the size of its whole records and removes the
// files it does not name.
func tidy(path string, unnamed []string, valid, size int) error {
	if valid < size {
		if err := os.Truncate(filepath.Join(path, manifestName), int64(valid)); err != nil {
			return err
		}
	}
	for _, name := range unnamed {
		if err := os.Remove(filepath.Join(path, name)); err != nil {
			return err
		}
	}

	return nil
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
	if readAdvice {
		// Blocks are read a chunk here and a filter page there, each asked
		// for in one piece; the directories and the footer behind them are
		// read side by side and keep the kernel's own read-ahead.
		adviseRandom(data[:p.BlocksEnd()&^(pageSize-1)])
		p.SetReadAhead(func(off, n int) { adviseWillNeed(data, off, n) })
	}
	ref := &partFileRef{name: name, meta: meta, data: data}
	h := NewHandle(p, func() {
		_ = data.Unmap()
		f.Close()
		if ref.dead.Load() {
			os.Remove(file)
		}
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
	for _, ref := range d.live {
		adviseDontNeed(ref.data)
	}
}

// PartFile is a part being written; it becomes visible through Commit, which
// records Meta with it.
type PartFile struct {
	Meta PartMeta

	f    *os.File
	name string
	tmp  string
}

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
func (pf *PartFile) Write(p []byte) (int, error) { return pf.f.Write(p) }

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
func (d *Dir) Commit(add []*PartFile, remove []*Handle) (handles []*Handle, err error) {
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
	gone := make([]string, 0, len(remove))
	for i, h := range remove {
		ref := d.live[h]
		if ref == nil || slices.Contains(remove[:i], h) {
			return handles, ErrUnknownPart
		}
		gone = append(gone, ref.name)
	}
	recorded = true
	if _, err := d.log.Write(appendRecord(nil, names, metas, gone)); err != nil {
		return handles, err
	}
	if err := syncFile(d.log); err != nil {
		return handles, err
	}
	d.records++
	for i, h := range handles {
		d.live[h] = refs[i]
	}
	for _, h := range remove {
		d.live[h].dead.Store(true)
		delete(d.live, h)
	}
	if err := d.table.Apply(handles, remove); err != nil {
		return nil, err
	}
	if d.records > manifestRewriteAfter {
		if err := d.rewrite(); err != nil {
			return nil, err
		}
	}

	return handles, nil
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
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err = f.Write(appendRecord(nil, names, metas, nil)); err == nil {
		err = syncFile(f)
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, file); err != nil {
		return err
	}
	if err := syncDir(d.path); err != nil {
		return err
	}
	if d.log != nil {
		d.log.Close()
	}
	d.log, err = os.OpenFile(file, os.O_WRONLY|os.O_APPEND, 0o644)
	d.records = 1

	return err
}

// Close releases the table's parts and the manifest; parts held by
// snapshots stay readable until those are released.
func (d *Dir) Close() error {
	d.table.Close()
	d.mu.Lock()
	defer d.mu.Unlock()

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
