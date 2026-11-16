package colstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
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

// manifestRewriteAfter is the number of manifest records after which the
// manifest is replaced by a single record listing the live parts.
var manifestRewriteAfter = 1024

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ErrManifest reports a manifest that names a part the directory cannot
// provide.
var ErrManifest = errors.New("colstore: manifest names a missing or damaged part")

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
	dead atomic.Bool
}

// PartMeta is what the manifest records about a part besides its name. Parts
// that share a Run hold one sorted sequence of rows cut by key range; Tier
// counts the merges that produced it.
type PartMeta struct {
	Run  uint64
	Tier int
}

// OpenDir opens the table stored in path, creating the directory when it
// does not exist and removing files no manifest record names.
func OpenDir(path string) (*Dir, error) {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, err
	}
	d := &Dir{path: path, table: NewTable(), live: map[*Handle]*partFileRef{}}
	// Run 0 is left to parts without a run.
	d.nextRun.Store(1)
	names, records, err := d.replay()
	if err != nil {
		return nil, err
	}
	d.records = records

	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, partSuffix) && !strings.HasSuffix(name, tmpSuffix) {
			continue
		}
		if id, ok := partID(name); ok && id >= d.nextID.Load() {
			d.nextID.Store(id + 1)
		}
		if _, ok := names[name]; !ok {
			if err := os.Remove(filepath.Join(path, name)); err != nil {
				return nil, err
			}
		}
	}

	handles := make([]*Handle, 0, len(names))
	for name, meta := range names {
		if meta.Run >= d.nextRun.Load() {
			d.nextRun.Store(meta.Run + 1)
		}
		h, err := d.open(name, meta)
		if err != nil {
			for _, opened := range handles {
				opened.release()
			}

			return nil, fmt.Errorf("%w: %s: %v", ErrManifest, name, err)
		}
		handles = append(handles, h)
	}
	if err := d.table.Apply(handles, nil); err != nil {
		return nil, err
	}
	if d.log, err = os.OpenFile(filepath.Join(path, manifestName), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644); err != nil {
		return nil, err
	}
	if d.records > manifestRewriteAfter {
		if err := d.rewrite(); err != nil {
			return nil, err
		}
	}

	return d, nil
}

func partID(name string) (uint64, bool) {
	name = strings.TrimSuffix(strings.TrimSuffix(name, tmpSuffix), partSuffix)
	id, err := strconv.ParseUint(name, 16, 64)

	return id, err == nil
}

// replay returns the live part names the manifest describes and the number
// of records it holds, cutting off a record torn by a crash.
func (d *Dir) replay() (names map[string]PartMeta, records int, err error) {
	names = map[string]PartMeta{}
	file := filepath.Join(d.path, manifestName)
	data, err := os.ReadFile(file)
	if errors.Is(err, os.ErrNotExist) {
		return names, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	valid := 0
	for rest := data; len(rest) >= 8; {
		size := int(binary.LittleEndian.Uint32(rest))
		if size > len(rest)-8 || crc32.Checksum(rest[8:8+size], castagnoli) != binary.LittleEndian.Uint32(rest[4:]) {
			break
		}
		if !applyRecord(names, rest[8:8+size]) {
			break
		}
		rest = rest[8+size:]
		valid = len(data) - len(rest)
		records++
	}
	if valid < len(data) {
		if err := os.Truncate(file, int64(valid)); err != nil {
			return nil, 0, err
		}
	}

	return names, records, nil
}

// A manifest record lists the parts a change adds, each with its run and
// tier, and then the parts it removes.
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
			payload = payload[used+used2:]
			names[name] = PartMeta{Run: run, Tier: int(tier)}
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
// deletes it once dead, when its last reader is gone.
func (d *Dir) open(name string, meta PartMeta) (*Handle, error) {
	file := filepath.Join(d.path, name)
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	data, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		f.Close()

		return nil, err
	}
	p, err := part.Open(data)
	if err != nil {
		_ = data.Unmap()
		f.Close()

		return nil, err
	}
	ref := &partFileRef{name: name, meta: meta}
	h := NewHandle(p, func() {
		_ = data.Unmap()
		f.Close()
		if ref.dead.Load() {
			os.Remove(file)
		}
	})
	h.PartMeta = meta
	d.mu.Lock()
	d.live[h] = ref
	d.mu.Unlock()

	return h, nil
}

// NewRun returns a run number no part of the table has.
func (d *Dir) NewRun() uint64 { return d.nextRun.Add(1) - 1 }

// Table returns the table of live parts.
func (d *Dir) Table() *Table { return d.table }

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
// or none of it. It returns the handles of the added parts.
func (d *Dir) Commit(add []*PartFile, remove []*Handle) ([]*Handle, error) {
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
	handles := make([]*Handle, 0, len(add))
	for i, name := range names {
		h, err := d.open(name, metas[i])
		if err != nil {
			return nil, err
		}
		handles = append(handles, h)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	gone := make([]string, 0, len(remove))
	for _, h := range remove {
		ref := d.live[h]
		if ref == nil {
			return nil, ErrUnknownPart
		}
		gone = append(gone, ref.name)
	}
	if _, err := d.log.Write(appendRecord(nil, names, metas, gone)); err != nil {
		return nil, err
	}
	if err := d.log.Sync(); err != nil {
		return nil, err
	}
	d.records++
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
	if err := pf.f.Sync(); err != nil {
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
		err = f.Sync()
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
	err = f.Sync()
	if cerr := f.Close(); err == nil {
		err = cerr
	}

	return err
}
