package colstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"
)

// ErrInvalidArchive reports an invalid partition archive or archive path.
var ErrInvalidArchive = errors.New("colstore: invalid partition archive")

// ErrArchiveExists reports a detach destination that already exists.
var ErrArchiveExists = errors.New("colstore: archive destination already exists")

// ErrIncompatibleArchive reports a partition from an index with different settings.
var ErrIncompatibleArchive = errors.New("colstore: partition archive has different index settings")

const (
	archiveManifest = "partition.json"
	// Bound the metadata read into memory from a user-supplied directory.
	maxArchiveManifest = 64 << 20
)

// An archive names each part by its position in Parts. Names from a foreign
// manifest are never interpreted as paths. On attach each part starts a run
// in the destination's own run namespace.
type partitionArchive struct {
	Version        int           `json:"version"`
	ID             int64         `json:"partition_id"`
	Width          time.Duration `json:"partition_ns"`
	SortKey        []string      `json:"sort_key"`
	TimeFields     []string      `json:"time_fields"`
	PartDictionary bool          `json:"part_dictionary"`
	Parts          []archivePart `json:"parts"`
}

type archivePart struct {
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

func archivePartName(i int) string { return fmt.Sprintf("%016x.part", i) }

func (s *Store) archiveTimeFields() []string {
	if s.opts.TimeField != "" {
		return append([]string{s.opts.TimeField}, s.opts.TimeFields...)
	}
	return s.opts.TimeFields
}

// archiveDirectory pins each directory from the volume root to the archive.
// The recorded identities let a writer detect a move before publishing or
// removing anything at the operator's chosen location.
type archiveDirectory struct {
	root   *os.Root
	parent *archiveDirectory
	name   string
	info   os.FileInfo
	data   os.FileInfo
}

func (d *archiveDirectory) close() {
	for ; d != nil; d = d.parent {
		d.root.Close()
	}
}

func (d *archiveDirectory) verify() error {
	for ; d.parent != nil; d = d.parent {
		current, err := d.parent.root.Lstat(d.name)
		if err != nil || !os.SameFile(current, d.info) {
			return fmt.Errorf("%w: directory %s moved", ErrInvalidArchive, d.root.Name())
		}
	}
	return nil
}

func (d *archiveDirectory) openChild(name string) (*archiveDirectory, error) {
	if !filepath.IsLocal(name) || filepath.Base(name) != name || name == "." {
		return nil, fmt.Errorf("%w: invalid directory name %q", ErrInvalidArchive, name)
	}
	info, err := d.root.Lstat(name)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidArchive, err)
	}
	if !info.IsDir() || os.SameFile(info, d.data) {
		return nil, fmt.Errorf("%w: choose a directory outside the data directory without symbolic links", ErrInvalidArchive)
	}
	root, err := d.root.OpenRoot(name)
	if err != nil {
		return nil, err
	}
	actual, err := root.Stat(".")
	if err != nil || !os.SameFile(actual, info) {
		root.Close()
		return nil, fmt.Errorf("%w: %s changed while opening", ErrInvalidArchive, name)
	}
	return &archiveDirectory{root: root, parent: d, name: name, info: info, data: d.data}, nil
}

// archiveParent resolves existing aliases such as /var on macOS, then opens
// the canonical parent one component at a time. All subsequent operations use
// these directory handles and a single local basename.
func (s *Store) archiveParent(path string) (*archiveDirectory, string, error) {
	if !filepath.IsAbs(path) {
		return nil, "", fmt.Errorf("%w: supply an absolute path on the server", ErrInvalidArchive)
	}
	path = filepath.Clean(path)
	name := filepath.Base(path)
	if !filepath.IsLocal(name) || name == "." {
		return nil, "", fmt.Errorf("%w: invalid archive directory name", ErrInvalidArchive)
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		return nil, "", fmt.Errorf("%w: archive parent: %w", ErrInvalidArchive, err)
	}
	protected := s.opts.DataDir
	if protected == "" {
		protected = s.dir.path
	}
	data, err := os.Stat(protected)
	if err != nil {
		return nil, "", err
	}
	volume := filepath.VolumeName(parent)
	root, err := os.OpenRoot(volume + string(filepath.Separator))
	if err != nil {
		return nil, "", err
	}
	info, err := root.Stat(".")
	if err != nil {
		root.Close()
		return nil, "", err
	}
	if os.SameFile(info, data) {
		root.Close()
		return nil, "", fmt.Errorf("%w: archive must be outside the data directory", ErrInvalidArchive)
	}
	dir := &archiveDirectory{root: root, info: info, data: data}
	for component := range strings.SplitSeq(strings.TrimPrefix(parent, volume), string(filepath.Separator)) {
		if component == "" {
			continue
		}
		child, err := dir.openChild(component)
		if err != nil {
			dir.close()
			return nil, "", err
		}
		dir = child
	}
	return dir, name, nil
}

// sync makes entries durable through the same handle used to create them.
func (d *archiveDirectory) sync() error {
	if runtime.GOOS == "windows" {
		return nil
	}
	f, err := d.root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(syncFile(f), f.Close())
}

// archiveWriter reserves a destination and copies the parts of a partition
// as they are captured. Its manifest is published only after the final batch.
type archiveWriter struct {
	dir       *Dir
	target    *archiveDirectory
	manifest  partitionArchive
	created   map[string]os.FileInfo
	published bool
}

func (s *Store) beginPartitionArchive(dest string, id int64) (*archiveWriter, error) {
	parent, name, err := s.archiveParent(dest)
	if err != nil {
		return nil, err
	}
	if err := parent.verify(); err != nil {
		parent.close()
		return nil, err
	}
	if err := parent.root.Mkdir(name, 0o700); err != nil {
		defer parent.close()
		if errors.Is(err, os.ErrExist) {
			if entry, statErr := parent.root.Lstat(name); statErr == nil && (os.SameFile(entry, parent.data) || entry.Mode()&os.ModeSymlink != 0) {
				return nil, fmt.Errorf("%w: invalid archive destination %s", ErrInvalidArchive, dest)
			}
			return nil, fmt.Errorf("%w: %s", ErrArchiveExists, dest)
		}
		return nil, err
	}
	target, err := parent.openChild(name)
	if err != nil {
		// No identity was established for the entry that Mkdir created.
		parent.close()
		return nil, err
	}
	return &archiveWriter{
		dir: s.dir, target: target, created: make(map[string]os.FileInfo),
		manifest: partitionArchive{
			Version: 1, ID: id, Width: s.opts.Partition, SortKey: s.opts.SortKey,
			TimeFields: s.archiveTimeFields(), PartDictionary: s.opts.Part.Dictionary,
		},
	}, nil
}

// close discards an incomplete export. An interruption after publication
// but before the caller drops the partition leaves both copies.
func (w *archiveWriter) close() {
	if w.published || w.target.verify() != nil {
		w.target.close()
		return
	}
	for name, created := range w.created {
		if entry, err := w.target.root.Lstat(name); err == nil && os.SameFile(created, entry) {
			_ = w.target.root.Remove(name)
		}
	}
	parent, name := w.target.parent, w.target.name
	// On Windows the directory handle must close before removal.
	w.target.root.Close()
	defer parent.close()
	if entry, err := parent.root.Lstat(name); err == nil && os.SameFile(entry, w.target.info) {
		_ = parent.root.Remove(name)
	}
}

// create records ownership of a new archive file for cancellation cleanup.
func (w *archiveWriter) create(name string) (*os.File, error) {
	f, err := w.target.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	w.created[name] = info
	return f, nil
}

// add requires a pinned snapshot containing the handles. New writes can
// continue alongside the copy until the caller captures the final batch.
func (w *archiveWriter) add(ctx context.Context, handles []*Handle) error {
	if err := w.target.verify(); err != nil {
		return err
	}
	for _, h := range handles {
		w.dir.mu.Lock()
		ref := w.dir.live[h]
		w.dir.mu.Unlock()
		if ref == nil {
			return ErrUnknownPart
		}
		f, err := w.create(archivePartName(len(w.manifest.Parts)))
		if err != nil {
			return err
		}
		info, err := copyArchivePart(ctx, f, bytes.NewReader(ref.data))
		if err == nil {
			err = syncFile(f)
		}
		if err = errors.Join(err, f.Close()); err != nil {
			return err
		}
		w.manifest.Parts = append(w.manifest.Parts, info)
	}
	return ctx.Err()
}

func (w *archiveWriter) publish(ctx context.Context) error {
	if err := w.target.verify(); err != nil {
		return err
	}
	tmp := archiveManifest + tmpSuffix
	f, err := w.create(tmp)
	if err != nil {
		return err
	}
	if err = json.NewEncoder(f).Encode(&w.manifest); err == nil {
		err = checkManifestSize(f)
	}
	if err == nil {
		err = syncFile(f)
	}
	if err = errors.Join(err, f.Close(), ctx.Err()); err != nil {
		return err
	}
	if err := w.target.verify(); err != nil {
		return err
	}
	for name, created := range w.created {
		entry, err := w.target.root.Lstat(name)
		if err != nil || !os.SameFile(entry, created) {
			return fmt.Errorf("%w: %s changed before publication", ErrInvalidArchive, name)
		}
	}
	if _, err := w.target.root.Lstat(archiveManifest); err == nil {
		return fmt.Errorf("%w: %s", ErrArchiveExists, archiveManifest)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// The archive directory belongs to this writer. Expose the complete,
	// synced manifest in one rename and preserve it if directory sync fails.
	if err := w.target.root.Rename(tmp, archiveManifest); err != nil {
		return err
	}
	w.published = true
	if err := errors.Join(w.target.sync(), w.target.parent.sync()); err != nil {
		return err
	}
	return w.target.verify()
}

type archiveReader struct {
	ctx context.Context
	r   io.Reader
}

func (r archiveReader) Read(buf []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(buf)
}

func copyArchivePart(ctx context.Context, dst io.Writer, src io.Reader) (archivePart, error) {
	digest := sha256.New()
	n, err := io.Copy(io.MultiWriter(dst, digest), archiveReader{ctx, src})
	return archivePart{Bytes: n, SHA256: hex.EncodeToString(digest.Sum(nil))}, err
}

// openArchiveFile only accepts regular files beneath the archive root. Check
// before opening to avoid blocking on a FIFO and again on the opened file.
func openArchiveFile(root *os.Root, name string) (*os.File, error) {
	info, err := root.Lstat(name)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidArchive, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: %s is not a regular file", ErrInvalidArchive, name)
	}
	f, err := root.Open(name)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidArchive, err)
	}
	actual, err := f.Stat()
	if err != nil || !os.SameFile(info, actual) {
		f.Close()
		return nil, fmt.Errorf("%w: %s changed while opening", ErrInvalidArchive, name)
	}
	return f, nil
}

func (s *Store) readPartitionArchive(ctx context.Context, src string) (int64, []*PartFile, error) {
	parent, name, err := s.archiveParent(src)
	if err != nil {
		return 0, nil, err
	}
	target, err := parent.openChild(name)
	if err != nil {
		parent.close()
		return 0, nil, err
	}
	defer target.close()
	if err := target.verify(); err != nil {
		return 0, nil, err
	}
	root := target.root
	f, err := openArchiveFile(root, archiveManifest)
	if err != nil {
		return 0, nil, err
	}
	defer f.Close()
	if err := checkManifestSize(f); err != nil {
		return 0, nil, err
	}
	var m partitionArchive
	decoder := json.NewDecoder(archiveReader{ctx, io.LimitReader(f, maxArchiveManifest+1)})
	if err := decoder.Decode(&m); err != nil {
		if ctx.Err() != nil {
			return 0, nil, ctx.Err()
		}
		return 0, nil, fmt.Errorf("%w: %s: %w", ErrInvalidArchive, archiveManifest, err)
	}
	var extra json.RawMessage
	if decoder.Decode(&extra) != io.EOF || m.Version != 1 || len(m.Parts) == 0 || m.Width <= 0 {
		return 0, nil, fmt.Errorf("%w: invalid %s", ErrInvalidArchive, archiveManifest)
	}
	if m.Width != s.opts.Partition || !slices.Equal(m.SortKey, s.opts.SortKey) ||
		!slices.Equal(m.TimeFields, s.archiveTimeFields()) || m.PartDictionary != s.opts.Part.Dictionary {
		return 0, nil, ErrIncompatibleArchive
	}
	// A retry of a successful import should report the conflict even when
	// the index no longer has room for a second copy of the archive.
	if s.hasPartition(m.ID) {
		return 0, nil, ErrPartitionExists
	}
	var files []*PartFile
	for i, info := range m.Parts {
		pf, err := s.stageArchivePart(ctx, root, archivePartName(i), info, m.ID)
		if err != nil {
			for _, staged := range files {
				staged.Abort()
			}
			return 0, nil, err
		}
		files = append(files, pf)
	}
	return m.ID, files, nil
}

func checkManifestSize(f *os.File) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() > maxArchiveManifest {
		return fmt.Errorf("%w: %s exceeds %d bytes", ErrInvalidArchive, archiveManifest, maxArchiveManifest)
	}
	return nil
}

func (s *Store) stageArchivePart(ctx context.Context, root *os.Root, name string, info archivePart, id int64) (*PartFile, error) {
	f, err := openArchiveFile(root, name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Bytes <= 0 || info.Bytes != stat.Size() || len(info.SHA256) != 2*sha256.Size {
		return nil, fmt.Errorf("%w: size or checksum of %s", ErrInvalidArchive, name)
	}
	pf, err := s.dir.Create()
	if err != nil {
		return nil, err
	}
	pf.Meta.Partition = id
	got, err := copyArchivePart(ctx, pf, io.LimitReader(f, info.Bytes))
	if err != nil || got != info {
		pf.Abort()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%w: checksum of %s", ErrInvalidArchive, name)
	}
	// Validate the private copy. The operator can modify the archive as
	// soon as it has been read, without changing what is committed.
	h, _, err := s.dir.open(filepath.Base(pf.tmp), pf.Meta)
	if err != nil {
		pf.Abort()
		return nil, fmt.Errorf("%w: %s: %w", ErrInvalidArchive, name, err)
	}
	valid := h.Ordered() && slices.Equal(h.SortKey(), s.opts.SortKey) && h.Rows > 0 && h.MinTime <= h.MaxTime &&
		partitionID(h.MinTime, s.opts.Partition) == id && partitionID(h.MaxTime, s.opts.Partition) == id
	h.release()
	if !valid {
		pf.Abort()
		return nil, fmt.Errorf("%w: time bounds or ordering of %s", ErrInvalidArchive, name)
	}
	return pf, nil
}
