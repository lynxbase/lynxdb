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
	"slices"
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

func (s *Store) archivePath(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("%w: supply an absolute path on the server", ErrInvalidArchive)
	}
	path = filepath.Clean(path)
	parent, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("%w: archive parent: %w", ErrInvalidArchive, err)
		}
		return "", err
	}
	path = filepath.Join(parent, filepath.Base(path))
	entry, err := os.Lstat(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if entry != nil && entry.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("%w: %s is a symbolic link", ErrInvalidArchive, path)
	}
	protected := s.opts.DataDir
	if protected == "" {
		protected = s.dir.path
	}
	data, err := os.Stat(protected)
	if err != nil {
		return "", err
	}
	// SameFile also recognizes the data directory under another spelling
	// on a case-insensitive filesystem.
	for dir := path; ; {
		if os.SameFile(entry, data) {
			return "", fmt.Errorf("%w: choose a directory outside %s", ErrInvalidArchive, protected)
		}
		if dir == filepath.Dir(dir) {
			return path, nil
		}
		dir = filepath.Dir(dir)
		entry, err = os.Stat(dir)
		if err != nil {
			return "", err
		}
	}
}

// archiveWriter reserves a destination and copies the parts of a partition
// as they are captured. Its manifest is published only after the final batch.
type archiveWriter struct {
	dir       *Dir
	root      *os.Root
	manifest  partitionArchive
	written   int
	published bool
}

func (s *Store) beginPartitionArchive(dest string, id int64) (*archiveWriter, error) {
	dest, err := s.archivePath(dest)
	if err != nil {
		return nil, err
	}
	if err := os.Mkdir(dest, 0o700); err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("%w: %s", ErrArchiveExists, dest)
		}
		return nil, err
	}
	root, err := os.OpenRoot(dest)
	if err != nil {
		os.Remove(dest)
		return nil, err
	}
	return &archiveWriter{
		dir: s.dir, root: root,
		manifest: partitionArchive{
			Version: 1, ID: id, Width: s.opts.Partition, SortKey: s.opts.SortKey,
			TimeFields: s.archiveTimeFields(), PartDictionary: s.opts.Part.Dictionary,
		},
	}, nil
}

// close discards an incomplete export. An interruption after publication
// but before the caller drops the partition leaves both copies.
func (w *archiveWriter) close() {
	if w.published {
		w.root.Close()
		return
	}
	_ = w.root.Remove(archiveManifest + tmpSuffix)
	for i := range w.written {
		_ = w.root.Remove(archivePartName(i))
	}
	dest := w.root.Name()
	w.root.Close()
	os.Remove(dest)
}

// add requires a pinned snapshot containing the handles. New writes can
// continue alongside the copy until the caller captures the final batch.
func (w *archiveWriter) add(ctx context.Context, handles []*Handle) error {
	for _, h := range handles {
		w.dir.mu.Lock()
		ref := w.dir.live[h]
		w.dir.mu.Unlock()
		if ref == nil {
			return ErrUnknownPart
		}
		f, err := w.root.OpenFile(archivePartName(w.written), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return err
		}
		w.written++
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
	tmp := archiveManifest + tmpSuffix
	f, err := w.root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
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
	if err := w.root.Rename(tmp, archiveManifest); err != nil {
		return err
	}
	w.published = true
	dest := w.root.Name()
	return errors.Join(syncDir(dest), syncDir(filepath.Dir(dest)))
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
	src, err := s.archivePath(src)
	if err != nil {
		return 0, nil, err
	}
	root, err := os.OpenRoot(src)
	if err != nil {
		return 0, nil, fmt.Errorf("%w: %w", ErrInvalidArchive, err)
	}
	defer root.Close()
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
