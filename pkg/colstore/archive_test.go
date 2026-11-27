package colstore

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"
)

func TestPartitionArchiveValidation(t *testing.T) {
	opts := StoreOptions{TimeFields: []string{"when"}, Partition: time.Hour, SortKey: []string{"name"}, MergeFanIn: -1, FlushInterval: -1}
	open := func(opts StoreOptions) *Store {
		t.Helper()
		s, err := OpenStore(t.TempDir(), opts)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Close() })
		return s
	}
	source, dest := open(opts), open(opts)
	for _, doc := range []string{`{"name":"first"}`, `{"name":"second"}`} {
		if _, err := source.AppendAt([]byte(doc), -17); err != nil {
			t.Fatal(err)
		}
		if err := source.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	snap := source.Table().Snapshot()
	defer snap.Release()
	parts := snap.Parts()
	if len(parts) != 2 || parts[0].Partition != -1 {
		t.Fatalf("got %d parts", len(parts))
	}
	archive := filepath.Join(t.TempDir(), "archive")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	start := func() *archiveWriter {
		t.Helper()
		w, err := source.beginPartitionArchive(archive, -1)
		if err != nil {
			t.Fatal(err)
		}
		return w
	}
	w := start()
	if err := w.add(t.Context(), parts[:1]); err != nil {
		t.Fatal(err)
	}
	if _, _, err := dest.readPartitionArchive(t.Context(), archive); !errors.Is(err, ErrInvalidArchive) {
		t.Fatalf("attach before publication: %v", err)
	}
	if err := w.add(ctx, parts[1:]); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled detach: %v", err)
	}
	w.close()
	if _, err := os.Stat(archive); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("incomplete archive remains: %v", err)
	}
	w = start()
	defer w.close()
	for _, h := range parts {
		if err := w.add(t.Context(), []*Handle{h}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := source.beginPartitionArchive(archive, -1); !errors.Is(err, ErrArchiveExists) {
		t.Fatalf("overwrite: %v", err)
	}
	settings := opts
	settings.TimeFields = nil
	if _, _, err := open(settings).readPartitionArchive(t.Context(), archive); !errors.Is(err, ErrIncompatibleArchive) {
		t.Fatalf("different settings: %v", err)
	}
	manifest := filepath.Join(archive, archiveManifest)
	original, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var m partitionArchive
	if err := json.Unmarshal(original, &m); err != nil {
		t.Fatal(err)
	}
	bad := func() {
		t.Helper()
		if _, _, err := dest.readPartitionArchive(t.Context(), archive); !errors.Is(err, ErrInvalidArchive) {
			t.Fatalf("invalid archive: %v", err)
		}
		if tmp, err := filepath.Glob(filepath.Join(dest.dir.path, "*"+tmpSuffix)); err != nil || len(tmp) != 0 {
			t.Fatalf("staged files: %v, %v", tmp, err)
		}
	}
	changeManifest := func(m partitionArchive) {
		t.Helper()
		data, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(manifest, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	m.ID++
	changeManifest(m)
	bad()
	m.ID--
	m.Version++
	changeManifest(m)
	bad()
	m.Version--
	changeManifest(m)
	file := filepath.Join(archive, archivePartName(1))
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	damaged := slices.Clone(data)
	damaged[len(data)/2] ^= 1
	if err := os.WriteFile(file, damaged, 0o600); err != nil {
		t.Fatal(err)
	}
	bad()
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	bad()
	if err := os.WriteFile(file, data, 0o600); err != nil {
		t.Fatal(err)
	}
	id, staged, err := dest.readPartitionArchive(t.Context(), archive)
	if err != nil {
		t.Fatal(err)
	}
	got, err := dest.dir.Commit(staged, nil)
	if err != nil || id != -1 || len(got) != 2 || got[0].Run == got[1].Run {
		t.Fatalf("attach id %d, parts %v, err %v", id, got, err)
	}
	for _, h := range got {
		if h.Rows != 1 || h.MinTime != -17 || h.Partition != -1 {
			t.Fatalf("unexpected attached part: %v", h)
		}
	}
}

func TestPartitionArchivePaths(t *testing.T) {
	dir := t.TempDir()
	index := filepath.Join(dir, "index")
	store, err := OpenStore(index, StoreOptions{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, path := range []string{"relative", dir, index, filepath.Join(dir, "sibling"), filepath.Join(index, "new")} {
		if _, err := store.beginPartitionArchive(path, 0); !errors.Is(err, ErrInvalidArchive) {
			t.Errorf("%s: %v", path, err)
		}
	}
	link := filepath.Join(t.TempDir(), "linked")
	if err := os.Symlink(index, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	for _, path := range []string{link, filepath.Join(link, "new")} {
		if _, err := store.beginPartitionArchive(path, 0); !errors.Is(err, ErrInvalidArchive) {
			t.Errorf("%s: %v", path, err)
		}
	}
	root, err := os.OpenRoot(filepath.Dir(link))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if _, err := openArchiveFile(root, filepath.Base(link)); !errors.Is(err, ErrInvalidArchive) {
		t.Fatalf("archive member symlink: %v", err)
	}
}

func TestArchiveWriterKeepsExistingEntries(t *testing.T) {
	s, err := OpenStore(t.TempDir(), StoreOptions{Partition: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	target := filepath.Join(t.TempDir(), "archive")
	w, err := s.beginPartitionArchive(target, 0)
	if err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(target, archiveManifest)
	const original = "reserved"
	if err := os.WriteFile(manifest, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := w.publish(t.Context()); !errors.Is(err, ErrArchiveExists) {
		t.Fatalf("publishing over existing manifest: %v", err)
	}
	w.close()
	if b, err := os.ReadFile(manifest); err != nil || string(b) != original {
		t.Fatalf("existing contents after cleanup: %q, %v", b, err)
	}
}

func TestArchiveDirectoryMovedDuringExport(t *testing.T) {
	s, err := OpenStore(t.TempDir(), StoreOptions{Partition: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	temp := t.TempDir()
	parent, next := filepath.Join(temp, "out"), filepath.Join(temp, "moved")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(parent, "archive")
	w, err := s.beginPartitionArchive(target, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(parent, next); err != nil {
		w.close()
		t.Skipf("cannot rename an open directory: %v", err)
	}
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := w.publish(t.Context()); !errors.Is(err, ErrInvalidArchive) {
		t.Fatalf("publication after directory move: %v", err)
	}
	w.close()
	for _, p := range []string{target, filepath.Join(next, "archive")} {
		entries, err := os.ReadDir(p)
		if err != nil || len(entries) != 0 {
			t.Fatalf("%s after cleanup: %v, %v", p, entries, err)
		}
	}
}

func TestArchiveThroughParentAlias(t *testing.T) {
	s, err := OpenStore(t.TempDir(), StoreOptions{Partition: time.Hour, FlushInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	parent := t.TempDir()
	alias := filepath.Join(t.TempDir(), "out")
	if err := os.Symlink(parent, alias); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	when := time.Unix(3600, 0)
	if _, err := s.AppendAt([]byte(`{"msg":"first"}`), when.UnixNano()); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(alias, "archive")
	detached, err := s.DetachPartition(t.Context(), when, archive)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(parent, "archive", archiveManifest)); err != nil {
		t.Fatal(err)
	}
	attached, err := s.AttachPartition(t.Context(), archive)
	if err != nil || attached != detached || attached.Events != 1 {
		t.Fatalf("detach %+v, attach %+v, error %v", detached, attached, err)
	}
}

func TestArchivePublicationOnDirectorySyncFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("directory sync is unavailable")
	}
	archive := filepath.Join(t.TempDir(), "export")
	manifest := filepath.Join(archive, archiveManifest)
	failure := errors.New("directory sync failed")
	replaceSyncFile(t, func(f *os.File) error {
		if filepath.Base(f.Name()) == archiveManifest+tmpSuffix {
			if _, err := os.Stat(manifest); !errors.Is(err, os.ErrNotExist) {
				return errors.New("archive manifest visible before sync")
			}
		}
		dest, _ := os.Stat(archive)
		info, err := f.Stat()
		if err == nil && os.SameFile(info, dest) {
			return failure
		}
		return f.Sync()
	})
	opts := StoreOptions{Partition: time.Hour, MergeFanIn: -1, FlushInterval: -1}
	s, err := OpenStore(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	start := time.Unix(3600, 0)
	if _, err := s.AppendAt([]byte(`{"n":42}`), start.UnixNano()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DetachPartition(t.Context(), start, archive); !errors.Is(err, failure) {
		t.Fatalf("detach after directory sync failure: %v", err)
	}
	if rows, sum := rowsAndSumOf(t, s.Table()); rows != 1 || sum != 42 {
		t.Fatalf("live source: %d rows, sum %d", rows, sum)
	}
	dest, err := OpenStore(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer dest.Close()
	if got, err := dest.AttachPartition(t.Context(), archive); err != nil || got.Events != 1 {
		t.Fatalf("attach published archive: %+v, %v", got, err)
	}
}
