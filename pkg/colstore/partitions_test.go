package colstore

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPartitionChangesKeepReadersAndBufferedAppends(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	unpause := func() { unblock.Do(func() { close(release) }) }
	replaceSyncFile(t, func(f *os.File) error {
		if filepath.Base(f.Name()) == archiveManifest+tmpSuffix {
			close(entered)
			<-release
		}
		return nil
	})
	opts := StoreOptions{Partition: time.Hour, TimeField: "ts", MergeFanIn: -1, FlushInterval: -1}
	path := t.TempDir()
	s, err := OpenStore(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer unpause()
	ctx := t.Context()
	start := time.Unix(firstTimedHour*3600, 0).UTC()
	if _, err := s.Append(timedDocs(0, 20, 2)); err != nil {
		t.Fatal(err)
	}
	list, err := s.ListPartitions(ctx)
	if err != nil || len(list) != 2 || list[0].Start != start || list[0].End != list[1].Start || list[0].Events != 10 || list[0].Bytes <= 0 || list[0].Parts == 0 {
		t.Fatalf("partitions after buffered append = %+v, %v", list, err)
	}
	snap := s.Table().Snapshot()
	defer snap.Release()
	var pinned []string
	for _, h := range snap.Parts() {
		if h.Partition == firstTimedHour {
			pinned = append(pinned, s.dir.live[h].name)
		}
	}
	dest := filepath.Join(t.TempDir(), "archive")
	result := make(chan error, 1)
	go func() {
		st, err := s.DetachPartition(ctx, start, dest)
		if err == nil && st != list[0] {
			t.Errorf("detach returned %+v, want %+v", st, list[0])
		}
		result <- err
	}()
	select {
	case <-entered:
	case err := <-result:
		t.Fatalf("detach failed before writing archive: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("detach did not start writing")
	}
	// Read through the normal snapshot entry point while the archive waits
	// for durability. An append crossing this boundary starts a new span.
	if rows, _ := rowsAndSumOf(t, s.Table()); rows != 20 {
		t.Fatalf("query during detach read %d events", rows)
	}
	appended := make(chan error, 1)
	go func() {
		_, err := s.Append(timedDocs(100, 1, 1))
		appended <- err
	}()
	unpause()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if err := <-appended; err != nil {
		t.Fatal(err)
	}
	for _, name := range pinned {
		if _, err := os.Stat(filepath.Join(path, name)); err != nil {
			t.Fatal(err)
		}
	}
	if rows, sum, _ := sumN(t, snap, ScanOptions{}); rows != 20 || sum != 190 {
		t.Fatalf("reader after detach: %d events, sum %d", rows, sum)
	}
	snap.Release()
	for _, name := range pinned {
		if _, err := os.Stat(filepath.Join(path, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("released part %s: %v", name, err)
		}
	}
	if _, err := s.AttachPartition(ctx, dest); !errors.Is(err, ErrPartitionExists) {
		t.Fatalf("attach over buffered late append: %v", err)
	}
	if st, err := s.DropPartition(ctx, start); err != nil || st.Events != 1 {
		t.Fatalf("drop late append = %+v, %v", st, err)
	}
	if _, err := s.DropPartition(ctx, start); !errors.Is(err, ErrPartitionNotFound) {
		t.Fatalf("repeat drop = %v", err)
	}
	before := s.Table().Snapshot()
	defer before.Release()
	if st, err := s.AttachPartition(ctx, dest); err != nil || st != list[0] {
		t.Fatalf("attach = %+v, %v", st, err)
	}
	if rows, _, _ := sumN(t, before, ScanOptions{}); rows != 10 {
		t.Fatalf("reader acquired before attach saw %d rows", rows)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenStore(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if rows, sum := rowsAndSumOf(t, s.Table()); rows != 20 || sum != 190 {
		t.Fatalf("reopened index: %d events, sum %d", rows, sum)
	}
	if n, err := s.DropBefore(start.Add(time.Hour).UnixNano()); err != nil || n != 10 {
		t.Fatalf("retention of attached span: %d events, %v", n, err)
	}
}

func TestPartitionBoundsRoundTripAtTimeLimits(t *testing.T) {
	opts := StoreOptions{Partition: 7 * time.Hour, MergeFanIn: -1, FlushInterval: -1}
	s, err := OpenStore(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, ns := range []int64{math.MinInt64, -1, math.MaxInt64} {
		if _, err := s.AppendAt([]byte(`{"n":1}`), ns); err != nil {
			t.Fatal(err)
		}
	}
	list, err := s.ListPartitions(t.Context())
	if err != nil || len(list) != 3 {
		t.Fatalf("partitions = %+v, %v", list, err)
	}
	for i, ns := range []int64{math.MinInt64, -1, math.MaxInt64} {
		ts := time.Unix(0, ns)
		st := list[i]
		if ts.Before(st.Start) || !ts.Before(st.End) || st.End.Sub(st.Start) != opts.Partition {
			t.Fatalf("time %s outside span %+v", ts, st)
		}
		if _, err := s.DropPartition(t.Context(), st.Start.Add(time.Nanosecond)); !errors.Is(err, ErrInvalidPartition) {
			t.Fatalf("drop unaligned start: %v", err)
		}
		if dropped, err := s.DropPartition(t.Context(), st.Start); err != nil || st != dropped {
			t.Fatalf("drop of %+v = %+v, %v", st, dropped, err)
		}
	}
}

func TestDropPartitionCanCancelWhileMergeFinishes(t *testing.T) {
	held, release := make(chan struct{}), make(chan struct{})
	var once, unblock sync.Once
	unpause := func() { unblock.Do(func() { close(release) }) }
	beforeMergeCommit = func(PartMeta) {
		once.Do(func() { close(held) })
		<-release
	}
	defer func() { beforeMergeCommit = nil }()
	start := time.Now().UTC().Truncate(time.Hour)
	s, err := OpenStore(t.TempDir(), StoreOptions{Partition: time.Hour, MergeFanIn: 2, FlushInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer unpause()
	for range 2 {
		if _, err := s.AppendAt([]byte("{\"n\":1}\n{\"n\":2}"), start.UnixNano()); err != nil {
			t.Fatal(err)
		}
		if err := s.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("merge did not reach commit")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if _, err := s.DropPartition(ctx, start); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drop during merge = %v", err)
	}
	if rows, _ := rowsAndSumOf(t, s.Table()); rows != 4 {
		t.Fatalf("cancelled drop left %d events", rows)
	}
	unpause()
	if dropped, err := s.DropPartition(t.Context(), start); err != nil || dropped.Events != 4 {
		t.Fatalf("drop across merge = %+v, %v", dropped, err)
	}
}

func TestPartitionFailureAfterArchivePublication(t *testing.T) {
	path := t.TempDir()
	var failNext atomic.Bool
	writeErr := errors.New("manifest sync failed")
	replaceSyncFile(t, func(f *os.File) error {
		if f.Name() == filepath.Join(path, manifestName) && failNext.Swap(false) {
			return writeErr
		}
		return nil
	})
	opts := StoreOptions{Partition: time.Hour, MergeFanIn: -1, FlushInterval: -1}
	s, err := OpenStore(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	start := time.Unix(3600, 0).UTC()
	if _, err := s.AppendAt([]byte(`{"n":42}`), start.UnixNano()); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "archive")
	failNext.Store(true)
	if _, err := s.DetachPartition(t.Context(), start, dest); !errors.Is(err, writeErr) {
		t.Fatalf("failed detach returned %v", err)
	}
	if rows, sum := rowsAndSumOf(t, s.Table()); rows != 1 || sum != 42 {
		t.Fatalf("source after failed detach has %d rows, sum %d", rows, sum)
	}
	if _, err := s.DropPartition(t.Context(), start); err != nil {
		t.Fatal(err)
	}
	failNext.Store(true)
	if _, err := s.AttachPartition(t.Context(), dest); !errors.Is(err, writeErr) {
		t.Fatalf("failed attach returned %v", err)
	}
	if rows, _ := rowsAndSumOf(t, s.Table()); rows != 0 {
		t.Fatalf("failed attach published %d rows", rows)
	}
	if _, err := s.AttachPartition(t.Context(), dest); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenStore(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if rows, sum := rowsAndSumOf(t, s.Table()); rows != 1 || sum != 42 {
		t.Fatalf("source after retry and reopen has %d rows, sum %d", rows, sum)
	}
}
