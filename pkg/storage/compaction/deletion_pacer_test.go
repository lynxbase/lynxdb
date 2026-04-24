package compaction

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDeletionPacer_EnqueueAndDrain(t *testing.T) {
	dir := t.TempDir()

	var paths []string
	for i := 0; i < 5; i++ {
		p := filepath.Join(dir, "seg_"+string(rune('a'+i))+".lsg")
		if err := os.WriteFile(p, make([]byte, 1024), 0o644); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, p)
	}

	pacer := NewDeletionPacer(0) // default rate

	for _, p := range paths {
		pacer.Enqueue(p, 1024)
	}

	if pacer.Pending() != 5 {
		t.Fatalf("expected 5 pending, got %d", pacer.Pending())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		pacer.DrainLoop(ctx)
		close(done)
	}()

	<-done

	// All files should be deleted.
	for _, p := range paths {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("file %s still exists after drain", filepath.Base(p))
		}
	}

	if pacer.Pending() != 0 {
		t.Errorf("expected 0 pending after drain, got %d", pacer.Pending())
	}
}

func TestDeletionPacer_ShutdownFlush(t *testing.T) {
	dir := t.TempDir()

	p := filepath.Join(dir, "test.lsg")
	if err := os.WriteFile(p, make([]byte, 512), 0o644); err != nil {
		t.Fatal(err)
	}

	// Use very low rate so the tick wouldn't normally delete in time.
	pacer := NewDeletionPacer(1) // 1 byte/sec

	pacer.Enqueue(p, 512)

	// Cancel immediately — should flush remaining on shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	pacer.DrainLoop(ctx)

	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Error("file should have been deleted on shutdown flush")
	}
}
