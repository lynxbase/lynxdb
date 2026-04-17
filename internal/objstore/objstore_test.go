package objstore

import (
	"context"
	"errors"
	"testing"
)

func TestMemStore_PutGet(t *testing.T) {
	store := NewMemStore()
	ctx := context.Background()

	data := []byte("hello world")
	if err := store.Put(ctx, "test/key1", data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := store.Get(ctx, "test/key1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "hello world" {
		t.Errorf("got %q, want %q", got, "hello world")
	}
}

func TestMemStore_GetRange(t *testing.T) {
	store := NewMemStore()
	ctx := context.Background()

	data := []byte("0123456789ABCDEF")
	store.Put(ctx, "range/key", data)

	got, err := store.GetRange(ctx, "range/key", 4, 6)
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	if string(got) != "456789" {
		t.Errorf("got %q, want %q", got, "456789")
	}
}

func TestMemStore_GetRange_Clamp(t *testing.T) {
	store := NewMemStore()
	ctx := context.Background()

	data := []byte("short")
	store.Put(ctx, "key", data)

	got, err := store.GetRange(ctx, "key", 3, 100)
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	if string(got) != "rt" {
		t.Errorf("got %q, want %q", got, "rt")
	}
}

func TestMemStore_Delete(t *testing.T) {
	store := NewMemStore()
	ctx := context.Background()

	store.Put(ctx, "del/key", []byte("data"))
	store.Delete(ctx, "del/key")

	exists, _ := store.Exists(ctx, "del/key")
	if exists {
		t.Error("key still exists after delete")
	}
}

func TestMemStore_List(t *testing.T) {
	store := NewMemStore()
	ctx := context.Background()

	store.Put(ctx, "seg/001.lsg", []byte("a"))
	store.Put(ctx, "seg/002.lsg", []byte("b"))
	store.Put(ctx, "other/003.lsg", []byte("c"))

	keys, err := store.List(ctx, "seg/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("got %d keys, want 2", len(keys))
	}
	if keys[0] != "seg/001.lsg" || keys[1] != "seg/002.lsg" {
		t.Errorf("keys: %v", keys)
	}
}

func TestMemStore_Exists(t *testing.T) {
	store := NewMemStore()
	ctx := context.Background()

	exists, _ := store.Exists(ctx, "nope")
	if exists {
		t.Error("should not exist")
	}

	store.Put(ctx, "nope", []byte("x"))
	exists, _ = store.Exists(ctx, "nope")
	if !exists {
		t.Error("should exist after put")
	}
}

func TestMemStore_GetNotFound(t *testing.T) {
	store := NewMemStore()
	ctx := context.Background()

	_, err := store.Get(ctx, "missing")
	if err == nil {
		t.Error("expected error for missing key")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if !IsNotFound(err) {
		t.Fatal("IsNotFound should report true")
	}
}

func TestMemStore_IsolatesCopies(t *testing.T) {
	store := NewMemStore()
	ctx := context.Background()

	data := []byte("original")
	store.Put(ctx, "key", data)

	// Modify the original slice.
	data[0] = 'X'

	got, _ := store.Get(ctx, "key")
	if string(got) != "original" {
		t.Errorf("stored data was modified: %q", got)
	}

	// Modify the returned slice.
	got[0] = 'Y'
	got2, _ := store.Get(ctx, "key")
	if string(got2) != "original" {
		t.Errorf("returned data was modified: %q", got2)
	}
}

func TestWriterTo(t *testing.T) {
	store := NewMemStore()
	ctx := context.Background()

	w := NewWriterTo(store, "writer/key")
	w.Write([]byte("hello "))
	w.Write([]byte("world"))
	if err := w.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, _ := store.Get(ctx, "writer/key")
	if string(got) != "hello world" {
		t.Errorf("got %q", got)
	}
}
