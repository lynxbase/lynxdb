package savedqueries

import (
	"errors"
	"os"
	"strings"
	"testing"
)

func TestStore_CreateAndGet(t *testing.T) {
	s := OpenInMemory()
	input := &SavedQueryInput{Name: "test query", Q: "FROM main | search error"}
	sq := input.ToSavedQuery()
	if err := s.Create(sq); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(sq.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "test query" {
		t.Fatalf("name: %q", got.Name)
	}
	if got.Q != "FROM main | search error" {
		t.Fatalf("q: %q", got.Q)
	}
	if !strings.HasPrefix(got.ID, "sq_") {
		t.Fatalf("id: %q", got.ID)
	}
}

func TestStore_CreateDuplicateName(t *testing.T) {
	s := OpenInMemory()
	sq1 := (&SavedQueryInput{Name: "dup", Q: "q1"}).ToSavedQuery()
	sq2 := (&SavedQueryInput{Name: "dup", Q: "q2"}).ToSavedQuery()
	s.Create(sq1)
	if err := s.Create(sq2); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}
}

func TestStore_List(t *testing.T) {
	s := OpenInMemory()
	for _, name := range []string{"charlie", "alpha", "bravo"} {
		sq := (&SavedQueryInput{Name: name, Q: "q"}).ToSavedQuery()
		s.Create(sq)
	}
	list := s.List()
	if len(list) != 3 {
		t.Fatalf("len: %d", len(list))
	}
	if list[0].Name != "alpha" || list[1].Name != "bravo" || list[2].Name != "charlie" {
		t.Fatalf("order: %v %v %v", list[0].Name, list[1].Name, list[2].Name)
	}
}

func TestStore_ListEmpty(t *testing.T) {
	s := OpenInMemory()
	list := s.List()
	if list == nil {
		t.Fatal("expected empty slice, not nil")
	}
	if len(list) != 0 {
		t.Fatalf("len: %d", len(list))
	}
}

func TestStore_Update(t *testing.T) {
	s := OpenInMemory()
	sq := (&SavedQueryInput{Name: "orig", Q: "q1"}).ToSavedQuery()
	s.Create(sq)
	sq.Q = "q2"
	if err := s.Update(sq); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(sq.ID)
	if got.Q != "q2" {
		t.Fatalf("q: %q", got.Q)
	}
}

func TestStore_UpdateNotFound(t *testing.T) {
	s := OpenInMemory()
	sq := &SavedQuery{ID: "sq_nonexistent", Name: "x", Q: "q"}
	if err := s.Update(sq); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestStore_Delete(t *testing.T) {
	s := OpenInMemory()
	sq := (&SavedQueryInput{Name: "del", Q: "q"}).ToSavedQuery()
	s.Create(sq)
	if err := s.Delete(sq.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(sq.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("expected not found after delete")
	}
}

func TestStore_DeleteNotFound(t *testing.T) {
	s := OpenInMemory()
	if err := s.Delete("sq_nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestStore_Persistence(t *testing.T) {
	dir := t.TempDir()
	s1, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	sq := (&SavedQueryInput{Name: "persist", Q: "FROM main"}).ToSavedQuery()
	s1.Create(sq)

	s2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s2.Get(sq.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "persist" {
		t.Fatalf("name: %q", got.Name)
	}
}

func TestStore_InMemoryNoPersist(t *testing.T) {
	dir := t.TempDir()
	s := OpenInMemory()
	sq := (&SavedQueryInput{Name: "mem", Q: "q"}).ToSavedQuery()
	s.Create(sq)
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatalf("expected no files, got %d", len(entries))
	}
}

func TestValidate_EmptyName(t *testing.T) {
	input := &SavedQueryInput{Q: "q"}
	if err := input.Validate(); !errors.Is(err, ErrNameEmpty) {
		t.Fatalf("expected ErrNameEmpty, got %v", err)
	}
}

func TestValidate_EmptyQuery(t *testing.T) {
	input := &SavedQueryInput{Name: "n"}
	if err := input.Validate(); !errors.Is(err, ErrQueryEmpty) {
		t.Fatalf("expected ErrQueryEmpty, got %v", err)
	}
}

func TestValidate_QueryAlias(t *testing.T) {
	input := &SavedQueryInput{Name: "n", Query: "FROM main"}
	if err := input.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestGenerateID(t *testing.T) {
	id := generateID()
	if !strings.HasPrefix(id, "sq_") {
		t.Fatalf("id: %q", id)
	}
	if len(id) != 19 { // "sq_" + 16 hex chars
		t.Fatalf("id length: %d", len(id))
	}
}
