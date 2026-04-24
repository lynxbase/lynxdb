package views

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func testDef(name string) ViewDefinition {
	return ViewDefinition{
		Name:    name,
		Version: 1,
		Type:    ViewTypeProjection,
		Query:   "source=nginx | fields _time, uri",
		Columns: []ColumnDef{
			{Name: "_time", Type: event.FieldTypeTimestamp},
			{Name: "uri", Type: event.FieldTypeString},
		},
		Status:    ViewStatusCreating,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}

func TestViewRegistry_CreateAndGet(t *testing.T) {
	dir := t.TempDir()
	r, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	def := testDef("mv_test")
	if err := r.Create(def); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := r.Get("mv_test")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "mv_test" {
		t.Errorf("Name: got %q, want %q", got.Name, "mv_test")
	}
	if got.Version != 1 {
		t.Errorf("Version: got %d, want 1", got.Version)
	}
	if len(got.Columns) != 2 {
		t.Errorf("Columns: got %d, want 2", len(got.Columns))
	}
}

func TestViewRegistry_CreateDuplicate(t *testing.T) {
	dir := t.TempDir()
	r, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	def := testDef("mv_test")
	if err := r.Create(def); err != nil {
		t.Fatalf("Create: %v", err)
	}

	err = r.Create(def)
	if !errors.Is(err, ErrViewAlreadyExists) {
		t.Errorf("duplicate create: got %v, want %v", err, ErrViewAlreadyExists)
	}
}

func TestViewRegistry_List(t *testing.T) {
	dir := t.TempDir()
	r, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	for _, name := range []string{"charlie", "alpha", "bravo"} {
		if err := r.Create(testDef(name)); err != nil {
			t.Fatalf("Create %s: %v", name, err)
		}
	}

	list := r.List()
	if len(list) != 3 {
		t.Fatalf("List: got %d, want 3", len(list))
	}
	if list[0].Name != "alpha" {
		t.Errorf("List[0]: got %q, want %q", list[0].Name, "alpha")
	}
	if list[1].Name != "bravo" {
		t.Errorf("List[1]: got %q, want %q", list[1].Name, "bravo")
	}
	if list[2].Name != "charlie" {
		t.Errorf("List[2]: got %q, want %q", list[2].Name, "charlie")
	}
}

func TestViewRegistry_Drop(t *testing.T) {
	dir := t.TempDir()
	r, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	if err := r.Create(testDef("mv_test")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := r.Drop("mv_test"); err != nil {
		t.Fatalf("Drop: %v", err)
	}

	_, err = r.Get("mv_test")
	if !errors.Is(err, ErrViewNotFound) {
		t.Errorf("Get after drop: got %v, want %v", err, ErrViewNotFound)
	}
}

func TestViewRegistry_DropNotFound(t *testing.T) {
	dir := t.TempDir()
	r, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	err = r.Drop("nonexistent")
	if !errors.Is(err, ErrViewNotFound) {
		t.Errorf("Drop nonexistent: got %v, want %v", err, ErrViewNotFound)
	}
}

func TestViewRegistry_Persistence(t *testing.T) {
	dir := t.TempDir()

	r1, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := r1.Create(testDef("mv_persist")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	r1.Close()

	// Reopen and verify.
	r2, err := Open(dir)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer r2.Close()

	got, err := r2.Get("mv_persist")
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if got.Name != "mv_persist" {
		t.Errorf("Name: got %q, want %q", got.Name, "mv_persist")
	}
	if got.Version != 1 {
		t.Errorf("Version: got %d, want 1", got.Version)
	}
}

func TestViewRegistry_Update(t *testing.T) {
	dir := t.TempDir()
	r, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	def := testDef("mv_test")
	def.Status = ViewStatusCreating
	if err := r.Create(def); err != nil {
		t.Fatalf("Create: %v", err)
	}

	def.Status = ViewStatusActive
	if err := r.Update(def); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got, err := r.Get("mv_test")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != ViewStatusActive {
		t.Errorf("Status: got %q, want %q", got.Status, ViewStatusActive)
	}
}

func TestViewRegistry_ConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	r, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := "mv_" + string(rune('a'+idx))
			def := testDef(name)
			if err := r.Create(def); err != nil {
				t.Errorf("concurrent Create %s: %v", name, err)
			}
		}(i)
	}
	wg.Wait()

	list := r.List()
	if len(list) != 10 {
		t.Errorf("List: got %d, want 10", len(list))
	}
}
