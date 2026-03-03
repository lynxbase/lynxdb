package alerts

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testAlert(name string) *Alert {
	in := AlertInput{
		Name:     name,
		Query:    "search index=main level=error | stats count",
		Interval: "1m",
		Channels: []NotificationChannel{
			{Type: ChannelWebhook, Name: "ops", Config: map[string]interface{}{"url": "https://example.com/hook"}},
		},
	}

	return in.ToAlert()
}

func TestStoreCRUD(t *testing.T) {
	s := OpenInMemory()

	// Create.
	a := testAlert("alert-1")
	if err := s.Create(a); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// List.
	list := s.List()
	if len(list) != 1 {
		t.Fatalf("List: got %d, want 1", len(list))
	}
	if list[0].Name != "alert-1" {
		t.Fatalf("List[0].Name = %q, want %q", list[0].Name, "alert-1")
	}

	// Get.
	got, err := s.Get(a.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name != "alert-1" {
		t.Fatalf("Get.Name = %q, want %q", got.Name, "alert-1")
	}

	// Update.
	got.Name = "alert-1-updated"
	if err := s.Update(got); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got2, _ := s.Get(a.ID)
	if got2.Name != "alert-1-updated" {
		t.Fatalf("after Update, Name = %q, want %q", got2.Name, "alert-1-updated")
	}

	// Delete.
	if err := s.Delete(a.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get(a.ID); !errors.Is(err, ErrAlertNotFound) {
		t.Fatalf("Get after Delete: got %v, want ErrAlertNotFound", err)
	}
}

func TestStoreDuplicateName(t *testing.T) {
	s := OpenInMemory()
	a1 := testAlert("dup")
	if err := s.Create(a1); err != nil {
		t.Fatal(err)
	}
	a2 := testAlert("dup")
	if err := s.Create(a2); !errors.Is(err, ErrAlertAlreadyExists) {
		t.Fatalf("expected ErrAlertAlreadyExists, got %v", err)
	}
}

func TestStoreNotFound(t *testing.T) {
	s := OpenInMemory()
	if _, err := s.Get("nonexistent"); !errors.Is(err, ErrAlertNotFound) {
		t.Fatalf("Get: got %v, want ErrAlertNotFound", err)
	}
	if err := s.Update(&Alert{ID: "nonexistent"}); !errors.Is(err, ErrAlertNotFound) {
		t.Fatalf("Update: got %v, want ErrAlertNotFound", err)
	}
	if err := s.Delete("nonexistent"); !errors.Is(err, ErrAlertNotFound) {
		t.Fatalf("Delete: got %v, want ErrAlertNotFound", err)
	}
}

func TestStoreUpdateStatus(t *testing.T) {
	s := OpenInMemory()
	a := testAlert("status-test")
	if err := s.Create(a); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	triggered := now.Add(-time.Second)
	if err := s.UpdateStatus(a.ID, StatusTriggered, now, &triggered); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}

	got, _ := s.Get(a.ID)
	if got.Status != StatusTriggered {
		t.Fatalf("Status = %v, want %v", got.Status, StatusTriggered)
	}
	if got.LastChecked == nil || got.LastChecked.Unix() != now.Unix() {
		t.Fatalf("LastChecked mismatch")
	}
	if got.LastTriggered == nil || got.LastTriggered.Unix() != triggered.Unix() {
		t.Fatalf("LastTriggered mismatch")
	}
}

func TestStorePersistence(t *testing.T) {
	dir := t.TempDir()

	// Open, create, close.
	s1, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	a := testAlert("persist-test")
	if err := s1.Create(a); err != nil {
		t.Fatal(err)
	}

	// Verify file exists.
	if _, err := os.Stat(filepath.Join(dir, alertsFile)); err != nil {
		t.Fatalf("alerts.json not created: %v", err)
	}

	// Reopen and verify data survives.
	s2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	list := s2.List()
	if len(list) != 1 {
		t.Fatalf("after reopen: got %d alerts, want 1", len(list))
	}
	if list[0].Name != "persist-test" {
		t.Fatalf("after reopen: Name = %q, want %q", list[0].Name, "persist-test")
	}
}

func TestStoreConcurrent(t *testing.T) {
	s := OpenInMemory()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			a := testAlert("")
			a.Name = a.ID // unique name per goroutine
			s.Create(a)
			s.List()
			s.Get(a.ID)
			now := time.Now()
			s.UpdateStatus(a.ID, StatusOK, now, nil)
		}(i)
	}
	wg.Wait()

	list := s.List()
	if len(list) != 10 {
		t.Fatalf("concurrent: got %d, want 10", len(list))
	}
}
