package sources

import (
	"sync"
	"testing"
)

func TestRegistry_Register(t *testing.T) {
	r := NewRegistry()
	r.Register("nginx")
	r.Register("postgres")
	r.Register("api-gateway")

	if r.Count() != 3 {
		t.Fatalf("expected 3 sources, got %d", r.Count())
	}

	// Idempotent.
	r.Register("nginx")
	if r.Count() != 3 {
		t.Fatalf("duplicate register changed count: got %d", r.Count())
	}

	// Empty string ignored.
	r.Register("")
	if r.Count() != 3 {
		t.Fatalf("empty register changed count: got %d", r.Count())
	}
}

func TestRegistry_Contains(t *testing.T) {
	r := NewRegistry()
	r.Register("nginx")
	r.Register("postgres")

	if !r.Contains("nginx") {
		t.Error("expected Contains(nginx) = true")
	}
	if !r.Contains("postgres") {
		t.Error("expected Contains(postgres) = true")
	}
	if r.Contains("redis") {
		t.Error("expected Contains(redis) = false")
	}
}

func TestRegistry_List(t *testing.T) {
	r := NewRegistry()
	r.Register("postgres")
	r.Register("nginx")
	r.Register("api-gateway")

	list := r.List()
	expected := []string{"api-gateway", "nginx", "postgres"}
	if len(list) != len(expected) {
		t.Fatalf("expected %d sources, got %d", len(expected), len(list))
	}
	for i, name := range expected {
		if list[i] != name {
			t.Errorf("list[%d] = %q, want %q", i, list[i], name)
		}
	}

	// Verify List returns a copy (mutation safety).
	list[0] = "mutated"
	if r.List()[0] == "mutated" {
		t.Error("List returned a reference to internal slice, expected a copy")
	}
}

func TestRegistry_MatchAll(t *testing.T) {
	r := NewRegistry()
	r.Register("nginx")
	r.Register("postgres")
	r.Register("redis")

	matched := r.Match("*")
	if len(matched) != 3 {
		t.Fatalf("Match(*) returned %d, want 3", len(matched))
	}
}

func TestRegistry_MatchGlob(t *testing.T) {
	r := NewRegistry()
	r.Register("nginx")
	r.Register("api-gateway")
	r.Register("postgres")
	r.Register("logs-web")
	r.Register("logs-api")
	r.Register("logs-db")

	tests := []struct {
		pattern string
		want    []string
	}{
		{"logs*", []string{"logs-api", "logs-db", "logs-web"}},
		{"logs-*", []string{"logs-api", "logs-db", "logs-web"}},
		{"nginx", []string{"nginx"}},
		{"*gateway*", []string{"api-gateway"}},
		{"nonexistent*", nil},
		{"logs-?b", []string{"logs-db"}},
		{"*", []string{"api-gateway", "logs-api", "logs-db", "logs-web", "nginx", "postgres"}},
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			got := r.Match(tt.pattern)
			if len(got) != len(tt.want) {
				t.Fatalf("Match(%q) returned %d results, want %d: %v", tt.pattern, len(got), len(tt.want), got)
			}
			for i, name := range tt.want {
				if got[i] != name {
					t.Errorf("Match(%q)[%d] = %q, want %q", tt.pattern, i, got[i], name)
				}
			}
		})
	}
}

func TestRegistry_RegisterAll(t *testing.T) {
	r := NewRegistry()
	r.RegisterAll([]string{"postgres", "nginx", "redis", "", "nginx"})

	if r.Count() != 3 {
		t.Fatalf("expected 3 sources, got %d", r.Count())
	}

	list := r.List()
	expected := []string{"nginx", "postgres", "redis"}
	for i, name := range expected {
		if list[i] != name {
			t.Errorf("list[%d] = %q, want %q", i, list[i], name)
		}
	}
}

func TestRegistry_Concurrency(t *testing.T) {
	r := NewRegistry()
	names := []string{"a", "b", "c", "d", "e", "f", "g", "h"}

	var wg sync.WaitGroup
	// Concurrent writers.
	for _, name := range names {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				r.Register(n)
			}
		}(name)
	}
	// Concurrent readers.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = r.List()
				_ = r.Match("*")
				_ = r.Contains("a")
				_ = r.Count()
			}
		}()
	}
	wg.Wait()

	if r.Count() != len(names) {
		t.Errorf("after concurrent access: count = %d, want %d", r.Count(), len(names))
	}
}

func TestRegistry_Empty(t *testing.T) {
	r := NewRegistry()

	if r.Count() != 0 {
		t.Errorf("empty registry count = %d, want 0", r.Count())
	}
	if len(r.List()) != 0 {
		t.Errorf("empty registry list len = %d, want 0", len(r.List()))
	}
	if len(r.Match("*")) != 0 {
		t.Errorf("empty registry match(*) len = %d, want 0", len(r.Match("*")))
	}
	if r.Contains("anything") {
		t.Error("empty registry Contains returned true")
	}
}
