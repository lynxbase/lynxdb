package views

import (
	"testing"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
)

func TestFilter_Compile_SingleField(t *testing.T) {
	f, err := Compile("source=nginx")
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if len(f.predicates) != 1 {
		t.Fatalf("predicates: got %d, want 1", len(f.predicates))
	}
	if f.predicates[0].field != "source" || f.predicates[0].op != "=" || f.predicates[0].value != "nginx" {
		t.Errorf("predicate: %+v", f.predicates[0])
	}

	// Behavioral verification: the compiled filter should match/reject events correctly.
	// "source" is a builtin field alias for Event.Source, so set the struct field directly.
	matching := makeEvent(nil)
	matching.Source = "nginx"
	if !f.Match(matching) {
		t.Error("compiled filter should match event with source=nginx")
	}

	nonMatching := makeEvent(nil)
	nonMatching.Source = "api"
	if f.Match(nonMatching) {
		t.Error("compiled filter should reject event with source=api")
	}
}

func TestFilter_Compile_MultipleFields(t *testing.T) {
	f, err := Compile("source=nginx status>=500")
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if len(f.predicates) != 2 {
		t.Fatalf("predicates: got %d, want 2", len(f.predicates))
	}
}

func TestFilter_Compile_Empty(t *testing.T) {
	f, err := Compile("")
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if len(f.predicates) != 0 {
		t.Errorf("predicates: got %d, want 0", len(f.predicates))
	}
}

func makeEvent(fields map[string]string) *event.Event {
	e := event.NewEvent(time.Now(), "test")
	e.Index = "main"
	for k, v := range fields {
		switch k {
		case "_source":
			e.Source = v
		case "host":
			e.Host = v
		default:
			e.SetField(k, event.StringValue(v))
		}
	}

	return e
}

func TestFilter_Match_StringEquals(t *testing.T) {
	f, _ := Compile("_source=nginx")
	e := makeEvent(map[string]string{"_source": "nginx"})
	if !f.Match(e) {
		t.Error("expected match for source=nginx")
	}
}

func TestFilter_Match_StringNotEquals(t *testing.T) {
	f, _ := Compile("_source=nginx")
	e := makeEvent(map[string]string{"_source": "api"})
	if f.Match(e) {
		t.Error("expected no match for source=api")
	}
}

func TestFilter_Match_NumericGte(t *testing.T) {
	f, _ := Compile("status>=500")
	e := makeEvent(map[string]string{"status": "500"})
	if !f.Match(e) {
		t.Error("expected match for status=500 >= 500")
	}
}

func TestFilter_Match_NumericLt(t *testing.T) {
	f, _ := Compile("status>=500")
	e := makeEvent(map[string]string{"status": "200"})
	if f.Match(e) {
		t.Error("expected no match for status=200 >= 500")
	}
}

func TestFilter_Match_MultiplePredicates(t *testing.T) {
	f, _ := Compile("_source=nginx status>=500")

	// Both match.
	e1 := makeEvent(map[string]string{"_source": "nginx", "status": "500"})
	if !f.Match(e1) {
		t.Error("expected match for both predicates")
	}

	// Only source matches.
	e2 := makeEvent(map[string]string{"_source": "nginx", "status": "200"})
	if f.Match(e2) {
		t.Error("expected no match when status doesn't match")
	}
}

func TestFilter_Match_EmptyFilter(t *testing.T) {
	f, _ := Compile("")
	e := makeEvent(map[string]string{"anything": "goes"})
	if !f.Match(e) {
		t.Error("empty filter should match everything")
	}
}
