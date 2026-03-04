package unpack

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestDockerParser_Basic(t *testing.T) {
	input := `{"log":"hello world\n","stream":"stderr","time":"2026-01-01T00:00:00.000000000Z"}`

	p := &DockerParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["log"].String() != "hello world" {
		t.Errorf("log: got %q, want %q", fields["log"].String(), "hello world")
	}
	if fields["stream"].String() != "stderr" {
		t.Errorf("stream: got %q, want %q", fields["stream"].String(), "stderr")
	}
	if fields["time"].String() != "2026-01-01T00:00:00.000000000Z" {
		t.Errorf("time: got %q, want %q", fields["time"].String(), "2026-01-01T00:00:00.000000000Z")
	}
}

func TestDockerParser_NoTrailingNewline(t *testing.T) {
	input := `{"log":"no newline here","stream":"stdout","time":"2026-01-01T00:00:00Z"}`

	p := &DockerParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["log"].String() != "no newline here" {
		t.Errorf("log: got %q, want %q", fields["log"].String(), "no newline here")
	}
}

func TestDockerParser_ExtraFields(t *testing.T) {
	// Docker can include additional fields depending on driver config.
	input := `{"log":"msg\n","stream":"stdout","time":"2026-01-01T00:00:00Z","attrs":{"tag":"myapp"}}`

	p := &DockerParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["log"].String() != "msg" {
		t.Errorf("log: got %q, want %q", fields["log"].String(), "msg")
	}
	// Nested "attrs" is flattened by JSONParser.
	if fields["attrs.tag"].String() != "myapp" {
		t.Errorf("attrs.tag: got %q, want %q", fields["attrs.tag"].String(), "myapp")
	}
}

func TestDockerParser_Empty(t *testing.T) {
	p := &DockerParser{}
	err := p.Parse("", func(key string, val event.Value) bool {
		t.Fatalf("should not emit on empty input")
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
}

func TestDockerParser_NotJSON(t *testing.T) {
	p := &DockerParser{}
	called := false
	err := p.Parse("not json", func(key string, val event.Value) bool {
		called = true
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if called {
		t.Error("should not emit for non-JSON input")
	}
}

func TestDockerParser_ShortCircuit(t *testing.T) {
	input := `{"log":"msg\n","stream":"stdout","time":"2026-01-01T00:00:00Z"}`

	p := &DockerParser{}
	count := 0
	err := p.Parse(input, func(key string, val event.Value) bool {
		count++
		return false // stop after first
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 emit, got %d", count)
	}
}

func TestDockerParser_Name(t *testing.T) {
	p := &DockerParser{}
	if p.Name() != "docker" {
		t.Errorf("Name: got %q, want %q", p.Name(), "docker")
	}
}
