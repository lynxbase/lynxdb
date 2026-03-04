package unpack

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestRedisParser_Basic(t *testing.T) {
	input := `12345:M 14 Feb 2026 14:52:01.234 * Ready to accept connections`

	p := &RedisParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["pid"].AsInt() != 12345 {
		t.Errorf("pid: got %v, want 12345", fields["pid"])
	}
	if fields["role_char"].String() != "M" {
		t.Errorf("role_char: got %q, want %q", fields["role_char"].String(), "M")
	}
	if fields["role"].String() != "master" {
		t.Errorf("role: got %q, want %q", fields["role"].String(), "master")
	}
	if fields["timestamp"].String() != "14 Feb 2026 14:52:01.234" {
		t.Errorf("timestamp: got %q, want %q", fields["timestamp"].String(), "14 Feb 2026 14:52:01.234")
	}
	if fields["level_char"].String() != "*" {
		t.Errorf("level_char: got %q, want %q", fields["level_char"].String(), "*")
	}
	if fields["level"].String() != "notice" {
		t.Errorf("level: got %q, want %q", fields["level"].String(), "notice")
	}
	if fields["message"].String() != "Ready to accept connections" {
		t.Errorf("message: got %q, want %q", fields["message"].String(), "Ready to accept connections")
	}
}

func TestRedisParser_Sentinel(t *testing.T) {
	input := `99:X 01 Jan 2026 00:00:00.000 # +sdown master mymaster 127.0.0.1 6379`

	p := &RedisParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["pid"].AsInt() != 99 {
		t.Errorf("pid: got %v, want 99", fields["pid"])
	}
	if fields["role"].String() != "sentinel" {
		t.Errorf("role: got %q, want %q", fields["role"].String(), "sentinel")
	}
	if fields["level"].String() != "warning" {
		t.Errorf("level: got %q, want %q", fields["level"].String(), "warning")
	}
}

func TestRedisParser_Replica(t *testing.T) {
	input := `42:S 10 Mar 2026 12:00:00.000 - MASTER <-> REPLICA sync started`

	p := &RedisParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["role"].String() != "replica" {
		t.Errorf("role: got %q, want %q", fields["role"].String(), "replica")
	}
	if fields["level"].String() != "verbose" {
		t.Errorf("level: got %q, want %q", fields["level"].String(), "verbose")
	}
}

func TestRedisParser_Debug(t *testing.T) {
	input := `1:C 01 Jan 2026 00:00:00.000 . debug message here`

	p := &RedisParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["role"].String() != "rdb_child" {
		t.Errorf("role: got %q, want %q", fields["role"].String(), "rdb_child")
	}
	if fields["level"].String() != "debug" {
		t.Errorf("level: got %q, want %q", fields["level"].String(), "debug")
	}
}

func TestRedisParser_Empty(t *testing.T) {
	p := &RedisParser{}
	err := p.Parse("", func(key string, val event.Value) bool {
		t.Fatalf("should not emit on empty input")
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
}

func TestRedisParser_Name(t *testing.T) {
	p := &RedisParser{}
	if p.Name() != "redis" {
		t.Errorf("Name: got %q, want %q", p.Name(), "redis")
	}
}
