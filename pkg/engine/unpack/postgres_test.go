package unpack

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestPostgresParser_Basic(t *testing.T) {
	input := `2026-02-14 14:52:01.234 UTC [12345] postgres@mydb LOG:  connection authorized: user=postgres database=mydb`

	p := &PostgresParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	expect := map[string]string{
		"timestamp": "2026-02-14 14:52:01.234 UTC",
		"user":      "postgres",
		"database":  "mydb",
		"severity":  "LOG",
	}
	for k, want := range expect {
		got := fields[k].String()
		if got != want {
			t.Errorf("%s: got %q, want %q", k, got, want)
		}
	}

	if fields["pid"].AsInt() != 12345 {
		t.Errorf("pid: got %v, want 12345", fields["pid"])
	}

	if fields["message"].String() != "connection authorized: user=postgres database=mydb" {
		t.Errorf("message: got %q", fields["message"].String())
	}
}

func TestPostgresParser_Duration(t *testing.T) {
	input := `2026-02-14 14:52:01.234 UTC [12345] postgres@mydb LOG:  duration: 3.456 ms`

	p := &PostgresParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["duration_ms"].AsFloat() != 3.456 {
		t.Errorf("duration_ms: got %v, want 3.456", fields["duration_ms"])
	}
}

func TestPostgresParser_DurationWithStatement(t *testing.T) {
	input := `2026-02-14 14:52:01.234 UTC [12345] postgres@mydb LOG:  duration: 1.234 ms  statement: SELECT * FROM users WHERE id = 42`

	p := &PostgresParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["duration_ms"].AsFloat() != 1.234 {
		t.Errorf("duration_ms: got %v, want 1.234", fields["duration_ms"])
	}
	stmt := fields["statement"].String()
	if stmt != "SELECT * FROM users WHERE id = 42" {
		t.Errorf("statement: got %q, want %q", stmt, "SELECT * FROM users WHERE id = 42")
	}
}

func TestPostgresParser_DurationWithoutStatement(t *testing.T) {
	// Plain duration without statement suffix — no statement field emitted.
	input := `2026-02-14 14:52:01.234 UTC [12345] postgres@mydb LOG:  duration: 3.456 ms`

	p := &PostgresParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["duration_ms"].AsFloat() != 3.456 {
		t.Errorf("duration_ms: got %v, want 3.456", fields["duration_ms"])
	}
	if _, ok := fields["statement"]; ok {
		t.Errorf("statement should not be emitted for plain duration lines, got %q", fields["statement"].String())
	}
}

func TestPostgresParser_Error(t *testing.T) {
	input := `2026-02-14 14:52:01.234 UTC [99] app@testdb ERROR:  relation "users" does not exist`

	p := &PostgresParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["severity"].String() != "ERROR" {
		t.Errorf("severity: got %q, want %q", fields["severity"].String(), "ERROR")
	}
	if fields["user"].String() != "app" {
		t.Errorf("user: got %q, want %q", fields["user"].String(), "app")
	}
	if fields["database"].String() != "testdb" {
		t.Errorf("database: got %q, want %q", fields["database"].String(), "testdb")
	}
}

func TestPostgresParser_Empty(t *testing.T) {
	p := &PostgresParser{}
	err := p.Parse("", func(key string, val event.Value) bool {
		t.Fatalf("should not emit on empty input")
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
}

func TestPostgresParser_Name(t *testing.T) {
	p := &PostgresParser{}
	if p.Name() != "postgres" {
		t.Errorf("Name: got %q, want %q", p.Name(), "postgres")
	}
}
