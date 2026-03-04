package unpack

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestMySQLSlowParser_Basic(t *testing.T) {
	input := "# Time: 2026-02-14T14:52:01.234567Z\n" +
		"# User@Host: root[root] @ localhost [127.0.0.1]  Id:    42\n" +
		"# Query_time: 3.456789  Lock_time: 0.000123  Rows_sent: 10  Rows_examined: 50000\n" +
		"SELECT * FROM users WHERE status = 'active';"

	p := &MySQLSlowParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	expect := map[string]string{
		"timestamp": "2026-02-14T14:52:01.234567Z",
		"user":      "root",
		"host":      "localhost",
		"client_ip": "127.0.0.1",
		"statement": "SELECT * FROM users WHERE status = 'active'",
	}
	for k, want := range expect {
		got := fields[k].String()
		if got != want {
			t.Errorf("%s: got %q, want %q", k, got, want)
		}
	}

	if fields["connection_id"].AsInt() != 42 {
		t.Errorf("connection_id: got %v, want 42", fields["connection_id"])
	}

	// Check float metrics
	if fields["query_time"].AsFloat() != 3.456789 {
		t.Errorf("query_time: got %v, want 3.456789", fields["query_time"])
	}
	if fields["lock_time"].AsFloat() != 0.000123 {
		t.Errorf("lock_time: got %v, want 0.000123", fields["lock_time"])
	}
	if fields["rows_sent"].AsInt() != 10 {
		t.Errorf("rows_sent: got %v, want 10", fields["rows_sent"])
	}
	if fields["rows_examined"].AsInt() != 50000 {
		t.Errorf("rows_examined: got %v, want 50000", fields["rows_examined"])
	}
}

func TestMySQLSlowParser_MultiLineSQL(t *testing.T) {
	input := "# Time: 2026-01-01T00:00:00Z\n" +
		"# User@Host: app[app] @  [10.0.0.1]  Id:   1\n" +
		"# Query_time: 1.000000  Lock_time: 0.000000  Rows_sent: 0  Rows_examined: 0\n" +
		"SELECT u.id, u.name\n" +
		"FROM users u\n" +
		"JOIN orders o ON o.user_id = u.id\n" +
		"WHERE u.active = 1;"

	p := &MySQLSlowParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	stmt := fields["statement"].String()
	if stmt != "SELECT u.id, u.name FROM users u JOIN orders o ON o.user_id = u.id WHERE u.active = 1" {
		t.Errorf("statement: got %q", stmt)
	}

	if fields["client_ip"].String() != "10.0.0.1" {
		t.Errorf("client_ip: got %q, want %q", fields["client_ip"].String(), "10.0.0.1")
	}
}

func TestMySQLSlowParser_NoTimestamp(t *testing.T) {
	// Older MySQL versions may not include the Time line.
	input := "# User@Host: root[root] @ localhost [127.0.0.1]\n" +
		"# Query_time: 0.500000  Lock_time: 0.000000  Rows_sent: 1  Rows_examined: 1\n" +
		"SELECT 1;"

	p := &MySQLSlowParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if _, ok := fields["timestamp"]; ok {
		t.Error("timestamp should not be present")
	}
	if fields["user"].String() != "root" {
		t.Errorf("user: got %q, want %q", fields["user"].String(), "root")
	}
	if fields["statement"].String() != "SELECT 1" {
		t.Errorf("statement: got %q", fields["statement"].String())
	}
}

func TestMySQLSlowParser_Empty(t *testing.T) {
	p := &MySQLSlowParser{}
	err := p.Parse("", func(key string, val event.Value) bool {
		t.Fatalf("should not emit on empty input")
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
}

func TestMySQLSlowParser_Name(t *testing.T) {
	p := &MySQLSlowParser{}
	if p.Name() != "mysql_slow" {
		t.Errorf("Name: got %q, want %q", p.Name(), "mysql_slow")
	}
}
