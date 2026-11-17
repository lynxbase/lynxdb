package regression

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/database"
	"github.com/lynxbase/lynxdb/pkg/ingest/ndjson"
	"github.com/lynxbase/lynxdb/pkg/query"
)

// Project root resolution

func projectRoot() string {
	_, filename, _, _ := runtime.Caller(0)

	// test/regression/helpers_test.go → project root is ../..
	return filepath.Join(filepath.Dir(filename), "..", "..")
}

func testdataPath(relPath string) string {
	return filepath.Join(projectRoot(), "testdata", relPath)
}

// Shared database fixtures (initialized once per test binary run)

var (
	sshOnce   sync.Once
	sshDB     *database.DB
	sshInitOK bool

	openstackOnce   sync.Once
	openstackDB     *database.DB
	openstackInitOK bool
)

// TestMain closes the shared databases, which removes their directories.
func TestMain(m *testing.M) {
	code := m.Run()
	for _, db := range []*database.DB{sshDB, openstackDB} {
		if db != nil {
			_ = db.Close()
		}
	}
	os.Exit(code)
}

// loadLogDB opens a database in a temporary directory and loads the lines of
// a log file into the index main.
func loadLogDB(path, source string) (*database.DB, error) {
	db, err := database.Open(database.Config{})
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	conv, err := ndjson.NewText("")
	if err != nil {
		return nil, err
	}
	conv.Source = source
	if err := conv.Read(f, func(b []byte) error {
		_, err := db.Append(database.DefaultSource, b)
		return err
	}); err != nil {
		return nil, err
	}

	return db, db.Sync()
}

// sshEngine returns a shared database with OpenSSH_2k.log loaded into "main".
// The database is created once and reused across all tests.
func sshEngine(t *testing.T) *database.DB {
	t.Helper()

	sshOnce.Do(func() {
		var err error
		sshDB, err = loadLogDB(testdataPath("logs/OpenSSH_2k.log"), "ssh")
		if err != nil {
			t.Fatalf("load OpenSSH_2k.log: %v", err)
		}
		sshInitOK = true
	})

	if !sshInitOK {
		t.Fatal("SSH database initialization failed in a prior test")
	}

	return sshDB
}

// openstackEngine returns a shared database with OpenStack_2k.log loaded into "main".
func openstackEngine(t *testing.T) *database.DB {
	t.Helper()

	openstackOnce.Do(func() {
		var err error
		openstackDB, err = loadLogDB(testdataPath("logs/OpenStack_2k.log"), "openstack")
		if err != nil {
			t.Fatalf("load OpenStack_2k.log: %v", err)
		}
		openstackInitOK = true
	})

	if !openstackInitOK {
		t.Fatal("OpenStack database initialization failed in a prior test")
	}

	return openstackDB
}

// Query helper

// mustQuery runs a PPL query against the database and fatals on error.
func mustQuery(t *testing.T, db *database.DB, q string) []map[string]interface{} {
	t.Helper()

	c, err := query.Compile(q)
	if err != nil {
		t.Fatalf("compile %q: %v", q, err)
	}
	res, err := db.Query(context.Background(), c, database.QueryOptions{})
	if err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	maps := res.Maps()
	rows := make([]map[string]interface{}, len(maps))
	for i, m := range maps {
		row := make(map[string]interface{}, len(m))
		for k, v := range m {
			row[k] = v.Interface()
		}
		rows[i] = row
	}

	return rows
}

// Assertion helpers

// requireRowCount fatals if the number of rows doesn't match expected.
func requireRowCount(t *testing.T, rows []map[string]interface{}, expected int) {
	t.Helper()

	if len(rows) != expected {
		t.Fatalf("expected %d rows, got %d", expected, len(rows))
	}
}

// requireAggValue asserts the first row's integer field equals expected.
func requireAggValue(t *testing.T, rows []map[string]interface{}, field string, expected int) {
	t.Helper()

	if len(rows) == 0 {
		t.Fatalf("expected at least 1 row for field %q, got 0", field)
	}

	got := toInt(rows[0][field])
	if got != expected {
		t.Errorf("expected %s=%d, got %d", field, expected, got)
	}
}

// getInt extracts an integer value from the first row.
func getInt(rows []map[string]interface{}, field string) int {
	if len(rows) == 0 {
		return 0
	}

	return toInt(rows[0][field])
}

// getFloat extracts a float value from the first row.
func getFloat(rows []map[string]interface{}, field string) float64 {
	if len(rows) == 0 {
		return 0
	}

	return toFloat(rows[0][field])
}

// getStr extracts a string value from the first row.
func getStr(rows []map[string]interface{}, field string) string {
	if len(rows) == 0 {
		return ""
	}

	v := rows[0][field]
	if v == nil {
		return ""
	}

	return fmt.Sprint(v)
}

// rowsToMap pivots rows into a lookup map from keyField to valueField (as int).
func rowsToMap(rows []map[string]interface{}, keyField, valueField string) map[string]int {
	m := make(map[string]int, len(rows))
	for _, row := range rows {
		key := fmt.Sprint(row[keyField])
		m[key] = toInt(row[valueField])
	}

	return m
}

// Type conversions

func toInt(v interface{}) int {
	switch val := v.(type) {
	case float64:
		return int(val)
	case int64:
		return int(val)
	case int:
		return val
	case uint64:
		return int(val)
	default:
		return 0
	}
}

func toFloat(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case int64:
		return float64(val)
	case int:
		return float64(val)
	case uint64:
		return float64(val)
	default:
		return 0
	}
}
