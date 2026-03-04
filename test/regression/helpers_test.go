package regression

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/storage"
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

// Shared engine fixtures (initialized once per test binary run)

var (
	sshOnce   sync.Once
	sshEng    *storage.Engine
	sshCount  int
	sshInitOK bool

	openstackOnce   sync.Once
	openstackEng    *storage.Engine
	openstackCount  int
	openstackInitOK bool
)

// sshEngine returns a shared engine with OpenSSH_2k.log ingested into "main".
// The engine is created once and reused across all tests.
func sshEngine(t *testing.T) *storage.Engine {
	t.Helper()

	sshOnce.Do(func() {
		sshEng = storage.NewEphemeralEngine()
		var err error
		sshCount, err = sshEng.IngestFile(testdataPath("logs/OpenSSH_2k.log"), storage.IngestOpts{Source: "ssh"})
		if err != nil {
			t.Fatalf("ingest OpenSSH_2k.log: %v", err)
		}
		if sshCount == 0 {
			t.Fatal("no events ingested from OpenSSH_2k.log")
		}
		sshInitOK = true
	})

	if !sshInitOK {
		t.Fatal("SSH engine initialization failed in a prior test")
	}

	return sshEng
}

// openstackEngine returns a shared engine with OpenStack_2k.log ingested into "main".
func openstackEngine(t *testing.T) *storage.Engine {
	t.Helper()

	openstackOnce.Do(func() {
		openstackEng = storage.NewEphemeralEngine()
		var err error
		openstackCount, err = openstackEng.IngestFile(testdataPath("logs/OpenStack_2k.log"), storage.IngestOpts{Source: "openstack"})
		if err != nil {
			t.Fatalf("ingest OpenStack_2k.log: %v", err)
		}
		if openstackCount == 0 {
			t.Fatal("no events ingested from OpenStack_2k.log")
		}
		openstackInitOK = true
	})

	if !openstackInitOK {
		t.Fatal("OpenStack engine initialization failed in a prior test")
	}

	return openstackEng
}

// Query helper

// mustQuery executes an SPL2 query against the engine and fatals on error.
func mustQuery(t *testing.T, eng *storage.Engine, spl2 string) []map[string]interface{} {
	t.Helper()

	ctx := context.Background()
	res, _, err := eng.Query(ctx, spl2, storage.QueryOpts{})
	if err != nil {
		t.Fatalf("query %q: %v", spl2, err)
	}

	return res.Rows
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
