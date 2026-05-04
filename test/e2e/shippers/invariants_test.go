//go:build e2e

package shippers

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/client"
)

func waitForSourceCount(t *testing.T, rig *TestRig, source string, want int, logs func() string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	query := `FROM main | STATS count AS total BY _source`

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var last int
	for ctx.Err() == nil {
		result, err := rig.Client.QuerySync(ctx, query, "", "")
		if err == nil {
			last = sourceCountFromResult(result, source)
			if last >= want {
				return
			}
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
	t.Fatalf("source %q count = %d, want >= %d\nsources seen:\n%s\nshipper logs:\n%s",
		source, last, want, sourceSummary(t, rig), logs())
}

func sourceCountFromResult(result *client.QueryResult, source string) int {
	if result == nil || result.Aggregate == nil || len(result.Aggregate.Rows) == 0 {
		return 0
	}
	sourceIdx := columnIndex(result, "_source")
	totalIdx := columnIndex(result, "total")
	if sourceIdx < 0 || totalIdx < 0 {
		return 0
	}
	for _, row := range result.Aggregate.Rows {
		if sourceIdx >= len(row) || totalIdx >= len(row) {
			continue
		}
		if fmt.Sprint(row[sourceIdx]) == source {
			return intValue(row[totalIdx])
		}
	}
	return 0
}

func columnIndex(result *client.QueryResult, field string) int {
	for i, col := range result.Aggregate.Columns {
		if col == field {
			return i
		}
	}
	return -1
}

func intValue(v interface{}) int {
	switch v := v.(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	case int32:
		return int(v)
	case uint:
		return int(v)
	case uint64:
		return int(v)
	case uint32:
		return int(v)
	case json.Number:
		n, _ := strconv.Atoi(v.String())
		return n
	default:
		return 0
	}
}

func sourceSummary(t *testing.T, rig *TestRig) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := rig.Client.QuerySync(ctx, `FROM main | STATS count AS total BY _source`, "", "")
	if err != nil {
		return err.Error()
	}
	if result == nil || result.Aggregate == nil {
		return "<none>"
	}
	return fmt.Sprintf("columns=%v rows=%v", result.Aggregate.Columns, result.Aggregate.Rows)
}
