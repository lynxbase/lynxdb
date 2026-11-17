//go:build e2e

package shippers

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/client"
)

// fixtureText is held by every line that writeFixture writes.
const fixtureText = "shipper fixture"

// countQuery returns a query that counts, into the column total, the events
// of an index (or index glob) that satisfy cond.
func countQuery(index, cond string) string {
	return fmt.Sprintf("source=`%s` | where %s | stats count() as total", index, cond)
}

// holds returns a condition that field contains text.
func holds(field, text string) string {
	return fmt.Sprintf("like(%s, %q)", field, "%"+text+"%")
}

// waitForCount polls a countQuery until its total reaches want.
func waitForCount(t *testing.T, rig *TestRig, query string, want int, logs func() string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var last int
	var lastErr error
	for ctx.Err() == nil {
		result, err := rig.Client.QuerySync(ctx, query, "", "")
		lastErr = err
		if err == nil {
			last = totalOf(result)
			if last >= want {
				return
			}
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
	t.Fatalf("%s: total = %d, want >= %d (last error: %v)\nstored:\n%s\nshipper logs:\n%s",
		query, last, want, lastErr, storeSummary(rig), logs())
}

// requireIndexNames fails unless an index matches pattern.
func requireIndexNames(t *testing.T, rig *TestRig, pattern string) {
	t.Helper()
	re := regexp.MustCompile(pattern)
	indexes, err := rig.Client.Indexes(context.Background())
	if err != nil {
		t.Fatalf("Indexes: %v", err)
	}
	for _, ix := range indexes {
		if re.MatchString(ix.Name) {
			return
		}
	}
	t.Fatalf("no index matches %s:\n%s", pattern, storeSummary(rig))
}

func totalOf(result *client.QueryResult) int {
	if result == nil || result.Aggregate == nil || len(result.Aggregate.Rows) == 0 {
		return 0
	}
	for i, col := range result.Aggregate.Columns {
		if col == "total" && i < len(result.Aggregate.Rows[0]) {
			return intValue(result.Aggregate.Rows[0][i])
		}
	}
	return 0
}

func intValue(v interface{}) int {
	switch v := v.(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	case json.Number:
		n, _ := strconv.Atoi(v.String())
		return n
	default:
		return 0
	}
}

// storeSummary lists the indexes with their event counts and a few events.
func storeSummary(rig *TestRig) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var b strings.Builder
	indexes, err := rig.Client.Indexes(ctx)
	if err != nil {
		return err.Error()
	}
	for _, ix := range indexes {
		fmt.Fprintf(&b, "index %s: %d events\n", ix.Name, ix.Events)
	}
	result, err := rig.Client.QuerySync(ctx, `source=* | head 3`, "", "")
	if err != nil {
		fmt.Fprintf(&b, "sample: %v\n", err)
		return b.String()
	}
	if result.Events != nil {
		for _, ev := range result.Events.Events {
			line, _ := json.Marshal(ev)
			fmt.Fprintf(&b, "sample: %s\n", line)
		}
	}
	return b.String()
}
