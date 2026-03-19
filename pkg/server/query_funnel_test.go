package server

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/memgov"
	"github.com/lynxbase/lynxdb/pkg/stats"
)

// Tests for computeQueryFunnel

func TestComputeQueryFunnel_ScanAndDedup(t *testing.T) {
	ss := &SearchStats{RowsScanned: 50_000}
	stages := []PipelineStage{
		{Name: "Scan", InputRows: 0, OutputRows: 10_000},
		{Name: "Filter", InputRows: 10_000, OutputRows: 5_000},
		{Name: "Dedup", InputRows: 5_000, OutputRows: 2_000},
	}
	computeQueryFunnel(ss, stages)

	if ss.RowsInRange != 10_000 {
		t.Errorf("expected RowsInRange=10000, got %d", ss.RowsInRange)
	}
	if ss.RowsAfterDedup != 2_000 {
		t.Errorf("expected RowsAfterDedup=2000, got %d", ss.RowsAfterDedup)
	}
}

func TestComputeQueryFunnel_NoDedup(t *testing.T) {
	ss := &SearchStats{RowsScanned: 1_000}
	stages := []PipelineStage{
		{Name: "Scan", InputRows: 0, OutputRows: 1_000},
		{Name: "Filter", InputRows: 1_000, OutputRows: 500},
	}
	computeQueryFunnel(ss, stages)

	if ss.RowsInRange != 1_000 {
		t.Errorf("expected RowsInRange=1000, got %d", ss.RowsInRange)
	}
	if ss.RowsAfterDedup != 0 {
		t.Errorf("expected RowsAfterDedup=0 when no dedup stage, got %d", ss.RowsAfterDedup)
	}
}

func TestComputeQueryFunnel_NoScan_FallbackToRowsScanned(t *testing.T) {
	ss := &SearchStats{RowsScanned: 42_000}
	// No Scan stage present (e.g., partial agg pipeline).
	stages := []PipelineStage{
		{Name: "Aggregate", InputRows: 1_000, OutputRows: 50},
	}
	computeQueryFunnel(ss, stages)

	// Fallback: RowsInRange = RowsScanned.
	if ss.RowsInRange != 42_000 {
		t.Errorf("expected RowsInRange=42000 (fallback), got %d", ss.RowsInRange)
	}
}

func TestComputeQueryFunnel_EmptyStages(t *testing.T) {
	ss := &SearchStats{RowsScanned: 100}
	computeQueryFunnel(ss, nil)

	if ss.RowsInRange != 100 {
		t.Errorf("expected RowsInRange=100 (fallback), got %d", ss.RowsInRange)
	}
	if ss.RowsAfterDedup != 0 {
		t.Errorf("expected RowsAfterDedup=0, got %d", ss.RowsAfterDedup)
	}
}

// Tests for applySpillAndPoolStats

func TestApplySpillAndPoolStats_SpillingStages(t *testing.T) {
	ss := &SearchStats{}
	stages := []PipelineStage{
		{Name: "Sort", SpillBytes: 1024},
		{Name: "Aggregate", SpillBytes: 2048},
		{Name: "Filter", SpillBytes: 0},
	}
	applySpillAndPoolStats(ss, stages)

	if !ss.SpilledToDisk {
		t.Error("expected SpilledToDisk=true")
	}
	if ss.SpillBytes != 3072 {
		t.Errorf("expected SpillBytes=3072, got %d", ss.SpillBytes)
	}
	if ss.SpillFiles != 2 {
		t.Errorf("expected SpillFiles=2, got %d", ss.SpillFiles)
	}
}

func TestApplySpillAndPoolStats_NoSpill(t *testing.T) {
	ss := &SearchStats{}
	stages := []PipelineStage{
		{Name: "Filter", SpillBytes: 0},
		{Name: "Scan", SpillBytes: 0},
	}

	applySpillAndPoolStats(ss, stages)

	if ss.SpilledToDisk {
		t.Error("expected SpilledToDisk=false")
	}
	if ss.SpillBytes != 0 {
		t.Errorf("expected SpillBytes=0, got %d", ss.SpillBytes)
	}
	if ss.SpillFiles != 0 {
		t.Errorf("expected SpillFiles=0, got %d", ss.SpillFiles)
	}
}

// Tests for classifyErrorType

func TestClassifyErrorType_Nil(t *testing.T) {
	result := classifyErrorType(nil)
	if result != "" {
		t.Errorf("expected empty for nil error, got %q", result)
	}
}

func TestClassifyErrorType_Timeout(t *testing.T) {
	result := classifyErrorType(context.DeadlineExceeded)
	if result != "timeout" {
		t.Errorf("expected 'timeout', got %q", result)
	}
}

func TestClassifyErrorType_WrappedTimeout(t *testing.T) {
	err := fmt.Errorf("query failed: %w", context.DeadlineExceeded)
	result := classifyErrorType(err)
	if result != "timeout" {
		t.Errorf("expected 'timeout' for wrapped deadline, got %q", result)
	}
}

func TestClassifyErrorType_BudgetExceeded(t *testing.T) {
	err := &memgov.BudgetExceededError{
		Monitor:   "query",
		Requested: 1024,
		Current:   999,
		Limit:     1000,
	}
	result := classifyErrorType(err)
	if result != "memory" {
		t.Errorf("expected 'memory', got %q", result)
	}
}

func TestClassifyErrorType_PoolExhausted(t *testing.T) {
	err := &memgov.PoolExhaustedError{
		Pool:      "global",
		Requested: 1024,
		Current:   999,
		Limit:     1000,
	}
	result := classifyErrorType(err)
	if result != "memory" {
		t.Errorf("expected 'memory', got %q", result)
	}
}

func TestClassifyErrorType_GenericError(t *testing.T) {
	err := errors.New("something went wrong")
	result := classifyErrorType(err)
	if result != "execution" {
		t.Errorf("expected 'execution', got %q", result)
	}
}

// Tests for convertStageStats (exclusive timing)

func TestConvertStageStats_ExclusiveTiming(t *testing.T) {
	// Volcano model: Sort includes Scan time. Sort took 100ms total,
	// Scan (child) took 80ms, so Sort's exclusive = 20ms.
	stages := []stats.StageStats{
		{Name: "Sort", Duration: 100 * time.Millisecond, InputRows: 1000, OutputRows: 1000},
		{Name: "Scan", Duration: 80 * time.Millisecond, InputRows: 0, OutputRows: 1000},
	}

	result := convertStageStats(stages)

	if len(result) != 2 {
		t.Fatalf("expected 2 stages, got %d", len(result))
	}

	// Sort exclusive = 100ms - 80ms = 20ms.
	sortExclusive := result[0].ExclusiveMS
	if sortExclusive < 19.9 || sortExclusive > 20.1 {
		t.Errorf("expected Sort exclusive ~20ms, got %f", sortExclusive)
	}

	// Scan (leaf) exclusive = inclusive = 80ms.
	scanExclusive := result[1].ExclusiveMS
	if scanExclusive < 79.9 || scanExclusive > 80.1 {
		t.Errorf("expected Scan exclusive ~80ms, got %f", scanExclusive)
	}
}

func TestConvertStageStats_Empty(t *testing.T) {
	result := convertStageStats(nil)
	if result != nil {
		t.Errorf("expected nil for empty stages, got %v", result)
	}
}

func TestConvertStageStats_SingleStage(t *testing.T) {
	stages := []stats.StageStats{
		{Name: "Scan", Duration: 50 * time.Millisecond, InputRows: 0, OutputRows: 500},
	}

	result := convertStageStats(stages)

	if len(result) != 1 {
		t.Fatalf("expected 1 stage, got %d", len(result))
	}

	// Single stage: exclusive == inclusive.
	if result[0].ExclusiveMS < 49.9 || result[0].ExclusiveMS > 50.1 {
		t.Errorf("expected exclusive ~50ms, got %f", result[0].ExclusiveMS)
	}
}

func TestConvertStageStats_SpillFields(t *testing.T) {
	stages := []stats.StageStats{
		{
			Name:        "Sort",
			Duration:    10 * time.Millisecond,
			InputRows:   1000,
			OutputRows:  1000,
			MemoryBytes: 4096,
			SpilledRows: 500,
			SpillBytes:  2048,
		},
	}

	result := convertStageStats(stages)

	if result[0].MemoryBytes != 4096 {
		t.Errorf("expected MemoryBytes=4096, got %d", result[0].MemoryBytes)
	}
	if result[0].SpilledRows != 500 {
		t.Errorf("expected SpilledRows=500, got %d", result[0].SpilledRows)
	}
	if result[0].SpillBytes != 2048 {
		t.Errorf("expected SpillBytes=2048, got %d", result[0].SpillBytes)
	}
}

func TestConvertStageStats_ThreeStageChain(t *testing.T) {
	// Sort(200ms) -> Filter(150ms) -> Scan(100ms)
	// Exclusive: Sort=50ms, Filter=50ms, Scan=100ms.
	stages := []stats.StageStats{
		{Name: "Sort", Duration: 200 * time.Millisecond},
		{Name: "Filter", Duration: 150 * time.Millisecond},
		{Name: "Scan", Duration: 100 * time.Millisecond},
	}

	result := convertStageStats(stages)

	if len(result) != 3 {
		t.Fatalf("expected 3 stages, got %d", len(result))
	}

	// Sort: 200 - 150 = 50ms.
	if result[0].ExclusiveMS < 49.9 || result[0].ExclusiveMS > 50.1 {
		t.Errorf("expected Sort exclusive ~50ms, got %f", result[0].ExclusiveMS)
	}
	// Filter: 150 - 100 = 50ms.
	if result[1].ExclusiveMS < 49.9 || result[1].ExclusiveMS > 50.1 {
		t.Errorf("expected Filter exclusive ~50ms, got %f", result[1].ExclusiveMS)
	}
	// Scan (leaf): 100ms.
	if result[2].ExclusiveMS < 99.9 || result[2].ExclusiveMS > 100.1 {
		t.Errorf("expected Scan exclusive ~100ms, got %f", result[2].ExclusiveMS)
	}
}
