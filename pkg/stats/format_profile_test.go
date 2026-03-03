package stats

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestFormatProfile_PhaseBreakdown(t *testing.T) {
	s := &QueryStats{
		TotalDuration:     40 * time.Millisecond,
		ParseDuration:     200 * time.Microsecond,
		OptimizeDuration:  100 * time.Microsecond,
		ExecDuration:      39 * time.Millisecond,
		SerializeDuration: 700 * time.Microsecond,
		Ephemeral:         true,
		ScannedRows:       1000,
		ResultRows:        10,
	}

	var buf bytes.Buffer
	FormatProfile(&buf, s)
	out := buf.String()

	if !strings.Contains(out, "Query Profile") {
		t.Error("missing Query Profile header")
	}
	if !strings.Contains(out, "Phase Breakdown") {
		t.Error("missing Phase Breakdown section")
	}
	if !strings.Contains(out, "parse") {
		t.Error("missing parse phase")
	}
	if !strings.Contains(out, "exec") {
		t.Error("missing exec phase")
	}
}

func TestFormatProfile_Segments(t *testing.T) {
	s := &QueryStats{
		TotalDuration:        100 * time.Millisecond,
		ExecDuration:         90 * time.Millisecond,
		TotalSegments:        45,
		ScannedSegments:      12,
		BloomSkippedSegments: 20,
		TimeSkippedSegments:  8,
		StatSkippedSegments:  5,
		ScannedRows:          1_200_000,
		MatchedRows:          847,
		ResultRows:           10,
	}

	var buf bytes.Buffer
	FormatProfile(&buf, s)
	out := buf.String()

	if !strings.Contains(out, "Segments:") {
		t.Error("missing Segments section")
	}
	if !strings.Contains(out, "12/45") {
		t.Error("missing segment count 12/45")
	}
	if !strings.Contains(out, "bloom:") {
		t.Error("missing bloom skip count")
	}
}

func TestFormatProfile_RowFunnel(t *testing.T) {
	s := &QueryStats{
		TotalDuration: 10 * time.Millisecond,
		ExecDuration:  8 * time.Millisecond,
		ScannedRows:   100_000,
		MatchedRows:   500,
		ResultRows:    10,
		ScanType:      ScanTypeFilteredScan,
	}

	var buf bytes.Buffer
	FormatProfile(&buf, s)
	out := buf.String()

	if !strings.Contains(out, "Rows:") {
		t.Error("missing Rows section")
	}
	if !strings.Contains(out, "scanned") {
		t.Error("missing 'scanned' label")
	}
	if !strings.Contains(out, "matched") {
		t.Error("missing 'matched' label")
	}
	if !strings.Contains(out, "results") {
		t.Error("missing 'results' label")
	}
}

func TestFormatProfile_Pipeline(t *testing.T) {
	s := &QueryStats{
		TotalDuration: 40 * time.Millisecond,
		ExecDuration:  35 * time.Millisecond,
		ScannedRows:   100_000,
		ResultRows:    10,
		Stages: []StageStats{
			{Name: "Scan", InputRows: 100_000, OutputRows: 100_000, Duration: 12 * time.Millisecond},
			{Name: "Filter", InputRows: 100_000, OutputRows: 847, Duration: 8 * time.Millisecond},
			{Name: "Stats", InputRows: 847, OutputRows: 10, Duration: 14 * time.Millisecond},
		},
	}

	var buf bytes.Buffer
	FormatProfile(&buf, s)
	out := buf.String()

	if !strings.Contains(out, "Pipeline:") {
		t.Error("missing Pipeline section")
	}
	if !strings.Contains(out, "Scan") {
		t.Error("missing Scan stage")
	}
	if !strings.Contains(out, "Filter") {
		t.Error("missing Filter stage")
	}
	if !strings.Contains(out, "Stats") {
		t.Error("missing Stats stage")
	}
}

func TestFormatProfile_Optimizations(t *testing.T) {
	s := &QueryStats{
		TotalDuration:        10 * time.Millisecond,
		ExecDuration:         8 * time.Millisecond,
		PartialAggUsed:       true,
		VectorizedFilterUsed: true,
	}

	var buf bytes.Buffer
	FormatProfile(&buf, s)
	out := buf.String()

	if !strings.Contains(out, "Optimizations:") {
		t.Error("missing Optimizations section")
	}
	if !strings.Contains(out, "partial aggregation") {
		t.Error("missing partial aggregation optimization")
	}
}

func TestFormatProfile_OptimizerRules(t *testing.T) {
	s := &QueryStats{
		TotalDuration:       10 * time.Millisecond,
		ExecDuration:        8 * time.Millisecond,
		OptimizerTotalRules: 28,
		OptimizerRules: []OptimizerRuleDetail{
			{Name: "PredicatePushdown", Description: "Pushes WHERE filters before aggregation", Count: 1},
			{Name: "BloomFilterPruning", Description: "Adds bloom terms for segment skip", Count: 2},
		},
	}

	var buf bytes.Buffer
	FormatProfile(&buf, s)
	out := buf.String()

	if !strings.Contains(out, "Optimizer") {
		t.Error("missing Optimizer section")
	}
	if !strings.Contains(out, "2/28") {
		t.Error("missing rule count 2/28")
	}
	if !strings.Contains(out, "PredicatePushdown") {
		t.Error("missing PredicatePushdown rule")
	}
}

func TestFormatProfile_Resources(t *testing.T) {
	s := &QueryStats{
		TotalDuration:   10 * time.Millisecond,
		ExecDuration:    8 * time.Millisecond,
		PeakMemoryBytes: 12 * 1024 * 1024,
		CPUTimeUser:     35 * time.Millisecond,
	}

	var buf bytes.Buffer
	FormatProfile(&buf, s)
	out := buf.String()

	if !strings.Contains(out, "Resources:") {
		t.Error("missing Resources section")
	}
	if !strings.Contains(out, "memory:") {
		t.Error("missing memory in resources")
	}
	if !strings.Contains(out, "CPU:") {
		t.Error("missing CPU in resources")
	}
}

func TestFormatProfile_Recommendations(t *testing.T) {
	s := &QueryStats{
		TotalDuration: 10 * time.Millisecond,
		ExecDuration:  8 * time.Millisecond,
		Recommendations: []Recommendation{
			{Category: "performance", Priority: "medium", Message: "Add --since to narrow scan"},
			{Category: "info", Priority: "low", Message: "First execution"},
		},
	}

	var buf bytes.Buffer
	FormatProfile(&buf, s)
	out := buf.String()

	if !strings.Contains(out, "Recommendations:") {
		t.Error("missing Recommendations section")
	}
	if !strings.Contains(out, "Add --since") {
		t.Error("missing first recommendation")
	}
	if !strings.Contains(out, "First execution") {
		t.Error("missing second recommendation")
	}
}

func TestFormatProfileJSON_Structure(t *testing.T) {
	s := &QueryStats{
		TotalDuration:        40 * time.Millisecond,
		ParseDuration:        200 * time.Microsecond,
		OptimizeDuration:     100 * time.Microsecond,
		ExecDuration:         39 * time.Millisecond,
		SerializeDuration:    700 * time.Microsecond,
		ScannedRows:          120_000,
		MatchedRows:          847,
		ResultRows:           10,
		TotalSegments:        45,
		ScannedSegments:      12,
		BloomSkippedSegments: 20,
		TimeSkippedSegments:  8,
		PeakMemoryBytes:      12 * 1024 * 1024,
		CPUTimeUser:          35 * time.Millisecond,
		Stages: []StageStats{
			{Name: "Scan", InputRows: 120_000, OutputRows: 120_000, Duration: 12 * time.Millisecond},
			{Name: "Filter", InputRows: 120_000, OutputRows: 847, Duration: 8 * time.Millisecond},
		},
		OptimizerRules: []OptimizerRuleDetail{
			{Name: "PredicatePushdown", Description: "Pushes WHERE before aggregation", Count: 1},
		},
		OptimizerTotalRules: 28,
		Recommendations: []Recommendation{
			{Category: "performance", Priority: "medium", Message: "Add --since"},
		},
	}

	var buf bytes.Buffer
	if err := FormatProfileJSON(&buf, s); err != nil {
		t.Fatalf("FormatProfileJSON error: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, buf.String())
	}

	profile, ok := result["profile"].(map[string]interface{})
	if !ok {
		t.Fatal("missing profile key in JSON output")
	}

	if profile["total_ms"] == nil {
		t.Error("missing total_ms")
	}
	if profile["scanned_rows"] == nil {
		t.Error("missing scanned_rows")
	}
	if profile["segments"] == nil {
		t.Error("missing segments")
	}
	if profile["pipeline"] == nil {
		t.Error("missing pipeline")
	}
	if profile["optimizer"] == nil {
		t.Error("missing optimizer")
	}
	if profile["recommendations"] == nil {
		t.Error("missing recommendations")
	}
	if profile["resources"] == nil {
		t.Error("missing resources")
	}
}

func TestFormatProfileJSON_EmptyStats(t *testing.T) {
	s := &QueryStats{
		TotalDuration: 5 * time.Millisecond,
		Ephemeral:     true,
	}

	var buf bytes.Buffer
	if err := FormatProfileJSON(&buf, s); err != nil {
		t.Fatalf("FormatProfileJSON error: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	profile := result["profile"].(map[string]interface{})

	// Should not have segments, pipeline, etc. when empty.
	if profile["segments"] != nil {
		t.Error("should not have segments when TotalSegments=0")
	}
	if profile["pipeline"] != nil {
		t.Error("should not have pipeline when no stages")
	}
}

func TestFormatProfile_OperatorBudgets(t *testing.T) {
	s := &QueryStats{
		TotalDuration: 50 * time.Millisecond,
		ExecDuration:  45 * time.Millisecond,
		OperatorBudgets: []OperatorBudgetStats{
			{Name: "sort", SoftLimit: 48 << 20, PeakBytes: 42 << 20, Phase: "complete", Spilled: false},
			{Name: "aggregate", SoftLimit: 48 << 20, PeakBytes: 11 << 20, Phase: "complete", Spilled: true},
		},
	}

	var buf bytes.Buffer
	FormatProfile(&buf, s)
	out := buf.String()

	if !strings.Contains(out, "Memory Budget:") {
		t.Error("missing Memory Budget section")
	}
	if !strings.Contains(out, "OPERATOR") {
		t.Error("missing OPERATOR column header")
	}
	if !strings.Contains(out, "BUDGET") {
		t.Error("missing BUDGET column header")
	}
	if !strings.Contains(out, "PEAK") {
		t.Error("missing PEAK column header")
	}
	if !strings.Contains(out, "SPILLED") {
		t.Error("missing SPILLED column header")
	}
	if !strings.Contains(out, "sort") {
		t.Error("missing sort operator in budget table")
	}
	if !strings.Contains(out, "aggregate") {
		t.Error("missing aggregate operator in budget table")
	}
	if !strings.Contains(out, "yes") {
		t.Error("missing 'yes' for spilled aggregate")
	}
}

func TestFormatProfileJSON_OperatorBudgets(t *testing.T) {
	s := &QueryStats{
		TotalDuration: 50 * time.Millisecond,
		ExecDuration:  45 * time.Millisecond,
		OperatorBudgets: []OperatorBudgetStats{
			{Name: "sort", SoftLimit: 48 << 20, PeakBytes: 42 << 20, Phase: "complete", Spilled: false},
			{Name: "aggregate", SoftLimit: 48 << 20, PeakBytes: 11 << 20, Phase: "complete", Spilled: true},
		},
	}

	var buf bytes.Buffer
	if err := FormatProfileJSON(&buf, s); err != nil {
		t.Fatalf("FormatProfileJSON error: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, buf.String())
	}

	profile := result["profile"].(map[string]interface{})
	budgets, ok := profile["budgets"]
	if !ok {
		t.Fatal("missing budgets key in JSON profile")
	}
	budgetList, ok := budgets.([]interface{})
	if !ok {
		t.Fatalf("budgets is not an array: %T", budgets)
	}
	if len(budgetList) != 2 {
		t.Fatalf("expected 2 budgets, got %d", len(budgetList))
	}

	first := budgetList[0].(map[string]interface{})
	if first["name"] != "sort" {
		t.Errorf("expected first budget name 'sort', got %v", first["name"])
	}
	if first["spilled"] != false {
		t.Errorf("expected sort spilled=false, got %v", first["spilled"])
	}

	second := budgetList[1].(map[string]interface{})
	if second["name"] != "aggregate" {
		t.Errorf("expected second budget name 'aggregate', got %v", second["name"])
	}
	if second["spilled"] != true {
		t.Errorf("expected aggregate spilled=true, got %v", second["spilled"])
	}
}

func TestFormatProfile_NoBudgetsWhenEmpty(t *testing.T) {
	s := &QueryStats{
		TotalDuration: 10 * time.Millisecond,
		ExecDuration:  8 * time.Millisecond,
	}

	var buf bytes.Buffer
	FormatProfile(&buf, s)
	out := buf.String()

	if strings.Contains(out, "Memory Budget:") {
		t.Error("should not show Memory Budget section when no budgets")
	}
}

func TestComputeExclusiveTiming(t *testing.T) {
	// Volcano pipeline: Scan (12ms) → Filter (8ms) → Stats (14ms)
	// In pull model: Scan inclusive 12ms, Filter inclusive = 8ms
	// (but Scan is not a child of Filter — in Volcano the pull chain is
	//  Stats calls Filter calls Scan, so the ordering in Stages is
	//  top-to-bottom = Scan, Filter, Stats where Stats is the root).
	// For this test, reorder as root→leaf: Stats(14ms) → Filter(8ms) → Scan(12ms)
	// Exclusive: Stats = 14-8=6, Filter = 8-12=-4→0, Scan = 12 (leaf)
	// But realistically, let's use a proper inclusive timing chain:
	// Root: Aggregate(30ms) → Filter(20ms) → Scan(15ms)
	// Exclusive: Aggregate = 30-20=10, Filter = 20-15=5, Scan = 15 (leaf)
	stages := []StageStats{
		{Name: "Aggregate", Duration: 30 * time.Millisecond},
		{Name: "Filter", Duration: 20 * time.Millisecond},
		{Name: "Scan", Duration: 15 * time.Millisecond},
	}
	ComputeExclusiveTiming(stages)

	if stages[0].ExclusiveNS != 10_000_000 {
		t.Errorf("Aggregate exclusive: expected 10ms (10000000ns), got %d", stages[0].ExclusiveNS)
	}
	if stages[1].ExclusiveNS != 5_000_000 {
		t.Errorf("Filter exclusive: expected 5ms (5000000ns), got %d", stages[1].ExclusiveNS)
	}
	if stages[2].ExclusiveNS != 15_000_000 {
		t.Errorf("Scan exclusive: expected 15ms (15000000ns), got %d", stages[2].ExclusiveNS)
	}
}

func TestComputeExclusiveTiming_Empty(t *testing.T) {
	// Should not panic on empty input.
	ComputeExclusiveTiming(nil)
	ComputeExclusiveTiming([]StageStats{})
}

func TestComputeExclusiveTiming_SingleStage(t *testing.T) {
	stages := []StageStats{
		{Name: "Scan", Duration: 10 * time.Millisecond},
	}
	ComputeExclusiveTiming(stages)

	if stages[0].ExclusiveNS != 10_000_000 {
		t.Errorf("Single stage exclusive: expected 10ms, got %d ns", stages[0].ExclusiveNS)
	}
}

func TestFormatProfile_PipelineExclusive(t *testing.T) {
	s := &QueryStats{
		TotalDuration: 40 * time.Millisecond,
		ExecDuration:  35 * time.Millisecond,
		ScannedRows:   100_000,
		ResultRows:    10,
		Stages: []StageStats{
			{Name: "Aggregate", InputRows: 847, OutputRows: 10, Duration: 30 * time.Millisecond},
			{Name: "Filter", InputRows: 100_000, OutputRows: 847, Duration: 20 * time.Millisecond},
			{Name: "Scan", InputRows: 100_000, OutputRows: 100_000, Duration: 15 * time.Millisecond},
		},
	}

	var buf bytes.Buffer
	FormatProfile(&buf, s)
	out := buf.String()

	// The pipeline table should contain a SELF column.
	if !strings.Contains(out, "SELF") {
		t.Error("missing SELF column header in pipeline breakdown")
	}
}

func TestFormatProfile_ServerPhases(t *testing.T) {
	// Server queries have scan/pipeline breakdown instead of exec.
	s := &QueryStats{
		TotalDuration:     100 * time.Millisecond,
		ParseDuration:     1 * time.Millisecond,
		OptimizeDuration:  500 * time.Microsecond,
		ScanDuration:      40 * time.Millisecond,
		PipelineDuration:  55 * time.Millisecond,
		SerializeDuration: 3 * time.Millisecond,
		ScannedRows:       500_000,
		ResultRows:        25,
	}

	var buf bytes.Buffer
	FormatProfile(&buf, s)
	out := buf.String()

	if !strings.Contains(out, "scan") {
		t.Error("missing scan phase in server mode")
	}
	if !strings.Contains(out, "pipeline") {
		t.Error("missing pipeline phase in server mode")
	}
	// Should NOT show "exec" when scan/pipeline are present.
	lines := strings.Split(out, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "exec") && strings.Contains(out, "Phase Breakdown") {
			t.Error("should not show 'exec' when scan/pipeline breakdown is available")
		}
	}
}
