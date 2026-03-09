package meta

import (
	"testing"
)

func TestApplyUpdateFieldCatalog_SingleNode(t *testing.T) {
	fsm := NewMetaFSM(testLogger())

	resp := applyCommand(t, fsm, CmdUpdateFieldCatalog, UpdateFieldCatalogPayload{
		NodeID: "ingest-1",
		Fields: []FieldDelta{
			{
				Name:  "level",
				Type:  "string",
				Count: 1000,
				TopValues: []FieldValueEntry{
					{Value: "INFO", Count: 700},
					{Value: "ERROR", Count: 200},
					{Value: "WARN", Count: 100},
				},
			},
			{
				Name:  "status",
				Type:  "int",
				Count: 500,
			},
		},
	}, 1)

	if resp != nil {
		t.Fatalf("expected nil response, got: %v", resp)
	}

	state := fsm.State()
	if len(state.FieldCatalog) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(state.FieldCatalog))
	}

	level := state.FieldCatalog["level"]
	if level == nil {
		t.Fatal("expected level field")
	}
	if level.TotalCount != 1000 {
		t.Errorf("expected total count 1000, got %d", level.TotalCount)
	}
	if level.Type != "string" {
		t.Errorf("expected type string, got %s", level.Type)
	}
	if len(level.TopValues) != 3 {
		t.Errorf("expected 3 top values, got %d", len(level.TopValues))
	}
	if level.NodeCounts["ingest-1"] != 1000 {
		t.Errorf("expected node count 1000, got %d", level.NodeCounts["ingest-1"])
	}

	status := state.FieldCatalog["status"]
	if status == nil {
		t.Fatal("expected status field")
	}
	if status.TotalCount != 500 {
		t.Errorf("expected total count 500, got %d", status.TotalCount)
	}
}

func TestApplyUpdateFieldCatalog_MultiNode_Merge(t *testing.T) {
	fsm := NewMetaFSM(testLogger())

	// Node 1 reports.
	applyCommand(t, fsm, CmdUpdateFieldCatalog, UpdateFieldCatalogPayload{
		NodeID: "ingest-1",
		Fields: []FieldDelta{
			{
				Name:  "level",
				Type:  "string",
				Count: 1000,
				TopValues: []FieldValueEntry{
					{Value: "INFO", Count: 700},
					{Value: "ERROR", Count: 300},
				},
			},
		},
	}, 1)

	// Node 2 reports.
	applyCommand(t, fsm, CmdUpdateFieldCatalog, UpdateFieldCatalogPayload{
		NodeID: "ingest-2",
		Fields: []FieldDelta{
			{
				Name:  "level",
				Type:  "string",
				Count: 2000,
				TopValues: []FieldValueEntry{
					{Value: "INFO", Count: 1500},
					{Value: "WARN", Count: 500},
				},
			},
		},
	}, 2)

	state := fsm.State()
	level := state.FieldCatalog["level"]
	if level == nil {
		t.Fatal("expected level field")
	}

	// Total should be sum of both nodes.
	if level.TotalCount != 3000 {
		t.Errorf("expected total count 3000, got %d", level.TotalCount)
	}

	// Both nodes should be tracked.
	if level.NodeCounts["ingest-1"] != 1000 {
		t.Errorf("expected ingest-1 count 1000, got %d", level.NodeCounts["ingest-1"])
	}
	if level.NodeCounts["ingest-2"] != 2000 {
		t.Errorf("expected ingest-2 count 2000, got %d", level.NodeCounts["ingest-2"])
	}

	// Top values should be merged: INFO=2200, WARN=500, ERROR=300.
	if len(level.TopValues) != 3 {
		t.Fatalf("expected 3 top values, got %d", len(level.TopValues))
	}
	if level.TopValues[0].Value != "INFO" || level.TopValues[0].Count != 2200 {
		t.Errorf("expected top value INFO=2200, got %s=%d", level.TopValues[0].Value, level.TopValues[0].Count)
	}
}

func TestApplyUpdateFieldCatalog_TopValuesLimit(t *testing.T) {
	fsm := NewMetaFSM(testLogger())

	// Report 15 unique values — should be trimmed to top 10.
	topValues := make([]FieldValueEntry, 15)
	for i := range topValues {
		topValues[i] = FieldValueEntry{
			Value: string(rune('A' + i)),
			Count: int64(15 - i),
		}
	}

	applyCommand(t, fsm, CmdUpdateFieldCatalog, UpdateFieldCatalogPayload{
		NodeID: "ingest-1",
		Fields: []FieldDelta{
			{
				Name:      "tag",
				Type:      "string",
				Count:     100,
				TopValues: topValues,
			},
		},
	}, 1)

	state := fsm.State()
	tag := state.FieldCatalog["tag"]
	if len(tag.TopValues) != 10 {
		t.Errorf("expected 10 top values, got %d", len(tag.TopValues))
	}
}

func TestMergeTopValues(t *testing.T) {
	existing := []FieldValueEntry{
		{Value: "a", Count: 10},
		{Value: "b", Count: 5},
	}
	incoming := []FieldValueEntry{
		{Value: "a", Count: 3},
		{Value: "c", Count: 8},
	}

	merged := mergeTopValues(existing, incoming, 3)
	if len(merged) != 3 {
		t.Fatalf("expected 3 values, got %d", len(merged))
	}

	// Should be: a=13, c=8, b=5 (sorted by count desc).
	if merged[0].Value != "a" || merged[0].Count != 13 {
		t.Errorf("expected a=13, got %s=%d", merged[0].Value, merged[0].Count)
	}
	if merged[1].Value != "c" || merged[1].Count != 8 {
		t.Errorf("expected c=8, got %s=%d", merged[1].Value, merged[1].Count)
	}
	if merged[2].Value != "b" || merged[2].Count != 5 {
		t.Errorf("expected b=5, got %s=%d", merged[2].Value, merged[2].Count)
	}
}
