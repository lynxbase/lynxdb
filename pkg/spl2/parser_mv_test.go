package spl2

import (
	"testing"
)

// Materialize tests

func TestParse_Materialize_Basic(t *testing.T) {
	q, err := Parse(`| search source=nginx | stats count by uri | materialize "mv_test"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	// Should have search + stats + materialize.
	var mc *MaterializeCommand
	for _, cmd := range q.Commands {
		if m, ok := cmd.(*MaterializeCommand); ok {
			mc = m
		}
	}
	if mc == nil {
		t.Fatal("expected MaterializeCommand in pipeline")
	}
	if mc.Name != "mv_test" {
		t.Errorf("Name: got %q, want %q", mc.Name, "mv_test")
	}
	if mc.Retention != "" {
		t.Errorf("Retention: got %q, want empty", mc.Retention)
	}
}

func TestParse_Materialize_WithRetention(t *testing.T) {
	q, err := Parse(`| stats count by uri | materialize "mv_test" retention=30d`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	var mc *MaterializeCommand
	for _, cmd := range q.Commands {
		if m, ok := cmd.(*MaterializeCommand); ok {
			mc = m
		}
	}
	if mc == nil {
		t.Fatal("expected MaterializeCommand")
	}
	if mc.Retention != "30d" {
		t.Errorf("Retention: got %q, want %q", mc.Retention, "30d")
	}
}

func TestParse_Materialize_WithPartitionBy(t *testing.T) {
	q, err := Parse(`| stats count by uri | materialize "mv_test" partition_by=date_field`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	var mc *MaterializeCommand
	for _, cmd := range q.Commands {
		if m, ok := cmd.(*MaterializeCommand); ok {
			mc = m
		}
	}
	if mc == nil {
		t.Fatal("expected MaterializeCommand")
	}
	if len(mc.PartitionBy) != 1 || mc.PartitionBy[0] != "date_field" {
		t.Errorf("PartitionBy: got %v, want [date_field]", mc.PartitionBy)
	}
}

func TestParse_Materialize_WithAllOptions(t *testing.T) {
	q, err := Parse(`| stats count by uri | materialize "mv_test" retention=90d partition_by=host,date`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	var mc *MaterializeCommand
	for _, cmd := range q.Commands {
		if m, ok := cmd.(*MaterializeCommand); ok {
			mc = m
		}
	}
	if mc == nil {
		t.Fatal("expected MaterializeCommand")
	}
	if mc.Retention != "90d" {
		t.Errorf("Retention: got %q, want %q", mc.Retention, "90d")
	}
	if len(mc.PartitionBy) != 2 || mc.PartitionBy[0] != "host" || mc.PartitionBy[1] != "date" {
		t.Errorf("PartitionBy: got %v, want [host date]", mc.PartitionBy)
	}
}

func TestParse_Materialize_MissingName(t *testing.T) {
	_, err := Parse(`| materialize`)
	if err == nil {
		t.Error("expected error for missing name")
	}
}

func TestParse_Materialize_Roundtrip(t *testing.T) {
	input := `| materialize "mv_test" retention=30d`
	q1, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	var mc *MaterializeCommand
	for _, cmd := range q1.Commands {
		if m, ok := cmd.(*MaterializeCommand); ok {
			mc = m
		}
	}
	if mc == nil {
		t.Fatal("expected MaterializeCommand")
	}

	// String() should produce parseable output.
	output := "| " + mc.String()
	q2, err := Parse(output)
	if err != nil {
		t.Fatalf("re-Parse %q: %v", output, err)
	}

	var mc2 *MaterializeCommand
	for _, cmd := range q2.Commands {
		if m, ok := cmd.(*MaterializeCommand); ok {
			mc2 = m
		}
	}
	if mc2 == nil {
		t.Fatal("expected MaterializeCommand in re-parsed output")
	}
	if mc2.Name != mc.Name {
		t.Errorf("roundtrip Name: got %q, want %q", mc2.Name, mc.Name)
	}
	if mc2.Retention != mc.Retention {
		t.Errorf("roundtrip Retention: got %q, want %q", mc2.Retention, mc.Retention)
	}
}

// From tests

func TestParse_From_Basic(t *testing.T) {
	q, err := Parse(`| from mv_errors`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if len(q.Commands) == 0 {
		t.Fatal("expected at least one command")
	}
	fc, ok := q.Commands[0].(*FromCommand)
	if !ok {
		t.Fatalf("expected FromCommand, got %T", q.Commands[0])
	}
	if fc.ViewName != "mv_errors" {
		t.Errorf("ViewName: got %q, want %q", fc.ViewName, "mv_errors")
	}
}

func TestParse_From_WithPipe(t *testing.T) {
	q, err := Parse(`| from mv_errors | where source="nginx" | head 10`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if len(q.Commands) != 3 {
		t.Fatalf("expected 3 commands, got %d", len(q.Commands))
	}

	fc, ok := q.Commands[0].(*FromCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected FromCommand, got %T", q.Commands[0])
	}
	if fc.ViewName != "mv_errors" {
		t.Errorf("ViewName: got %q, want %q", fc.ViewName, "mv_errors")
	}

	if _, ok := q.Commands[1].(*WhereCommand); !ok {
		t.Errorf("cmd[1]: expected WhereCommand, got %T", q.Commands[1])
	}
	if _, ok := q.Commands[2].(*HeadCommand); !ok {
		t.Errorf("cmd[2]: expected HeadCommand, got %T", q.Commands[2])
	}
}

func TestParse_From_MissingName(t *testing.T) {
	_, err := Parse(`| from`)
	if err == nil {
		t.Error("expected error for missing view name")
	}
}

func TestParse_From_Roundtrip(t *testing.T) {
	q1, err := Parse(`| from mv_test`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	fc := q1.Commands[0].(*FromCommand)

	output := "| " + fc.String()
	q2, err := Parse(output)
	if err != nil {
		t.Fatalf("re-Parse %q: %v", output, err)
	}
	fc2 := q2.Commands[0].(*FromCommand)
	if fc2.ViewName != fc.ViewName {
		t.Errorf("roundtrip ViewName: got %q, want %q", fc2.ViewName, fc.ViewName)
	}
}

// Views tests

func TestParse_Views_ListAll(t *testing.T) {
	q, err := Parse(`| views`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 1 {
		t.Fatalf("expected 1 command, got %d", len(q.Commands))
	}
	vc, ok := q.Commands[0].(*ViewsCommand)
	if !ok {
		t.Fatalf("expected ViewsCommand, got %T", q.Commands[0])
	}
	if vc.Name != "" {
		t.Errorf("Name: got %q, want empty", vc.Name)
	}
}

func TestParse_Views_ByName(t *testing.T) {
	q, err := Parse(`| views "mv_errors"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	vc := q.Commands[0].(*ViewsCommand)
	if vc.Name != "mv_errors" {
		t.Errorf("Name: got %q, want %q", vc.Name, "mv_errors")
	}
}

func TestParse_Views_AlterRetention(t *testing.T) {
	q, err := Parse(`| views "mv_errors" retention=30d`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	vc := q.Commands[0].(*ViewsCommand)
	if vc.Name != "mv_errors" {
		t.Errorf("Name: got %q, want %q", vc.Name, "mv_errors")
	}
	if vc.Retention != "30d" {
		t.Errorf("Retention: got %q, want %q", vc.Retention, "30d")
	}
}

// Dropview tests

func TestParse_Dropview_Basic(t *testing.T) {
	q, err := Parse(`| dropview "mv_errors"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	dc, ok := q.Commands[0].(*DropviewCommand)
	if !ok {
		t.Fatalf("expected DropviewCommand, got %T", q.Commands[0])
	}
	if dc.Name != "mv_errors" {
		t.Errorf("Name: got %q, want %q", dc.Name, "mv_errors")
	}
}

func TestParse_Dropview_MissingName(t *testing.T) {
	_, err := Parse(`| dropview`)
	if err == nil {
		t.Error("expected error for missing name")
	}
}

// Full pipeline test

func TestParse_FullPipeline_FromWithCommands(t *testing.T) {
	q, err := Parse(`| from mv_test | where x="y" | head 10`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if len(q.Commands) != 3 {
		t.Fatalf("expected 3 commands, got %d", len(q.Commands))
	}

	if _, ok := q.Commands[0].(*FromCommand); !ok {
		t.Errorf("cmd[0]: expected FromCommand, got %T", q.Commands[0])
	}
	if _, ok := q.Commands[1].(*WhereCommand); !ok {
		t.Errorf("cmd[1]: expected WhereCommand, got %T", q.Commands[1])
	}
	if _, ok := q.Commands[2].(*HeadCommand); !ok {
		t.Errorf("cmd[2]: expected HeadCommand, got %T", q.Commands[2])
	}
}
