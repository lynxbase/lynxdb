package views

import (
	"errors"
	"testing"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
)

func validDef() ViewDefinition {
	return ViewDefinition{
		Name:    "mv_test",
		Version: 1,
		Type:    ViewTypeProjection,
		Query:   "source=nginx | fields _time, uri, status",
		Columns: []ColumnDef{
			{Name: "_time", Type: event.FieldTypeTimestamp},
			{Name: "uri", Type: event.FieldTypeString},
			{Name: "status", Type: event.FieldTypeInt},
		},
		Status:    ViewStatusCreating,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}

func TestViewDefinition_Validate(t *testing.T) {
	// Valid definition passes.
	d := validDef()
	if err := d.Validate(); err != nil {
		t.Fatalf("valid def should pass: %v", err)
	}

	// Empty name fails.
	d2 := validDef()
	d2.Name = ""
	if err := d2.Validate(); !errors.Is(err, ErrViewNameEmpty) {
		t.Errorf("empty name: got %v, want %v", err, ErrViewNameEmpty)
	}

	// Invalid chars fail.
	d3 := validDef()
	d3.Name = "mv test!@#"
	if err := d3.Validate(); !errors.Is(err, ErrViewNameInvalid) {
		t.Errorf("invalid name: got %v, want %v", err, ErrViewNameInvalid)
	}

	// Empty columns fail.
	d4 := validDef()
	d4.Columns = nil
	if err := d4.Validate(); !errors.Is(err, ErrNoColumns) {
		t.Errorf("empty columns: got %v, want %v", err, ErrNoColumns)
	}
}

func TestViewDefinition_Validate_Retention(t *testing.T) {
	// Zero retention is OK.
	d := validDef()
	d.Retention = 0
	if err := d.Validate(); err != nil {
		t.Fatalf("zero retention should pass: %v", err)
	}

	// Positive retention is OK.
	d.Retention = 30 * 24 * time.Hour
	if err := d.Validate(); err != nil {
		t.Fatalf("positive retention should pass: %v", err)
	}

	// Negative retention fails.
	d.Retention = -1 * time.Hour
	if err := d.Validate(); !errors.Is(err, ErrInvalidRetention) {
		t.Errorf("negative retention: got %v, want %v", err, ErrInvalidRetention)
	}
}
