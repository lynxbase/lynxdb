package dashboards

import (
	"errors"
	"os"
	"strings"
	"testing"
)

func validInput() DashboardInput {
	return DashboardInput{
		Name: "Test Dashboard",
		Panels: []Panel{
			{ID: "p1", Title: "Error Rate", Type: "timechart", Q: "FROM main | stats count", Position: PanelPosition{X: 0, Y: 0, W: 6, H: 4}},
		},
	}
}

func TestStore_CreateAndGet(t *testing.T) {
	s := OpenInMemory()
	input := validInput()
	dash := input.ToDashboard()
	if err := s.Create(dash); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(dash.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "Test Dashboard" {
		t.Fatalf("name: %q", got.Name)
	}
	if len(got.Panels) != 1 {
		t.Fatalf("panels: %d", len(got.Panels))
	}
}

func TestStore_CreateDuplicateName(t *testing.T) {
	s := OpenInMemory()
	d1 := validInput().ToDashboard()
	d2 := validInput().ToDashboard()
	s.Create(d1)
	if err := s.Create(d2); !errors.Is(err, ErrDashboardAlreadyExists) {
		t.Fatalf("expected ErrDashboardAlreadyExists, got %v", err)
	}
}

func TestStore_List(t *testing.T) {
	s := OpenInMemory()
	for _, name := range []string{"Charlie", "Alpha", "Bravo"} {
		input := validInput()
		input.Name = name
		s.Create(input.ToDashboard())
	}
	list := s.List()
	if len(list) != 3 {
		t.Fatalf("len: %d", len(list))
	}
	if list[0].Name != "Alpha" || list[1].Name != "Bravo" || list[2].Name != "Charlie" {
		t.Fatalf("order: %v %v %v", list[0].Name, list[1].Name, list[2].Name)
	}
}

func TestStore_ListEmpty(t *testing.T) {
	s := OpenInMemory()
	list := s.List()
	if list == nil {
		t.Fatal("expected empty slice, not nil")
	}
	if len(list) != 0 {
		t.Fatalf("len: %d", len(list))
	}
}

func TestStore_Update(t *testing.T) {
	s := OpenInMemory()
	dash := validInput().ToDashboard()
	s.Create(dash)
	dash.Panels = append(dash.Panels, Panel{ID: "p2", Title: "New", Type: "table", Q: "q", Position: PanelPosition{X: 6, Y: 0, W: 6, H: 4}})
	if err := s.Update(dash); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(dash.ID)
	if len(got.Panels) != 2 {
		t.Fatalf("panels: %d", len(got.Panels))
	}
}

func TestStore_UpdateNotFound(t *testing.T) {
	s := OpenInMemory()
	dash := &Dashboard{ID: "dsh_nonexistent", Name: "x"}
	if err := s.Update(dash); !errors.Is(err, ErrDashboardNotFound) {
		t.Fatalf("expected ErrDashboardNotFound, got %v", err)
	}
}

func TestStore_Delete(t *testing.T) {
	s := OpenInMemory()
	dash := validInput().ToDashboard()
	s.Create(dash)
	if err := s.Delete(dash.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(dash.ID); !errors.Is(err, ErrDashboardNotFound) {
		t.Fatal("expected not found")
	}
}

func TestStore_DeleteNotFound(t *testing.T) {
	s := OpenInMemory()
	if err := s.Delete("dsh_nope"); !errors.Is(err, ErrDashboardNotFound) {
		t.Fatalf("expected ErrDashboardNotFound, got %v", err)
	}
}

func TestStore_Persistence(t *testing.T) {
	dir := t.TempDir()
	s1, _ := OpenStore(dir)
	dash := validInput().ToDashboard()
	s1.Create(dash)

	s2, _ := OpenStore(dir)
	got, err := s2.Get(dash.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "Test Dashboard" {
		t.Fatalf("name: %q", got.Name)
	}
}

func TestStore_InMemoryNoPersist(t *testing.T) {
	dir := t.TempDir()
	s := OpenInMemory()
	s.Create(validInput().ToDashboard())
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatalf("expected no files, got %d", len(entries))
	}
}

func TestValidate_EmptyName(t *testing.T) {
	input := validInput()
	input.Name = ""
	if err := input.Validate(); !errors.Is(err, ErrDashboardNameEmpty) {
		t.Fatalf("expected ErrDashboardNameEmpty, got %v", err)
	}
}

func TestValidate_NoPanels(t *testing.T) {
	input := DashboardInput{Name: "x", Panels: []Panel{}}
	if err := input.Validate(); !errors.Is(err, ErrNoPanels) {
		t.Fatalf("expected ErrNoPanels, got %v", err)
	}
}

func TestValidate_TooManyPanels(t *testing.T) {
	input := validInput()
	input.Panels = make([]Panel, 51)
	for i := range input.Panels {
		input.Panels[i] = Panel{ID: strings.Repeat("a", 1) + string(rune('A'+i%26)) + string(rune('0'+i/26)), Title: "t", Type: "table", Q: "q", Position: PanelPosition{W: 6, H: 4}}
	}
	if err := input.Validate(); !errors.Is(err, ErrTooManyPanels) {
		t.Fatalf("expected ErrTooManyPanels, got %v", err)
	}
}

func TestValidate_InvalidPanelType(t *testing.T) {
	input := validInput()
	input.Panels[0].Type = "invalid"
	if err := input.Validate(); !errors.Is(err, ErrInvalidPanelType) {
		t.Fatalf("expected ErrInvalidPanelType, got %v", err)
	}
}

func TestValidate_InvalidPosition(t *testing.T) {
	tests := []struct {
		name string
		pos  PanelPosition
	}{
		{"w=0", PanelPosition{W: 0, H: 4}},
		{"w=13", PanelPosition{W: 13, H: 4}},
		{"h=0", PanelPosition{W: 6, H: 0}},
		{"h=21", PanelPosition{W: 6, H: 21}},
		{"x=-1", PanelPosition{X: -1, W: 6, H: 4}},
		{"x=12", PanelPosition{X: 12, W: 6, H: 4}},
		{"y=-1", PanelPosition{X: 0, Y: -1, W: 6, H: 4}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := validInput()
			input.Panels[0].Position = tt.pos
			if err := input.Validate(); !errors.Is(err, ErrInvalidPanelPosition) {
				t.Fatalf("expected ErrInvalidPanelPosition, got %v", err)
			}
		})
	}
}

func TestValidate_DuplicatePanelID(t *testing.T) {
	input := validInput()
	input.Panels = append(input.Panels, Panel{ID: "p1", Title: "dup", Type: "table", Q: "q", Position: PanelPosition{W: 6, H: 4}})
	if err := input.Validate(); !errors.Is(err, ErrPanelIDDuplicate) {
		t.Fatalf("expected ErrPanelIDDuplicate, got %v", err)
	}
}

func TestValidate_EmptyPanelFields(t *testing.T) {
	t.Run("empty id", func(t *testing.T) {
		input := validInput()
		input.Panels[0].ID = ""
		if err := input.Validate(); !errors.Is(err, ErrPanelIDEmpty) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("empty title", func(t *testing.T) {
		input := validInput()
		input.Panels[0].Title = ""
		if err := input.Validate(); !errors.Is(err, ErrPanelTitleEmpty) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("empty q", func(t *testing.T) {
		input := validInput()
		input.Panels[0].Q = ""
		if err := input.Validate(); !errors.Is(err, ErrPanelQueryEmpty) {
			t.Fatalf("got %v", err)
		}
	})
}

func TestValidate_InvalidVariableType(t *testing.T) {
	input := validInput()
	input.Variables = []DashboardVariable{{Name: "v", Type: "unknown"}}
	if err := input.Validate(); !errors.Is(err, ErrInvalidVariableType) {
		t.Fatalf("expected ErrInvalidVariableType, got %v", err)
	}
}

func TestValidate_TooManyVariables(t *testing.T) {
	input := validInput()
	input.Variables = make([]DashboardVariable, 21)
	for i := range input.Variables {
		input.Variables[i] = DashboardVariable{Name: "v", Type: "custom"}
	}
	if err := input.Validate(); !errors.Is(err, ErrTooManyVariables) {
		t.Fatalf("expected ErrTooManyVariables, got %v", err)
	}
}

func TestValidate_ValidDashboard(t *testing.T) {
	input := validInput()
	input.Variables = []DashboardVariable{{Name: "env", Type: "field_values", Field: "env"}}
	if err := input.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestGenerateID(t *testing.T) {
	id := generateDashboardID()
	if !strings.HasPrefix(id, "dsh_") {
		t.Fatalf("id: %q", id)
	}
	if len(id) != 20 { // "dsh_" + 16 hex chars
		t.Fatalf("id length: %d", len(id))
	}
}
