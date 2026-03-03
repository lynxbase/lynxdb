package dashboards

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

var validPanelTypes = map[string]bool{
	"timechart": true, "table": true, "bar": true, "line": true,
	"area": true, "stat": true, "pie": true,
}

var validVariableTypes = map[string]bool{
	"field_values": true, "custom": true,
}

type PanelPosition struct {
	X int `json:"x"`
	Y int `json:"y"`
	W int `json:"w"`
	H int `json:"h"`
}

type Panel struct {
	ID       string        `json:"id"`
	Title    string        `json:"title"`
	Type     string        `json:"type"`
	Q        string        `json:"q"`
	From     string        `json:"from,omitempty"`
	Position PanelPosition `json:"position"`
}

type DashboardVariable struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Field   string `json:"field"`
	Default string `json:"default,omitempty"`
	Label   string `json:"label,omitempty"`
}

type Dashboard struct {
	ID        string              `json:"id"`
	Name      string              `json:"name"`
	Panels    []Panel             `json:"panels"`
	Variables []DashboardVariable `json:"variables,omitempty"`
	CreatedAt time.Time           `json:"created_at"`
	UpdatedAt time.Time           `json:"updated_at"`
}

type DashboardInput struct {
	Name      string              `json:"name"`
	Panels    []Panel             `json:"panels"`
	Variables []DashboardVariable `json:"variables,omitempty"`
}

func (i DashboardInput) Validate() error {
	if i.Name == "" {
		return ErrDashboardNameEmpty
	}
	if len(i.Name) > 200 {
		return ErrDashboardNameTooLong
	}
	if len(i.Panels) == 0 {
		return ErrNoPanels
	}
	if len(i.Panels) > 50 {
		return ErrTooManyPanels
	}
	if len(i.Variables) > 20 {
		return ErrTooManyVariables
	}
	panelIDs := make(map[string]bool, len(i.Panels))
	for _, p := range i.Panels {
		if p.ID == "" {
			return ErrPanelIDEmpty
		}
		if panelIDs[p.ID] {
			return ErrPanelIDDuplicate
		}
		panelIDs[p.ID] = true
		if p.Title == "" {
			return ErrPanelTitleEmpty
		}
		if p.Q == "" {
			return ErrPanelQueryEmpty
		}
		if !validPanelTypes[p.Type] {
			return ErrInvalidPanelType
		}
		if p.Position.W < 1 || p.Position.W > 12 {
			return ErrInvalidPanelPosition
		}
		if p.Position.H < 1 || p.Position.H > 20 {
			return ErrInvalidPanelPosition
		}
		if p.Position.X < 0 || p.Position.X > 11 {
			return ErrInvalidPanelPosition
		}
		if p.Position.Y < 0 {
			return ErrInvalidPanelPosition
		}
	}
	for _, v := range i.Variables {
		if !validVariableTypes[v.Type] {
			return ErrInvalidVariableType
		}
	}

	return nil
}

func (i DashboardInput) ToDashboard() *Dashboard {
	now := time.Now()

	return &Dashboard{
		ID:        generateDashboardID(),
		Name:      i.Name,
		Panels:    i.Panels,
		Variables: i.Variables,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func generateDashboardID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)

	return "dsh_" + hex.EncodeToString(b)
}
