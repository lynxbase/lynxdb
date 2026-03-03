package dashboards

import "errors"

var (
	ErrDashboardNotFound      = errors.New("dashboard not found")
	ErrDashboardNameEmpty     = errors.New("dashboard name is required")
	ErrDashboardNameTooLong   = errors.New("dashboard name exceeds 200 characters")
	ErrNoPanels               = errors.New("at least one panel is required")
	ErrTooManyPanels          = errors.New("dashboard exceeds 50 panels")
	ErrTooManyVariables       = errors.New("dashboard exceeds 20 variables")
	ErrInvalidPanelType       = errors.New("invalid panel type")
	ErrInvalidVariableType    = errors.New("invalid variable type")
	ErrPanelIDEmpty           = errors.New("panel id is required")
	ErrPanelIDDuplicate       = errors.New("duplicate panel id")
	ErrPanelTitleEmpty        = errors.New("panel title is required")
	ErrPanelQueryEmpty        = errors.New("panel query is required")
	ErrInvalidPanelPosition   = errors.New("invalid panel position")
	ErrDashboardAlreadyExists = errors.New("dashboard with this name already exists")
)
