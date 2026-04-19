package alerts

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
)

// Manager owns the alert store, scheduler, and dispatcher.
type Manager struct {
	store      *AlertStore
	scheduler  *Scheduler
	dispatcher *Dispatcher
	queryFn    QueryFunc
	logger     *slog.Logger
}

// NewManager creates a Manager. If dataDir is empty, alerts are in-memory only.
func NewManager(dataDir string, factory NotifierFactory, queryFn QueryFunc, logger *slog.Logger) (*Manager, error) {
	var store *AlertStore
	var err error
	if dataDir != "" {
		store, err = OpenStore(filepath.Join(dataDir, "alerts"))
		if err != nil {
			return nil, err
		}
	} else {
		store = OpenInMemory()
	}

	dispatcher := NewDispatcher(factory, logger)
	scheduler := NewScheduler(store, dispatcher, queryFn, logger)

	return &Manager{
		store:      store,
		scheduler:  scheduler,
		dispatcher: dispatcher,
		queryFn:    queryFn,
		logger:     logger,
	}, nil
}

// Start starts the alert scheduler.
func (m *Manager) Start(ctx context.Context) {
	m.scheduler.Start(ctx)
}

// Stop stops the alert scheduler.
func (m *Manager) Stop() {
	m.scheduler.Stop()
}

// List returns all alerts.
func (m *Manager) List() []Alert {
	return m.store.List()
}

// Get returns an alert by ID.
func (m *Manager) Get(id string) (*Alert, error) {
	return m.store.Get(id)
}

// Create validates input, persists the alert, and starts its scheduler.
func (m *Manager) Create(input *AlertInput) (*Alert, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	alert := input.ToAlert()
	if err := m.store.Create(alert); err != nil {
		return nil, err
	}
	m.scheduler.ScheduleAlert(*alert)

	return alert, nil
}

// Update validates input, replaces the alert, and reschedules it.
func (m *Manager) Update(id string, input *AlertInput) (*Alert, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}

	existing, err := m.store.Get(id)
	if err != nil {
		return nil, err
	}

	enabled := true
	if input.Enabled != nil {
		enabled = *input.Enabled
	}

	existing.Name = input.Name
	existing.Query = input.Query
	existing.Interval = input.Interval
	existing.Channels = input.Channels
	existing.Enabled = enabled

	if err := m.store.Update(existing); err != nil {
		return nil, err
	}

	m.scheduler.UnscheduleAlert(id)
	m.scheduler.ScheduleAlert(*existing)

	return existing, nil
}

// PatchEnabled updates only the enabled state of an existing alert.
func (m *Manager) PatchEnabled(id string, enabled bool) (*Alert, error) {
	existing, err := m.store.Get(id)
	if err != nil {
		return nil, err
	}

	existing.Enabled = enabled
	if err := m.store.Update(existing); err != nil {
		return nil, err
	}

	m.scheduler.UnscheduleAlert(id)
	m.scheduler.ScheduleAlert(*existing)

	return existing, nil
}

// Delete removes an alert and stops its scheduler.
func (m *Manager) Delete(id string) error {
	m.scheduler.UnscheduleAlert(id)

	return m.store.Delete(id)
}

// TestChannels sends test notifications to all enabled channels.
func (m *Manager) TestChannels(ctx context.Context, id string) ([]ChannelResult, error) {
	alert, err := m.store.Get(id)
	if err != nil {
		return nil, err
	}

	return m.dispatcher.TestChannels(ctx, *alert), nil
}

// TestAlert executes the alert query without sending notifications (dry run).
func (m *Manager) TestAlert(ctx context.Context, id string) (map[string]interface{}, error) {
	alert, err := m.store.Get(id)
	if err != nil {
		return nil, err
	}

	rows, queryErr := m.queryFn(ctx, alert.Query)
	wouldTrigger := queryErr == nil && len(rows) > 0

	var channelNames []string
	for _, ch := range alert.Channels {
		if ch.IsEnabled() {
			channelNames = append(channelNames, ch.Name)
		}
	}

	result := map[string]interface{}{
		"would_trigger":            wouldTrigger,
		"channels_that_would_fire": channelNames,
	}
	if queryErr != nil {
		result["error"] = queryErr.Error()
	} else {
		result["result"] = map[string]interface{}{
			"rows":  rows,
			"count": len(rows),
		}
		var msg string
		if wouldTrigger {
			msg = fmt.Sprintf("Alert %q would trigger — query returned %d rows", alert.Name, len(rows))
		} else {
			msg = fmt.Sprintf("Alert %q would NOT trigger — query returned 0 rows", alert.Name)
		}
		result["message"] = msg
	}

	return result, nil
}
