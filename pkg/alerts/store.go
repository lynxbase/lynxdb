package alerts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const alertsFile = "alerts.json"

// AlertStore manages alert definitions with optional persistent storage.
type AlertStore struct {
	mu        sync.RWMutex
	persistMu sync.Mutex        // serializes disk writes (separate from data lock)
	alerts    map[string]*Alert // keyed by ID
	dir       string            // empty = in-memory only
}

// OpenStore loads or creates an alert store in the given directory.
func OpenStore(dir string) (*AlertStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("alerts: mkdir %s: %w", dir, err)
	}

	s := &AlertStore{
		alerts: make(map[string]*Alert),
		dir:    dir,
	}

	path := filepath.Join(dir, alertsFile)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}

		return nil, fmt.Errorf("alerts: read %s: %w", path, err)
	}

	var list []*Alert
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("alerts: unmarshal %s: %w", path, err)
	}
	for _, a := range list {
		s.alerts[a.ID] = a
	}

	return s, nil
}

// OpenInMemory creates an in-memory alert store (no disk persistence).
func OpenInMemory() *AlertStore {
	return &AlertStore{alerts: make(map[string]*Alert)}
}

// List returns all alerts sorted by name.
func (s *AlertStore) List() []Alert {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]Alert, 0, len(s.alerts))
	for _, a := range s.alerts {
		out = append(out, *a)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})

	return out
}

func (s *AlertStore) Get(id string) (*Alert, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	a, ok := s.alerts[id]
	if !ok {
		return nil, ErrAlertNotFound
	}
	cp := *a

	return &cp, nil
}

// Create adds a new alert. Returns ErrAlertAlreadyExists if the name is taken.
func (s *AlertStore) Create(alert *Alert) error {
	s.mu.Lock()

	for _, existing := range s.alerts {
		if existing.Name == alert.Name {
			s.mu.Unlock()

			return ErrAlertAlreadyExists
		}
	}

	cp := *alert
	s.alerts[alert.ID] = &cp
	data, err := s.snapshotLocked()
	s.mu.Unlock()
	if err != nil {
		return err
	}

	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	return s.persist(data)
}

// Update fully replaces an alert by ID.
func (s *AlertStore) Update(alert *Alert) error {
	s.mu.Lock()

	if _, ok := s.alerts[alert.ID]; !ok {
		s.mu.Unlock()

		return ErrAlertNotFound
	}

	cp := *alert
	s.alerts[alert.ID] = &cp
	data, err := s.snapshotLocked()
	s.mu.Unlock()
	if err != nil {
		return err
	}

	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	return s.persist(data)
}

func (s *AlertStore) Delete(id string) error {
	s.mu.Lock()

	if _, ok := s.alerts[id]; !ok {
		s.mu.Unlock()

		return ErrAlertNotFound
	}

	delete(s.alerts, id)
	data, err := s.snapshotLocked()
	s.mu.Unlock()
	if err != nil {
		return err
	}

	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	return s.persist(data)
}

// UpdateStatus updates only the status fields of an alert.
func (s *AlertStore) UpdateStatus(id string, status AlertStatus, lastChecked time.Time, lastTriggered *time.Time) error {
	s.mu.Lock()

	a, ok := s.alerts[id]
	if !ok {
		s.mu.Unlock()

		return ErrAlertNotFound
	}

	a.Status = status
	a.LastChecked = &lastChecked
	if lastTriggered != nil {
		a.LastTriggered = lastTriggered
	}
	data, err := s.snapshotLocked()
	s.mu.Unlock()
	if err != nil {
		return err
	}

	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	return s.persist(data)
}

// snapshotLocked marshals the current alerts to JSON. Caller must hold s.mu.
func (s *AlertStore) snapshotLocked() ([]byte, error) {
	if s.dir == "" {
		return nil, nil // in-memory mode
	}

	list := make([]*Alert, 0, len(s.alerts))
	for _, a := range s.alerts {
		list = append(list, a)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})

	data, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("alerts: marshal: %w", err)
	}

	return data, nil
}

// persist writes pre-marshaled data to disk atomically. Called WITHOUT the lock held.
func (s *AlertStore) persist(data []byte) error {
	if data == nil {
		return nil // in-memory mode
	}

	path := filepath.Join(s.dir, alertsFile)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("alerts: write tmp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("alerts: rename: %w", err)
	}

	return nil
}
