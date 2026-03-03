package dashboards

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const dashboardsFile = "dashboards.json"

type DashboardStore struct {
	mu         sync.RWMutex
	dashboards map[string]*Dashboard
	dir        string
}

func OpenStore(dir string) (*DashboardStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("dashboards: mkdir %s: %w", dir, err)
	}
	s := &DashboardStore{dashboards: make(map[string]*Dashboard), dir: dir}
	path := filepath.Join(dir, dashboardsFile)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}

		return nil, fmt.Errorf("dashboards: read %s: %w", path, err)
	}
	var list []*Dashboard
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("dashboards: unmarshal: %w", err)
	}
	for _, d := range list {
		s.dashboards[d.ID] = d
	}

	return s, nil
}

func OpenInMemory() *DashboardStore {
	return &DashboardStore{dashboards: make(map[string]*Dashboard)}
}

func (s *DashboardStore) List() []Dashboard {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Dashboard, 0, len(s.dashboards))
	for _, d := range s.dashboards {
		out = append(out, *d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

func (s *DashboardStore) Get(id string) (*Dashboard, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.dashboards[id]
	if !ok {
		return nil, ErrDashboardNotFound
	}
	cp := *d

	return &cp, nil
}

func (s *DashboardStore) Create(dash *Dashboard) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.dashboards {
		if existing.Name == dash.Name {
			return ErrDashboardAlreadyExists
		}
	}
	cp := *dash
	s.dashboards[dash.ID] = &cp

	return s.persistLocked()
}

func (s *DashboardStore) Update(dash *Dashboard) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.dashboards[dash.ID]; !ok {
		return ErrDashboardNotFound
	}
	cp := *dash
	s.dashboards[dash.ID] = &cp

	return s.persistLocked()
}

func (s *DashboardStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.dashboards[id]; !ok {
		return ErrDashboardNotFound
	}
	delete(s.dashboards, id)

	return s.persistLocked()
}

func (s *DashboardStore) persistLocked() error {
	if s.dir == "" {
		return nil
	}
	list := make([]*Dashboard, 0, len(s.dashboards))
	for _, d := range s.dashboards {
		list = append(list, d)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	data, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return fmt.Errorf("dashboards: marshal: %w", err)
	}
	path := filepath.Join(s.dir, dashboardsFile)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("dashboards: write tmp: %w", err)
	}

	return os.Rename(tmp, path)
}
