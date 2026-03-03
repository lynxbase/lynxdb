package savedqueries

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

var (
	ErrNotFound      = errors.New("saved query not found")
	ErrAlreadyExists = errors.New("saved query with this name already exists")
	ErrNameEmpty     = errors.New("name is required")
	ErrQueryEmpty    = errors.New("query is required")
)

type SavedQuery struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Q         string    `json:"q"`
	From      string    `json:"from,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type SavedQueryInput struct {
	Name  string `json:"name"`
	Q     string `json:"q"`
	Query string `json:"query"` // alias for Q
	From  string `json:"from,omitempty"`
}

func (i *SavedQueryInput) Validate() error {
	if i.Name == "" {
		return ErrNameEmpty
	}
	q := i.Q
	if q == "" {
		q = i.Query
	}
	if q == "" {
		return ErrQueryEmpty
	}

	return nil
}

func (i *SavedQueryInput) effectiveQuery() string {
	if i.Q != "" {
		return i.Q
	}

	return i.Query
}

func (i *SavedQueryInput) ToSavedQuery() *SavedQuery {
	now := time.Now()

	return &SavedQuery{
		ID:        generateID(),
		Name:      i.Name,
		Q:         i.effectiveQuery(),
		From:      i.From,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func generateID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)

	return "sq_" + hex.EncodeToString(b)
}

const storeFile = "saved-queries.json"

type Store struct {
	mu      sync.RWMutex
	queries map[string]*SavedQuery
	dir     string
}

func OpenStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("savedqueries: mkdir %s: %w", dir, err)
	}
	s := &Store{queries: make(map[string]*SavedQuery), dir: dir}
	path := filepath.Join(dir, storeFile)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}

		return nil, fmt.Errorf("savedqueries: read %s: %w", path, err)
	}
	var list []*SavedQuery
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("savedqueries: unmarshal: %w", err)
	}
	for _, q := range list {
		s.queries[q.ID] = q
	}

	return s, nil
}

func OpenInMemory() *Store {
	return &Store{queries: make(map[string]*SavedQuery)}
}

func (s *Store) List() []SavedQuery {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]SavedQuery, 0, len(s.queries))
	for _, q := range s.queries {
		out = append(out, *q)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

func (s *Store) Get(id string) (*SavedQuery, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	q, ok := s.queries[id]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *q

	return &cp, nil
}

func (s *Store) Create(sq *SavedQuery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.queries {
		if existing.Name == sq.Name {
			return ErrAlreadyExists
		}
	}
	cp := *sq
	s.queries[sq.ID] = &cp

	return s.persistLocked()
}

func (s *Store) Update(sq *SavedQuery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.queries[sq.ID]; !ok {
		return ErrNotFound
	}
	cp := *sq
	s.queries[sq.ID] = &cp

	return s.persistLocked()
}

func (s *Store) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.queries[id]; !ok {
		return ErrNotFound
	}
	delete(s.queries, id)

	return s.persistLocked()
}

func (s *Store) persistLocked() error {
	if s.dir == "" {
		return nil
	}
	list := make([]*SavedQuery, 0, len(s.queries))
	for _, q := range s.queries {
		list = append(list, q)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	data, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return fmt.Errorf("savedqueries: marshal: %w", err)
	}
	path := filepath.Join(s.dir, storeFile)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("savedqueries: write tmp: %w", err)
	}

	return os.Rename(tmp, path)
}
