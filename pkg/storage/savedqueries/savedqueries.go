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
	ID        string                 `json:"id"`
	Name      string                 `json:"name"`
	Q         string                 `json:"q"`
	From      string                 `json:"from,omitempty"`
	Source    string                 `json:"source,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt time.Time              `json:"created_at"`
	UpdatedAt time.Time              `json:"updated_at"`
}

type SavedQueryInput struct {
	Name     string                 `json:"name"`
	Q        string                 `json:"q"`
	Query    string                 `json:"query"` // alias for Q
	From     string                 `json:"from,omitempty"`
	Source   string                 `json:"source,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
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
		Source:    i.Source,
		Metadata:  i.Metadata,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func generateID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		// Fallback: use timestamp-based ID if crypto/rand fails.
		b = []byte(fmt.Sprintf("%016x", time.Now().UnixNano()))
	}

	return "sq_" + hex.EncodeToString(b)
}

const storeFile = "saved-queries.json"

type Store struct {
	mu        sync.RWMutex
	persistMu sync.Mutex // serializes disk writes (separate from data lock)
	queries   map[string]*SavedQuery
	dir       string
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
	for _, existing := range s.queries {
		if existing.Name == sq.Name {
			s.mu.Unlock()

			return ErrAlreadyExists
		}
	}
	cp := *sq
	s.queries[sq.ID] = &cp
	data, err := s.snapshotLocked()
	s.mu.Unlock()
	if err != nil {
		return err
	}

	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	return s.persist(data)
}

func (s *Store) Update(sq *SavedQuery) error {
	s.mu.Lock()
	if _, ok := s.queries[sq.ID]; !ok {
		s.mu.Unlock()

		return ErrNotFound
	}
	cp := *sq
	s.queries[sq.ID] = &cp
	data, err := s.snapshotLocked()
	s.mu.Unlock()
	if err != nil {
		return err
	}

	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	return s.persist(data)
}

func (s *Store) Delete(id string) error {
	s.mu.Lock()
	if _, ok := s.queries[id]; !ok {
		s.mu.Unlock()

		return ErrNotFound
	}
	delete(s.queries, id)
	data, err := s.snapshotLocked()
	s.mu.Unlock()
	if err != nil {
		return err
	}

	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	return s.persist(data)
}

// snapshotLocked marshals the current queries to JSON. Caller must hold s.mu.
func (s *Store) snapshotLocked() ([]byte, error) {
	if s.dir == "" {
		return nil, nil
	}
	list := make([]*SavedQuery, 0, len(s.queries))
	for _, q := range s.queries {
		list = append(list, q)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	data, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("savedqueries: marshal: %w", err)
	}

	return data, nil
}

// persist writes pre-marshaled data to disk atomically. Called WITHOUT the lock held.
func (s *Store) persist(data []byte) error {
	if data == nil {
		return nil
	}
	path := filepath.Join(s.dir, storeFile)
	tmp := path + ".tmp"

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("savedqueries: create tmp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()

		return fmt.Errorf("savedqueries: write tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()

		return fmt.Errorf("savedqueries: sync tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("savedqueries: close tmp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("savedqueries: rename: %w", err)
	}

	return syncDir(s.dir)
}

// syncDir fsyncs a directory to ensure metadata operations (rename, link)
// are persisted to stable storage.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("savedqueries: sync dir %s: %w", dir, err)
	}
	if err := d.Sync(); err != nil {
		d.Close()

		return fmt.Errorf("savedqueries: sync dir %s: %w", dir, err)
	}

	return d.Close()
}
