package shell

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const maxHistoryEntries = 1000

// historyRecord is the JSONL on-disk format for a single history entry.
type historyRecord struct {
	Query      string `json:"q"`
	Timestamp  string `json:"ts,omitempty"`
	DurationMS int64  `json:"ms,omitempty"`
}

// History provides persistent query history with cursor-based navigation.
type History struct {
	entries   []string        // query strings (for navigation and ghost text)
	records   []historyRecord // full records with timestamps/durations
	cursor    int
	path      string
	jsonlMode bool // true once we detect or create JSONL entries
}

// NewHistory loads history from the default path (~/.local/share/lynxdb/history).
func NewHistory() *History {
	h := &History{path: historyPath()}

	if h.path != "" {
		data, err := os.ReadFile(h.path)
		if err == nil {
			h.load(strings.TrimSpace(string(data)))
		}
	}

	h.cursor = len(h.entries)

	return h
}

// load detects format and parses history data.
func (h *History) load(data string) {
	if data == "" {
		return
	}

	lines := strings.Split(data, "\n")

	// Detect format: if the first non-empty line starts with '{', treat as JSONL.
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l == "" {
			continue
		}

		if strings.HasPrefix(l, "{") {
			h.loadJSONL(lines)
		} else {
			h.loadLegacy(lines)
		}

		return
	}
}

func (h *History) loadJSONL(lines []string) {
	h.jsonlMode = true

	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l == "" {
			continue
		}

		var rec historyRecord
		if err := json.Unmarshal([]byte(l), &rec); err != nil {
			// Fallback: treat as plain text.
			h.entries = append(h.entries, l)
			h.records = append(h.records, historyRecord{Query: l})

			continue
		}

		if rec.Query != "" {
			h.entries = append(h.entries, rec.Query)
			h.records = append(h.records, rec)
		}
	}
}

func (h *History) loadLegacy(lines []string) {
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l != "" {
			h.entries = append(h.entries, l)
			h.records = append(h.records, historyRecord{Query: l})
		}
	}
}

// Add appends an entry to history and resets the cursor.
// Newlines are normalized to spaces since the history file is newline-delimited.
func (h *History) Add(entry string) {
	entry = strings.ReplaceAll(strings.TrimSpace(entry), "\n", " ")
	if entry == "" {
		return
	}

	// Deduplicate consecutive entries.
	if len(h.entries) > 0 && h.entries[len(h.entries)-1] == entry {
		h.cursor = len(h.entries)

		return
	}

	h.jsonlMode = true

	rec := historyRecord{
		Query:     entry,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	h.entries = append(h.entries, entry)
	h.records = append(h.records, rec)

	// Trim to limit.
	if len(h.entries) > maxHistoryEntries {
		h.entries = h.entries[len(h.entries)-maxHistoryEntries:]
		h.records = h.records[len(h.records)-maxHistoryEntries:]
	}

	h.cursor = len(h.entries)
}

// SetLastDuration stamps the most recent history entry with execution duration.
func (h *History) SetLastDuration(elapsed time.Duration) {
	if len(h.records) == 0 {
		return
	}

	h.records[len(h.records)-1].DurationMS = elapsed.Milliseconds()
}

// Prev moves the cursor backward and returns the entry.
func (h *History) Prev() (string, bool) {
	if h.cursor <= 0 {
		return "", false
	}

	h.cursor--

	return h.entries[h.cursor], true
}

// Next moves the cursor forward and returns the entry, or empty if at end.
func (h *History) Next() (string, bool) {
	if h.cursor >= len(h.entries)-1 {
		h.cursor = len(h.entries)

		return "", true
	}

	h.cursor++

	return h.entries[h.cursor], true
}

// Reset moves the cursor to the end (past last entry).
func (h *History) Reset() {
	h.cursor = len(h.entries)
}

// Entries returns the last n entries as plain strings for display.
func (h *History) Entries(n int) []string {
	start := 0
	if len(h.entries) > n {
		start = len(h.entries) - n
	}

	return h.entries[start:]
}

// SuggestFromPrefix returns the most recent history entry that starts with
// the given prefix (but is not identical to it). Returns empty if no match.
func (h *History) SuggestFromPrefix(prefix string) string {
	if prefix == "" {
		return ""
	}

	for i := len(h.entries) - 1; i >= 0; i-- {
		if strings.HasPrefix(h.entries[i], prefix) && h.entries[i] != prefix {
			return h.entries[i]
		}
	}

	return ""
}

// Save persists history to disk in JSONL format.
func (h *History) Save() error {
	if h.path == "" {
		return nil
	}

	var b strings.Builder

	for _, rec := range h.records {
		data, err := json.Marshal(rec)
		if err != nil {
			continue
		}

		b.Write(data)
		b.WriteByte('\n')
	}

	return os.WriteFile(h.path, []byte(b.String()), 0o644)
}

// historyPath returns the path for the shell history file.
func historyPath() string {
	dataDir, err := os.UserHomeDir()
	if err != nil {
		return ""
	}

	dir := filepath.Join(dataDir, ".local", "share", "lynxdb")
	_ = os.MkdirAll(dir, 0o755)

	return filepath.Join(dir, "history")
}
