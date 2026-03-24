package shell

import (
	"os"
	"path/filepath"
	"strings"
)

const maxHistoryEntries = 1000

// History provides persistent query history with cursor-based navigation.
type History struct {
	entries []string
	cursor  int
	path    string
}

// NewHistory loads history from the default path (~/.local/share/lynxdb/history).
func NewHistory() *History {
	h := &History{path: historyPath()}

	if h.path != "" {
		data, err := os.ReadFile(h.path)
		if err == nil {
			lines := strings.Split(strings.TrimSpace(string(data)), "\n")
			for _, l := range lines {
				l = strings.TrimSpace(l)
				if l != "" {
					h.entries = append(h.entries, l)
				}
			}
		}
	}

	h.cursor = len(h.entries)

	return h
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

	h.entries = append(h.entries, entry)

	// Trim to limit.
	if len(h.entries) > maxHistoryEntries {
		h.entries = h.entries[len(h.entries)-maxHistoryEntries:]
	}

	h.cursor = len(h.entries)
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

// Entries returns the last n entries for display.
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

// Save persists history to disk.
func (h *History) Save() error {
	if h.path == "" {
		return nil
	}

	return os.WriteFile(h.path, []byte(strings.Join(h.entries, "\n")+"\n"), 0o644)
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
