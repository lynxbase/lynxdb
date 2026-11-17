package shell

import (
	"context"
	"sync"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/lynxbase/lynxdb/pkg/client"
)

// sidebarDataMsg carries server status, indexes, and stats fetched concurrently.
type sidebarDataMsg struct {
	server  *client.ServerStatus
	indexes []client.IndexInfo
}

// sidebarRefreshTickMsg triggers a periodic sidebar data refresh.
type sidebarRefreshTickMsg struct{}

const sidebarRefreshInterval = 60 * time.Second

// fetchSidebarDataCmd fetches server status and indexes concurrently.
func fetchSidebarDataCmd(c *client.Client) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var wg sync.WaitGroup
		var server *client.ServerStatus
		var indexes []client.IndexInfo

		wg.Add(2)

		go func() {
			defer wg.Done()
			server, _ = c.Status(ctx)
		}()

		go func() {
			defer wg.Done()
			indexes, _ = c.Indexes(ctx)
		}()

		wg.Wait()

		return sidebarDataMsg{
			server:  server,
			indexes: indexes,
		}
	}
}

// sidebarRefreshTickCmd returns a tea.Cmd that fires sidebarRefreshTickMsg after the interval.
func sidebarRefreshTickCmd() tea.Cmd {
	return tea.Tick(sidebarRefreshInterval, func(t time.Time) tea.Msg {
		return sidebarRefreshTickMsg{}
	})
}

// buildQueryStats converts query result metadata into a sidebar snapshot.
func buildQueryStats(query string, rows []map[string]interface{}, elapsed time.Duration, meta *client.Meta) *QueryStatsSnapshot {
	qs := &QueryStatsSnapshot{
		Query:        query,
		Elapsed:      formatElapsedShell(elapsed),
		RowsReturned: len(rows),
	}

	if meta != nil && meta.Stats != nil {
		s := meta.Stats
		qs.RowsScanned = s.RowsScanned
		qs.Segments = s.SegmentsTotal
		qs.BloomSkipped = s.SegmentsSkippedBF + s.SegmentsSkippedRange
	}

	return qs
}

// popupDebounceMsg fires after 300ms to trigger auto-popup.
type popupDebounceMsg struct{ seq int }

const popupDebounceInterval = 300 * time.Millisecond

// popupDebounceCmd returns a tea.Cmd that fires popupDebounceMsg after the interval.
func popupDebounceCmd(seq int) tea.Cmd {
	return tea.Tick(popupDebounceInterval, func(t time.Time) tea.Msg {
		return popupDebounceMsg{seq: seq}
	})
}

const lynxAnimationInterval = 160 * time.Millisecond

func lynxAnimationTickCmd() tea.Cmd {
	return tea.Tick(lynxAnimationInterval, func(t time.Time) tea.Msg {
		return lynxTickMsg{}
	})
}

// explainDebounceMsg fires after the debounce interval to trigger an explain call.
// The seq field lets us ignore stale debounce ticks.
type explainDebounceMsg struct{ seq int }

// explainResultMsg carries the result of a debounced explain API call.
type explainResultMsg struct {
	result *client.ExplainResult
	err    error
}

const explainDebounceInterval = 500 * time.Millisecond

// explainDebounceCmd returns a tea.Cmd that fires explainDebounceMsg after the interval.
func explainDebounceCmd(seq int) tea.Cmd {
	return tea.Tick(explainDebounceInterval, func(t time.Time) tea.Msg {
		return explainDebounceMsg{seq: seq}
	})
}

// fetchExplainCmd calls the explain API for the given query.
func fetchExplainCmd(c *client.Client, query string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		result, err := c.Explain(ctx, query)

		return explainResultMsg{result: result, err: err}
	}
}
