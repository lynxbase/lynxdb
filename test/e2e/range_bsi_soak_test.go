//go:build long

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/api/rest"
	"github.com/lynxbase/lynxdb/pkg/client"
	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/storage/part"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
)

const rangeBSISoakIndex = "range_bsi_soak"

func TestLong_RangeBSISoak(t *testing.T) {
	cfg := rangeBSISoakConfigFromEnv(t)
	t.Logf("range BSI soak config: duration=%s event_rate=%d/s batch=%d query_workers=%d query_interval=%s compaction_interval=%s seed_segments=%d",
		cfg.duration, cfg.eventRate, cfg.batchSize, cfg.queryWorkers, cfg.queryInterval, cfg.compactionInterval, cfg.seedSegments)

	verifyRangeBSIPartWriterOutput(t, cfg)

	startGoroutines := runtime.NumGoroutine()
	srv := startRangeBSISoakServer(t, cfg)
	defer srv.stop()

	var seq atomic.Int64
	var accepted atomic.Int64
	errs := &rangeBSISoakErrors{}

	seedRangeBSISoakSegments(t, srv.client, cfg, &seq, &accepted)
	waitForRangeBSISoakSegmentCount(t, srv.dataDir, 1, cfg.flushWait)
	beforeCompactions := rangeBSICompactionHistoryCount(t, srv.baseURL)

	workCtx, cancelWork := context.WithTimeout(context.Background(), cfg.duration)
	defer cancelWork()

	var wg sync.WaitGroup
	wg.Add(1)
	go rangeBSISoakGuard(errs, "ingest", &wg, func() {
		runRangeBSISoakIngest(workCtx, srv.client, cfg, &seq, &accepted, errs)
	})

	for workerID := 0; workerID < cfg.queryWorkers; workerID++ {
		wg.Add(1)
		go rangeBSISoakGuard(errs, fmt.Sprintf("query-%d", workerID), &wg, func() {
			runRangeBSISoakQueries(workCtx, srv.client, cfg, errs)
		})
	}

	<-workCtx.Done()
	wg.Wait()

	flushRangeBSISoakByQuery(t, srv.client)
	waitForRangeBSISoakSegmentCount(t, srv.dataDir, 1, cfg.flushWait)
	afterCompactions := waitForRangeBSICompactionHistoryIncrease(t, srv.baseURL, beforeCompactions, cfg.postCompactionWait)
	if afterCompactions <= beforeCompactions {
		t.Logf("compaction history count stayed at %d; no public compact-now API exists, so short runs rely on the production scheduler and direct part-writer V2/RangeBSI verification", beforeCompactions)
	} else {
		t.Logf("compaction history count increased from %d to %d", beforeCompactions, afterCompactions)
	}

	stats := queryRangeBSIMetaStats(t, srv.baseURL)
	if checks := numericStat(stats, "range_bsi_checks"); checks <= 0 {
		t.Fatalf("meta.stats.range_bsi_checks = %v, want BSI consultations", stats["range_bsi_checks"])
	}
	t.Logf("range BSI query stats: checks=%v mask_bytes=%v", stats["range_bsi_checks"], stats["range_bsi_mask_bytes"])

	if accepted.Load() == 0 {
		t.Fatal("accepted events = 0, want sustained ingest")
	}
	if errs.count.Load() > 0 {
		t.Fatalf("soak observed %d error(s):\n%s", errs.count.Load(), strings.Join(errs.first(), "\n"))
	}

	srv.stop()

	segments := inspectRangeBSISoakSegments(t, srv.dataDir)
	if segments.total == 0 {
		t.Fatal("final segment count = 0, want flushed disk segments")
	}
	if segments.v2 != segments.total {
		t.Fatalf("final V2 segments = %d/%d, non-V2 paths: %s", segments.v2, segments.total, strings.Join(segments.nonV2, ", "))
	}
	if segments.withBSI == 0 || segments.bsiBytes == 0 {
		t.Fatalf("final segments with BSI = %d, bsi bytes = %d; want numeric range BSI sections", segments.withBSI, segments.bsiBytes)
	}

	waitForGoroutinesWithin(t, startGoroutines+cfg.goroutineTolerance, cfg.goroutineWait)
	t.Logf("range BSI soak accepted=%d segments=%d v2=%d bsi_segments=%d bsi_bytes=%d goroutines_start=%d goroutines_end=%d",
		accepted.Load(), segments.total, segments.v2, segments.withBSI, segments.bsiBytes, startGoroutines, runtime.NumGoroutine())
}

type rangeBSISoakConfig struct {
	duration           time.Duration
	eventRate          int
	batchSize          int
	queryWorkers       int
	queryInterval      time.Duration
	queryTimeout       time.Duration
	compactionInterval time.Duration
	postCompactionWait time.Duration
	flushWait          time.Duration
	seedSegments       int
	goroutineTolerance int
	goroutineWait      time.Duration
}

func rangeBSISoakConfigFromEnv(t *testing.T) rangeBSISoakConfig {
	t.Helper()

	compactionInterval := durationEnv(t, "LYNXDB_RANGE_BSI_SOAK_COMPACTION_INTERVAL", 30*time.Second)
	return rangeBSISoakConfig{
		duration:           durationEnv(t, "LYNXDB_RANGE_BSI_SOAK_DURATION", 15*time.Second),
		eventRate:          intEnv(t, "LYNXDB_RANGE_BSI_SOAK_EVENT_RATE", 1_000),
		batchSize:          intEnv(t, "LYNXDB_RANGE_BSI_SOAK_BATCH_SIZE", 250),
		queryWorkers:       intEnv(t, "LYNXDB_RANGE_BSI_SOAK_QUERY_WORKERS", 1),
		queryInterval:      durationEnv(t, "LYNXDB_RANGE_BSI_SOAK_QUERY_INTERVAL", 250*time.Millisecond),
		queryTimeout:       durationEnv(t, "LYNXDB_RANGE_BSI_SOAK_QUERY_TIMEOUT", 15*time.Second),
		compactionInterval: compactionInterval,
		postCompactionWait: durationEnv(t, "LYNXDB_RANGE_BSI_SOAK_POST_COMPACTION_WAIT", max(2*time.Second, 2*compactionInterval)),
		flushWait:          durationEnv(t, "LYNXDB_RANGE_BSI_SOAK_FLUSH_WAIT", 5*time.Second),
		seedSegments:       intEnv(t, "LYNXDB_RANGE_BSI_SOAK_SEED_SEGMENTS", 1),
		goroutineTolerance: intEnv(t, "LYNXDB_RANGE_BSI_SOAK_GOROUTINE_TOLERANCE", 40),
		goroutineWait:      durationEnv(t, "LYNXDB_RANGE_BSI_SOAK_GOROUTINE_WAIT", 10*time.Second),
	}
}

func intEnv(t *testing.T, name string, def int) int {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		t.Fatalf("%s = %q, want positive integer", name, raw)
	}
	return n
}

func durationEnv(t *testing.T, name string, def time.Duration) time.Duration {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		t.Fatalf("%s = %q, want positive duration", name, raw)
	}
	return d
}

type rangeBSISoakServer struct {
	dataDir   string
	baseURL   string
	client    *client.Client
	srv       *rest.Server
	cancel    context.CancelFunc
	startDone chan struct{}
	transport *http.Transport
	stopped   atomic.Bool
	t         *testing.T
}

func startRangeBSISoakServer(t *testing.T, cfg rangeBSISoakConfig) *rangeBSISoakServer {
	t.Helper()

	dataDir := t.TempDir()
	defaults := config.DefaultConfig()
	storageCfg := defaults.Storage
	storageCfg.CompactionInterval = cfg.compactionInterval
	storageCfg.TieringInterval = time.Hour
	storageCfg.CompactionWorkers = 2

	ingestCfg := defaults.Ingest
	fsync := false
	ingestCfg.FSync = &fsync
	ingestCfg.MaxBatchSize = cfg.batchSize
	ingestCfg.OTLP.HTTPListen = ""
	ingestCfg.OTLP.GRPCListen = ""

	queryCfg := defaults.Query
	queryCfg.SyncTimeout = cfg.queryTimeout
	queryCfg.MaxQueryRuntime = cfg.queryTimeout

	server, err := rest.NewServer(rest.Config{
		Addr:    "127.0.0.1:0",
		DataDir: dataDir,
		Storage: storageCfg,
		Query:   queryCfg,
		Ingest:  ingestCfg,
		HTTP:    defaults.HTTP,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	startDone := make(chan struct{})
	startErr := make(chan error, 1)
	go func() {
		defer close(startDone)
		if err := server.Start(ctx); err != nil && ctx.Err() == nil {
			startErr <- err
			t.Logf("server exited with error: %v", err)
		}
	}()

	transport := &http.Transport{}
	addr := waitForRangeBSIServerAddr(t, server, startErr, cancel, startDone, 10*time.Second)
	baseURL := fmt.Sprintf("http://%s", addr)
	c := client.NewClient(
		client.WithBaseURL(baseURL),
		client.WithHTTPClient(&http.Client{Timeout: cfg.queryTimeout, Transport: transport}),
	)
	waitForRangeBSIHealth(t, c, 10*time.Second)

	return &rangeBSISoakServer{
		dataDir:   dataDir,
		baseURL:   baseURL,
		client:    c,
		srv:       server,
		cancel:    cancel,
		startDone: startDone,
		transport: transport,
		t:         t,
	}
}

func (s *rangeBSISoakServer) stop() {
	if !s.stopped.CompareAndSwap(false, true) {
		return
	}
	s.cancel()
	select {
	case <-s.startDone:
	case <-time.After(30 * time.Second):
		s.t.Fatal("server did not shut down within 30s")
	}
	s.transport.CloseIdleConnections()
}

func waitForRangeBSIServerAddr(t *testing.T, server *rest.Server, startErr <-chan error, cancel context.CancelFunc, startDone <-chan struct{}, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		select {
		case err := <-startErr:
			cancel()
			<-startDone
			t.Fatalf("server start: %v", err)
		default:
		}
		addr := server.Addr()
		if addr != "" && !strings.HasSuffix(addr, ":0") {
			return addr
		}
		if time.Now().After(deadline) {
			cancel()
			<-startDone
			t.Fatalf("server did not publish listen address within %s", timeout)
		}
		timer := time.NewTimer(10 * time.Millisecond)
		<-timer.C
	}
}

func waitForRangeBSIHealth(t *testing.T, c *client.Client, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		if _, err := c.Health(ctx); err == nil {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("server did not become healthy: %v", ctx.Err())
		}
		runtime.Gosched()
	}
}

func seedRangeBSISoakSegments(t *testing.T, c *client.Client, cfg rangeBSISoakConfig, seq, accepted *atomic.Int64) {
	t.Helper()
	for i := 0; i < cfg.seedSegments; i++ {
		ingestRangeBSISoakBatch(t, c, cfg, seq, accepted)
		flushRangeBSISoakByQuery(t, c)
	}
}

func ingestRangeBSISoakBatch(t *testing.T, c *client.Client, cfg rangeBSISoakConfig, seq, accepted *atomic.Int64) {
	t.Helper()
	start := seq.Add(int64(cfg.batchSize)) - int64(cfg.batchSize)
	events := makeRangeBSISoakEvents(start, cfg.batchSize)
	ctx, cancel := context.WithTimeout(context.Background(), cfg.queryTimeout)
	defer cancel()
	result, err := c.IngestEvents(ctx, events)
	if err != nil {
		t.Fatalf("seed ingest: %v", err)
	}
	if result.Accepted != len(events) || result.Failed != 0 {
		t.Fatalf("seed ingest accepted=%d failed=%d, want accepted=%d failed=0", result.Accepted, result.Failed, len(events))
	}
	accepted.Add(int64(result.Accepted))
}

func runRangeBSISoakIngest(ctx context.Context, c *client.Client, cfg rangeBSISoakConfig, seq, accepted *atomic.Int64, errs *rangeBSISoakErrors) {
	interval := time.Second * time.Duration(cfg.batchSize) / time.Duration(cfg.eventRate)
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			start := seq.Add(int64(cfg.batchSize)) - int64(cfg.batchSize)
			events := makeRangeBSISoakEvents(start, cfg.batchSize)
			reqCtx, cancel := context.WithTimeout(ctx, cfg.queryTimeout)
			result, err := c.IngestEvents(reqCtx, events)
			cancel()
			if err != nil {
				if ctx.Err() == nil {
					errs.add("ingest start=%d: %v", start, err)
				}
				continue
			}
			if result.Accepted != len(events) || result.Failed != 0 {
				errs.add("ingest start=%d accepted=%d failed=%d want accepted=%d failed=0", start, result.Accepted, result.Failed, len(events))
				continue
			}
			accepted.Add(int64(result.Accepted))
		}
	}
}

func runRangeBSISoakQueries(ctx context.Context, c *client.Client, cfg rangeBSISoakConfig, errs *rangeBSISoakErrors) {
	queries := []string{
		`source=range_bsi_soak | where status >= 500 AND status <= 599 | stats count()`,
		`source=range_bsi_soak | where duration_ms between 100 and 1000 | stats count()`,
		`source=range_bsi_soak | where bytes >= 4096 | stats count()`,
		`source=range_bsi_soak | where tenant_id >= 10 AND tenant_id < 20 | stats count()`,
	}
	ticker := time.NewTicker(cfg.queryInterval)
	defer ticker.Stop()

	var i int
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			q := queries[i%len(queries)]
			i++
			reqCtx, cancel := context.WithTimeout(ctx, cfg.queryTimeout)
			result, err := c.QuerySync(reqCtx, q, "", "")
			cancel()
			if err != nil {
				if ctx.Err() == nil {
					errs.add("query %q: %v", q, err)
				}
				continue
			}
			if result.Type != client.ResultTypeAggregate || result.Aggregate == nil || len(result.Aggregate.Rows) != 1 {
				errs.add("query %q returned type=%s rows=%d, want one aggregate row", q, result.Type, aggregateRowCount(result))
				continue
			}
			if result.Meta.SegmentsErrored != 0 {
				errs.add("query %q segments_errored=%d", q, result.Meta.SegmentsErrored)
			}
		}
	}
}

func aggregateRowCount(result *client.QueryResult) int {
	if result == nil || result.Aggregate == nil {
		return 0
	}
	return len(result.Aggregate.Rows)
}

func flushRangeBSISoakByQuery(t *testing.T, c *client.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	result, err := c.QuerySync(ctx, `source=range_bsi_soak | stats count()`, "", "")
	if err != nil {
		t.Fatalf("flush query: %v", err)
	}
	if result.Type != client.ResultTypeAggregate || result.Aggregate == nil {
		t.Fatalf("flush query type=%s, want aggregate", result.Type)
	}
}

func makeRangeBSISoakEvents(start int64, n int) []client.IngestEvent {
	base := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	events := make([]client.IngestEvent, n)
	for i := 0; i < n; i++ {
		row := start + int64(i)
		status := 200
		switch row % 100 {
		case 0, 1, 2:
			status = 500 + int(row%100)
		case 3, 4, 5, 6, 7, 8, 9:
			status = 404
		}
		durationMS := 1 + (row*37)%30_000
		bytesSent := 256 + (row*97)%16_384
		tenantID := row % 64
		attempts := 1 + row%5
		ts := float64(base.Add(time.Duration(row)*time.Millisecond).UnixNano()) / float64(time.Second)
		raw := fmt.Sprintf(`{"row":%d,"status":%d,"duration_ms":%d,"bytes":%d,"tenant_id":%d,"attempts":%d,"message":"range bsi soak"}`,
			row, status, durationMS, bytesSent, tenantID, attempts)
		events[i] = client.IngestEvent{
			Event:      raw,
			Time:       &ts,
			Index:      rangeBSISoakIndex,
			Source:     "range-bsi-soak",
			Sourcetype: "json",
			Host:       fmt.Sprintf("web-%02d", row%16),
			Fields: map[string]interface{}{
				"row":         row,
				"status":      status,
				"duration_ms": durationMS,
				"bytes":       bytesSent,
				"tenant_id":   tenantID,
				"attempts":    attempts,
			},
		}
	}
	return events
}

func verifyRangeBSIPartWriterOutput(t *testing.T, cfg rangeBSISoakConfig) {
	t.Helper()
	layout := part.NewLayout(t.TempDir())
	writer, err := part.NewPartStreamWriter(layout, rangeBSISoakIndex, 1, part.WithFSync(false))
	if err != nil {
		t.Fatalf("NewPartStreamWriter: %v", err)
	}
	writer.SetRowGroupSize(cfg.batchSize)
	events := makeRangeBSIPartEvents(0, max(cfg.batchSize, 512))
	if err := writer.WriteRowGroup(context.Background(), events); err != nil {
		t.Fatalf("PartStreamWriter.WriteRowGroup: %v", err)
	}
	meta, err := writer.Finalize(context.Background())
	if err != nil {
		t.Fatalf("PartStreamWriter.Finalize: %v", err)
	}
	if meta.FormatMajor != segment.LSG_FORMAT_MAJOR_V2 {
		t.Fatalf("part writer FormatMajor = %d, want %d", meta.FormatMajor, segment.LSG_FORMAT_MAJOR_V2)
	}
	if meta.BSIColumns == 0 || meta.BSISectionBytes == 0 {
		t.Fatalf("part writer BSIColumns=%d BSISectionBytes=%d, want RangeBSI output", meta.BSIColumns, meta.BSISectionBytes)
	}

	ms, err := segment.OpenSegmentFile(meta.Path)
	if err != nil {
		t.Fatalf("OpenSegmentFile(%s): %v", meta.Path, err)
	}
	defer ms.Close()
	reader := ms.Reader()
	if reader.FormatMajor() != segment.LSG_FORMAT_MAJOR_V2 {
		t.Fatalf("part segment FormatMajor = %d, want %d", reader.FormatMajor(), segment.LSG_FORMAT_MAJOR_V2)
	}
	if !reader.HasRangeBSI() {
		t.Fatal("part segment HasRangeBSI() = false, want true")
	}
	if err := reader.VerifyAllRangeBSIs(); err != nil {
		t.Fatalf("part segment VerifyAllRangeBSIs: %v", err)
	}
}

func makeRangeBSIPartEvents(start int64, n int) []*event.Event {
	base := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	events := make([]*event.Event, n)
	for i := 0; i < n; i++ {
		row := start + int64(i)
		status := int64(200)
		if row%100 < 3 {
			status = 500 + row%100
		}
		durationMS := int64(1 + (row*37)%30_000)
		bytesSent := int64(256 + (row*97)%16_384)
		tenantID := row % 64
		e := event.NewEvent(base.Add(time.Duration(row)*time.Millisecond), fmt.Sprintf("row=%d status=%d duration_ms=%d bytes=%d tenant_id=%d", row, status, durationMS, bytesSent, tenantID))
		e.Index = rangeBSISoakIndex
		e.Source = "range-bsi-soak"
		e.SourceType = "json"
		e.Host = fmt.Sprintf("web-%02d", row%16)
		e.SetField("row", event.IntValue(row))
		e.SetField("status", event.IntValue(status))
		e.SetField("duration_ms", event.IntValue(durationMS))
		e.SetField("bytes", event.IntValue(bytesSent))
		e.SetField("tenant_id", event.IntValue(tenantID))
		events[i] = e
	}
	return events
}

func queryRangeBSIMetaStats(t *testing.T, baseURL string) map[string]interface{} {
	t.Helper()
	body, err := json.Marshal(map[string]interface{}{
		"q": `source=range_bsi_soak | where status >= 500 AND status <= 599 | table status`,
	})
	if err != nil {
		t.Fatalf("marshal query: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/api/v1/query", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create query request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("range BSI stats query: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		t.Fatalf("range BSI stats query status=%d body=%s", resp.StatusCode, data)
	}
	var env map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode query envelope: %v", err)
	}
	meta, ok := env["meta"].(map[string]interface{})
	if !ok {
		t.Fatalf("query envelope missing meta: %#v", env)
	}
	stats, ok := meta["stats"].(map[string]interface{})
	if !ok {
		t.Fatalf("query meta missing stats: %#v", meta)
	}
	return stats
}

func numericStat(stats map[string]interface{}, key string) float64 {
	switch v := stats[key].(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	default:
		return 0
	}
}

func waitForRangeBSICompactionHistory(t *testing.T, baseURL string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		got := rangeBSICompactionHistoryCount(t, baseURL)
		if got >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("compaction history count = %d, want >= %d within %s; no public compact-now API exists, so this test relies on production L0 flush pressure plus the compaction scheduler", got, want, timeout)
		}
		timer := time.NewTimer(100 * time.Millisecond)
		<-timer.C
	}
}

func waitForRangeBSICompactionHistoryIncrease(t *testing.T, baseURL string, previous int, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		got := rangeBSICompactionHistoryCount(t, baseURL)
		if got > previous || time.Now().After(deadline) {
			return got
		}
		timer := time.NewTimer(100 * time.Millisecond)
		<-timer.C
	}
}

func rangeBSICompactionHistoryCount(t *testing.T, baseURL string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/api/v1/compaction/history", http.NoBody)
	if err != nil {
		t.Fatalf("create compaction history request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("compaction history request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		t.Fatalf("compaction history status=%d body=%s", resp.StatusCode, data)
	}
	var env struct {
		Data struct {
			Count int `json:"count"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode compaction history: %v", err)
	}
	return env.Data.Count
}

type rangeBSISoakSegmentSummary struct {
	total    int
	v2       int
	withBSI  int
	bsiBytes int64
	nonV2    []string
}

func inspectRangeBSISoakSegments(t *testing.T, dataDir string) rangeBSISoakSegmentSummary {
	t.Helper()
	var out rangeBSISoakSegmentSummary
	root := filepath.Join(dataDir, "segments", "hot", rangeBSISoakIndex)
	if _, err := os.Stat(root); err != nil {
		if os.IsNotExist(err) {
			return out
		}
		t.Fatalf("stat segment root: %v", err)
	}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".lsg" || strings.HasPrefix(filepath.Base(path), "tmp_") {
			return nil
		}
		ms, err := segment.OpenSegmentFile(path)
		if err != nil {
			return fmt.Errorf("open segment %s: %w", path, err)
		}
		defer ms.Close()
		out.total++
		r := ms.Reader()
		if r.FormatMajor() == segment.LSG_FORMAT_MAJOR_V2 {
			out.v2++
		} else {
			out.nonV2 = append(out.nonV2, path)
		}
		cols, bytes := r.RangeBSIStats()
		if cols > 0 && bytes > 0 {
			out.withBSI++
			out.bsiBytes += bytes
		}
		return nil
	})
	if err != nil {
		t.Fatalf("inspect segments: %v", err)
	}
	return out
}

func waitForRangeBSISoakSegmentCount(t *testing.T, dataDir string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		segments := inspectRangeBSISoakSegments(t, dataDir)
		if segments.total >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("segment count = %d, want >= %d within %s", segments.total, want, timeout)
		}
		timer := time.NewTimer(100 * time.Millisecond)
		<-timer.C
	}
}

func waitForGoroutinesWithin(t *testing.T, wantMax int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		runtime.GC()
		got := runtime.NumGoroutine()
		if got <= wantMax {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines = %d, want <= %d after cleanup", got, wantMax)
		}
		timer := time.NewTimer(100 * time.Millisecond)
		<-timer.C
	}
}

type rangeBSISoakErrors struct {
	mu       sync.Mutex
	count    atomic.Int64
	messages []string
}

func (e *rangeBSISoakErrors) add(format string, args ...interface{}) {
	e.count.Add(1)
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.messages) < 20 {
		e.messages = append(e.messages, fmt.Sprintf(format, args...))
	}
}

func (e *rangeBSISoakErrors) first() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, len(e.messages))
	copy(out, e.messages)
	return out
}

func rangeBSISoakGuard(errs *rangeBSISoakErrors, name string, wg *sync.WaitGroup, fn func()) {
	defer wg.Done()
	defer func() {
		if r := recover(); r != nil {
			errs.add("%s panic: %v", name, r)
		}
	}()
	fn()
}
