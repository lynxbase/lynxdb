package rest

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ingest/pipeline"
)

// setESHeaders sets standard Elasticsearch compatibility headers.
// Filebeat 8.x checks X-Elastic-Product and rejects responses without it.
func setESHeaders(w http.ResponseWriter) {
	w.Header().Set("X-Elastic-Product", "Elasticsearch")
}

// esFieldMapping controls how ES document fields map to LynxDB event fields.
// Parsed once per request from URL query parameters.
type esFieldMapping struct {
	MsgField  string // If non-empty, extract this doc field as _raw instead of full JSON.
	TimeField string // Which doc field to use for _time (default: "@timestamp").
}

// parseFieldMapping parses optional VL-style query parameters from the request URL.
func parseFieldMapping(r *http.Request) esFieldMapping {
	q := r.URL.Query()
	m := esFieldMapping{TimeField: "@timestamp"}
	if v := q.Get("_msg_field"); v != "" {
		m.MsgField = v
	}
	if v := q.Get("_time_field"); v != "" {
		m.TimeField = v
	}
	return m
}

// decompressBody returns an io.ReadCloser that decompresses the request body
// if Content-Encoding is gzip, or returns r.Body unchanged otherwise.
// The caller must close the returned reader.
func decompressBody(r *http.Request) (io.ReadCloser, error) {
	if r.Header.Get("Content-Encoding") != "gzip" {
		return r.Body, nil
	}
	gz, err := gzip.NewReader(r.Body)
	if err != nil {
		return nil, fmt.Errorf("gzip decode: %w", err)
	}
	return gz, nil
}

type esBulkAction struct {
	Index  *esBulkActionMeta `json:"index,omitempty"`
	Create *esBulkActionMeta `json:"create,omitempty"`
	Update *esBulkActionMeta `json:"update,omitempty"`
	Delete *esBulkActionMeta `json:"delete,omitempty"`
}

type esBulkActionMeta struct {
	Index string `json:"_index"`
	ID    string `json:"_id"`
	Type  string `json:"_type"` // ignored
}

func (a *esBulkAction) meta() (*esBulkActionMeta, string) {
	if a.Index != nil {
		return a.Index, "index"
	}
	if a.Create != nil {
		return a.Create, "create"
	}
	if a.Update != nil {
		return a.Update, "update"
	}
	if a.Delete != nil {
		return a.Delete, "delete"
	}

	return nil, ""
}

type esBulkResponse struct {
	Took   int64              `json:"took"`
	Errors bool               `json:"errors"`
	Items  []esBulkItemResult `json:"items"`
}

type esBulkItemResult struct {
	Index  *esBulkItemStatus `json:"index,omitempty"`
	Create *esBulkItemStatus `json:"create,omitempty"`
}

type esBulkItemStatus struct {
	ID     string           `json:"_id"`
	Index  string           `json:"_index"`
	Status int              `json:"status"`
	Result string           `json:"result,omitempty"`
	Error  *esBulkItemError `json:"error,omitempty"`
}

type esBulkItemError struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

type esIndexDocResponse struct {
	ID     string `json:"_id"`
	Index  string `json:"_index"`
	Result string `json:"result"`
}

type esClusterInfoResponse struct {
	Name        string        `json:"name"`
	ClusterName string        `json:"cluster_name"`
	ClusterUUID string        `json:"cluster_uuid"`
	Version     esVersionInfo `json:"version"`
	Tagline     string        `json:"tagline"`
}

type esVersionInfo struct {
	Number        string `json:"number"`
	BuildFlavor   string `json:"build_flavor"`
	BuildType     string `json:"build_type"`
	BuildHash     string `json:"build_hash"`
	LuceneVersion string `json:"lucene_version"`
}

func generateESDocID() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("lynxdb-%d", time.Now().UnixNano())
	}

	return hex.EncodeToString(b)
}

var esTimestampFormats = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05.000Z",
}

func parseESTimestamp(v interface{}) time.Time {
	switch ts := v.(type) {
	case string:
		for _, layout := range esTimestampFormats {
			if t, err := time.Parse(layout, ts); err == nil {
				return t
			}
		}
	case float64:
		sec := int64(ts)
		nsec := int64((ts - float64(sec)) * 1e9)

		return time.Unix(sec, nsec)
	}

	return time.Time{}
}

func esDocToEvent(doc map[string]interface{}, indexName string) *event.Event {
	return esDocToEventWithMapping(doc, indexName, esFieldMapping{TimeField: "@timestamp"})
}

func esDocToEventWithMapping(doc map[string]interface{}, indexName string, fm esFieldMapping) *event.Event {
	e := event.NewEvent(time.Time{}, "")
	e.SourceType = "json"
	e.Index = "main"

	// Extract timestamp using configured field.
	timeField := fm.TimeField
	if timeField == "" {
		timeField = "@timestamp"
	}
	if ts, ok := doc[timeField]; ok {
		e.Time = parseESTimestamp(ts)
		delete(doc, timeField)
	} else if timeField != "timestamp" {
		// Fallback to "timestamp" if configured field not found.
		if ts, ok := doc["timestamp"]; ok {
			e.Time = parseESTimestamp(ts)
			delete(doc, "timestamp")
		}
	}

	// Map _index to source.
	if indexName != "" {
		e.Source = indexName
	}

	// Extract host.
	if h, ok := doc["host"]; ok {
		switch hv := h.(type) {
		case string:
			e.Host = hv
			delete(doc, "host")
		case map[string]interface{}:
			if name, ok := hv["name"]; ok {
				if s, ok := name.(string); ok {
					e.Host = s
				}
			}
			delete(doc, "host")
		}
	}
	if e.Host == "" {
		if agent, ok := doc["agent"]; ok {
			if agentMap, ok := agent.(map[string]interface{}); ok {
				if hostname, ok := agentMap["hostname"]; ok {
					if s, ok := hostname.(string); ok {
						e.Host = s
					}
				}
			}
		}
	}

	// Build _raw: either a specific message field or the full JSON doc.
	if fm.MsgField != "" {
		if msgVal, ok := doc[fm.MsgField]; ok {
			e.Raw = fmt.Sprint(msgVal)
		}
	}
	if e.Raw == "" {
		if raw, err := json.Marshal(doc); err == nil {
			e.Raw = string(raw)
		}
	}

	// Map remaining fields.
	for k, v := range doc {
		e.Fields[k] = event.ValueFromInterface(v)
	}

	return e
}

// esPendingItem tracks a parsed bulk item awaiting commit confirmation (H4 fix).
// ItemIdx is the pre-allocated slot in the items slice, preserving request ordering
// even when error items and success items are interleaved.
type esPendingItem struct {
	action  string
	index   string
	docID   string
	itemIdx int // index into items slice
}

func (s *Server) handleESBulk(w http.ResponseWriter, r *http.Request) {
	setESHeaders(w)

	if !s.requireScope(w, r, auth.ScopeIngest) {
		return
	}

	start := time.Now()

	ingestCfg := s.currentIngestConfig()
	batchSize := ingestCfg.MaxBatchSize
	if batchSize == 0 {
		batchSize = 1000
	}

	// Decompress gzip body if Content-Encoding: gzip (Filebeat default).
	body, err := decompressBody(r)
	if err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": map[string]interface{}{
				"type":   "mapper_parsing_exception",
				"reason": fmt.Sprintf("failed to decompress request body: %v", err),
			},
			"status": http.StatusBadRequest,
		})
		return
	}
	defer body.Close()

	// Parse optional field mapping from query parameters.
	fm := parseFieldMapping(r)

	scanner := bufio.NewScanner(body)
	bufp := scannerBufPool.Get().(*[]byte)
	maxLineBytes := ingestCfg.MaxLineBytes
	if maxLineBytes <= 0 {
		maxLineBytes = 1 << 20 // 1 MB default
	}
	scanner.Buffer(*bufp, maxLineBytes)
	defer scannerBufPool.Put(bufp)

	pipe := ingestPipelineForConfig(ingestCfg)
	var items []esBulkItemResult
	batch := make([]*event.Event, 0, batchSize)
	// H4 fix: track pending items per batch; only mark success AFTER commit.
	// Each pending item holds its slot index in items to preserve request ordering.
	pending := make([]esPendingItem, 0, batchSize)
	hasErrors := false

	// commitBatch processes the current batch and fills in pending item slots.
	commitBatch := func() {
		if len(batch) == 0 {
			return
		}
		if err := processESBatch(r.Context(), pipe, batch, s); err != nil {
			for _, p := range pending {
				items[p.itemIdx] = makeErrorItem(p.action, p.index, p.docID,
					http.StatusInternalServerError, "ingest_exception", err.Error())
			}
			hasErrors = true
		} else {
			for _, p := range pending {
				items[p.itemIdx] = makeSuccessItem(p.action, p.index, p.docID)
			}
		}
		batch = batch[:0]
		pending = pending[:0]
	}

	for scanner.Scan() {
		actionLine := strings.TrimSpace(scanner.Text())
		if actionLine == "" {
			continue
		}

		var action esBulkAction
		if err := json.Unmarshal([]byte(actionLine), &action); err != nil {
			items = append(items, makeErrorItem("index", "", "", http.StatusBadRequest,
				"mapper_parsing_exception", fmt.Sprintf("malformed action line: %v", err)))
			hasErrors = true
			// Try to consume data line.
			scanner.Scan()

			continue
		}

		meta, actionName := action.meta()
		if meta == nil {
			items = append(items, makeErrorItem("index", "", "", http.StatusBadRequest,
				"mapper_parsing_exception", "no recognized action"))
			hasErrors = true
			scanner.Scan()

			continue
		}

		switch actionName {
		case "update":
			items = append(items, makeErrorItem("index", meta.Index, meta.ID, http.StatusBadRequest,
				"action_request_validation_exception", "update action is not supported"))
			hasErrors = true
			scanner.Scan() // consume data line

			continue
		case "delete":
			items = append(items, makeErrorItem("index", meta.Index, meta.ID, http.StatusBadRequest,
				"action_request_validation_exception", "delete action is not supported"))
			hasErrors = true
			// delete has no data line
			continue
		}

		// index or create: read data line.
		if !scanner.Scan() {
			items = append(items, makeErrorItem(actionName, meta.Index, meta.ID, http.StatusBadRequest,
				"mapper_parsing_exception", "missing data line after action"))
			hasErrors = true

			continue
		}

		dataLine := strings.TrimSpace(scanner.Text())
		var doc map[string]interface{}
		if err := json.Unmarshal([]byte(dataLine), &doc); err != nil {
			items = append(items, makeErrorItem(actionName, meta.Index, meta.ID, http.StatusBadRequest,
				"mapper_parsing_exception", fmt.Sprintf("invalid data JSON: %v", err)))
			hasErrors = true

			continue
		}

		ev := esDocToEventWithMapping(doc, meta.Index, fm)
		batch = append(batch, ev)

		docID := meta.ID
		if docID == "" {
			docID = generateESDocID()
		}
		// H4 fix: reserve a slot in items now (preserves request order),
		// but fill it in only after commit succeeds/fails.
		slotIdx := len(items)
		items = append(items, esBulkItemResult{}) // placeholder
		pending = append(pending, esPendingItem{action: actionName, index: meta.Index, docID: docID, itemIdx: slotIdx})

		if len(batch) >= batchSize {
			commitBatch()
		}
	}

	// Flush remaining batch.
	commitBatch()

	took := time.Since(start).Milliseconds()
	if items == nil {
		items = []esBulkItemResult{}
	}

	respondJSON(w, http.StatusOK, esBulkResponse{
		Took:   took,
		Errors: hasErrors,
		Items:  items,
	})
}

func processESBatch(ctx context.Context, pipe *pipeline.Pipeline, batch []*event.Event, s *Server) error {
	processed, err := pipe.Process(batch)
	if err != nil {
		return err
	}

	return s.engine.IngestContext(ctx, processed)
}

func makeSuccessItem(action, index, id string) esBulkItemResult {
	status := &esBulkItemStatus{
		ID:     id,
		Index:  index,
		Status: http.StatusCreated,
		Result: "created",
	}
	switch action {
	case "create":
		return esBulkItemResult{Create: status}
	default:
		return esBulkItemResult{Index: status}
	}
}

func makeErrorItem(action, index, id string, httpStatus int, errType, reason string) esBulkItemResult {
	status := &esBulkItemStatus{
		ID:     id,
		Index:  index,
		Status: httpStatus,
		Error:  &esBulkItemError{Type: errType, Reason: reason},
	}
	switch action {
	case "create":
		return esBulkItemResult{Create: status}
	default:
		return esBulkItemResult{Index: status}
	}
}

func (s *Server) handleESIndexDoc(w http.ResponseWriter, r *http.Request) {
	setESHeaders(w)

	if !s.requireScope(w, r, auth.ScopeIngest) {
		return
	}

	indexName, ok := requirePathValue(r, w, "index")
	if !ok {
		return
	}

	// Decompress gzip body if Content-Encoding: gzip.
	body, err := decompressBody(r)
	if err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": map[string]interface{}{
				"type":   "mapper_parsing_exception",
				"reason": fmt.Sprintf("failed to decompress request body: %v", err),
			},
			"status": http.StatusBadRequest,
		})
		return
	}
	defer body.Close()

	var doc map[string]interface{}
	if err := json.NewDecoder(body).Decode(&doc); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": map[string]interface{}{
				"type":   "mapper_parsing_exception",
				"reason": fmt.Sprintf("invalid JSON: %v", err),
			},
			"status": http.StatusBadRequest,
		})

		return
	}

	ev := esDocToEvent(doc, indexName)
	pipe := s.ingestPipeline()
	processed, err := pipe.Process([]*event.Event{ev})
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": map[string]interface{}{
				"type":   "ingest_exception",
				"reason": err.Error(),
			},
			"status": http.StatusInternalServerError,
		})

		return
	}

	if respondIngestError(w, s.engine.IngestContext(r.Context(), processed)) {
		return
	}

	docID := generateESDocID()
	respondJSON(w, http.StatusCreated, esIndexDocResponse{
		ID:     docID,
		Index:  indexName,
		Result: "created",
	})
}

func (s *Server) handleESClusterInfo(w http.ResponseWriter, r *http.Request) {
	setESHeaders(w)
	respondJSON(w, http.StatusOK, esClusterInfoResponse{
		Name:        "lynxdb",
		ClusterName: "lynxdb",
		ClusterUUID: "lynxdb-single-node",
		Version: esVersionInfo{
			Number:        "8.11.0",
			BuildFlavor:   "default",
			BuildType:     "tar",
			BuildHash:     "000000",
			LuceneVersion: "9.8.0",
		},
		Tagline: "LynxDB — Splunk-power log analytics in a single binary",
	})
}

// handleESStub is a catch-all handler for ES management endpoints that Filebeat
// calls during startup (ILM policies, index templates, ingest pipelines, etc.).
// LynxDB does not implement these APIs and must not acknowledge them as successful.
func (s *Server) handleESStub(w http.ResponseWriter, r *http.Request) {
	slog.Warn("unsupported elasticsearch management endpoint",
		"method", r.Method,
		"path", r.URL.Path)
	setESHeaders(w)
	respondJSON(w, http.StatusNotImplemented, map[string]interface{}{
		"error":  "unsupported Elasticsearch management endpoint",
		"status": http.StatusNotImplemented,
	})
}
