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
	"regexp"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ingest/limits"
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
	MsgField                string // If non-empty, extract this doc field as _raw instead of full JSON.
	TimeField               string // Which doc field to use for _time (default: "@timestamp").
	StripLogstashDateSuffix bool
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

	indexSet bool
}

func (m *esBulkActionMeta) UnmarshalJSON(data []byte) error {
	type alias esBulkActionMeta
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	var a alias
	if v, ok := raw["_index"]; ok {
		a.indexSet = true
		if err := json.Unmarshal(v, &a.Index); err != nil {
			return fmt.Errorf("_index must be a string")
		}
	}
	if v, ok := raw["_id"]; ok {
		if err := json.Unmarshal(v, &a.ID); err != nil {
			return fmt.Errorf("_id must be a string")
		}
	}
	if v, ok := raw["_type"]; ok {
		if err := json.Unmarshal(v, &a.Type); err != nil {
			return fmt.Errorf("_type must be a string")
		}
	}

	*m = esBulkActionMeta(a)
	return nil
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
	Number                           string `json:"number"`
	BuildFlavor                      string `json:"build_flavor"`
	BuildType                        string `json:"build_type"`
	BuildHash                        string `json:"build_hash"`
	BuildDate                        string `json:"build_date"`
	BuildSnapshot                    bool   `json:"build_snapshot"`
	LuceneVersion                    string `json:"lucene_version"`
	MinimumWireCompatibilityVersion  string `json:"minimum_wire_compatibility_version"`
	MinimumIndexCompatibilityVersion string `json:"minimum_index_compatibility_version"`
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

var (
	esDollarIndexTemplateRE  = regexp.MustCompile(`\$\{([^}]+)\}`)
	esBracketIndexTemplateRE = regexp.MustCompile(`%\{\[([^\]]+)\](?::([^}]*))?\}`)
)

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
	e.Index = indexName

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

	if source, ok := esDocSource(doc); ok {
		e.Source = source
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
	if path, ok := esLogFilePath(doc); ok {
		e.Fields["log.file.path"] = event.StringValue(path)
	}
	if targetIndex, ok := esTargetIndex(doc); ok {
		if _, exists := e.Fields["target_index"]; !exists {
			e.Fields["target_index"] = event.StringValue(targetIndex)
		}
	}

	return e
}

func esDocSource(doc map[string]interface{}) (string, bool) {
	if path, ok := esLogFilePath(doc); ok {
		return path, true
	}
	if source, ok := doc["source"].(string); ok && source != "" {
		delete(doc, "source")

		return source, true
	}

	return "", false
}

func esLogFilePath(doc map[string]interface{}) (string, bool) {
	if path, ok := esStringField(doc,
		"log.file.path",
		"attributes.log.file.path",
		"resource.attributes.log.file.path",
		"fields.log.file.path",
	); ok && path != "" {
		return path, true
	}
	logValue, ok := doc["log"].(map[string]interface{})
	if !ok {
		return "", false
	}
	fileValue, ok := logValue["file"].(map[string]interface{})
	if !ok {
		return "", false
	}
	path, ok := fileValue["path"].(string)
	if !ok || path == "" {
		return "", false
	}

	return path, true
}

func esTargetIndex(doc map[string]interface{}) (string, bool) {
	return esStringField(doc,
		"target_index",
		"attributes.target_index",
		"resource.attributes.target_index",
		"fields.target_index",
	)
}

func esStringField(doc map[string]interface{}, names ...string) (string, bool) {
	for _, name := range names {
		if v, ok := doc[name]; ok {
			s, ok := v.(string)
			return s, ok
		}
		if v, ok := esNestedStringField(doc, name); ok {
			return v, true
		}
	}

	return "", false
}

func esNestedStringField(doc map[string]interface{}, path string) (string, bool) {
	parts := strings.Split(path, ".")
	if len(parts) < 2 {
		return "", false
	}
	for i := 1; i < len(parts); i++ {
		container, ok := esNestedObject(doc, parts[:i])
		if !ok {
			continue
		}
		remainingKey := strings.Join(parts[i:], ".")
		if v, ok := container[remainingKey].(string); ok {
			return v, true
		}
	}
	var cur interface{} = doc
	for _, part := range parts {
		obj, ok := cur.(map[string]interface{})
		if !ok {
			return "", false
		}
		cur, ok = obj[part]
		if !ok {
			return "", false
		}
	}
	s, ok := cur.(string)

	return s, ok
}

func esNestedObject(doc map[string]interface{}, parts []string) (map[string]interface{}, bool) {
	var cur interface{} = doc
	for _, part := range parts {
		obj, ok := cur.(map[string]interface{})
		if !ok {
			return nil, false
		}
		cur, ok = obj[part]
		if !ok {
			return nil, false
		}
	}
	obj, ok := cur.(map[string]interface{})

	return obj, ok
}

func stripLogstashDateSuffix(indexName string) string {
	if len(indexName) < len("-2006.01.02")+1 {
		return indexName
	}
	suffixStart := len(indexName) - len("-2006.01.02")
	if indexName[suffixStart] != '-' {
		return indexName
	}
	for i, ch := range indexName[suffixStart+1:] {
		switch i {
		case 4, 7:
			if ch != '.' {
				return indexName
			}
		default:
			if ch < '0' || ch > '9' {
				return indexName
			}
		}
	}
	return indexName[:suffixStart]
}

func resolveESBulkIndexName(dataStreamName, pathIndex string, meta *esBulkActionMeta, doc map[string]interface{}) (string, error) {
	switch {
	case dataStreamName != "":
		return validateESIndexName(dataStreamName)
	case meta != nil && meta.indexSet:
		indexName, err := expandESIndexTemplate(meta.Index, doc)
		if err != nil {
			return "", err
		}

		return validateESIndexName(indexName)
	case pathIndex != "":
		return validateESIndexName(pathIndex)
	}

	if target, ok := doc["target_index"]; ok {
		targetIndex, ok := target.(string)
		if !ok {
			return "", fmt.Errorf("target_index must be a string")
		}

		return validateESIndexName(targetIndex)
	}
	if targetIndex, ok := esStringField(doc,
		"attributes.target_index",
		"resource.attributes.target_index",
		"fields.target_index",
	); ok {
		return validateESIndexName(targetIndex)
	}

	return validateESIndexName("default")
}

func expandESIndexTemplate(indexName string, doc map[string]interface{}) (string, error) {
	var missing string
	expanded := esDollarIndexTemplateRE.ReplaceAllStringFunc(indexName, func(match string) string {
		field := esDollarIndexTemplateRE.FindStringSubmatch(match)[1]
		if value, ok := esTemplateFieldValue(doc, field); ok {
			return value
		}
		missing = field

		return ""
	})
	if missing != "" {
		return "", fmt.Errorf("index template field %q is missing or not a string", missing)
	}

	expanded = esBracketIndexTemplateRE.ReplaceAllStringFunc(expanded, func(match string) string {
		parts := esBracketIndexTemplateRE.FindStringSubmatch(match)
		field := parts[1]
		fallback := parts[2]
		if value, ok := esTemplateFieldValue(doc, field); ok && value != "" {
			return value
		}
		if fallback != "" {
			return fallback
		}
		missing = field

		return ""
	})
	if missing != "" {
		return "", fmt.Errorf("index template field %q is missing or not a string", missing)
	}

	return expanded, nil
}

func esTemplateFieldValue(doc map[string]interface{}, field string) (string, bool) {
	if value, ok := esStringField(doc, field); ok {
		return value, true
	}
	if !strings.Contains(field, ".") {
		return esStringField(doc, "attributes."+field, "resource.attributes."+field, "fields."+field)
	}

	return "", false
}

func validateESIndexName(indexName string) (string, error) {
	if indexName == "" {
		return "", fmt.Errorf("index name is empty")
	}
	if len(indexName) > 255 {
		return "", fmt.Errorf("index name %q exceeds 255 bytes", indexName)
	}
	if indexName == "." || indexName == ".." || strings.Contains(indexName, "..") {
		return "", fmt.Errorf("index name %q contains path traversal", indexName)
	}
	if strings.ContainsAny(indexName, `/\`) {
		return "", fmt.Errorf("index name %q must not contain path separators", indexName)
	}
	if strings.HasPrefix(indexName, "-") || strings.HasPrefix(indexName, "_") || strings.HasPrefix(indexName, "+") {
		return "", fmt.Errorf("index name %q starts with an unsupported character", indexName)
	}
	for _, ch := range indexName {
		switch {
		case ch >= 'a' && ch <= 'z':
		case ch >= '0' && ch <= '9':
		case ch == '.' || ch == '_' || ch == '-' || ch == '+':
		default:
			return "", fmt.Errorf("index name %q contains unsupported character %q", indexName, ch)
		}
	}

	return indexName, nil
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
	if !esCompatEnabled(s.currentIngestConfig()) {
		w.Header().Set("Retry-After", "5")
		respondJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"error": map[string]interface{}{
				"type":   "unavailable",
				"reason": "Elasticsearch compatibility ingest is disabled",
			},
			"status": http.StatusServiceUnavailable,
		})
		return
	}

	if !s.requireScope(w, r, auth.ScopeIngest) {
		return
	}
	contentType := strings.ToLower(strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0]))
	if contentType != "" && contentType != "application/x-ndjson" && contentType != "application/json" {
		respondJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": map[string]interface{}{
				"type":   "content_type_exception",
				"reason": "bulk endpoint requires application/x-ndjson or application/json",
			},
			"status": http.StatusBadRequest,
		})
		return
	}

	start := time.Now()
	metricResult := "error"
	defer func() {
		if s.promMetrics != nil {
			s.promMetrics.RecordESBulkRequest(metricResult, time.Since(start).Seconds())
		}
	}()

	ingestCfg := s.currentIngestConfig()
	pathIndex := r.PathValue("index")
	dataStreamName := r.PathValue("data_stream")

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
	fm.StripLogstashDateSuffix = ingestCfg.ESCompat.StripLogstashDateSuffix || ingestCfg.MaxBodySize == 0

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
	batch := make([]*event.Event, 0)
	// H4 fix: track pending items per batch; only mark success AFTER commit.
	// Each pending item holds its slot index in items to preserve request ordering.
	pending := make([]esPendingItem, 0)
	hasErrors := false

	// commitBatch processes the full request batch and fills pending item slots.
	commitBatch := func() bool {
		if len(batch) == 0 {
			return true
		}
		if err := processESBatch(r.Context(), pipe, batch, s); err != nil {
			if respondIngestError(w, err) {
				for _, p := range pending {
					s.recordESBulkItem(p.action, "rejected")
				}
				return false
			}
			for _, p := range pending {
				items[p.itemIdx] = makeErrorItem(p.action, p.index, p.docID,
					http.StatusInternalServerError, "ingest_exception", err.Error())
				s.recordESBulkItem(p.action, "rejected")
			}
			hasErrors = true
		} else {
			for _, p := range pending {
				items[p.itemIdx] = makeSuccessItem(p.action, p.index, p.docID)
				s.recordESBulkItem(p.action, "ok")
			}
		}
		batch = batch[:0]
		pending = pending[:0]
		return true
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
			s.recordESBulkItem("index", "rejected")
			hasErrors = true
			// Try to consume data line.
			scanner.Scan()

			continue
		}

		meta, actionName := action.meta()
		if meta == nil {
			items = append(items, makeErrorItem("index", "", "", http.StatusBadRequest,
				"mapper_parsing_exception", "no recognized action"))
			s.recordESBulkItem("index", "rejected")
			hasErrors = true
			scanner.Scan()

			continue
		}

		switch actionName {
		case "update":
			items = append(items, makeErrorItem("index", meta.Index, meta.ID, http.StatusBadRequest,
				"action_request_validation_exception", "update action is not supported"))
			s.recordESBulkItem("update", "rejected")
			hasErrors = true
			scanner.Scan() // consume data line

			continue
		case "delete":
			items = append(items, makeErrorItem("index", meta.Index, meta.ID, http.StatusBadRequest,
				"action_request_validation_exception", "delete action is not supported"))
			s.recordESBulkItem("delete", "rejected")
			hasErrors = true
			continue
		}

		// index or create: read data line.
		if !scanner.Scan() {
			items = append(items, makeErrorItem(actionName, meta.Index, meta.ID, http.StatusBadRequest,
				"mapper_parsing_exception", "missing data line after action"))
			s.recordESBulkItem(actionName, "rejected")
			hasErrors = true

			continue
		}

		dataLine := strings.TrimSpace(scanner.Text())
		var doc map[string]interface{}
		if err := json.Unmarshal([]byte(dataLine), &doc); err != nil {
			items = append(items, makeErrorItem(actionName, meta.Index, meta.ID, http.StatusBadRequest,
				"mapper_parsing_exception", fmt.Sprintf("invalid data JSON: %v", err)))
			s.recordESBulkItem(actionName, "rejected")
			hasErrors = true

			continue
		}

		indexName, err := resolveESBulkIndexName(dataStreamName, pathIndex, meta, doc)
		if err != nil {
			items = append(items, makeErrorItem(actionName, meta.Index, meta.ID, http.StatusBadRequest,
				"invalid_index_name_exception", err.Error()))
			s.recordESBulkItem(actionName, "rejected")
			hasErrors = true

			continue
		}
		ev := esDocToEventWithMapping(doc, indexName, fm)
		batch = append(batch, ev)

		docID := meta.ID
		if docID == "" {
			docID = generateESDocID()
		}
		// H4 fix: reserve a slot in items now (preserves request order),
		// but fill it in only after commit succeeds/fails.
		slotIdx := len(items)
		items = append(items, esBulkItemResult{})
		pending = append(pending, esPendingItem{action: actionName, index: indexName, docID: docID, itemIdx: slotIdx})
	}
	if err := scanner.Err(); err != nil {
		if limits.IsTooLarge(err) {
			respondJSON(w, http.StatusRequestEntityTooLarge, map[string]interface{}{
				"error": map[string]interface{}{
					"type":   "request_entity_too_large",
					"reason": err.Error(),
				},
				"status": http.StatusRequestEntityTooLarge,
			})
			return
		}
		respondJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": map[string]interface{}{
				"type":   "mapper_parsing_exception",
				"reason": err.Error(),
			},
			"status": http.StatusBadRequest,
		})
		return
	}

	// Flush remaining batch.
	if !commitBatch() {
		return
	}

	took := time.Since(start).Milliseconds()
	if items == nil {
		items = []esBulkItemResult{}
	}
	if hasErrors {
		metricResult = "partial"
	} else {
		metricResult = "success"
	}

	respondJSON(w, http.StatusOK, esBulkResponse{
		Took:   took,
		Errors: hasErrors,
		Items:  items,
	})
}

func (s *Server) recordESBulkItem(action, result string) {
	if s.promMetrics != nil {
		s.promMetrics.RecordESBulkItem(action, result)
	}
}

func esCompatEnabled(ingest config.IngestConfig) bool {
	if ingest.ESCompat.AdvertisedVersion == "" && ingest.ESCompat.ClusterName == "" {
		return true
	}
	return ingest.ESCompat.Enabled
}

func processESBatch(ctx context.Context, pipe *pipeline.Pipeline, batch []*event.Event, s *Server) error {
	processed, err := pipe.Process(batch)
	if err != nil {
		return err
	}

	return s.submitDurableShipperEvents(ctx, processed)
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
	if !esCompatEnabled(s.currentIngestConfig()) {
		w.Header().Set("Retry-After", "5")
		respondJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"error": map[string]interface{}{
				"type":   "unavailable",
				"reason": "Elasticsearch compatibility ingest is disabled",
			},
			"status": http.StatusServiceUnavailable,
		})
		return
	}

	if !s.requireScope(w, r, auth.ScopeIngest) {
		return
	}

	indexName, ok := requirePathValue(r, w, "index")
	if !ok {
		return
	}
	indexName, err := validateESIndexName(indexName)
	if err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]interface{}{
			"error": map[string]interface{}{
				"type":   "invalid_index_name_exception",
				"reason": err.Error(),
			},
			"status": http.StatusBadRequest,
		})

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

	if respondIngestError(w, s.submitShipperEvents(r.Context(), processed)) {
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
	if s.promMetrics != nil {
		s.promMetrics.RecordESHandshake("cluster")
	}
	if s.esHandshake == nil {
		respondInternalError(w, "elasticsearch compatibility handshake is not initialized")
		return
	}
	s.esHandshake.ServeHTTP(w, r)
}

func (s *Server) esCompatibilityHandler(kind string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.promMetrics != nil {
			s.promMetrics.RecordESHandshake(kind)
		}
		next.ServeHTTP(w, r)
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
