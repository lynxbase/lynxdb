package rest

import (
	"bufio"
	"bytes"
	"compress/gzip"
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
	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/database"
	"github.com/lynxbase/lynxdb/pkg/ingest/limits"
)

// setESHeaders sets standard Elasticsearch compatibility headers.
// Filebeat 8.x checks X-Elastic-Product and rejects responses without it.
func setESHeaders(w http.ResponseWriter) {
	w.Header().Set("X-Elastic-Product", "Elasticsearch")
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

func generateESDocID() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("lynxdb-%d", time.Now().UnixNano())
	}

	return hex.EncodeToString(b)
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
	if strings.HasPrefix(indexName, "-") || strings.HasPrefix(indexName, "_") || strings.HasPrefix(indexName, "+") ||
		strings.HasPrefix(indexName, ".") {
		return "", fmt.Errorf("index name %q starts with an unsupported character", indexName)
	}
	for _, ch := range indexName {
		switch {
		case ch >= 'a' && ch <= 'z':
		case ch >= '0' && ch <= '9':
		case ch == '.' || ch == '_' || ch == '-':
		default:
			return "", fmt.Errorf("index name %q contains unsupported character %q", indexName, ch)
		}
	}

	return indexName, nil
}

// esIndexDocs gathers the documents of a bulk request bound for one index
// and the item slots they answer.
type esIndexDocs struct {
	docs  []byte
	items []esPendingItem
}

// esPendingItem is a document waiting for its append; itemIdx is its slot
// in the items of the response, which keep the order of the request.
type esPendingItem struct {
	action  string
	docID   string
	itemIdx int
}

func respondESError(w http.ResponseWriter, status int, errType, reason string) {
	respondJSON(w, status, map[string]interface{}{
		"error":  map[string]interface{}{"type": errType, "reason": reason},
		"status": status,
	})
}

func (s *Server) esIngestDisabled(w http.ResponseWriter) bool {
	if esCompatEnabled(s.currentIngestConfig()) {
		return false
	}
	w.Header().Set("Retry-After", "5")
	respondESError(w, http.StatusServiceUnavailable, "unavailable", "Elasticsearch compatibility ingest is disabled")

	return true
}

// esDocument reports whether line is a JSON object.
func esDocument(line []byte) bool {
	return len(line) > 0 && line[0] == '{' && json.Valid(line)
}

// handleESBulk takes an Elasticsearch bulk body: it drops the action lines
// and appends each document line, as it is, to the index its action names,
// else the index or data stream of the URL, else the default index.
func (s *Server) handleESBulk(w http.ResponseWriter, r *http.Request) {
	setESHeaders(w)
	if s.esIngestDisabled(w) {
		return
	}
	if !s.requireScope(w, r, auth.ScopeIngest) {
		return
	}
	contentType := strings.ToLower(strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0]))
	if contentType != "" && contentType != "application/x-ndjson" && contentType != "application/json" {
		respondESError(w, http.StatusBadRequest, "content_type_exception",
			"bulk endpoint requires application/x-ndjson or application/json")
		return
	}

	start := time.Now()
	metricResult := "error"
	defer func() {
		s.promMetrics.RecordESBulkRequest(metricResult, time.Since(start).Seconds())
	}()

	defaultIndex := database.DefaultSource
	if name := r.PathValue("index"); name != "" {
		defaultIndex = name
	} else if name := r.PathValue("data_stream"); name != "" {
		defaultIndex = name
	}

	body, err := decompressBody(r)
	if err != nil {
		respondESError(w, http.StatusBadRequest, "mapper_parsing_exception",
			fmt.Sprintf("failed to decompress request body: %v", err))
		return
	}
	defer body.Close()

	maxLineBytes := s.currentIngestConfig().MaxLineBytes
	if maxLineBytes <= 0 {
		maxLineBytes = 1 << 20
	}
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 64*1024), maxLineBytes)

	var items []esBulkItemResult
	byIndex := map[string]*esIndexDocs{}
	var order []string
	hasErrors := false
	reject := func(action, index, id string, status int, errType, reason string) {
		items = append(items, makeErrorItem(action, index, id, status, errType, reason))
		s.recordESBulkItem(action, "rejected")
		hasErrors = true
	}

	for scanner.Scan() {
		actionLine := bytes.TrimSpace(scanner.Bytes())
		if len(actionLine) == 0 {
			continue
		}

		var action esBulkAction
		if err := json.Unmarshal(actionLine, &action); err != nil {
			reject("index", "", "", http.StatusBadRequest, "mapper_parsing_exception",
				fmt.Sprintf("malformed action line: %v", err))
			scanner.Scan()
			continue
		}
		meta, actionName := action.meta()
		switch {
		case meta == nil:
			reject("index", "", "", http.StatusBadRequest, "mapper_parsing_exception", "no recognized action")
			scanner.Scan()
			continue
		case actionName == "update":
			reject("index", meta.Index, meta.ID, http.StatusBadRequest,
				"action_request_validation_exception", "update action is not supported")
			scanner.Scan()
			continue
		case actionName == "delete":
			reject("index", meta.Index, meta.ID, http.StatusBadRequest,
				"action_request_validation_exception", "delete action is not supported")
			continue
		}

		if !scanner.Scan() {
			reject(actionName, meta.Index, meta.ID, http.StatusBadRequest,
				"mapper_parsing_exception", "missing data line after action")
			continue
		}
		doc := bytes.TrimSpace(scanner.Bytes())
		if !esDocument(doc) {
			reject(actionName, meta.Index, meta.ID, http.StatusBadRequest,
				"mapper_parsing_exception", "data line is not a JSON object")
			continue
		}
		index := defaultIndex
		if meta.indexSet {
			index = meta.Index
		}
		if _, err := validateESIndexName(index); err != nil {
			reject(actionName, index, meta.ID, http.StatusBadRequest, "invalid_index_name_exception", err.Error())
			continue
		}

		docID := meta.ID
		if docID == "" {
			docID = generateESDocID()
		}
		d := byIndex[index]
		if d == nil {
			d = &esIndexDocs{}
			byIndex[index] = d
			order = append(order, index)
		}
		d.docs = append(append(d.docs, doc...), '\n')
		d.items = append(d.items, esPendingItem{action: actionName, docID: docID, itemIdx: len(items)})
		items = append(items, esBulkItemResult{})
	}
	if err := scanner.Err(); err != nil {
		if limits.IsTooLarge(err) {
			respondESError(w, http.StatusRequestEntityTooLarge, "request_entity_too_large", err.Error())
			return
		}
		respondESError(w, http.StatusBadRequest, "mapper_parsing_exception", err.Error())
		return
	}

	appended := 0
	for _, index := range order {
		d := byIndex[index]
		_, err := s.db.AppendLines(index, d.docs)
		if err == nil {
			appended += len(d.items)
			s.promMetrics.RecordIngest(len(d.items), len(d.docs))
			continue
		}
		s.promMetrics.RecordIngestError()
		if appended == 0 && respondIngestError(w, err) {
			for _, p := range d.items {
				s.recordESBulkItem(p.action, "rejected")
			}
			return
		}
		for _, p := range d.items {
			items[p.itemIdx] = makeErrorItem(p.action, index, p.docID,
				http.StatusInternalServerError, "ingest_exception", err.Error())
			s.recordESBulkItem(p.action, "rejected")
		}
		d.items = nil
		hasErrors = true
	}
	// Shippers count a 200 as stored, so the documents are durable first.
	if err := s.db.Sync(); err != nil {
		if respondIngestError(w, err) {
			return
		}
	}
	for _, index := range order {
		for _, p := range byIndex[index].items {
			items[p.itemIdx] = makeSuccessItem(p.action, index, p.docID)
			s.recordESBulkItem(p.action, "ok")
		}
	}

	if items == nil {
		items = []esBulkItemResult{}
	}
	if hasErrors {
		metricResult = "partial"
	} else {
		metricResult = "success"
	}
	respondJSON(w, http.StatusOK, esBulkResponse{
		Took:   time.Since(start).Milliseconds(),
		Errors: hasErrors,
		Items:  items,
	})
}

func (s *Server) recordESBulkItem(action, result string) {
	s.promMetrics.RecordESBulkItem(action, result)
}

func esCompatEnabled(ingest config.IngestConfig) bool {
	if ingest.ESCompat.AdvertisedVersion == "" && ingest.ESCompat.ClusterName == "" {
		return true
	}
	return ingest.ESCompat.Enabled
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

// handleESIndexDoc appends the body of a single-document index request to
// the index of its path as one line.
func (s *Server) handleESIndexDoc(w http.ResponseWriter, r *http.Request) {
	setESHeaders(w)
	if s.esIngestDisabled(w) {
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
		respondESError(w, http.StatusBadRequest, "invalid_index_name_exception", err.Error())
		return
	}

	body, err := decompressBody(r)
	if err != nil {
		respondESError(w, http.StatusBadRequest, "mapper_parsing_exception",
			fmt.Sprintf("failed to decompress request body: %v", err))
		return
	}
	defer body.Close()

	raw, err := io.ReadAll(body)
	if err != nil {
		respondESError(w, http.StatusBadRequest, "mapper_parsing_exception", fmt.Sprintf("read body: %v", err))
		return
	}
	var line bytes.Buffer
	if err := json.Compact(&line, raw); err != nil || !esDocument(line.Bytes()) {
		reason := "body is not a JSON object"
		if err != nil {
			reason = fmt.Sprintf("invalid JSON: %v", err)
		}
		respondESError(w, http.StatusBadRequest, "mapper_parsing_exception", reason)
		return
	}
	line.WriteByte('\n')

	if _, err := s.db.AppendLines(indexName, line.Bytes()); err != nil {
		s.promMetrics.RecordIngestError()
		respondIngestError(w, err)
		return
	}
	s.promMetrics.RecordIngest(1, line.Len())

	respondJSON(w, http.StatusCreated, esIndexDocResponse{
		ID:     generateESDocID(),
		Index:  indexName,
		Result: "created",
	})
}

func (s *Server) handleESClusterInfo(w http.ResponseWriter, r *http.Request) {
	s.promMetrics.RecordESHandshake("cluster")
	if s.esHandshake == nil {
		respondInternalError(w, "elasticsearch compatibility handshake is not initialized")
		return
	}
	s.esHandshake.ServeHTTP(w, r)
}

func (s *Server) esCompatibilityHandler(kind string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.promMetrics.RecordESHandshake(kind)
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
