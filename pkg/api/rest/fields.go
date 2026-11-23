package rest

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/engine/columnar"
	"github.com/lynxbase/lynxdb/pkg/query"
	"github.com/lynxbase/lynxdb/pkg/timerange"
)

// pplName quotes an index or field name for a PPL query; * stays as it is.
func pplName(name string) (string, error) {
	if name == "*" {
		return name, nil
	}
	if name == "" || strings.ContainsAny(name, "`\n\r") {
		return "", fmt.Errorf("name %q cannot be used in a query", name)
	}

	return "`" + name + "`", nil
}

// runInternalQuery compiles and runs a query the server builds itself over
// [from, to); a zero end leaves that side open.
func (s *Server) runInternalQuery(r *http.Request, src string, from, to time.Time) (*columnar.Result, error) {
	c, err := query.Compile(src)
	if err != nil {
		return nil, err
	}
	opts, _ := queryTimeRange("", "")
	opts.From, opts.To = from, to

	return s.db.Query(r.Context(), c, opts)
}

// handleListFields lists the fields of the indexes, each with the indexes
// that hold it; ?index= or ?source= narrows them to one index and ?prefix=
// to the names that start with it.
func (s *Server) handleListFields(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeQuery) {
		return
	}
	s.syncForRead()
	names := s.db.Names()
	if index := firstNonEmpty(r.URL.Query().Get("index"), r.URL.Query().Get("source")); index != "" {
		names = []string{index}
	}
	prefix := strings.TrimSpace(r.URL.Query().Get("prefix"))
	byField := map[string][]string{}
	for _, index := range names {
		fields, ok := s.db.Fields(index)
		if !ok {
			continue
		}
		for _, f := range fields {
			if strings.HasPrefix(f, prefix) {
				byField[f] = append(byField[f], index)
			}
		}
	}
	out := make([]map[string]interface{}, 0, len(byField))
	for name, indexes := range byField {
		out = append(out, map[string]interface{}{"name": name, "indexes": indexes})
	}
	sort.Slice(out, func(i, j int) bool { return out[i]["name"].(string) < out[j]["name"].(string) })
	respondData(w, http.StatusOK, map[string]interface{}{"fields": out})
}

// handleFieldValues returns the most frequent values of a field with their
// counts, among the events that hold it.
func (s *Server) handleFieldValues(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeQuery) {
		return
	}
	start := time.Now()
	fieldName, ok := requirePathValue(r, w, "name")
	if !ok {
		return
	}
	limit := parseIntParam(r, "limit", 10)
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}
	field, err := pplName(fieldName)
	if err != nil {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error())
		return
	}
	index, err := pplName(firstNonEmpty(r.URL.Query().Get("index"), "*"))
	if err != nil {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error())
		return
	}
	now := time.Now()
	tr, err := timerange.ParseOptionalRange(
		firstNonEmpty(r.URL.Query().Get("from"), sinceParam(r.URL.Query().Get("since"))), r.URL.Query().Get("to"), now)
	if err != nil {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error())
		return
	}
	var from, to time.Time
	if tr != nil {
		from, to = tr.Earliest, tr.Latest
	}

	// Events without the field would form a group of their own.
	src := fmt.Sprintf("source=%s | where isnotnull(%s) | stats count() as count by %s | sort - count | head %d",
		index, field, field, limit)
	res, err := s.runInternalQuery(r, src, from, to)
	if err != nil {
		respondQueryFailure(w, err)
		return
	}
	values := make([]map[string]interface{}, 0, res.Len())
	for _, row := range res.Maps() {
		var value interface{}
		if v, ok := row[fieldName]; ok {
			value = v.Interface()
		}
		var count interface{}
		if v, ok := row["count"]; ok {
			count = v.Interface()
		}
		values = append(values, map[string]interface{}{"value": value, "count": count})
	}

	respondData(w, http.StatusOK, map[string]interface{}{
		"field":  fieldName,
		"values": values,
	}, WithTook(time.Since(start)))
}

// sinceParam turns a since of the form 15m into the relative time -15m.
func sinceParam(since string) string {
	since = strings.TrimSpace(since)
	if since == "" || strings.HasPrefix(since, "-") {
		return since
	}

	return "-" + since
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}

	return ""
}
