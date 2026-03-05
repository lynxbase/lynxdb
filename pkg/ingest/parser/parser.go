package parser

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// LoadIndex loads a log file and parses it into ResultRows for an index.
func LoadIndex(path, indexName, format string) ([]spl2.ResultRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var rows []spl2.ResultRow
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		row, err := parseLine(line, indexName, format)
		if err != nil {
			continue
		}
		rows = append(rows, row)
	}

	return rows, scanner.Err()
}

func parseLine(line, indexName, format string) (spl2.ResultRow, error) {
	switch format {
	case "nginx":
		return parseNginx(line, indexName)
	case "audit":
		return parseAudit(line, indexName)
	case "json":
		return parseJSON(line, indexName)
	case "frontend":
		return parseFrontend(line, indexName)
	case "transactions":
		return parseTransactions(line, indexName)
	default:
		return spl2.ResultRow{}, fmt.Errorf("unknown format: %s", format)
	}
}

var nginxRe = regexp.MustCompile(`^(\S+) - (\S+) \[([^\]]+)\] "(\S+) (\S+?)(?:\?(\S+))? HTTP/\S+" (\d+) (\d+) "([^"]*)" "([^"]*)" rt=(\S+)$`)

func parseNginx(line, indexName string) (spl2.ResultRow, error) {
	m := nginxRe.FindStringSubmatch(line)
	if m == nil {
		return spl2.ResultRow{}, fmt.Errorf("no match")
	}

	t, _ := time.Parse("02/Jan/2006:15:04:05 -0700", m[3])
	status, _ := strconv.ParseInt(m[7], 10, 64)
	bytesVal, _ := strconv.ParseInt(m[8], 10, 64)
	rt, _ := strconv.ParseFloat(m[11], 64)

	fields := map[string]interface{}{
		"_raw":          line,
		"_time":         t.UnixNano(),
		"index":         indexName,
		"timestamp":     t.Format(time.RFC3339Nano),
		"clientip":      m[1],
		"user":          m[2],
		"method":        m[4],
		"uri_path":      m[5],
		"request_args":  m[6],
		"status":        status,
		"bytes":         bytesVal,
		"referer":       m[9],
		"user_agent":    m[10],
		"response_time": rt,
	}

	return spl2.ResultRow{Fields: fields}, nil
}

var auditTimestampRe = regexp.MustCompile(`^(\S+)\s+audit\[(\d+)\]:\s+type=(\S+)\s+(.*)$`)
var auditKVRe = regexp.MustCompile(`(\w+)=("[^"]*"|\S+)`)

func parseAudit(line, indexName string) (spl2.ResultRow, error) {
	m := auditTimestampRe.FindStringSubmatch(line)
	if m == nil {
		return spl2.ResultRow{}, fmt.Errorf("no match")
	}

	t, _ := time.Parse(time.RFC3339Nano, m[1])
	fields := map[string]interface{}{
		"_raw":      line,
		"_time":     t.UnixNano(),
		"index":     indexName,
		"timestamp": t.Format(time.RFC3339Nano),
		"type":      m[3],
	}

	rest := m[4]
	kvMatches := auditKVRe.FindAllStringSubmatch(rest, -1)
	for _, kv := range kvMatches {
		key := kv[1]
		val := strings.Trim(kv[2], `"`)
		if key == "type" {
			continue
		}
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			fields[key] = i
		} else {
			fields[key] = val
		}
	}

	msgIdx := strings.Index(rest, "msg='")
	if msgIdx >= 0 {
		msgStart := msgIdx + 5
		msgEnd := strings.Index(rest[msgStart:], "'")
		if msgEnd >= 0 {
			innerMsg := rest[msgStart : msgStart+msgEnd]
			fields["msg"] = innerMsg
			innerKVs := auditKVRe.FindAllStringSubmatch(innerMsg, -1)
			for _, kv := range innerKVs {
				key := kv[1]
				val := strings.Trim(kv[2], `"`)
				fields[key] = val
			}
		}
	}

	// Audit log "key=" field appears at end of line.
	keyRe := regexp.MustCompile(`\bkey=(\S+)`)
	km := keyRe.FindStringSubmatch(line)
	if km != nil {
		fields["key"] = km[1]
	}

	return spl2.ResultRow{Fields: fields}, nil
}

func parseJSON(line, indexName string) (spl2.ResultRow, error) {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(line), &data); err != nil {
		return spl2.ResultRow{}, err
	}

	fields := map[string]interface{}{
		"_raw":  line,
		"index": indexName,
	}

	for k, v := range data {
		switch val := v.(type) {
		case float64:
			// Preserve int types.
			if val == float64(int64(val)) && !strings.Contains(fmt.Sprint(v), ".") {
				fields[k] = int64(val)
			} else {
				fields[k] = val
			}
		default:
			fields[k] = v
		}
	}

	if ts, ok := data["timestamp"].(string); ok {
		t, err := time.Parse(time.RFC3339Nano, ts)
		if err == nil {
			fields["_time"] = t.UnixNano()
			fields["timestamp"] = ts
		}
	}

	// Coerce known numeric fields from float64 (JSON default) to int64.
	for _, key := range []string{"status", "duration_ms", "amount_cents"} {
		if v, ok := fields[key]; ok {
			if val, isFloat := v.(float64); isFloat {
				fields[key] = int64(val)
			}
		}
	}

	return spl2.ResultRow{Fields: fields}, nil
}

var frontendRe = regexp.MustCompile(`^(\S+)\s+\[(\w+)\]\s+\[([^\]]+)\]\s+(.+)$`)

func parseFrontend(line, indexName string) (spl2.ResultRow, error) {
	m := frontendRe.FindStringSubmatch(line)
	if m == nil {
		return spl2.ResultRow{}, fmt.Errorf("no match")
	}

	t, _ := time.Parse(time.RFC3339Nano, m[1])
	fields := map[string]interface{}{
		"_raw":      line,
		"_time":     t.UnixNano(),
		"index":     indexName,
		"timestamp": t.Format(time.RFC3339Nano),
		"level":     m[2],
		"component": m[3],
	}

	rest := m[4]
	// Split message from pipe-delimited KV pairs.
	parts := strings.Split(rest, " | ")
	if len(parts) > 0 {
		fields["message"] = strings.TrimSpace(parts[0])
	}

	for _, part := range parts[1:] {
		part = strings.TrimSpace(part)
		if eqIdx := strings.Index(part, "="); eqIdx > 0 {
			key := strings.TrimSpace(part[:eqIdx])
			val := strings.TrimSpace(part[eqIdx+1:])
			if i, err := strconv.ParseInt(val, 10, 64); err == nil {
				fields[key] = i
			} else if f, err := strconv.ParseFloat(val, 64); err == nil {
				fields[key] = f
			} else {
				fields[key] = val
			}
		}
	}

	return spl2.ResultRow{Fields: fields}, nil
}

func parseTransactions(line, indexName string) (spl2.ResultRow, error) {
	parts := strings.Split(line, "|")
	if len(parts) < 4 {
		return spl2.ResultRow{}, fmt.Errorf("too few fields")
	}

	t, _ := time.Parse(time.RFC3339Nano, strings.TrimSpace(parts[0]))
	fields := map[string]interface{}{
		"_raw":        line,
		"_time":       t.UnixNano(),
		"index":       indexName,
		"timestamp":   t.Format(time.RFC3339Nano),
		"txn_id":      strings.TrimSpace(parts[1]),
		"actor":       strings.TrimSpace(parts[2]),
		"action_type": strings.TrimSpace(parts[3]),
	}

	for _, part := range parts[4:] {
		part = strings.TrimSpace(part)
		if eqIdx := strings.Index(part, "="); eqIdx > 0 {
			key := strings.TrimSpace(part[:eqIdx])
			val := strings.Trim(strings.TrimSpace(part[eqIdx+1:]), `"`)
			if i, err := strconv.ParseInt(val, 10, 64); err == nil {
				fields[key] = i
			} else if f, err := strconv.ParseFloat(val, 64); err == nil {
				fields[key] = f
			} else {
				fields[key] = val
			}
		}
	}

	return spl2.ResultRow{Fields: fields}, nil
}

// LoadAllIndexes loads all 5 test indexes from the testdata/logs directory.
func LoadAllIndexes(logsDir string) (*spl2.IndexStore, error) {
	store := &spl2.IndexStore{Indexes: make(map[string][]spl2.ResultRow)}

	indexes := []struct {
		name   string
		file   string
		format string
	}{
		{"idx_nginx", "nginx_access.log", "nginx"},
		{"idx_audit", "audit_security.log", "audit"},
		{"idx_backend", "backend_server.log", "json"},
		{"idx_frontend", "frontend_console.log", "frontend"},
		{"idx_transactions", "audit_transactions.log", "transactions"},
	}

	for _, idx := range indexes {
		rows, err := LoadIndex(logsDir+"/"+idx.file, idx.name, idx.format)
		if err != nil {
			return nil, fmt.Errorf("loading %s: %w", idx.name, err)
		}
		store.Indexes[idx.name] = rows
	}

	return store, nil
}
