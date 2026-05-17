package shell

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/client"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

var builtinFields = []string{
	"_time", "_raw", "_source", "_sourcetype", "_timestamp",
	"host", "source", "sourcetype", "index",
}

var (
	currentWordRe = regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_.:-]*|\*)?$`)
	regexCtxRe    = regexp.MustCompile(`(?i)(?:=~|!~)(\s*["']?[^"'\s|()]*)$`)
	valueCtxRe    = regexp.MustCompile(`(?i)([A-Za-z_][A-Za-z0-9_.:-]*)\s*(?:==|!=|=|<=|>=|<|>)(\s*["']?([^"'\s|()]*))$`)
	timeCtxRe     = regexp.MustCompile(`(?i)\b(?:earliest|latest|_index_earliest|_index_latest)\s*=\s*([A-Za-z0-9@+\-]*)$`)
	sourceTimeRe  = regexp.MustCompile(`(?i)\b(?:from|index)\s+[^|\s]+\[$`)
	pipeStartRe   = regexp.MustCompile(`\|\s*\w*$`)
	sourceCtxRe   = regexp.MustCompile(`(?i)\b(?:from|index)\s+[A-Za-z0-9_.:$!*\-]*$`)
	fieldCtxRe    = regexp.MustCompile(`(?i)\b(?:by|where|group|order|keep|omit|on|table|fields|dedup|rename|using)\s+[A-Za-z0-9_.:-]*$`)
	fieldCommaRe  = regexp.MustCompile(`(?i)\b(?:by|keep|omit|table|fields|dedup)\s+[\w.:-]+(?:\s*,\s*[\w.:-]+)*,\s*[A-Za-z0-9_.:-]*$`)
	latencyRe     = regexp.MustCompile(`(?i)\blatency\s+[A-Za-z0-9_.:-]+\s+every\s+[0-9smhdw]+\s+(?:by\s+[A-Za-z0-9_.:-]+\s+)?compute\s+\w*$`)
	aggCtxRe      = regexp.MustCompile(`(?i)\b(?:compute|stats|timechart|eventstats|streamstats|enrich|running)\s+\w*$`)
	aggCommaRe    = regexp.MustCompile(`(?i)\b(?:compute|stats|timechart|eventstats|streamstats|enrich|running)\s+[\w().,\s]+,\s*\w*$`)
	evalCtxRe     = regexp.MustCompile(`(?i)\b(?:eval|let|where)\b[^|]*\w*$`)
)

const fieldValueCacheTTL = 30 * time.Second

type fieldValueCacheEntry struct {
	values  []CompletionItem
	fetched time.Time
}

// Completer provides context-aware SPL2 completions for the shell editor.
type Completer struct {
	commands  []CompletionItem
	aggFuncs  []CompletionItem
	evalFuncs []CompletionItem
	clauses   []CompletionItem
	operators []CompletionItem
	slashCmds []CompletionItem
	fields    []CompletionItem
	sources   []CompletionItem
	indexes   []CompletionItem

	fieldSet    map[string]struct{}
	fieldValues map[string][]CompletionItem
	valueCache  map[string]fieldValueCacheEntry
	client      *client.Client
	since       string
	now         func() time.Time
}

// NewCompleter creates a completer with static SPL2 vocabulary.
func NewCompleter() *Completer {
	c := &Completer{
		commands:  commandItems(spl2.KnownCommands()),
		aggFuncs:  functionItems(spl2.KnownAggregateFunctions(), "aggregate function"),
		evalFuncs: functionItems(appendCatalogs(spl2.KnownEvalFunctions(), spl2.KnownJSONFunctions()), "eval function"),
		clauses: completionItems([]string{
			"by", "as", "compute", "using", "extract", "if_missing", "per", "on",
			"into", "span", "window", "current", "maxspan", "startswith",
			"endswith", "type", "over", "limit", "cont", "usenull", "useother",
			"earliest", "latest", "_index_earliest", "_index_latest", "time",
			"asc", "desc", "inner", "outer", "left", "right",
		}, KindKeyword, "clause"),
		operators: completionItems([]string{
			"AND", "OR", "NOT", "XOR", "IN", "LIKE", "BETWEEN", "IS", "NULL",
		}, KindKeyword, "operator"),
		slashCmds: completionItems([]string{
			"/help", "/quit", "/exit", "/clear", "/history", "/fields", "/sources",
			"/explain", "/set", "/format", "/server", "/timing", "/since", "/save",
			"/run", "/queries", "/tail",
		}, KindSlashCmd, "shell command"),
		fieldSet:   map[string]struct{}{},
		valueCache: map[string]fieldValueCacheEntry{},
		now:        time.Now,
	}
	c.SetFields(nil)

	return c
}

func appendCatalogs(groups ...[]string) []string {
	total := 0
	for _, group := range groups {
		total += len(group)
	}

	out := make([]string, 0, total)
	for _, group := range groups {
		out = append(out, group...)
	}

	return out
}

func commandItems(commands []string) []CompletionItem {
	items := make([]CompletionItem, 0, len(commands)+len(queryTemplates))
	for _, cmd := range commands {
		cmd = strings.ToLower(cmd)
		detail := commandDocs[cmd]
		if detail == "" {
			detail = "command"
		}
		items = append(items, CompletionItem{
			Text:   cmd,
			Apply:  cmd,
			Kind:   KindCommand,
			Detail: detail,
			Boost:  2,
		})
	}
	items = append(items, queryTemplates...)

	return items
}

func functionItems(fns []string, detail string) []CompletionItem {
	return completionItems(fns, KindFunction, detail)
}

func completionItems(values []string, kind CompletionKind, detail string) []CompletionItem {
	items := make([]CompletionItem, 0, len(values))
	for _, value := range values {
		items = append(items, CompletionItem{
			Text:   value,
			Apply:  value,
			Kind:   kind,
			Detail: detail,
		})
	}

	return items
}

var commandDocs = map[string]string{
	"from": "source scope", "index": "source scope alias", "search": "SPL search filter",
	"where": "boolean filter", "stats": "aggregate rows", "eval": "compute fields",
	"sort": "sort rows", "head": "first rows", "tail": "last rows",
	"reverse": "reverse row order", "timechart": "time-bucket aggregate",
	"chart": "chart-style aggregate", "rex": "regex extraction", "regex": "regex filter",
	"replace": "replace field values", "fieldformat": "format display values",
	"fields": "keep/remove fields", "table": "project columns", "dedup": "deduplicate rows",
	"rename": "rename fields", "bin": "bucket values", "streamstats": "running statistics",
	"eventstats": "add aggregate fields", "join": "join datasets", "append": "append subsearch",
	"appendcols": "append subsearch columns", "appendpipe": "append subpipe result",
	"multisearch": "combine subsearches", "union": "merge result sets",
	"transaction": "group events", "xyseries": "pivot rows", "untable": "unpivot rows",
	"top": "frequent values", "rare": "least frequent values", "fillnull": "fill missing values",
	"materialize": "create materialized view", "views": "inspect views",
	"dropview": "drop materialized view", "unpack_json": "extract JSON fields",
	"json": "extract JSON paths", "unroll": "expand array values",
	"mvexpand": "expand multivalue field", "expand": "expand array values",
	"makeresults": "generate test rows", "makemv": "split multivalue field",
	"mvcombine": "combine rows into multivalue", "nomv": "join multivalue field",
	"pack_json": "pack fields as JSON", "tee": "copy stream to sink",
	"let": "eval alias", "keep": "fields + alias", "omit": "fields - alias",
	"select": "table with aliases", "group": "stats sugar", "every": "timechart sugar",
	"bucket": "bin sugar", "order": "sort sugar", "take": "head sugar",
	"rank": "sort/head sugar", "topby": "rank groups by metric",
	"bottomby": "rank groups ascending", "bottom": "bottom metric ranking",
	"running": "streamstats sugar", "enrich": "eventstats sugar",
	"parse": "structured extraction", "explode": "unroll sugar", "pack": "pack_json sugar",
	"lookup": "left lookup join", "latency": "p50/p95/p99 timechart",
	"errors": "level error/fatal aggregate", "rate": "count per time bucket",
	"proportion": "matching event ratio", "impact": "contribution by group",
	"baseline": "rolling baseline", "changes": "changed values",
	"exemplars": "representative rows", "percentiles": "p50..p99 aggregate",
	"slowest": "sort by duration", "glimpse": "field/value summary",
	"describe": "schema/source metadata", "use": "named fragment",
	"outliers": "mark statistical outliers", "compare": "previous-window comparison",
	"patterns": "message templates", "trace": "span tree fields",
	"rollup": "multiple time resolutions", "correlate": "field correlation",
	"sessionize": "time-gap sessions", "topology": "edge/node summaries",
}

var queryTemplates = []CompletionItem{
	{Text: "errors by service", Apply: "errors by service", Kind: KindTemplate, Detail: "shortcut", Boost: 1},
	{Text: "latency duration_ms every 1m", Apply: "latency duration_ms every 1m", Kind: KindTemplate, Detail: "shortcut", Boost: 1},
	{Text: "rate per 1m by service", Apply: "rate per 1m by service", Kind: KindTemplate, Detail: "shortcut", Boost: 1},
	{Text: "proportion status>=500 AS error_rate by service", Apply: "proportion status>=500 AS error_rate by service", Kind: KindTemplate, Detail: "shortcut", Boost: 1},
	{Text: "impact by service", Apply: "impact by service", Kind: KindTemplate, Detail: "shortcut", Boost: 1},
	{Text: "baseline error_rate window=12 by service", Apply: "baseline error_rate window=12 by service", Kind: KindTemplate, Detail: "shortcut", Boost: 1},
	{Text: "changes version by service", Apply: "changes version by service", Kind: KindTemplate, Detail: "shortcut", Boost: 1},
	{Text: "exemplars 3 by service", Apply: "exemplars 3 by service", Kind: KindTemplate, Detail: "shortcut", Boost: 1},
	{Text: "percentiles duration_ms by service", Apply: "percentiles duration_ms by service", Kind: KindTemplate, Detail: "shortcut", Boost: 1},
	{Text: "slowest 10 by duration_ms", Apply: "slowest 10 by duration_ms", Kind: KindTemplate, Detail: "shortcut", Boost: 1},
	{Text: "status>=500", Apply: "status>=500", Kind: KindTemplate, Detail: "free-hand search", Boost: 1},
	{Text: `"connection reset"`, Apply: `"connection reset"`, Kind: KindTemplate, Detail: "phrase search", Boost: 1},
}

var sourceTemplates = []CompletionItem{
	{Text: "*", Apply: "*", Kind: KindSource, Detail: "all authorized sources"},
	{Text: "logs*", Apply: "logs*", Kind: KindSource, Detail: "source glob"},
	{Text: "$fragment", Apply: "$fragment", Kind: KindSource, Detail: "CTE/source fragment"},
}

var timeTemplates = []CompletionItem{
	{Text: "[-15m]", Apply: "[-15m]", Kind: KindConstant, Detail: "last 15 minutes"},
	{Text: "[-1h]", Apply: "[-1h]", Kind: KindConstant, Detail: "last hour"},
	{Text: "[-24h]", Apply: "[-24h]", Kind: KindConstant, Detail: "last 24 hours"},
	{Text: "[-7d..-1d]", Apply: "[-7d..-1d]", Kind: KindConstant, Detail: "relative range"},
}

var timeValues = []CompletionItem{
	{Text: "-15m", Apply: "-15m", Kind: KindConstant, Detail: "relative time"},
	{Text: "-1h", Apply: "-1h", Kind: KindConstant, Detail: "relative time"},
	{Text: "-24h", Apply: "-24h", Kind: KindConstant, Detail: "relative time"},
	{Text: "-7d@d", Apply: "-7d@d", Kind: KindConstant, Detail: "snap to day"},
	{Text: "now", Apply: "now", Kind: KindConstant, Detail: "time modifier alias"},
	{Text: "now()", Apply: "now()", Kind: KindFunction, Detail: "time modifier alias"},
}

var regexTemplates = []CompletionItem{
	{Text: `"(?i)error|fatal"`, Apply: `"(?i)error|fatal"`, Kind: KindTemplate, Detail: "linear regex"},
	{Text: `"timeout|timed out"`, Apply: `"timeout|timed out"`, Kind: KindTemplate, Detail: "linear regex"},
	{Text: `"(?<field>\\w+)"`, Apply: `"(?<field>\\w+)"`, Kind: KindTemplate, Detail: "named capture"},
}

var latencyAggShorthands = completionItems([]string{
	"p50", "p75", "p90", "p95", "p99", "avg", "max", "count",
}, KindFunction, "latency aggregate")

// SetClient enables lazy field value completion via /fields/{field}/values.
func (c *Completer) SetClient(cl *client.Client) {
	c.client = cl
}

// SetSince scopes lazy value lookups to the shell time range when available.
func (c *Completer) SetSince(since string) {
	c.since = strings.TrimPrefix(since, "-")
}

// SetFields updates dynamic field names for completion, preserving built-ins.
func (c *Completer) SetFields(fields []string) {
	seen := make(map[string]struct{}, len(fields)+len(builtinFields))
	merged := make([]CompletionItem, 0, len(fields)+len(builtinFields))

	for _, f := range builtinFields {
		merged = append(merged, CompletionItem{Text: f, Apply: f, Kind: KindField, Detail: "built-in", Boost: 3})
		seen[f] = struct{}{}
	}

	for _, f := range fields {
		if f == "" {
			continue
		}
		if _, ok := seen[f]; ok {
			continue
		}
		merged = append(merged, CompletionItem{Text: f, Apply: f, Kind: KindField, Detail: "field", Boost: 2})
		seen[f] = struct{}{}
	}

	c.fields = merged
	c.fieldSet = seen
}

// SetFieldValues populates known field values from the /fields API response.
func (c *Completer) SetFieldValues(infos []client.FieldInfo) {
	c.fieldValues = make(map[string][]CompletionItem, len(infos))

	for _, fi := range infos {
		for i := range c.fields {
			if c.fields[i].Text == fi.Name && fi.Type != "" {
				c.fields[i].Detail = fi.Type
			}
		}

		if len(fi.TopValues) == 0 {
			continue
		}

		vals := make([]CompletionItem, 0, len(fi.TopValues))
		for _, tv := range fi.TopValues {
			if s := fmt.Sprintf("%v", tv.Value); s != "" {
				vals = append(vals, valueCompletion(s, tv.Count))
			}
		}

		if len(vals) > 0 {
			c.fieldValues[fi.Name] = vals
		}
	}
}

// SetSources updates the source names for completion after FROM/INDEX.
func (c *Completer) SetSources(sources []string) {
	c.sources = sourceItems(sources, "source")
}

// SetIndexes updates index names for completion after FROM/INDEX.
func (c *Completer) SetIndexes(indexes []string) {
	c.indexes = sourceItems(indexes, "source")
}

func sourceItems(names []string, detail string) []CompletionItem {
	items := make([]CompletionItem, 0, len(names))
	for _, name := range names {
		if name == "" {
			continue
		}
		items = append(items, CompletionItem{Text: name, Apply: name, Kind: KindSource, Detail: detail, Boost: 3})
	}

	return items
}

// MergeResultFields adds field names discovered from query results.
func (c *Completer) MergeResultFields(newFields []string) {
	for _, f := range newFields {
		if f == "" {
			continue
		}
		if _, ok := c.fieldSet[f]; !ok {
			c.fields = append(c.fields, CompletionItem{Text: f, Apply: f, Kind: KindField, Detail: "result field"})
			c.fieldSet[f] = struct{}{}
		}
	}
}

// Suggest returns full-line suggestions for ghost completion.
func (c *Completer) Suggest(value string) []string {
	items := c.SuggestAll(value)
	if len(items) == 0 {
		return nil
	}

	result := make([]string, 0, len(items))
	for _, item := range items {
		if item.FullLine != "" {
			result = append(result, item.FullLine)
		}
	}

	return result
}

// CompletionKind categorizes a completion item for display.
type CompletionKind int

const (
	KindCommand CompletionKind = iota
	KindField
	KindFunction
	KindValue
	KindKeyword
	KindSlashCmd
	KindSource
	KindTemplate
	KindConstant
)

// CompletionItem represents a single completion candidate for the popup.
type CompletionItem struct {
	Text     string
	Apply    string
	Kind     CompletionKind
	Detail   string
	Boost    int
	FullLine string
}

func (k CompletionKind) kindLabel() string {
	switch k {
	case KindCommand:
		return "cmd"
	case KindField:
		return "field"
	case KindFunction:
		return "fn"
	case KindValue:
		return "val"
	case KindKeyword:
		return "key"
	case KindSlashCmd:
		return "/"
	case KindSource:
		return "src"
	case KindTemplate:
		return "tpl"
	case KindConstant:
		return "const"
	default:
		return ""
	}
}

// SuggestAll returns matching completion items for popup display.
func (c *Completer) SuggestAll(value string) []CompletionItem {
	if value == "" {
		return nil
	}

	beforeCursor := value
	word := currentWord(beforeCursor)
	replaceStart := len(value) - len(word)
	lowerWord := strings.ToLower(word)

	if strings.HasPrefix(value, "/") {
		return withFullLine(value, 0, filterItems(c.slashCmds, strings.ToLower(value)), "")
	}

	if match := regexCtxRe.FindStringSubmatchIndex(beforeCursor); len(match) >= 4 {
		from := match[2]
		typed := strings.TrimLeft(beforeCursor[from:], " \t")
		from += len(beforeCursor[from:]) - len(typed)

		return withFullLine(value, from, regexTemplates, "")
	}

	if match := valueCtxRe.FindStringSubmatchIndex(beforeCursor); len(match) >= 8 {
		fieldName := beforeCursor[match[2]:match[3]]
		typedValue := strings.TrimLeft(beforeCursor[match[4]:match[5]], " \t")
		partial := beforeCursor[match[6]:match[7]]
		if c.knownField(fieldName) {
			values := c.valuesForField(fieldName)
			if len(values) > 0 {
				from := len(value) - max(len(typedValue), len(partial))
				if strings.HasPrefix(typedValue, `"`) || strings.HasPrefix(typedValue, `'`) {
					for i := range values {
						values[i].Apply = `"` + escapeCompletionValue(values[i].Text) + `"`
					}
				}

				return withFullLine(value, from, filterItems(values, strings.ToLower(partial)), "")
			}
		}
	}

	if match := timeCtxRe.FindStringSubmatchIndex(beforeCursor); len(match) >= 4 {
		from := match[2]

		return withFullLine(value, from, filterItems(timeValues, strings.ToLower(beforeCursor[from:])), "")
	}

	if sourceTimeRe.MatchString(beforeCursor) {
		return withFullLine(value, len(value)-1, timeTemplates, "")
	}

	if pipeStartRe.MatchString(beforeCursor) || strings.TrimSpace(beforeCursor) == word {
		if word == "" && strings.TrimSpace(beforeCursor) == "" {
			return nil
		}

		return withFullLine(value, replaceStart, filterItems(c.commands, lowerWord), "")
	}

	if sourceCtxRe.MatchString(beforeCursor) {
		items := append([]CompletionItem{}, c.indexes...)
		items = append(items, c.sources...)
		items = append(items, sourceTemplates...)

		return withFullLine(value, replaceStart, filterItems(dedupeItems(items), lowerWord), "")
	}

	if evalCtxRe.MatchString(beforeCursor) {
		items := append([]CompletionItem{}, c.evalFuncs...)
		items = append(items, c.fields...)
		items = append(items, c.operators...)

		return withFullLine(value, replaceStart, filterItems(items, lowerWord), "")
	}

	if fieldCommaRe.MatchString(beforeCursor) || fieldCtxRe.MatchString(beforeCursor) {
		return withFullLine(value, replaceStart, filterItems(c.fields, lowerWord), "")
	}

	if latencyRe.MatchString(beforeCursor) {
		return withFullLine(value, replaceStart, filterItems(latencyAggShorthands, lowerWord), "")
	}

	if aggCtxRe.MatchString(beforeCursor) || aggCommaRe.MatchString(beforeCursor) {
		return withFullLine(value, replaceStart, filterItems(c.aggFuncs, lowerWord), "")
	}

	if word == "" {
		return nil
	}

	if len(word) >= 2 {
		return withFullLine(value, replaceStart, filterItems(c.fields, lowerWord), "")
	}

	return nil
}

func currentWord(line string) string {
	m := currentWordRe.FindStringSubmatch(line)
	if len(m) == 0 {
		return ""
	}

	return m[0]
}

func filterItems(items []CompletionItem, lowerWord string) []CompletionItem {
	if lowerWord == "" {
		return applyBoost(items, lowerWord)
	}

	out := make([]CompletionItem, 0, len(items))
	for _, item := range items {
		if strings.HasPrefix(strings.ToLower(item.Text), lowerWord) &&
			strings.ToLower(item.Text) != lowerWord {
			out = append(out, item)
		}
	}

	return applyBoost(out, lowerWord)
}

func applyBoost(items []CompletionItem, lowerWord string) []CompletionItem {
	out := make([]CompletionItem, 0, len(items))
	for _, item := range items {
		if item.Apply == "" {
			item.Apply = item.Text
		}
		if lowerWord != "" && strings.HasPrefix(strings.ToLower(item.Text), lowerWord) {
			item.Boost += 2
		}
		out = append(out, item)
	}

	return out
}

func sortItems(items []CompletionItem) {
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Boost != items[j].Boost {
			return items[i].Boost > items[j].Boost
		}
		if items[i].Kind != items[j].Kind {
			return items[i].Kind < items[j].Kind
		}

		return strings.ToLower(items[i].Text) < strings.ToLower(items[j].Text)
	})
}

func withFullLine(value string, replaceStart int, items []CompletionItem, suffix string) []CompletionItem {
	if len(items) == 0 {
		return nil
	}

	prefix := value[:replaceStart]
	out := make([]CompletionItem, 0, len(items))
	for _, item := range items {
		apply := item.Apply
		if apply == "" {
			apply = item.Text
		}
		full := prefix + apply + suffix
		if full == value {
			continue
		}
		item.Apply = apply
		item.FullLine = full
		out = append(out, item)
	}
	sortItems(out)

	return out
}

func dedupeItems(items []CompletionItem) []CompletionItem {
	seen := make(map[string]struct{}, len(items))
	out := make([]CompletionItem, 0, len(items))
	for _, item := range items {
		if _, ok := seen[item.Text]; ok {
			continue
		}
		seen[item.Text] = struct{}{}
		out = append(out, item)
	}

	return out
}

func (c *Completer) knownField(field string) bool {
	_, ok := c.fieldSet[field]

	return ok
}

func (c *Completer) valuesForField(fieldName string) []CompletionItem {
	if cached, ok := c.valueCache[fieldName]; ok && c.now().Sub(cached.fetched) < fieldValueCacheTTL {
		return cloneItems(cached.values)
	}

	if c.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
		defer cancel()

		result, err := c.client.FieldValuesFiltered(ctx, fieldName, client.FieldValuesOpts{
			Limit: 20,
			Since: c.since,
		})
		if err == nil && result != nil {
			values := make([]CompletionItem, 0, len(result.Values))
			for _, v := range result.Values {
				if s := fmt.Sprintf("%v", v.Value); s != "" {
					values = append(values, valueCompletion(s, v.Count))
				}
			}
			c.valueCache[fieldName] = fieldValueCacheEntry{values: values, fetched: c.now()}
			if len(values) > 0 {
				return cloneItems(values)
			}
		}
	}

	return cloneItems(c.fieldValues[fieldName])
}

func cloneItems(items []CompletionItem) []CompletionItem {
	out := make([]CompletionItem, len(items))
	copy(out, items)

	return out
}

func escapeCompletionValue(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), `"`, `\"`)
}

func valueCompletion(value string, count int64) CompletionItem {
	apply := value
	if value == "" || strings.ContainsAny(value, " \t|()[]{},<>!=") {
		apply = `"` + escapeCompletionValue(value) + `"`
	}

	return CompletionItem{
		Text:   value,
		Apply:  apply,
		Kind:   KindValue,
		Detail: fmt.Sprintf("%d", count),
	}
}

// lastWord returns the last whitespace-delimited word from s.
func lastWord(s string) string {
	s = strings.TrimRight(s, " \t")
	lastSpace := strings.LastIndexAny(s, " \t")
	if lastSpace < 0 {
		return s
	}

	return s[lastSpace+1:]
}

// lastCommandWord returns the last significant SPL2 keyword in the text before cursor.
func lastCommandWord(s string) string {
	words := strings.Fields(s)
	for i := len(words) - 1; i >= 0; i-- {
		w := strings.TrimRight(words[i], ",|()")
		if w != "" {
			return w
		}
	}

	return ""
}

// extractFieldNames collects unique field names from result rows.
// It samples up to the first 10 rows to avoid scanning large result sets.
func extractFieldNames(rows []map[string]interface{}) []string {
	seen := make(map[string]struct{})

	limit := 10
	if len(rows) < limit {
		limit = len(rows)
	}

	for i := 0; i < limit; i++ {
		for k := range rows[i] {
			seen[k] = struct{}{}
		}
	}

	names := make([]string, 0, len(seen))
	for k := range seen {
		names = append(names, k)
	}

	sort.Strings(names)

	return names
}
