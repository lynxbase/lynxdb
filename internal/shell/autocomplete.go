package shell

import (
	"fmt"
	"sort"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/client"
)

// builtinFields are always present in the completion list regardless of
// whether the server has reported any fields yet.
var builtinFields = []string{"_timestamp", "_raw", "_source", "_time"}

// Completer provides context-aware SPL2 completions for the textinput.
type Completer struct {
	commands    []string
	aggFuncs    []string
	evalFuncs   []string
	keywords    []string
	slashCmds   []string
	fields      []string
	sources     []string
	fieldValues map[string][]string // field name → top value strings
}

// NewCompleter creates a completer with static SPL2 vocabulary.
func NewCompleter() *Completer {
	fields := make([]string, len(builtinFields))
	copy(fields, builtinFields)

	return &Completer{
		commands: []string{
			"FROM", "INDEX", "SEARCH", "WHERE", "STATS", "EVAL", "SORT", "HEAD", "TAIL", "REVERSE",
			"TIMECHART", "CHART", "REX", "REGEX", "REPLACE", "FIELDFORMAT", "FIELDS", "TABLE", "DEDUP", "RENAME", "BIN",
			"STREAMSTATS", "EVENTSTATS", "JOIN", "APPEND", "APPENDCOLS", "APPENDPIPE", "MULTISEARCH", "UNION", "TRANSACTION",
			"XYSERIES", "UNTABLE", "TOP", "RARE", "FILLNULL", "MATERIALIZE", "VIEWS", "DROPVIEW",
			"UNPACK_JSON", "UNPACK_LOGFMT", "UNPACK_SYSLOG", "UNPACK_COMBINED", "UNPACK_CLF", "UNPACK_NGINX_ERROR",
			"UNPACK_CEF", "UNPACK_KV", "UNPACK_DOCKER", "UNPACK_REDIS", "UNPACK_APACHE_ERROR", "UNPACK_POSTGRES",
			"UNPACK_MYSQL_SLOW", "UNPACK_HAPROXY", "UNPACK_LEEF", "UNPACK_W3C", "UNPACK_PATTERN", "JSON",
			"UNROLL", "MVEXPAND", "EXPAND", "MAKERESULTS", "MAKEMV", "MVCOMBINE", "NOMV", "PACK_JSON", "TEE",
			"ADDINFO", "CONVERT", "FIELDSUMMARY", "FLATTEN", "IPLOCATION", "TAGS", "TYPER", "THRU", "TIMEWRAP", "TSTATS", "MSTATS",
			"LET", "KEEP", "OMIT", "SELECT", "GROUP", "EVERY", "BUCKET", "ORDER", "TAKE",
			"RANK", "TOPBY", "BOTTOMBY", "BOTTOM", "RUNNING", "ENRICH", "PARSE", "EXPLODE",
			"PACK", "LOOKUP", "LATENCY", "ERRORS", "RATE", "PROPORTION", "IMPACT", "BASELINE",
			"CHANGES", "EXEMPLARS", "PERCENTILES", "SLOWEST", "GLIMPSE", "DESCRIBE", "USE", "OUTLIERS", "COMPARE",
			"PATTERNS", "TRACE", "ROLLUP", "CORRELATE", "SESSIONIZE", "TOPOLOGY",
		},
		aggFuncs: []string{
			"count", "sum", "avg", "min", "max", "dc", "values",
			"stdev", "perc50", "perc75", "perc90", "perc95", "perc99",
			"earliest", "latest",
		},
		evalFuncs: []string{
			"IF", "CASE", "match", "coalesce", "tonumber", "tostring",
			"round", "substr", "lower", "upper", "len", "ln",
			"mvjoin", "mvappend", "mvdedup", "isnotnull", "isnull", "strftime",
		},
		keywords: []string{
			"BY", "AS", "AND", "OR", "NOT", "IN", "SPAN",
			"ASC", "DESC", "TRUE", "FALSE", "NULL",
		},
		slashCmds: []string{
			"/help", "/quit", "/exit", "/clear", "/history",
			"/fields", "/sources", "/explain", "/set",
			"/format", "/server", "/timing", "/since",
			"/save", "/run", "/queries", "/tail",
		},
		fields: fields,
	}
}

// SetFields updates the dynamic field names for completion, preserving
// built-in fields that are always available.
func (c *Completer) SetFields(fields []string) {
	seen := make(map[string]struct{}, len(fields)+len(builtinFields))
	merged := make([]string, 0, len(fields)+len(builtinFields))

	for _, f := range builtinFields {
		merged = append(merged, f)
		seen[f] = struct{}{}
	}

	for _, f := range fields {
		if _, ok := seen[f]; !ok {
			merged = append(merged, f)
			seen[f] = struct{}{}
		}
	}

	c.fields = merged
}

// SetFieldValues populates known field values from the /fields API response.
func (c *Completer) SetFieldValues(infos []client.FieldInfo) {
	c.fieldValues = make(map[string][]string, len(infos))

	for _, fi := range infos {
		if len(fi.TopValues) == 0 {
			continue
		}

		vals := make([]string, 0, len(fi.TopValues))
		for _, tv := range fi.TopValues {
			if s := fmt.Sprintf("%v", tv.Value); s != "" {
				vals = append(vals, s)
			}
		}

		if len(vals) > 0 {
			c.fieldValues[fi.Name] = vals
		}
	}
}

// SetSources updates the source names for completion (used after FROM).
func (c *Completer) SetSources(sources []string) {
	c.sources = sources
}

// MergeResultFields adds field names discovered from query results,
// deduplicating against the existing fields list.
func (c *Completer) MergeResultFields(newFields []string) {
	existing := make(map[string]struct{}, len(c.fields))
	for _, f := range c.fields {
		existing[f] = struct{}{}
	}

	for _, f := range newFields {
		if _, ok := existing[f]; !ok {
			c.fields = append(c.fields, f)
			existing[f] = struct{}{}
		}
	}
}

// Suggest returns full-line completion suggestions that the textinput can match
// against the entire input value using its HasPrefix logic.
func (c *Completer) Suggest(value string) []string {
	if value == "" {
		return nil
	}

	// Slash commands — replace the entire input.
	if strings.HasPrefix(value, "/") {
		candidates := c.matchPrefix(value, c.slashCmds)
		return c.buildFullLine(value, 0, candidates, "")
	}

	// Field value pattern: field="partial → full-line with closing quote.
	if fieldName, partial, valueStart, ok := detectFieldValuePattern(value); ok {
		if values, found := c.fieldValues[fieldName]; found {
			candidates := c.matchValuePrefix(partial, values)

			// Determine whether we're inside quotes.
			hasOpenQuote := valueStart > 0 && value[valueStart-1] == '"'
			suffix := ""
			if hasOpenQuote {
				suffix = "\""
			}

			return c.buildFullLine(value, valueStart, candidates, suffix)
		}
	}

	// Find the current word being typed.
	word := lastWord(value)
	if word == "" {
		return nil
	}

	replaceStart := len(value) - len(word)

	// Context detection: what comes before the current word.
	before := strings.TrimSpace(value[:replaceStart])
	beforeLower := strings.ToLower(before)

	var candidates []string

	switch {
	// After | or at start → suggest commands.
	case before == "" || strings.HasSuffix(beforeLower, "|") || strings.HasSuffix(beforeLower, "| "):
		candidates = c.matchPrefixCI(word, c.commands)

	default:
		lastCmd := lastCommandWord(beforeLower)

		switch lastCmd {
		// Aggregation contexts: suggest agg functions + fields.
		case "stats", "timechart", "eventstats", "streamstats":
			candidates = c.matchPrefixCI(word, c.aggFuncs)
			candidates = append(candidates, c.matchPrefixCI(word, c.fields)...)

		// Field-selection contexts: suggest field names.
		case "by", "where", "eval", "sort", "table", "fields", "keep", "omit",
			"dedup", "rename", "top", "rare", "join", "on":
			candidates = c.matchPrefixCI(word, c.fields)

		// FROM context: suggest sources first, then fields as fallback.
		case "from":
			candidates = c.matchPrefixCI(word, c.sources)
			if len(candidates) == 0 {
				candidates = c.matchPrefixCI(word, c.fields)
			}

		// After AS: user is typing an alias name — no suggestions.
		case "as":
			candidates = nil

		// Default: match across all candidate pools including keywords.
		default:
			lowerWord := strings.ToLower(word)
			for _, list := range [][]string{c.commands, c.aggFuncs, c.evalFuncs, c.fields, c.keywords} {
				for _, s := range list {
					if strings.HasPrefix(strings.ToLower(s), lowerWord) && strings.ToLower(s) != lowerWord {
						candidates = append(candidates, s)
					}
				}
			}
		}
	}

	return c.buildFullLine(value, replaceStart, candidates, "")
}

// buildFullLine constructs full-line suggestions by replacing value[replaceStart:]
// with each candidate, appending suffix (e.g. closing quote) to each.
func (c *Completer) buildFullLine(value string, replaceStart int, candidates []string, suffix string) []string {
	if len(candidates) == 0 {
		return nil
	}

	prefix := value[:replaceStart]
	result := make([]string, 0, len(candidates))

	for _, cand := range candidates {
		full := prefix + cand + suffix
		// Skip if the suggestion exactly equals the current input.
		if full == value {
			continue
		}

		result = append(result, full)
	}

	return result
}

func (c *Completer) matchPrefix(prefix string, candidates []string) []string {
	var result []string
	lp := strings.ToLower(prefix)

	for _, s := range candidates {
		if strings.HasPrefix(strings.ToLower(s), lp) && strings.ToLower(s) != lp {
			result = append(result, s)
		}
	}

	return result
}

func (c *Completer) matchPrefixCI(word string, candidates []string) []string {
	var result []string
	lw := strings.ToLower(word)

	for _, s := range candidates {
		if strings.HasPrefix(strings.ToLower(s), lw) && strings.ToLower(s) != lw {
			result = append(result, s)
		}
	}

	return result
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

// detectFieldValuePattern detects field=value patterns and returns the field name,
// partial value typed so far, the byte offset where the value starts in input, and ok.
func detectFieldValuePattern(input string) (fieldName, partial string, valueStart int, ok bool) {
	trimmed := strings.TrimRight(input, " \t")
	eqIdx := strings.LastIndex(trimmed, "=")

	if eqIdx <= 0 {
		return "", "", 0, false
	}

	// Exclude !=, >=, <=
	if prev := trimmed[eqIdx-1]; prev == '!' || prev == '>' || prev == '<' {
		return "", "", 0, false
	}

	before := strings.TrimRight(trimmed[:eqIdx], " \t")
	fieldStart := strings.LastIndexAny(before, " \t|,(") + 1
	fieldName = before[fieldStart:]

	after := trimmed[eqIdx+1:]
	afterTrimmed := strings.TrimLeft(after, " \t")
	spacesSkipped := len(after) - len(afterTrimmed)

	// valueStart points into the original input, right after = plus spaces and optional quote.
	valueStart = eqIdx + 1 + spacesSkipped

	if strings.HasPrefix(afterTrimmed, "\"") {
		valueStart++ // skip the opening quote
		afterTrimmed = afterTrimmed[1:]
	}

	return fieldName, afterTrimmed, valueStart, fieldName != ""
}

func (c *Completer) matchValuePrefix(partial string, values []string) []string {
	if partial == "" {
		return values
	}

	lp := strings.ToLower(partial)

	var result []string
	for _, v := range values {
		if strings.HasPrefix(strings.ToLower(v), lp) && strings.ToLower(v) != lp {
			result = append(result, v)
		}
	}

	return result
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

// CompletionKind categorizes a completion item for display.
type CompletionKind int

const (
	KindCommand CompletionKind = iota
	KindField
	KindFunction
	KindValue
	KindKeyword
	KindSlashCmd
)

// CompletionItem represents a single completion candidate for the popup.
type CompletionItem struct {
	Text     string         // the completion text to insert
	Kind     CompletionKind // category for display
	FullLine string         // full-line suggestion (for applying)
}

// kindLabel returns a short label for display in the popup.
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
	default:
		return ""
	}
}

// SuggestAll returns all matching completion items for popup display.
// Each item includes the candidate text, its kind, and the full-line replacement.
func (c *Completer) SuggestAll(value string) []CompletionItem {
	if value == "" {
		return nil
	}

	// Slash commands.
	if strings.HasPrefix(value, "/") {
		candidates := c.matchPrefix(value, c.slashCmds)
		return c.buildItems(value, 0, candidates, "", KindSlashCmd)
	}

	// Field value pattern.
	if fieldName, partial, valueStart, ok := detectFieldValuePattern(value); ok {
		if values, found := c.fieldValues[fieldName]; found {
			candidates := c.matchValuePrefix(partial, values)
			hasOpenQuote := valueStart > 0 && value[valueStart-1] == '"'
			suffix := ""
			if hasOpenQuote {
				suffix = "\""
			}
			return c.buildItems(value, valueStart, candidates, suffix, KindValue)
		}
	}

	word := lastWord(value)
	if word == "" {
		return nil
	}

	replaceStart := len(value) - len(word)
	before := strings.TrimSpace(value[:replaceStart])
	beforeLower := strings.ToLower(before)

	var items []CompletionItem

	switch {
	case before == "" || strings.HasSuffix(beforeLower, "|") || strings.HasSuffix(beforeLower, "| "):
		items = c.buildItems(value, replaceStart, c.matchPrefixCI(word, c.commands), "", KindCommand)

	default:
		lastCmd := lastCommandWord(beforeLower)

		switch lastCmd {
		case "stats", "timechart", "eventstats", "streamstats":
			items = c.buildItems(value, replaceStart, c.matchPrefixCI(word, c.aggFuncs), "", KindFunction)
			items = append(items, c.buildItems(value, replaceStart, c.matchPrefixCI(word, c.fields), "", KindField)...)

		case "by", "where", "eval", "sort", "table", "fields", "keep", "omit",
			"dedup", "rename", "top", "rare", "join", "on":
			items = c.buildItems(value, replaceStart, c.matchPrefixCI(word, c.fields), "", KindField)

		case "from":
			items = c.buildItems(value, replaceStart, c.matchPrefixCI(word, c.sources), "", KindValue)
			if len(items) == 0 {
				items = c.buildItems(value, replaceStart, c.matchPrefixCI(word, c.fields), "", KindField)
			}

		case "as":
			// No suggestions while typing an alias.

		default:
			lowerWord := strings.ToLower(word)
			for _, pair := range []struct {
				list []string
				kind CompletionKind
			}{
				{c.commands, KindCommand},
				{c.aggFuncs, KindFunction},
				{c.evalFuncs, KindFunction},
				{c.fields, KindField},
				{c.keywords, KindKeyword},
			} {
				for _, s := range pair.list {
					if strings.HasPrefix(strings.ToLower(s), lowerWord) && strings.ToLower(s) != lowerWord {
						full := value[:replaceStart] + s
						items = append(items, CompletionItem{Text: s, Kind: pair.kind, FullLine: full})
					}
				}
			}
		}
	}

	return items
}

// buildItems constructs CompletionItems from candidates.
func (c *Completer) buildItems(value string, replaceStart int, candidates []string, suffix string, kind CompletionKind) []CompletionItem {
	if len(candidates) == 0 {
		return nil
	}

	prefix := value[:replaceStart]
	items := make([]CompletionItem, 0, len(candidates))

	for _, cand := range candidates {
		full := prefix + cand + suffix
		if full == value {
			continue
		}
		items = append(items, CompletionItem{Text: cand, Kind: kind, FullLine: full})
	}

	return items
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
