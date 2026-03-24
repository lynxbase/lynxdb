package spl2

import (
	"fmt"
	"strings"
)

// Program represents a full SPL2 program with optional CTEs and a main query.
type Program struct {
	Datasets []DatasetDef
	Main     *Query
}

// DatasetDef represents: $name = <query>.
type DatasetDef struct {
	Name  string
	Query *Query
}

// Query is the top-level AST node representing a full SPL2 query.
type Query struct {
	Source      *SourceClause
	Commands    []Command
	Annotations map[string]interface{} // optimizer → runtime hints
}

// Annotate sets an annotation on the query, initializing the map if needed.
func (q *Query) Annotate(key string, value interface{}) {
	if q.Annotations == nil {
		q.Annotations = make(map[string]interface{})
	}
	q.Annotations[key] = value
}

// GetAnnotation returns an annotation value and whether it exists.
func (q *Query) GetAnnotation(key string) (interface{}, bool) {
	if q.Annotations == nil {
		return nil, false
	}
	v, ok := q.Annotations[key]

	return v, ok
}

// SourceClause represents the FROM clause.
// It supports single sources, comma-separated lists, and glob patterns.
type SourceClause struct {
	Index      string           // primary source name (e.g., "idx_backend")
	Indices    []string         // multiple sources for FROM a, b, c syntax
	IsVariable bool             // true if $variable reference
	IsGlob     bool             // true if pattern contains wildcards (* or ?)
	TimeRange  *SourceTimeRange // inline time range: from nginx[-1h]
}

// SourceTimeRange represents an inline time range on a FROM clause.
// Parsed from: from nginx[-1h], from nginx[-7d..-1d], from nginx[@d].
type SourceTimeRange struct {
	Relative string // e.g., "-1h", "-7d" (start of range or single bound)
	End      string // e.g., "-1d" for range syntax "-7d..-1d" (empty for single bound)
	SnapTo   string // e.g., "d", "h" for @d, @h snap-to syntax
}

// IsSingleSource returns true if this clause references exactly one source
// (not a glob, not a multi-source list, not a variable).
func (sc *SourceClause) IsSingleSource() bool {
	return !sc.IsVariable && !sc.IsGlob && len(sc.Indices) == 0
}

// IsAllSources returns true if this clause means "scan all sources" (FROM *).
func (sc *SourceClause) IsAllSources() bool {
	return sc.IsGlob && sc.Index == "*"
}

// SourceNames returns all source names referenced by this clause.
// For single sources, returns a slice with one element.
// For multi-source (FROM a, b, c), returns the Indices slice.
// For globs, returns nil (must be resolved against a source registry).
func (sc *SourceClause) SourceNames() []string {
	if len(sc.Indices) > 0 {
		return sc.Indices
	}
	if sc.Index != "" && !sc.IsGlob {
		return []string{sc.Index}
	}

	return nil
}

// Command is the interface for all pipeline commands.
type Command interface {
	commandNode()
	String() string
}

// SearchCommand represents: search "term" or SEARCH index=<idx> <predicates>.
type SearchCommand struct {
	Term       string     // legacy: simple text search term
	Index      string     // for SEARCH index=<idx> syntax
	Predicates []Expr     // additional predicates after index=
	Expression SearchExpr // full search expression AST (when set, Term is ignored)
}

func (*SearchCommand) commandNode() {}
func (c *SearchCommand) String() string {
	if c.Expression != nil {
		return fmt.Sprintf("search %s", c.Expression)
	}
	if c.Index != "" {
		return fmt.Sprintf("search index=%s", c.Index)
	}

	return fmt.Sprintf("search %q", c.Term)
}

// WhereCommand represents: where <expr>.
type WhereCommand struct {
	Expr Expr
}

func (*WhereCommand) commandNode() {}
func (c *WhereCommand) String() string {
	return fmt.Sprintf("where %s", c.Expr)
}

// StatsCommand represents: stats <agg_funcs> [by <fields>].
type StatsCommand struct {
	Aggregations []AggExpr
	GroupBy      []string
}

func (*StatsCommand) commandNode() {}
func (c *StatsCommand) String() string {
	return fmt.Sprintf("stats <%d aggs> by %v", len(c.Aggregations), c.GroupBy)
}

// EvalCommand represents: eval <field>=<expr> [, <field>=<expr> ...].
type EvalCommand struct {
	Field       string
	Expr        Expr
	Assignments []EvalAssignment // for multi-assignment EVAL
}

type EvalAssignment struct {
	Field string
	Expr  Expr
}

func (*EvalCommand) commandNode() {}
func (c *EvalCommand) String() string {
	return fmt.Sprintf("eval %s=...", c.Field)
}

// SortCommand represents: sort [+/-]<field> ...
type SortCommand struct {
	Fields []SortField
}

type SortField struct {
	Name string
	Desc bool
}

func (*SortCommand) commandNode() {}
func (c *SortCommand) String() string {
	return fmt.Sprintf("sort <%d fields>", len(c.Fields))
}

// HeadCommand represents: head <n>.
type HeadCommand struct {
	Count int
}

func (*HeadCommand) commandNode() {}
func (c *HeadCommand) String() string {
	return fmt.Sprintf("head %d", c.Count)
}

// TailCommand represents: tail <n>.
type TailCommand struct {
	Count int
}

func (*TailCommand) commandNode() {}
func (c *TailCommand) String() string {
	return fmt.Sprintf("tail %d", c.Count)
}

// TimechartCommand represents: timechart span=<interval> <agg_funcs> [by <field>].
type TimechartCommand struct {
	Span         string // e.g., "5m", "1h"
	Aggregations []AggExpr
	GroupBy      []string
}

func (*TimechartCommand) commandNode() {}
func (c *TimechartCommand) String() string {
	return fmt.Sprintf("timechart span=%s <%d aggs>", c.Span, len(c.Aggregations))
}

// RexCommand represents: rex field=<field> "<regex>".
type RexCommand struct {
	Field   string // field to extract from (default: _raw)
	Pattern string // regex pattern with named groups
}

func (*RexCommand) commandNode() {}
func (c *RexCommand) String() string {
	return fmt.Sprintf("rex field=%s %q", c.Field, c.Pattern)
}

// FieldsCommand represents: fields <field1>, <field2>, ...
type FieldsCommand struct {
	Fields []string
	Remove bool // true if "fields - field1, field2"
}

func (*FieldsCommand) commandNode() {}
func (c *FieldsCommand) String() string {
	return fmt.Sprintf("fields %v", c.Fields)
}

// TableCommand represents: table <field1>, <field2>, ...
type TableCommand struct {
	Fields []string
}

func (*TableCommand) commandNode() {}
func (c *TableCommand) String() string {
	return fmt.Sprintf("table %v", c.Fields)
}

// DedupCommand represents: dedup [N] <field1>, <field2>, ...
type DedupCommand struct {
	Fields []string
	Limit  int // max events per unique key (0 = 1, the default dedup)
}

func (*DedupCommand) commandNode() {}
func (c *DedupCommand) String() string {
	return fmt.Sprintf("dedup %v", c.Fields)
}

// RenameCommand represents: rename <old> AS <new> [, <old> AS <new> ...].
type RenameCommand struct {
	Renames []RenamePair
}

type RenamePair struct {
	Old string
	New string
}

func (*RenameCommand) commandNode() {}
func (c *RenameCommand) String() string {
	return fmt.Sprintf("rename <%d pairs>", len(c.Renames))
}

// BinCommand represents: BIN <field> span=<duration> [AS <alias>].
type BinCommand struct {
	Field string
	Span  string // e.g., "2m", "15m", "1h"
	Alias string // optional alias
}

func (*BinCommand) commandNode() {}
func (c *BinCommand) String() string {
	return fmt.Sprintf("bin %s span=%s", c.Field, c.Span)
}

// StreamstatsCommand represents: STREAMSTATS [current=true/false] [window=N] <agg> [AS alias].
type StreamstatsCommand struct {
	Current      bool
	Window       int
	Aggregations []AggExpr
	GroupBy      []string
}

func (*StreamstatsCommand) commandNode() {}
func (c *StreamstatsCommand) String() string {
	return fmt.Sprintf("streamstats <%d aggs>", len(c.Aggregations))
}

// EventstatsCommand represents: EVENTSTATS <agg> [AS alias] [BY fields].
type EventstatsCommand struct {
	Aggregations []AggExpr
	GroupBy      []string
}

func (*EventstatsCommand) commandNode() {}
func (c *EventstatsCommand) String() string {
	return fmt.Sprintf("eventstats <%d aggs>", len(c.Aggregations))
}

// JoinCommand represents: JOIN type=inner/left <field> [subsearch].
type JoinCommand struct {
	JoinType string // "inner" or "left"
	Field    string
	Subquery *Query
}

func (*JoinCommand) commandNode() {}
func (c *JoinCommand) String() string {
	return fmt.Sprintf("join type=%s %s", c.JoinType, c.Field)
}

// AppendCommand represents: APPEND [subsearch].
type AppendCommand struct {
	Subquery *Query
}

func (*AppendCommand) commandNode() {}
func (c *AppendCommand) String() string {
	return "append [...]"
}

// MultisearchCommand represents: MULTISEARCH [search1] [search2] ...
type MultisearchCommand struct {
	Searches []*Query
}

func (*MultisearchCommand) commandNode() {}
func (c *MultisearchCommand) String() string {
	return fmt.Sprintf("multisearch <%d searches>", len(c.Searches))
}

// TransactionCommand represents: TRANSACTION <field> [maxspan=<dur>] [startswith=<expr>] [endswith=<expr>].
type TransactionCommand struct {
	Field      string
	MaxSpan    string // e.g., "2h"
	StartsWith string // expression string
	EndsWith   string // expression string
}

func (*TransactionCommand) commandNode() {}
func (c *TransactionCommand) String() string {
	return fmt.Sprintf("transaction %s", c.Field)
}

// XYSeriesCommand represents: XYSERIES <x_field> <y_field> <value_field>.
type XYSeriesCommand struct {
	XField     string
	YField     string
	ValueField string
}

func (*XYSeriesCommand) commandNode() {}
func (c *XYSeriesCommand) String() string {
	return fmt.Sprintf("xyseries %s %s %s", c.XField, c.YField, c.ValueField)
}

// TopCommand represents: top [N] <field> [by <field>].
type TopCommand struct {
	N       int
	Field   string
	ByField string
}

func (*TopCommand) commandNode() {}
func (c *TopCommand) String() string {
	if c.ByField != "" {
		return fmt.Sprintf("top %d %s by %s", c.N, c.Field, c.ByField)
	}

	return fmt.Sprintf("top %d %s", c.N, c.Field)
}

// RareCommand represents: rare [N] <field> [by <field>].
type RareCommand struct {
	N       int
	Field   string
	ByField string
}

func (*RareCommand) commandNode() {}
func (c *RareCommand) String() string {
	if c.ByField != "" {
		return fmt.Sprintf("rare %d %s by %s", c.N, c.Field, c.ByField)
	}

	return fmt.Sprintf("rare %d %s", c.N, c.Field)
}

// FillnullCommand represents: fillnull [value=<val>] [<field-list>].
type FillnullCommand struct {
	Value  string
	Fields []string
}

func (*FillnullCommand) commandNode() {}
func (c *FillnullCommand) String() string {
	return fmt.Sprintf("fillnull value=%s", c.Value)
}

// GlimpseCommand represents: glimpse — schema exploration that samples events
// and outputs field names, types, coverage, cardinality, and top values.
type GlimpseCommand struct{}

func (*GlimpseCommand) commandNode()     {}
func (c *GlimpseCommand) String() string { return "glimpse" }

// DescribeCommand represents: describe — logging passthrough that prints
// schema info to stderr on first batch, then yields batches unchanged.
type DescribeCommand struct {
	Output string // "stderr" (default), "json"
}

func (*DescribeCommand) commandNode()     {}
func (c *DescribeCommand) String() string { return "describe" }

// UseCommand represents: use <name> — imports a named pipeline fragment.
// Names can be simple ("parse_nginx"), namespaced ("@stdlib/parse_combined"),
// or team-scoped ("@myteam/error_classifier").
type UseCommand struct {
	Name string // e.g., "parse_nginx", "@stdlib/parse_combined"
}

func (*UseCommand) commandNode() {}
func (c *UseCommand) String() string {
	return fmt.Sprintf("use %s", c.Name)
}

// OutliersCommand represents: outliers field=<name> [method=iqr|zscore|mad] [threshold=<N>].
type OutliersCommand struct {
	Field     string  // numeric field to check (required)
	Method    string  // "iqr", "zscore", or "mad" (default: "iqr")
	Threshold float64 // sensitivity (default: 1.5 for IQR, 3.0 for zscore)
}

func (*OutliersCommand) commandNode() {}
func (c *OutliersCommand) String() string {
	return fmt.Sprintf("outliers field=%s method=%s threshold=%.1f", c.Field, c.Method, c.Threshold)
}

// CompareCommand represents: compare previous <duration> — re-executes the
// pipeline with a time shift and merges current + previous results.
type CompareCommand struct {
	Shift string // e.g., "1h", "24h", "7d"
}

func (*CompareCommand) commandNode() {}
func (c *CompareCommand) String() string {
	return fmt.Sprintf("compare previous %s", c.Shift)
}

// PatternsCommand represents: patterns [field=<name>] [max_templates=<N>] [similarity=<F>] —
// extracts log templates using the Drain algorithm.
type PatternsCommand struct {
	Field        string  // source field (default: "_raw")
	MaxTemplates int     // max templates (default: 50)
	Similarity   float64 // merge threshold 0.0-1.0 (default: 0.4)
}

func (*PatternsCommand) commandNode() {}
func (c *PatternsCommand) String() string {
	return fmt.Sprintf("patterns field=%s max_templates=%d similarity=%.1f", c.Field, c.MaxTemplates, c.Similarity)
}

// TraceCommand represents: trace [trace_id=<field>] [span_id=<field>] [parent_id=<field>] —
// groups events by trace_id, builds parent-child span trees, and emits rows
// enriched with _span_depth and _span_tree columns.
type TraceCommand struct {
	TraceIDField  string // default: "trace_id"
	SpanIDField   string // default: "span_id"
	ParentIDField string // default: "parent_span_id"
}

func (*TraceCommand) commandNode() {}
func (c *TraceCommand) String() string {
	return fmt.Sprintf("trace trace_id=%s span_id=%s parent_id=%s", c.TraceIDField, c.SpanIDField, c.ParentIDField)
}

// RollupCommand represents: rollup <span1>, <span2>, ... [by <fields>].
// Multi-resolution time bucketing: runs the child pipeline once per span
// resolution, adds a _resolution column, and unions the results.
type RollupCommand struct {
	Spans   []string // e.g., ["5m", "1h", "1d"]
	GroupBy []string
}

func (*RollupCommand) commandNode() {}
func (c *RollupCommand) String() string {
	return fmt.Sprintf("rollup %s", strings.Join(c.Spans, ", "))
}

// CorrelateCommand represents: correlate <field1> <field2> [method=pearson|spearman].
// Computes correlation between two numeric fields. Blocking operator.
type CorrelateCommand struct {
	Field1 string
	Field2 string
	Method string // "pearson" (default) or "spearman"
}

func (*CorrelateCommand) commandNode() {}
func (c *CorrelateCommand) String() string {
	return fmt.Sprintf("correlate %s %s", c.Field1, c.Field2)
}

// SessionizeCommand represents: sessionize maxpause=<duration> [by <group>].
// Groups events into sessions based on time gaps. Adds _session_id, _session_start, _session_end fields.
type SessionizeCommand struct {
	MaxPause string   // duration, e.g., "30m"
	GroupBy  []string // optional group-by fields
}

func (*SessionizeCommand) commandNode() {}
func (c *SessionizeCommand) String() string {
	if len(c.GroupBy) > 0 {
		return fmt.Sprintf("sessionize maxpause=%s by %s", c.MaxPause, joinStrings(c.GroupBy, ", "))
	}

	return fmt.Sprintf("sessionize maxpause=%s", c.MaxPause)
}

// TopologyCommand represents: topology [source_field=<f>] [dest_field=<f>] [weight_field=<f>] [max_nodes=<N>].
// Builds a connection graph from log events by grouping (source, destination) pairs.
type TopologyCommand struct {
	SourceField string // default "source"
	DestField   string // default "dest"
	WeightField string // optional weight field
	MaxNodes    int    // limit nodes, default 100
}

func (*TopologyCommand) commandNode() {}
func (c *TopologyCommand) String() string {
	return fmt.Sprintf("topology source_field=%s dest_field=%s", c.SourceField, c.DestField)
}

// Expr is the interface for all expressions.
type Expr interface {
	exprNode()
	String() string
}

// FieldExpr references a field by name.
type FieldExpr struct {
	Name     string
	Optional bool // true if accessed via ?. (optional chaining)
}

func (*FieldExpr) exprNode() {}
func (e *FieldExpr) String() string {
	return e.Name
}

// LiteralExpr represents a literal value (string, number).
type LiteralExpr struct {
	Value string
}

func (*LiteralExpr) exprNode() {}
func (e *LiteralExpr) String() string {
	return e.Value
}

// GlobExpr represents a glob/wildcard pattern.
type GlobExpr struct {
	Pattern string
}

func (*GlobExpr) exprNode() {}
func (e *GlobExpr) String() string {
	return e.Pattern
}

// CompareExpr represents a comparison: field op value.
type CompareExpr struct {
	Left  Expr
	Op    string // "=", "!=", "<", "<=", ">", ">="
	Right Expr
}

func (*CompareExpr) exprNode() {}
func (e *CompareExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", e.Left, e.Op, e.Right)
}

// BinaryExpr represents AND/OR.
type BinaryExpr struct {
	Left  Expr
	Op    string // "and", "or"
	Right Expr
}

func (*BinaryExpr) exprNode() {}
func (e *BinaryExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", e.Left, e.Op, e.Right)
}

// ArithExpr represents arithmetic: +, -, *, /.
type ArithExpr struct {
	Left  Expr
	Op    string // "+", "-", "*", "/"
	Right Expr
}

func (*ArithExpr) exprNode() {}
func (e *ArithExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", e.Left, e.Op, e.Right)
}

// NotExpr represents NOT.
type NotExpr struct {
	Expr Expr
}

func (*NotExpr) exprNode() {}
func (e *NotExpr) String() string {
	return fmt.Sprintf("(not %s)", e.Expr)
}

// FuncCallExpr represents a function call like count(), avg(field), IF(cond, a, b).
type FuncCallExpr struct {
	Name string
	Args []Expr
}

func (*FuncCallExpr) exprNode() {}
func (e *FuncCallExpr) String() string {
	return fmt.Sprintf("%s(%v)", e.Name, e.Args)
}

// InExpr represents: field IN (val1, val2, ...) or field NOT IN (val1, val2, ...)
type InExpr struct {
	Field   Expr
	Values  []Expr
	Negated bool // true for NOT IN
}

func (*InExpr) exprNode() {}
func (e *InExpr) String() string {
	if e.Negated {
		return fmt.Sprintf("%s not in (...)", e.Field)
	}

	return fmt.Sprintf("%s in (...)", e.Field)
}

// FStringPartAST is a single part of an f-string expression in the AST.
type FStringPartAST struct {
	Literal    string // literal text (non-empty for literal parts)
	Expr       string // raw expression text (non-empty for interpolated parts)
	ParsedExpr Expr   // parsed sub-expression (populated by parser for interpolated parts)
}

// FStringExpr represents f-string interpolation: f"{field}: {expr}".
type FStringExpr struct {
	Parts []FStringPartAST
}

func (*FStringExpr) exprNode()        {}
func (e *FStringExpr) String() string { return "f\"...\"" }

// MaterializeCommand represents: | materialize "name" [retention=30d] [partition_by=field1,field2].
type MaterializeCommand struct {
	Name        string
	Retention   string   // raw duration string, e.g. "30d", "90d", "" for default
	PartitionBy []string // optional partition fields
}

func (*MaterializeCommand) commandNode() {}
func (c *MaterializeCommand) String() string {
	s := fmt.Sprintf("materialize %q", c.Name)
	if c.Retention != "" {
		s += " retention=" + c.Retention
	}
	if len(c.PartitionBy) > 0 {
		s += " partition_by=" + joinStrings(c.PartitionBy, ",")
	}

	return s
}

// FromCommand represents: | from view_name.
type FromCommand struct {
	ViewName string
}

func (*FromCommand) commandNode() {}
func (c *FromCommand) String() string {
	return fmt.Sprintf("from %s", c.ViewName)
}

// ViewsCommand represents: | views ["name"] [retention=30d].
type ViewsCommand struct {
	Name      string // empty = list all
	Retention string // non-empty = alter retention
}

func (*ViewsCommand) commandNode() {}
func (c *ViewsCommand) String() string {
	s := "views"
	if c.Name != "" {
		s += fmt.Sprintf(" %q", c.Name)
	}
	if c.Retention != "" {
		s += " retention=" + c.Retention
	}

	return s
}

// DropviewCommand represents: | dropview "name".
type DropviewCommand struct {
	Name string
}

func (*DropviewCommand) commandNode() {}
func (c *DropviewCommand) String() string {
	return fmt.Sprintf("dropview %q", c.Name)
}

// UnpackCommand represents: | unpack_json/unpack_logfmt/unpack_syslog/unpack_combined/... [from <field>] [fields (<f1>, <f2>, ...)] [prefix "<p>"] [keep_original].
// All formats share identical option grammar — the Format string differentiates.
// The Delim/Assign/Quote fields are only used by the "kv" format.
// The Header field is only used by the "w3c" format.
// The Pattern field is only used by the "pattern" format.
type UnpackCommand struct {
	Format       string   // "json", "logfmt", "syslog", "combined", "clf", "nginx_error", "cef", "kv", "docker", "redis", "apache_error", "postgres", "mysql_slow", "haproxy", "leef", "w3c", "pattern"
	SourceField  string   // default: "_raw"
	Fields       []string // extract only these (nil = all)
	Prefix       string   // prefix for output field names
	KeepOriginal bool     // don't overwrite existing non-null fields
	Delim        string   // kv pair delimiter (default: " ", only used by "kv")
	Assign       string   // kv assignment character (default: "=", only used by "kv")
	Quote        string   // kv quote character (default: "\"", only used by "kv")
	Header       string   // W3C #Fields directive (only used by "w3c")
	Pattern      string   // user-defined extraction pattern (only used by "pattern")
}

func (*UnpackCommand) commandNode() {}
func (c *UnpackCommand) String() string {
	s := fmt.Sprintf("unpack_%s", c.Format)
	if c.SourceField != "" && c.SourceField != "_raw" {
		s += " from " + c.SourceField
	}
	if len(c.Fields) > 0 {
		s += fmt.Sprintf(" fields (%s)", joinStrings(c.Fields, ", "))
	}
	if c.Prefix != "" {
		s += fmt.Sprintf(" prefix %q", c.Prefix)
	}
	if c.KeepOriginal {
		s += " keep_original"
	}

	return s
}

// JsonPath represents a single path extraction for the | json command,
// with an optional AS alias for renaming the output field.
type JsonPath struct {
	Path  string // dot-separated path, e.g. "user.id"
	Alias string // optional output name (empty = use Path)
}

// OutputName returns the field name that should be used in the output.
// If an alias is set, returns the alias; otherwise returns the path.
func (jp JsonPath) OutputName() string {
	if jp.Alias != "" {
		return jp.Alias
	}
	return jp.Path
}

// JsonCommand represents: | json [field=<field>] [path="<p>" AS alias] [paths="<p1> AS a1, <p2>"].
// Lighter-weight shorthand for JSON extraction compared to full unpack_json.
type JsonCommand struct {
	SourceField string     // source field (default: "_raw")
	Paths       []JsonPath // specific dot-paths to extract (nil = all)
}

func (*JsonCommand) commandNode() {}
func (c *JsonCommand) String() string {
	s := "json"
	if c.SourceField != "" && c.SourceField != "_raw" {
		s += " field=" + c.SourceField
	}
	if len(c.Paths) > 0 {
		parts := make([]string, len(c.Paths))
		for i, jp := range c.Paths {
			if jp.Alias != "" {
				parts[i] = jp.Path + " AS " + jp.Alias
			} else {
				parts[i] = jp.Path
			}
		}
		s += fmt.Sprintf(" paths=%q", joinStrings(parts, ", "))
	}

	return s
}

func joinStrings(ss []string, sep string) string {
	if len(ss) == 0 {
		return ""
	}
	result := ss[0]
	for _, s := range ss[1:] {
		result += sep + s
	}

	return result
}

// UnrollCommand represents: | unroll field=<field> or | explode <field>[, <field2>, ...] [as <alias>].
// Explodes a JSON array field into multiple rows, one per element.
// If an element is an object, its keys are flattened with dot-notation prefix.
// When ExtraFields are specified, all fields are zip-expanded in parallel.
type UnrollCommand struct {
	Field       string   // primary field containing JSON array to explode
	Alias       string   // optional output field name (Lynx Flow: explode tags as tag; single-field only)
	ExtraFields []string // additional fields for zip-expansion (multi-field explode)
}

// AllFields returns Field followed by ExtraFields.
func (c *UnrollCommand) AllFields() []string {
	if len(c.ExtraFields) == 0 {
		return []string{c.Field}
	}
	all := make([]string, 0, 1+len(c.ExtraFields))
	return append(append(all, c.Field), c.ExtraFields...)
}

func (*UnrollCommand) commandNode() {}
func (c *UnrollCommand) String() string {
	if len(c.ExtraFields) > 0 {
		return fmt.Sprintf("explode %s, %s", c.Field, joinStrings(c.ExtraFields, ", "))
	}
	if c.Alias != "" {
		return fmt.Sprintf("unroll field=%s as %s", c.Field, c.Alias)
	}

	return fmt.Sprintf("unroll field=%s", c.Field)
}

// TeeCommand represents: | tee "<destination>" — side-effect passthrough.
// Writes each batch to a destination file, then yields the batch unchanged.
type TeeCommand struct {
	Destination string // file path
	Format      string // "json" (default)
}

func (*TeeCommand) commandNode()     {}
func (c *TeeCommand) String() string { return fmt.Sprintf("tee %q", c.Destination) }

// PackJsonCommand represents: | pack_json [<f1>, <f2>, ...] into <target>.
// Assembles event fields into a JSON string stored in target field.
// Without field list, packs all non-internal fields.
type PackJsonCommand struct {
	Fields []string // nil = all non-internal fields
	Target string   // output field name (required)
}

func (*PackJsonCommand) commandNode() {}
func (c *PackJsonCommand) String() string {
	if len(c.Fields) > 0 {
		return fmt.Sprintf("pack_json %s into %s", joinStrings(c.Fields, ", "), c.Target)
	}

	return fmt.Sprintf("pack_json into %s", c.Target)
}

// SelectCommand represents: | select <field> [as <alias>], ...
// Ordered projection with optional inline rename. Unlike FieldsCommand (keep),
// SelectCommand enforces output column order as specified in the command.
type SelectCommand struct {
	Columns []SelectColumn
}

// SelectColumn is a single column in a select command.
type SelectColumn struct {
	Name  string
	Alias string // empty = no rename
}

func (*SelectCommand) commandNode() {}
func (c *SelectCommand) String() string {
	return fmt.Sprintf("select <%d columns>", len(c.Columns))
}

// TopNCommand is an internal optimizer command: sort + head fused into a heap selection.
// Not user-facing — created by the earlyLimitRule optimizer.
type TopNCommand struct {
	Fields []SortField
	Limit  int
}

func (*TopNCommand) commandNode() {}
func (c *TopNCommand) String() string {
	return fmt.Sprintf("topn %d <%d fields>", c.Limit, len(c.Fields))
}

// ContainsGlobWildcard reports whether s contains glob wildcard characters.
func ContainsGlobWildcard(s string) bool {
	return strings.ContainsAny(s, "*?")
}

// FieldListHasGlob returns true if any field in the list contains a glob wildcard.
func FieldListHasGlob(fields []string) bool {
	for _, f := range fields {
		if ContainsGlobWildcard(f) {
			return true
		}
	}
	return false
}

// AggExpr represents an aggregation expression: func(args) [as alias].
type AggExpr struct {
	Func  string
	Args  []Expr
	Alias string
}
