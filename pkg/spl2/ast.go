package spl2

import "fmt"

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
	Index      string   // primary source name (e.g., "idx_backend")
	Indices    []string // multiple sources for FROM a, b, c syntax
	IsVariable bool     // true if $variable reference
	IsGlob     bool     // true if pattern contains wildcards (* or ?)
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

// Expr is the interface for all expressions.
type Expr interface {
	exprNode()
	String() string
}

// FieldExpr references a field by name.
type FieldExpr struct {
	Name string
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

// InExpr represents: field IN (val1, val2, ...)
type InExpr struct {
	Field  Expr
	Values []Expr
}

func (*InExpr) exprNode() {}
func (e *InExpr) String() string {
	return fmt.Sprintf("%s in (...)", e.Field)
}

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

// AggExpr represents an aggregation expression: func(args) [as alias].
type AggExpr struct {
	Func  string
	Args  []Expr
	Alias string
}
