package ast

// SearchExpr is a node of a search command's search expression.
type SearchExpr interface {
	Node
	isSearch()
}

// SearchAnd, SearchOr and SearchNot combine search expressions.
type SearchAnd struct {
	Base
	L, R SearchExpr
}

type SearchOr struct {
	Base
	L, R SearchExpr
}

type SearchNot struct {
	Base
	X SearchExpr
}

// SearchText is a free-text term or quoted phrase.
type SearchText struct {
	Base
	Text   string
	Quoted bool
}

// SearchCompare is field <op> value inside a search expression; Op is one
// of = != < <= > >=.
type SearchCompare struct {
	Base
	Field  *Field
	Op     string
	Value  string
	Quoted bool
	// Numeric is set when the value was written as a number literal.
	Numeric bool
	// Bool is set when the value was written as true or false.
	Bool bool
}

// SearchIn is field IN (v1, v2, ...) inside a search expression.
type SearchIn struct {
	Base
	Field  *Field
	Values []SearchCompare
}

// TimeModifier is earliest=<v> or latest=<v>.
type TimeModifier struct {
	Base
	Latest bool
	Value  string
	Quoted bool
}

func (*SearchAnd) isSearch()     {}
func (*SearchOr) isSearch()      {}
func (*SearchNot) isSearch()     {}
func (*SearchText) isSearch()    {}
func (*SearchCompare) isSearch() {}
func (*SearchIn) isSearch()      {}
func (*TimeModifier) isSearch()  {}

// SourceRef names an index or index pattern.
type SourceRef struct {
	Base
	Name string
}

// SearchCommand is "[search] source=<sources> [<search expression>]". When
// Sub is set the source is a parenthesized subsearch.
type SearchCommand struct {
	Base
	Sources []SourceRef
	Sub     *Pipeline
	Alias   string
	Filter  SearchExpr
}

func (*SearchCommand) Name() string { return "search" }

// DescribeCommand is "describe <source>".
type DescribeCommand struct {
	Base
	Source SourceRef
}

func (*DescribeCommand) Name() string { return "describe" }

// MakeResultsCommand is "makeresults [count=n] [format=csv|json] [data=...]".
type MakeResultsCommand struct {
	Base
	Count  int64
	Format string
	Data   string
	HasArg map[string]bool
}

func (*MakeResultsCommand) Name() string { return "makeresults" }

// MultiSearchCommand is "multisearch [subsearch] [subsearch] ...".
type MultiSearchCommand struct {
	Base
	Searches []*Pipeline
}

func (*MultiSearchCommand) Name() string { return "multisearch" }

// UnionCommand is "union [maxout=n] dataset, dataset ..."; a dataset is a
// subsearch or a source name.
type UnionCommand struct {
	Base
	MaxOut   int64
	Datasets []*Pipeline
}

func (*UnionCommand) Name() string { return "union" }

// WhereCommand is "where <condition>".
type WhereCommand struct {
	Base
	Cond Expr
}

func (*WhereCommand) Name() string { return "where" }

// FieldsCommand is "fields [+|-] f1, f2" or the table command.
type FieldsCommand struct {
	Base
	Command string
	Exclude bool
	Fields  []string
}

func (c *FieldsCommand) Name() string { return c.Command }

// RenamePair is "<from> as <to>".
type RenamePair struct {
	From string
	To   string
	Span Span
}

// RenameCommand is "rename a as b, ...".
type RenameCommand struct {
	Base
	Pairs []RenamePair
}

func (*RenameCommand) Name() string { return "rename" }

// ReplaceCommand is "replace 'p' WITH 'r', ... IN f1, f2".
type ReplaceCommand struct {
	Base
	Patterns     []string
	Replacements []string
	Fields       []*Field
}

func (*ReplaceCommand) Name() string { return "replace" }

// AggTerm is an aggregation with an optional alias. Func is the lower-case
// aggregate name with shortcut spellings preserved (c, dc, p95, perc95).
type AggTerm struct {
	Func  *Call
	Alias string
	// Text is the source text of the call, used as the default column name.
	Text string
	Span Span
}

// SpanExpr is span(field, interval) in a by clause.
type SpanExpr struct {
	Field *Field
	Value string
	Alias string
	Span  Span
}

// ByClause is "by [span(...),] f1, f2".
type ByClause struct {
	Span   *SpanExpr
	Fields []*Field
	// SpanFirst records whether the span came before the fields.
	SpanFirst bool
}

// StatsCommand covers stats, eventstats and streamstats.
type StatsCommand struct {
	Base
	Command string
	Aggs    []AggTerm
	By      *ByClause
	Args    []Arg
}

func (c *StatsCommand) Name() string { return c.Command }

// DedupCommand is "dedup [n] f1, f2 [keepempty=b] [consecutive=b]".
type DedupCommand struct {
	Base
	Count       int64
	Fields      []*Field
	KeepEmpty   bool
	Consecutive bool
}

func (*DedupCommand) Name() string { return "dedup" }

// SortField is one sort key.
type SortField struct {
	Field *Field
	Desc  bool
	// Cast is "", "auto", "str", "ip" or "num".
	Cast string
	Span Span
}

// SortCommand is "sort [count] keys".
type SortCommand struct {
	Base
	Count int64
	Keys  []SortField
}

func (*SortCommand) Name() string { return "sort" }

// ReverseCommand is "reverse".
type ReverseCommand struct{ Base }

func (*ReverseCommand) Name() string { return "reverse" }

// HeadCommand is "head [n] [from m]".
type HeadCommand struct {
	Base
	Count int64
	From  int64
}

func (*HeadCommand) Name() string { return "head" }

// EvalAssign is "field = expr".
type EvalAssign struct {
	Field *Field
	Expr  Expr
	Span  Span
}

// EvalCommand covers eval and fieldformat.
type EvalCommand struct {
	Base
	Command string
	Assigns []EvalAssign
}

func (c *EvalCommand) Name() string { return c.Command }

// BinCommand is "bin field [options] [as alias]".
type BinCommand struct {
	Base
	Field *Field
	Args  []Arg
	Alias string
}

func (*BinCommand) Name() string { return "bin" }

// TopCommand covers top and rare.
type TopCommand struct {
	Base
	Command string
	Count   int64
	Fields  []*Field
	By      []*Field
	Args    []Arg
}

func (c *TopCommand) Name() string { return c.Command }

// ParseCommand covers parse and grok: "<cmd> <source> '<pattern>'".
type ParseCommand struct {
	Base
	Command string
	Source  Expr
	Pattern string
}

func (c *ParseCommand) Name() string { return c.Command }

// RexCommand is "rex field=f [options] 'pattern'".
type RexCommand struct {
	Base
	Field   *Field
	Pattern string
	Args    []Arg
}

func (*RexCommand) Name() string { return "rex" }

// RegexCommand is "regex field = 'pattern'" or "regex field != 'pattern'".
type RegexCommand struct {
	Base
	Field   *Field
	Negate  bool
	Pattern string
}

func (*RegexCommand) Name() string { return "regex" }

// SpathCommand is "spath [input=f] [output=f] [path=]p".
type SpathCommand struct {
	Base
	Input  *Field
	Output *Field
	Path   string
}

func (*SpathCommand) Name() string { return "spath" }

// PatternsCommand is "patterns <source> [by ...] [options]".
type PatternsCommand struct {
	Base
	Source Expr
	By     []*Field
	Args   []Arg
}

func (*PatternsCommand) Name() string { return "patterns" }

// LookupPair is "field [as alias]".
type LookupPair struct {
	Field string
	Alias string
}

// LookupCommand is "lookup <source> mappings [replace|append|output outputs]".
type LookupCommand struct {
	Base
	Source  SourceRef
	Keys    []LookupPair
	Mode    string
	Outputs []LookupPair
}

func (*LookupCommand) Name() string { return "lookup" }

// FillnullCommand is fillnull in its "with ... in", "using", and "value="
// forms.
type FillnullCommand struct {
	Base
	Value  Expr
	Fields []*Field
	// Pairs holds per-field values of the "using" form.
	Pairs []EvalAssign
}

func (*FillnullCommand) Name() string { return "fillnull" }

// ConvertFunc is "func(field) [as alias]".
type ConvertFunc struct {
	Func  string
	Field *Field
	Alias string
}

// ConvertCommand is "convert [timeformat=...] auto(f), num(g) as h".
type ConvertCommand struct {
	Base
	TimeFormat string
	Funcs      []ConvertFunc
}

func (*ConvertCommand) Name() string { return "convert" }

// TrendlineTerm is "sma|wma(n, field) [as alias]".
type TrendlineTerm struct {
	Kind  string
	N     int64
	Field *Field
	Alias string
}

// TrendlineCommand is "trendline [sort key] terms".
type TrendlineCommand struct {
	Base
	Sort  *SortField
	Terms []TrendlineTerm
}

func (*TrendlineCommand) Name() string { return "trendline" }

// FieldCommand covers single-field commands: expand, mvexpand, flatten,
// nomv and mvcombine.
type FieldCommand struct {
	Base
	Command string
	Field   *Field
	Alias   string
	Aliases []string
	Args    []Arg
}

func (c *FieldCommand) Name() string { return c.Command }

// SubPipelineCommand covers append, appendcol and appendpipe, whose
// argument is a bracketed pipeline.
type SubPipelineCommand struct {
	Base
	Command  string
	Pipeline *Pipeline
	Args     []Arg
}

func (c *SubPipelineCommand) Name() string { return c.Command }

// JoinCommand is a join in either the SQL-like or the field-list form.
type JoinCommand struct {
	Base
	// Kind is the lower-case join type: inner, left, right, full, cross,
	// semi or anti.
	Kind       string
	LeftAlias  string
	RightAlias string
	Cond       Expr
	Fields     []*Field
	Right      *Pipeline
	Args       []Arg
}

func (*JoinCommand) Name() string { return "join" }

// TotalsCommand covers addtotals and addcoltotals.
type TotalsCommand struct {
	Base
	Command string
	Fields  []*Field
	Args    []Arg
}

func (c *TotalsCommand) Name() string { return c.Command }

// ChartCommand covers chart and timechart.
type ChartCommand struct {
	Base
	Command  string
	Agg      AggTerm
	Over     *Field
	OverArgs []Arg
	By       *Field
	ByArgs   []Arg
	Args     []Arg
}

func (c *ChartCommand) Name() string { return c.Command }

// TransposeCommand is "transpose [n] [column_name=...]".
type TransposeCommand struct {
	Base
	Rows       int64
	ColumnName string
}

func (*TransposeCommand) Name() string { return "transpose" }

// XYSeriesCommand is "xyseries [options] x yname in (values) ydata...".
type XYSeriesCommand struct {
	Base
	X      *Field
	Y      *Field
	Values []string
	Data   []*Field
	Args   []Arg
}

func (*XYSeriesCommand) Name() string { return "xyseries" }

// UnsupportedCommand is a command the grammar defines and LynxDB does not
// execute; the parser records it so that compilation can report why.
type UnsupportedCommand struct {
	Base
	Command string
}

func (c *UnsupportedCommand) Name() string { return c.Command }
