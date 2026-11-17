package ir

import (
	"fmt"
	"strconv"
	"strings"
)

// TimeColumn is the column that holds an event's timestamp.
const TimeColumn = "_time"

// RawColumn is the column that holds an event's original text.
const RawColumn = "_raw"

// Plan is a compiled query: a tree of logical operators.
type Plan struct {
	Root Node
}

// Schema returns the output schema of the plan.
func (p *Plan) Schema() Schema { return p.Root.Schema() }

// Node is a logical operator. Operators are immutable values: rewrites build
// new nodes with WithInputs instead of mutating shared ones.
type Node interface {
	// Inputs returns the operator's input operators in order.
	Inputs() []Node
	// WithInputs returns a shallow copy of the operator reading from the
	// given inputs; len(inputs) must equal len(Inputs()).
	WithInputs(inputs []Node) Node
	// Schema returns the operator's output schema.
	Schema() Schema
	// Label renders a one-line description for EXPLAIN.
	Label() string
	isNode()
}

// unary is implemented by operators with exactly one input, so traversals
// can reach it without allocating an input slice.
type unary interface {
	input() Node
}

func one(inputs []Node, op string) Node {
	if len(inputs) != 1 {
		panic(fmt.Sprintf("ir: %s takes 1 input, got %d", op, len(inputs)))
	}
	return inputs[0]
}

// Source names a dataset a Scan reads. A Pattern name may contain '*'.
type Source struct {
	Name    string
	Pattern bool
}

// Scan reads events from one or more sources. Sources empty means the
// execution environment's default source.
type Scan struct {
	Sources []Source
	// Time restricts the events read by event time.
	Time *TimeRange
	// Prune lists predicates the scan may use to skip data that cannot
	// satisfy them. They are hints: every one is also enforced by an
	// operator above the scan, so a scan may ignore any of them.
	Prune []Expr
	// Columns lists the columns operators above the scan read; nil means
	// all columns.
	Columns []string
	// Limit, when positive, bounds the number of rows operators above the
	// scan consume in scan order.
	Limit int64
	// Descending asks for newest-first event order.
	Descending bool
	Out        Schema
}

func (*Scan) Inputs() []Node { return nil }
func (s *Scan) WithInputs(in []Node) Node {
	if len(in) != 0 {
		panic("ir: Scan takes no inputs")
	}
	c := *s
	return &c
}
func (s *Scan) Schema() Schema { return s.Out }
func (*Scan) isNode()          {}
func (s *Scan) Label() string {
	var b strings.Builder
	b.WriteString("Scan(")
	if len(s.Sources) == 0 {
		b.WriteString("<default>")
	}
	for i, src := range s.Sources {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(src.Name)
	}
	b.WriteString(")")
	if !s.Time.IsZero() {
		b.WriteString(" time=" + s.Time.String())
	}
	if s.Descending {
		b.WriteString(" desc")
	}
	if s.Limit > 0 {
		b.WriteString(" limit=" + strconv.FormatInt(s.Limit, 10))
	}
	if s.Columns != nil {
		b.WriteString(" columns=[" + strings.Join(s.Columns, ", ") + "]")
	}
	for _, p := range s.Prune {
		b.WriteString(" prune=" + p.String())
	}
	return b.String()
}

// Values produces literal rows.
type Values struct {
	Out  Schema
	Rows [][]Expr
}

func (*Values) Inputs() []Node { return nil }
func (v *Values) WithInputs(in []Node) Node {
	if len(in) != 0 {
		panic("ir: Values takes no inputs")
	}
	c := *v
	return &c
}
func (v *Values) Schema() Schema { return v.Out }
func (*Values) isNode()          {}
func (v *Values) Label() string {
	return fmt.Sprintf("Values(rows=%d, columns=[%s])", len(v.Rows), strings.Join(v.Out.Names(), ", "))
}

// Empty produces no rows.
type Empty struct {
	Out Schema
}

func (*Empty) Inputs() []Node { return nil }
func (e *Empty) WithInputs(in []Node) Node {
	if len(in) != 0 {
		panic("ir: Empty takes no inputs")
	}
	c := *e
	return &c
}
func (e *Empty) Schema() Schema { return e.Out }
func (*Empty) isNode()          {}
func (*Empty) Label() string    { return "Empty()" }

// SourceSchema produces one row per column observed in a source.
type SourceSchema struct {
	Source Source
}

// SourceSchemaColumns is the output schema of SourceSchema: one row per
// column with its position and the type name of its observed values.
var SourceSchemaColumns = ClosedSchema(
	Column{Name: "TABLE_NAME", Type: String},
	Column{Name: "COLUMN_NAME", Type: String},
	Column{Name: "TYPE_NAME", Type: String},
	Column{Name: "ORDINAL_POSITION", Type: Int},
)

func (*SourceSchema) Inputs() []Node { return nil }
func (s *SourceSchema) WithInputs(in []Node) Node {
	if len(in) != 0 {
		panic("ir: SourceSchema takes no inputs")
	}
	c := *s
	return &c
}
func (*SourceSchema) Schema() Schema  { return SourceSchemaColumns }
func (*SourceSchema) isNode()         {}
func (s *SourceSchema) Label() string { return "SourceSchema(" + s.Source.Name + ")" }

// Filter keeps the rows for which Cond is TRUE.
type Filter struct {
	Input Node
	Cond  Expr
}

func (f *Filter) Inputs() []Node { return []Node{f.Input} }
func (f *Filter) input() Node    { return f.Input }
func (f *Filter) WithInputs(in []Node) Node {
	c := *f
	c.Input = one(in, "Filter")
	return &c
}
func (f *Filter) Schema() Schema { return f.Input.Schema() }
func (*Filter) isNode()          {}
func (f *Filter) Label() string  { return "Filter(" + f.Cond.String() + ")" }

// Project keeps (Drop false) or removes (Drop true) columns. Names may be
// wildcard patterns. Keeping emits the columns in the order the names are
// listed, a pattern contributing its matches in input order; a name that
// matches no column contributes nothing.
type Project struct {
	Input Node
	Names []string
	Drop  bool
}

func (p *Project) Inputs() []Node { return []Node{p.Input} }
func (p *Project) input() Node    { return p.Input }
func (p *Project) WithInputs(in []Node) Node {
	c := *p
	c.Input = one(in, "Project")
	return &c
}
func (*Project) isNode() {}
func (p *Project) Label() string {
	names := make([]string, len(p.Names))
	for i, n := range p.Names {
		names[i] = n
		if !IsNamePattern(n) {
			names[i] = QuoteName(n)
		}
	}
	if p.Drop {
		return "Project(drop " + strings.Join(names, ", ") + ")"
	}
	return "Project(" + strings.Join(names, ", ") + ")"
}
func (p *Project) Schema() Schema {
	in := p.Input.Schema()
	if p.Drop {
		var drop []string
		for _, n := range p.Names {
			drop = append(drop, in.MatchNames(n)...)
		}
		return in.Without(drop...)
	}
	out := Schema{}
	seen := map[string]bool{}
	for _, n := range p.Names {
		matches := in.MatchNames(n)
		if IsNamePattern(n) && in.Open {
			out.Open = true
		}
		if len(matches) == 0 && !IsNamePattern(n) && in.Open {
			matches = []string{n}
		}
		for _, m := range matches {
			if seen[m] {
				continue
			}
			seen[m] = true
			if c, ok := in.Lookup(m); ok {
				out.Columns = append(out.Columns, c)
			} else {
				out.Columns = append(out.Columns, Column{Name: m, Type: Any, Nullable: true})
			}
		}
	}
	return out
}

// Rename is one column rename. From and To may both contain '*'; the text a
// wildcard in From matches replaces the wildcard in To.
type Rename struct {
	From string
	To   string
}

// RenameColumns renames columns. A rename whose source is absent is a no-op;
// a rename onto an existing column replaces it.
type RenameColumns struct {
	Input   Node
	Renames []Rename
}

func (r *RenameColumns) Inputs() []Node { return []Node{r.Input} }
func (r *RenameColumns) input() Node    { return r.Input }
func (r *RenameColumns) WithInputs(in []Node) Node {
	c := *r
	c.Input = one(in, "RenameColumns")
	return &c
}
func (*RenameColumns) isNode() {}
func (r *RenameColumns) Label() string {
	parts := make([]string, len(r.Renames))
	for i, rn := range r.Renames {
		parts[i] = rn.From + " -> " + rn.To
	}
	return "Rename(" + strings.Join(parts, ", ") + ")"
}
func (r *RenameColumns) Schema() Schema {
	s := r.Input.Schema().Clone()
	for _, rn := range r.Renames {
		s = renameSchema(s, rn)
	}
	return s
}

func renameSchema(s Schema, rn Rename) Schema {
	var out Schema
	out.Open = s.Open
	renamed := map[string]Column{}
	var order []string
	for _, c := range s.Columns {
		to, ok := RenameTarget(rn, c.Name)
		if !ok {
			continue
		}
		cc := c
		cc.Name = to
		renamed[c.Name] = cc
		order = append(order, to)
	}
	targets := map[string]bool{}
	for _, t := range order {
		targets[t] = true
	}
	for _, c := range s.Columns {
		if nc, ok := renamed[c.Name]; ok {
			out.Columns = append(out.Columns, nc)
			continue
		}
		if targets[c.Name] {
			continue
		}
		out.Columns = append(out.Columns, c)
	}
	if len(renamed) == 0 && !IsNamePattern(rn.From) && s.Open {
		out = out.With(Column{Name: rn.To, Type: Any, Nullable: true})
	}
	return out
}

// RenameTarget returns the new name of column name under rn, or false when
// rn does not apply to it.
func RenameTarget(rn Rename, name string) (string, bool) {
	if !IsNamePattern(rn.From) {
		return rn.To, name == rn.From
	}
	parts := strings.Split(rn.From, "*")
	if !strings.HasPrefix(name, parts[0]) {
		return "", false
	}
	rest := name[len(parts[0]):]
	var caps []string
	for i := 1; i < len(parts); i++ {
		p := parts[i]
		if i == len(parts)-1 {
			if !strings.HasSuffix(rest, p) {
				return "", false
			}
			caps = append(caps, rest[:len(rest)-len(p)])
			break
		}
		j := strings.Index(rest, p)
		if j < 0 {
			return "", false
		}
		caps = append(caps, rest[:j])
		rest = rest[j+len(p):]
	}
	toParts := strings.Split(rn.To, "*")
	var b strings.Builder
	for i, p := range toParts {
		b.WriteString(p)
		if i < len(toParts)-1 && i < len(caps) {
			b.WriteString(caps[i])
		}
	}
	return b.String(), true
}

// Assign binds a column name to an expression.
type Assign struct {
	Name string
	Expr Expr
}

// Compute adds or replaces columns. Assignments run in order, and each sees
// the columns assigned before it.
type Compute struct {
	Input   Node
	Assigns []Assign
}

func (c *Compute) Inputs() []Node { return []Node{c.Input} }
func (c *Compute) input() Node    { return c.Input }
func (c *Compute) WithInputs(in []Node) Node {
	cc := *c
	cc.Input = one(in, "Compute")
	return &cc
}
func (*Compute) isNode() {}
func (c *Compute) Label() string {
	parts := make([]string, len(c.Assigns))
	for i, a := range c.Assigns {
		parts[i] = QuoteName(a.Name) + " = " + a.Expr.String()
	}
	return "Compute(" + strings.Join(parts, ", ") + ")"
}
func (c *Compute) Schema() Schema {
	s := c.Input.Schema()
	for _, a := range c.Assigns {
		s = s.With(Column{Name: a.Name, Type: a.Expr.Type(), Nullable: true})
	}
	return s
}

// NamedExpr is an expression with an output column name.
type NamedExpr struct {
	Name string
	Expr Expr
}

// NamedAgg is an aggregate call with an output column name.
type NamedAgg struct {
	Name string
	Agg  *AggCall
}

// AggregatePhase identifies a split of an aggregation across workers.
type AggregatePhase uint8

const (
	// AggComplete aggregates input rows to final values.
	AggComplete AggregatePhase = iota
	// AggPartial aggregates input rows to mergeable partial states.
	AggPartial
	// AggFinal merges partial states to final values.
	AggFinal
)

// Aggregate groups rows by Groups and computes Aggs per group. The output
// has the group columns followed by the aggregate columns. Without groups it
// produces exactly one row, even for empty input.
type Aggregate struct {
	Input  Node
	Groups []NamedExpr
	Aggs   []NamedAgg
	// DropNullGroups drops input rows whose group values contain a NULL
	// instead of forming a NULL group.
	DropNullGroups bool
	Phase          AggregatePhase
	// Limit, when positive, is the maximum number of groups a consumer
	// reads after ordering by OrderBy; it lets the executor keep a bounded
	// heap of groups.
	Limit   int64
	OrderBy []SortKey
}

func (a *Aggregate) Inputs() []Node { return []Node{a.Input} }
func (a *Aggregate) input() Node    { return a.Input }
func (a *Aggregate) WithInputs(in []Node) Node {
	c := *a
	c.Input = one(in, "Aggregate")
	return &c
}
func (*Aggregate) isNode() {}
func (a *Aggregate) Label() string {
	var b strings.Builder
	b.WriteString("Aggregate(")
	for i, ag := range a.Aggs {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(QuoteName(ag.Name) + " = " + ag.Agg.String())
	}
	if len(a.Groups) > 0 {
		b.WriteString(" BY ")
		for i, g := range a.Groups {
			if i > 0 {
				b.WriteString(", ")
			}
			if fr, ok := g.Expr.(*FieldRef); ok && fr.Name == g.Name {
				b.WriteString(QuoteName(g.Name))
			} else {
				b.WriteString(QuoteName(g.Name) + " = " + g.Expr.String())
			}
		}
	}
	b.WriteString(")")
	if a.DropNullGroups {
		b.WriteString(" drop_null_groups")
	}
	switch a.Phase {
	case AggPartial:
		b.WriteString(" phase=partial")
	case AggFinal:
		b.WriteString(" phase=final")
	}
	if a.Limit > 0 {
		b.WriteString(" limit=" + strconv.FormatInt(a.Limit, 10) + " order=" + sortKeysString(a.OrderBy))
	}
	return b.String()
}

// Column annotations shared by every aggregate schema; they are never
// modified.
var (
	groupMeta     = map[string]string{"role": "group"}
	aggregateMeta = map[string]string{"role": "aggregate"}
)

func (a *Aggregate) Schema() Schema {
	var s Schema
	if n := len(a.Groups) + len(a.Aggs); n > 0 {
		s.Columns = make([]Column, 0, n)
	}
	for _, g := range a.Groups {
		s.Columns = append(s.Columns, Column{Name: g.Name, Type: g.Expr.Type(), Nullable: !a.DropNullGroups, Meta: groupMeta})
	}
	for _, ag := range a.Aggs {
		s.Columns = append(s.Columns, Column{Name: ag.Name, Type: ag.Agg.Type(), Nullable: true, Meta: aggregateMeta})
	}
	return s
}

// FrameKind selects the rows a window function sees for each row.
type FrameKind uint8

const (
	// FramePartition evaluates each function over the whole partition and
	// attaches the result to every row of it.
	FramePartition FrameKind = iota + 1
	// FrameRunning evaluates each function over the rows of the partition up
	// to the current row in input order, optionally bounded by Rows or Span.
	FrameRunning
)

// Frame bounds the rows a running window function sees.
type Frame struct {
	Kind FrameKind
	// Rows, when positive, limits a running frame to the last Rows rows.
	Rows int
	// ExcludeCurrent leaves the current row out of a running frame.
	ExcludeCurrent bool
	// Span, when set, limits a running frame to rows whose time column is
	// within the duration before the current row's.
	Span Expr
	// ResetBefore and ResetAfter restart the running state before or after a
	// row for which the predicate is TRUE.
	ResetBefore Expr
	ResetAfter  Expr
}

// Window evaluates aggregate and window functions per row and appends their
// results as columns, keeping every input row in input order.
type Window struct {
	Input       Node
	PartitionBy []NamedExpr
	Funcs       []NamedAgg
	Frame       Frame
	// DropNullPartitions excludes rows whose partition values contain a
	// NULL from computation; they pass through with NULL results.
	DropNullPartitions bool
}

func (w *Window) Inputs() []Node { return []Node{w.Input} }
func (w *Window) input() Node    { return w.Input }
func (w *Window) WithInputs(in []Node) Node {
	c := *w
	c.Input = one(in, "Window")
	return &c
}
func (*Window) isNode() {}
func (w *Window) Label() string {
	var b strings.Builder
	if w.Frame.Kind == FrameRunning {
		b.WriteString("Window[running")
		if w.Frame.Rows > 0 {
			fmt.Fprintf(&b, " rows=%d", w.Frame.Rows)
		}
		if w.Frame.ExcludeCurrent {
			b.WriteString(" exclude_current")
		}
		if w.Frame.Span != nil {
			b.WriteString(" span=" + w.Frame.Span.String())
		}
		if w.Frame.ResetBefore != nil {
			b.WriteString(" reset_before=" + w.Frame.ResetBefore.String())
		}
		if w.Frame.ResetAfter != nil {
			b.WriteString(" reset_after=" + w.Frame.ResetAfter.String())
		}
		b.WriteString("](")
	} else {
		b.WriteString("Window[partition](")
	}
	for i, f := range w.Funcs {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(QuoteName(f.Name) + " = " + f.Agg.String())
	}
	if len(w.PartitionBy) > 0 {
		b.WriteString(" BY ")
		for i, p := range w.PartitionBy {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(p.Expr.String())
		}
	}
	b.WriteString(")")
	return b.String()
}
func (w *Window) Schema() Schema {
	s := w.Input.Schema()
	for _, f := range w.Funcs {
		s = s.With(Column{Name: f.Name, Type: f.Agg.Type(), Nullable: true})
	}
	return s
}

// SortKey is one ordering key. NULLs sort before all values when NullsFirst
// is set and after them otherwise.
type SortKey struct {
	Expr       Expr
	Desc       bool
	NullsFirst bool
}

func sortKeysString(keys []SortKey) string {
	parts := make([]string, len(keys))
	for i, k := range keys {
		s := k.Expr.String()
		if k.Desc {
			s += " DESC"
		}
		if k.NullsFirst {
			s += " NULLS FIRST"
		} else {
			s += " NULLS LAST"
		}
		parts[i] = s
	}
	return strings.Join(parts, ", ")
}

// Sort orders rows by Keys; ties keep input order.
type Sort struct {
	Input Node
	Keys  []SortKey
}

func (s *Sort) Inputs() []Node { return []Node{s.Input} }
func (s *Sort) input() Node    { return s.Input }
func (s *Sort) WithInputs(in []Node) Node {
	c := *s
	c.Input = one(in, "Sort")
	return &c
}
func (s *Sort) Schema() Schema { return s.Input.Schema() }
func (*Sort) isNode()          {}
func (s *Sort) Label() string  { return "Sort(" + sortKeysString(s.Keys) + ")" }

// Limit skips Offset rows and then passes at most Count rows; Count < 0
// means no upper bound.
type Limit struct {
	Input  Node
	Count  int64
	Offset int64
}

func (l *Limit) Inputs() []Node { return []Node{l.Input} }
func (l *Limit) input() Node    { return l.Input }
func (l *Limit) WithInputs(in []Node) Node {
	c := *l
	c.Input = one(in, "Limit")
	return &c
}
func (l *Limit) Schema() Schema { return l.Input.Schema() }
func (*Limit) isNode()          {}
func (l *Limit) Label() string {
	if l.Offset > 0 {
		return fmt.Sprintf("Limit(%d, offset=%d)", l.Count, l.Offset)
	}
	return fmt.Sprintf("Limit(%d)", l.Count)
}

// TopN is Sort followed by Limit, executed with a bounded heap.
type TopN struct {
	Input  Node
	Keys   []SortKey
	Count  int64
	Offset int64
}

func (t *TopN) Inputs() []Node { return []Node{t.Input} }
func (t *TopN) input() Node    { return t.Input }
func (t *TopN) WithInputs(in []Node) Node {
	c := *t
	c.Input = one(in, "TopN")
	return &c
}
func (t *TopN) Schema() Schema { return t.Input.Schema() }
func (*TopN) isNode()          {}
func (t *TopN) Label() string {
	s := fmt.Sprintf("TopN(%d, %s", t.Count, sortKeysString(t.Keys))
	if t.Offset > 0 {
		s += fmt.Sprintf(", offset=%d", t.Offset)
	}
	return s + ")"
}

// Reverse emits its input's rows in reverse order.
type Reverse struct {
	Input Node
}

func (r *Reverse) Inputs() []Node { return []Node{r.Input} }
func (r *Reverse) input() Node    { return r.Input }
func (r *Reverse) WithInputs(in []Node) Node {
	c := *r
	c.Input = one(in, "Reverse")
	return &c
}
func (r *Reverse) Schema() Schema { return r.Input.Schema() }
func (*Reverse) isNode()          {}
func (*Reverse) Label() string    { return "Reverse()" }

// Dedup keeps the first Count rows for each distinct combination of Keys.
// Rows where any key is NULL are dropped unless KeepNull is set, in which
// case they all pass. With Consecutive only adjacent duplicates collapse.
type Dedup struct {
	Input       Node
	Keys        []Expr
	Count       int64
	KeepNull    bool
	Consecutive bool
}

func (d *Dedup) Inputs() []Node { return []Node{d.Input} }
func (d *Dedup) input() Node    { return d.Input }
func (d *Dedup) WithInputs(in []Node) Node {
	c := *d
	c.Input = one(in, "Dedup")
	return &c
}
func (d *Dedup) Schema() Schema { return d.Input.Schema() }
func (*Dedup) isNode()          {}
func (d *Dedup) Label() string {
	s := fmt.Sprintf("Dedup(%s, count=%d", joinExprs(d.Keys), d.Count)
	if d.KeepNull {
		s += ", keep_null"
	}
	if d.Consecutive {
		s += ", consecutive"
	}
	return s + ")"
}

// JoinKind is the kind of a Join.
type JoinKind uint8

const (
	JoinInner JoinKind = iota + 1
	JoinLeft
	JoinRight
	JoinFull
	// JoinSemi emits each left row that has a match, once, with left columns.
	JoinSemi
	// JoinAnti emits each left row that has no match, with left columns.
	JoinAnti
	JoinCross
)

var joinKindNames = map[JoinKind]string{
	JoinInner: "inner", JoinLeft: "left", JoinRight: "right", JoinFull: "full",
	JoinSemi: "semi", JoinAnti: "anti", JoinCross: "cross",
}

func (k JoinKind) String() string { return joinKindNames[k] }

// JoinCollision decides which side's value a joined row keeps for a column
// name both sides produce.
type JoinCollision uint8

const (
	// CollisionRight keeps the right side's column.
	CollisionRight JoinCollision = iota
	// CollisionLeft keeps the left side's column.
	CollisionLeft
)

// Join combines rows of Left and Right. Rows match when every
// LeftKeys[i] = RightKeys[i] (NULL never matches) and Cond, when set, is TRUE
// for the combined row. MaxMatches, when positive, caps the right rows
// joined to one left row.
type Join struct {
	Left       Node
	Right      Node
	Kind       JoinKind
	LeftKeys   []Expr
	RightKeys  []Expr
	Cond       Expr
	MaxMatches int64
	Collision  JoinCollision
	// LeftQualifier and RightQualifier, when set, make each joined row also
	// carry every column of that side under "<qualifier>.<column>".
	LeftQualifier  string
	RightQualifier string
}

func (j *Join) Inputs() []Node { return []Node{j.Left, j.Right} }
func (j *Join) WithInputs(in []Node) Node {
	if len(in) != 2 {
		panic("ir: Join takes 2 inputs")
	}
	c := *j
	c.Left, c.Right = in[0], in[1]
	return &c
}
func (*Join) isNode() {}
func (j *Join) Label() string {
	var b strings.Builder
	b.WriteString("Join[" + j.Kind.String() + "](")
	for i := range j.LeftKeys {
		if i > 0 {
			b.WriteString(" AND ")
		}
		b.WriteString(j.LeftKeys[i].String() + " = " + j.RightKeys[i].String())
	}
	if j.Cond != nil {
		if len(j.LeftKeys) > 0 {
			b.WriteString(" AND ")
		}
		b.WriteString(j.Cond.String())
	}
	b.WriteString(")")
	if j.MaxMatches > 0 {
		fmt.Fprintf(&b, " max=%d", j.MaxMatches)
	}
	if j.Collision == CollisionLeft {
		b.WriteString(" keep_left")
	}
	if j.LeftQualifier != "" || j.RightQualifier != "" {
		b.WriteString(" qualify=" + j.LeftQualifier + "," + j.RightQualifier)
	}
	return b.String()
}
func (j *Join) Schema() Schema {
	l := j.Left.Schema()
	if j.Kind == JoinSemi || j.Kind == JoinAnti {
		return l
	}
	r := j.Right.Schema()
	out := Schema{Open: l.Open || r.Open}
	leftNullable := j.Kind == JoinRight || j.Kind == JoinFull
	rightNullable := j.Kind == JoinLeft || j.Kind == JoinFull
	for _, c := range l.Columns {
		if _, dup := r.Lookup(c.Name); dup && j.Collision == CollisionRight {
			continue
		}
		cc := c
		cc.Nullable = cc.Nullable || leftNullable
		out.Columns = append(out.Columns, cc)
	}
	for _, c := range r.Columns {
		if _, dup := l.Lookup(c.Name); dup && j.Collision == CollisionLeft {
			continue
		}
		cc := c
		cc.Nullable = cc.Nullable || rightNullable
		out.Columns = append(out.Columns, cc)
	}
	qualify := func(q string, s Schema, nullable bool) {
		if q == "" {
			return
		}
		for _, c := range s.Columns {
			cc := c
			cc.Name = q + "." + c.Name
			cc.Nullable = cc.Nullable || nullable
			out.Columns = append(out.Columns, cc)
		}
		if s.Open {
			out.Open = true
		}
	}
	qualify(j.LeftQualifier, l, leftNullable)
	qualify(j.RightQualifier, r, rightNullable)
	return out
}

// UnionMode selects how Union orders rows.
type UnionMode uint8

const (
	// UnionAll emits all rows of each input in turn.
	UnionAll UnionMode = iota
	// UnionByTime merges the inputs ordered by the time column, newest
	// first.
	UnionByTime
)

// Union concatenates the rows of its inputs, matching columns by name.
type Union struct {
	Sources []Node
	Mode    UnionMode
}

func (u *Union) Inputs() []Node { return u.Sources }
func (u *Union) WithInputs(in []Node) Node {
	c := *u
	c.Sources = append([]Node(nil), in...)
	return &c
}
func (*Union) isNode() {}
func (u *Union) Label() string {
	if u.Mode == UnionByTime {
		return fmt.Sprintf("Union[by_time](%d inputs)", len(u.Sources))
	}
	return fmt.Sprintf("Union(%d inputs)", len(u.Sources))
}
func (u *Union) Schema() Schema {
	ss := make([]Schema, len(u.Sources))
	for i, s := range u.Sources {
		ss[i] = s.Schema()
	}
	return UnionSchemas(ss...)
}

// ZipColumns pairs the n-th row of Left with the n-th row of Right and emits
// their combined columns; unpaired rows are emitted alone. Right columns
// replace Left columns of the same name only when Override is set.
type ZipColumns struct {
	Left     Node
	Right    Node
	Override bool
}

func (z *ZipColumns) Inputs() []Node { return []Node{z.Left, z.Right} }
func (z *ZipColumns) WithInputs(in []Node) Node {
	if len(in) != 2 {
		panic("ir: ZipColumns takes 2 inputs")
	}
	c := *z
	c.Left, c.Right = in[0], in[1]
	return &c
}
func (*ZipColumns) isNode() {}
func (z *ZipColumns) Label() string {
	if z.Override {
		return "ZipColumns(override)"
	}
	return "ZipColumns()"
}
func (z *ZipColumns) Schema() Schema {
	s := z.Left.Schema()
	for _, c := range z.Right.Schema().Columns {
		if _, ok := s.Lookup(c.Name); ok && !z.Override {
			continue
		}
		cc := c
		cc.Nullable = true
		s = s.With(cc)
	}
	if z.Right.Schema().Open {
		s.Open = true
	}
	return s
}

// ExtractMethod selects how Extract derives columns from text.
type ExtractMethod uint8

const (
	// ExtractRegex applies an RE2 pattern; each named group becomes a column.
	ExtractRegex ExtractMethod = iota + 1
	// ExtractJSON parses the text as JSON. With Path set it extracts the
	// value at that path into the single output column; without, every
	// top-level member becomes a column.
	ExtractJSON
)

var extractMethodNames = map[ExtractMethod]string{
	ExtractRegex: "regex", ExtractJSON: "json",
}

func (m ExtractMethod) String() string { return extractMethodNames[m] }

// Extract derives columns from a string expression. A row whose text does
// not match gets NoMatch in the outputs it would have produced: NULL, or the
// empty string when NoMatchEmpty is set. With MaxMatches > 1 a regex output
// holds an array of up to MaxMatches matches.
type Extract struct {
	Input        Node
	Source       Expr
	Method       ExtractMethod
	Pattern      string
	Path         string
	Outputs      []Column
	MaxMatches   int
	NoMatchEmpty bool
}

func (e *Extract) Inputs() []Node { return []Node{e.Input} }
func (e *Extract) input() Node    { return e.Input }
func (e *Extract) WithInputs(in []Node) Node {
	c := *e
	c.Input = one(in, "Extract")
	return &c
}
func (*Extract) isNode() {}
func (e *Extract) Label() string {
	s := "Extract[" + e.Method.String() + "](" + e.Source.String()
	if e.Pattern != "" {
		s += ", " + strconv.Quote(e.Pattern)
	}
	if e.Path != "" {
		s += ", path=" + strconv.Quote(e.Path)
	}
	names := make([]string, len(e.Outputs))
	for i, c := range e.Outputs {
		names[i] = c.Name
	}
	s += " -> " + strings.Join(names, ", ") + ")"
	if e.MaxMatches > 1 {
		s += fmt.Sprintf(" max_matches=%d", e.MaxMatches)
	}
	return s
}
func (e *Extract) Schema() Schema {
	s := e.Input.Schema()
	for _, c := range e.Outputs {
		s = s.With(c)
	}
	if e.Method == ExtractJSON && e.Path == "" {
		s.Open = true
	}
	return s
}

// PatternMethod selects the log-pattern mining algorithm.
type PatternMethod uint8

const (
	// PatternSimple replaces every maximal run of characters matching the
	// configured regex (by default alphanumeric runs) with a placeholder.
	PatternSimple PatternMethod = iota + 1
	// PatternCluster groups texts by token-level similarity and replaces
	// variable tokens with a placeholder.
	PatternCluster
)

// Patterns derives a log pattern from a text column. In row mode it adds the
// pattern column to each row; in aggregate mode it emits one row per
// pattern with its count and sample texts.
type Patterns struct {
	Input     Node
	Source    Expr
	Method    PatternMethod
	Output    string
	Aggregate bool
	// Regex is the character class PatternSimple replaces.
	Regex string
	// Threshold is PatternCluster's similarity threshold in (0, 1].
	Threshold float64
	// MaxSamples bounds the samples kept per pattern in aggregate mode.
	MaxSamples int
	GroupBy    []Expr
}

func (p *Patterns) Inputs() []Node { return []Node{p.Input} }
func (p *Patterns) input() Node    { return p.Input }
func (p *Patterns) WithInputs(in []Node) Node {
	c := *p
	c.Input = one(in, "Patterns")
	return &c
}
func (*Patterns) isNode() {}
func (p *Patterns) Label() string {
	m := "simple"
	if p.Method == PatternCluster {
		m = "cluster"
	}
	mode := "rows"
	if p.Aggregate {
		mode = "aggregate"
	}
	return fmt.Sprintf("Patterns[%s, %s](%s -> %s)", m, mode, p.Source, p.Output)
}

// Pattern-aggregate output column names.
const (
	PatternCountColumn   = "pattern_count"
	PatternSamplesColumn = "sample_logs"
)

func (p *Patterns) Schema() Schema {
	if !p.Aggregate {
		return p.Input.Schema().With(Column{Name: p.Output, Type: String, Nullable: true})
	}
	var s Schema
	for _, g := range p.GroupBy {
		if fr, ok := g.(*FieldRef); ok {
			s.Columns = append(s.Columns, Column{Name: fr.Name, Type: fr.T, Nullable: true})
		}
	}
	s.Columns = append(s.Columns,
		Column{Name: p.Output, Type: String},
		Column{Name: PatternCountColumn, Type: Int},
		Column{Name: PatternSamplesColumn, Type: ArrayOf(String)},
	)
	return s
}

// Unnest emits one row per element of an array column, with the element in
// column As (Field when As is empty). A non-array value passes as a single
// element; an empty array or NULL emits the row once with a NULL element.
// Limit, when positive, caps the rows emitted per input row.
type Unnest struct {
	Input Node
	Field string
	As    string
	Limit int
}

func (u *Unnest) Inputs() []Node { return []Node{u.Input} }
func (u *Unnest) input() Node    { return u.Input }
func (u *Unnest) WithInputs(in []Node) Node {
	c := *u
	c.Input = one(in, "Unnest")
	return &c
}
func (*Unnest) isNode() {}
func (u *Unnest) Label() string {
	s := "Unnest(" + QuoteName(u.Field)
	if u.As != "" && u.As != u.Field {
		s += " AS " + QuoteName(u.As)
	}
	if u.Limit > 0 {
		s += fmt.Sprintf(", limit=%d", u.Limit)
	}
	return s + ")"
}
func (u *Unnest) Schema() Schema {
	s := u.Input.Schema()
	t := Any
	if c, ok := s.Lookup(u.Field); ok {
		t = c.Type.ElemType()
	}
	name := u.Field
	if u.As != "" {
		name = u.As
	}
	return s.With(Column{Name: name, Type: t, Nullable: true})
}

// FlattenObject replaces an object column with one column per member,
// named "<field>.<member>" for nested members, recursively. With Aliases the
// top-level members become columns with those names in member order.
type FlattenObject struct {
	Input   Node
	Field   string
	Aliases []string
}

func (f *FlattenObject) Inputs() []Node { return []Node{f.Input} }
func (f *FlattenObject) input() Node    { return f.Input }
func (f *FlattenObject) WithInputs(in []Node) Node {
	c := *f
	c.Input = one(in, "FlattenObject")
	return &c
}
func (*FlattenObject) isNode() {}
func (f *FlattenObject) Label() string {
	s := "FlattenObject(" + QuoteName(f.Field)
	if len(f.Aliases) > 0 {
		s += " AS " + strings.Join(f.Aliases, ", ")
	}
	return s + ")"
}
func (f *FlattenObject) Schema() Schema {
	s := f.Input.Schema()
	for _, a := range f.Aliases {
		s = s.With(Column{Name: a, Type: Any, Nullable: true})
	}
	s.Open = true
	return s
}

// FillNull replaces NULL and absent values. With Fields set it fills those
// columns with their own values; otherwise it fills every column of the row
// with Default.
type FillNull struct {
	Input   Node
	Fields  []Assign
	Default Expr
}

func (f *FillNull) Inputs() []Node { return []Node{f.Input} }
func (f *FillNull) input() Node    { return f.Input }
func (f *FillNull) WithInputs(in []Node) Node {
	c := *f
	c.Input = one(in, "FillNull")
	return &c
}
func (*FillNull) isNode() {}
func (f *FillNull) Label() string {
	if len(f.Fields) == 0 {
		return "FillNull(* = " + f.Default.String() + ")"
	}
	parts := make([]string, len(f.Fields))
	for i, a := range f.Fields {
		parts[i] = QuoteName(a.Name) + " = " + a.Expr.String()
	}
	return "FillNull(" + strings.Join(parts, ", ") + ")"
}
func (f *FillNull) Schema() Schema {
	s := f.Input.Schema()
	for _, a := range f.Fields {
		t := a.Expr.Type()
		if c, ok := s.Lookup(a.Name); ok {
			t = Unify(c.Type, t)
		}
		s = s.With(Column{Name: a.Name, Type: t})
	}
	return s
}

// Transpose turns up to MaxRows rows into columns: each input column becomes
// an output row whose first column (HeaderColumn) holds the column name and
// whose following columns, named "row 1", "row 2", ..., hold the values.
type Transpose struct {
	Input        Node
	MaxRows      int
	HeaderColumn string
}

func (t *Transpose) Inputs() []Node { return []Node{t.Input} }
func (t *Transpose) input() Node    { return t.Input }
func (t *Transpose) WithInputs(in []Node) Node {
	c := *t
	c.Input = one(in, "Transpose")
	return &c
}
func (*Transpose) isNode() {}
func (t *Transpose) Label() string {
	return fmt.Sprintf("Transpose(max_rows=%d, header=%s)", t.MaxRows, t.HeaderColumn)
}
func (t *Transpose) Schema() Schema {
	s := Schema{Columns: []Column{{Name: t.HeaderColumn, Type: String}}}
	for i := 1; i <= t.MaxRows; i++ {
		s.Columns = append(s.Columns, Column{Name: fmt.Sprintf("row %d", i), Type: Any, Nullable: true})
	}
	return s
}

// Totals appends sums of numeric columns. With RowTotals each row gets a
// RowColumn holding the sum of its numeric Fields (all numeric columns when
// Fields is empty). With ColumnTotals a final row holds each column's sum,
// and LabelColumn, when set, holds LabelValue in that row.
type Totals struct {
	Input        Node
	Fields       []string
	RowTotals    bool
	RowColumn    string
	ColumnTotals bool
	LabelColumn  string
	LabelValue   string
}

func (t *Totals) Inputs() []Node { return []Node{t.Input} }
func (t *Totals) input() Node    { return t.Input }
func (t *Totals) WithInputs(in []Node) Node {
	c := *t
	c.Input = one(in, "Totals")
	return &c
}
func (*Totals) isNode() {}
func (t *Totals) Label() string {
	var parts []string
	if t.RowTotals {
		parts = append(parts, "row="+t.RowColumn)
	}
	if t.ColumnTotals {
		parts = append(parts, "column")
	}
	if len(t.Fields) > 0 {
		parts = append(parts, "fields=["+strings.Join(t.Fields, ", ")+"]")
	}
	return "Totals(" + strings.Join(parts, ", ") + ")"
}
func (t *Totals) Schema() Schema {
	s := t.Input.Schema()
	if t.RowTotals {
		s = s.With(Column{Name: t.RowColumn, Type: Float, Nullable: true})
	}
	if t.ColumnTotals && t.LabelColumn != "" {
		if _, ok := s.Lookup(t.LabelColumn); !ok {
			s = s.With(Column{Name: t.LabelColumn, Type: String, Nullable: true})
		}
	}
	return s
}

// CombineValues merges rows that are identical on every column except
// Field into one row whose Field holds the array of their Field values in
// input order.
type CombineValues struct {
	Input Node
	Field string
}

func (c *CombineValues) Inputs() []Node { return []Node{c.Input} }
func (c *CombineValues) input() Node    { return c.Input }
func (c *CombineValues) WithInputs(in []Node) Node {
	cc := *c
	cc.Input = one(in, "CombineValues")
	return &cc
}
func (*CombineValues) isNode()         {}
func (c *CombineValues) Label() string { return "CombineValues(" + QuoteName(c.Field) + ")" }
func (c *CombineValues) Schema() Schema {
	s := c.Input.Schema()
	t := Any
	if col, ok := s.Lookup(c.Field); ok {
		t = col.Type
	}
	return s.With(Column{Name: c.Field, Type: ArrayOf(t), Nullable: true})
}
