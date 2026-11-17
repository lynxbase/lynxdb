// Package ppl compiles OpenSearch Piped Processing Language queries into
// LynxDB's canonical IR.
//
// Compilation parses the query (package parser), resolves every field
// reference against the schema each command produces, resolves functions
// through the PPL function catalog onto the IR function registry, checks
// static types, and lowers each command to IR operators. The result is a
// validated ir.Plan; nothing here depends on how the plan is executed.
package ppl

import (
	"strings"

	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/ppl/ast"
	"github.com/lynxbase/lynxdb/pkg/ppl/parser"
)

// TimestampAlias is the OpenSearch time field name; LynxDB resolves it to
// the event time column.
const TimestampAlias = "@timestamp"

// Result is a compiled query.
type Result struct {
	Plan *ir.Plan
	// Explain is set when the query asked for its plan instead of rows.
	Explain     bool
	ExplainMode string
	// Sources lists the source names the query reads, in first-use order.
	Sources []string
}

// Compile parses and compiles a PPL query.
func Compile(src string) (*Result, error) {
	q, err := parser.Parse(src)
	if err != nil {
		if pe, ok := AsError(err); ok {
			return nil, pe
		}
		return nil, err
	}
	return CompileQuery(q)
}

// CompileQuery compiles a parsed PPL query.
func CompileQuery(q *ast.Query) (*Result, error) {
	c := &compiler{}
	root, err := c.pipeline(q.Pipeline)
	if err != nil {
		return nil, err
	}
	plan := &ir.Plan{Root: root}
	if err := ir.Validate(plan); err != nil {
		return nil, &Error{Kind: ErrSemantic, Pos: q.Span.Start, End: q.Span.End, Msg: err.Error()}
	}
	return &Result{Plan: plan, Explain: q.Explain, ExplainMode: q.ExplainMode, Sources: c.sources}, nil
}

type compiler struct {
	sources []string
	// st is the pipeline whose commands are being compiled.
	st *pipe
	// outer holds the pipelines enclosing the subquery being compiled,
	// innermost last.
	outer []*pipe
}

func (c *compiler) noteSource(name string) {
	for _, s := range c.sources {
		if s == name {
			return
		}
	}
	c.sources = append(c.sources, name)
}

// pipe is the state of a pipeline being compiled.
type pipe struct {
	node ir.Node
	// origin is the operator the pipeline's source command produced; it is
	// the input appendcol subsearches read.
	origin ir.Node
	// aliases maps join and source aliases to their qualifier.
	aliases map[string]bool
	// sch is the output schema of schNode, an earlier value of node.
	sch     ir.Schema
	schNode ir.Node
}

// schema returns the output schema of the pipeline, deriving it from the
// schema of the previous pipeline node instead of from the source.
func (p *pipe) schema() ir.Schema {
	if p.schNode != p.node {
		if p.schNode == nil {
			p.sch = p.node.Schema()
		} else {
			p.sch = deriveSchema(p.node, p.schNode, p.sch)
		}
		p.schNode = p.node
	}
	return p.sch
}

// deriveSchema returns the output schema of n given that known, a node
// that may occur below n, has the schema s.
func deriveSchema(n, known ir.Node, s ir.Schema) ir.Schema {
	if n == known {
		return s
	}
	var nodes [2]ir.Node
	ins := ir.AppendInputs(nodes[:0], n)
	if len(ins) == 0 {
		return n.Schema()
	}
	var schemas [2]ir.Schema
	in := schemas[:0]
	for _, x := range ins {
		in = append(in, deriveSchema(x, known, s))
	}
	return ir.DeriveSchema(n, in...)
}

func (c *compiler) pipeline(pl *ast.Pipeline) (ir.Node, error) {
	st, err := c.sourceCommand(pl.Commands[0])
	if err != nil {
		return nil, err
	}
	enclosing := c.st
	c.st = st
	for _, cmd := range pl.Commands[1:] {
		if err := c.command(st, cmd); err != nil {
			return nil, err
		}
	}
	c.st = enclosing
	return st.node, nil
}

// commands applies commands without a source command to an existing input.
func (c *compiler) commands(input ir.Node, origin ir.Node, cmds []ast.Command) (ir.Node, error) {
	st := &pipe{node: input, origin: origin}
	enclosing := c.st
	c.st = st
	for _, cmd := range cmds {
		if err := c.command(st, cmd); err != nil {
			return nil, err
		}
	}
	c.st = enclosing
	return st.node, nil
}

// EventSchema is the schema of events read from a source: the time and raw
// text columns are typed, every other column is discovered at read time.
func EventSchema() ir.Schema {
	return ir.OpenSchema(eventColumns...)
}

// eventColumns is shared by every event schema; schemas are never modified
// in place, and its full capacity makes appends copy.
var eventColumns = []ir.Column{
	{Name: ir.TimeColumn, Type: ir.Timestamp},
	{Name: ir.RawColumn, Type: ir.String, Nullable: true},
}

func canonicalField(name string) string {
	if name == TimestampAlias {
		return ir.TimeColumn
	}
	return name
}

// resolve returns a typed reference to a column of s.
func resolve(f *ast.Field, s ir.Schema) (*ir.FieldRef, error) {
	return resolveName(f, f.Name(), s)
}

func resolveName(n ast.Node, name string, s ir.Schema) (*ir.FieldRef, error) {
	name = canonicalField(name)
	t, ok := s.Resolve(name)
	if !ok {
		return nil, unknownField(n, name, s)
	}
	return &ir.FieldRef{Name: name, T: t}, nil
}

func unknownField(n ast.Node, name string, s ir.Schema) *Error {
	e := semanticErr(n, "field %s not found", ir.QuoteName(name))
	e.field = name
	if names := s.Names(); len(names) > 0 && len(names) <= 30 {
		e.Hint = "available fields: " + strings.Join(names, ", ")
	}
	return e
}

func (c *compiler) unsupportedCommand(cmd ast.Command) *Error {
	reason := "LynxDB does not implement it"
	if entry, ok := LookupCommand(cmd.Name()); ok && entry.Notes != "" {
		reason = entry.Notes
	}
	return errAt(ErrUnsupportedCommand, cmd, "the %s command is not supported: %s", cmd.Name(), reason)
}
