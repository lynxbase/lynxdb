package ppl

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/ppl/ast"
)

func (c *compiler) sourceCommand(cmd ast.Command) (*pipe, error) {
	switch n := cmd.(type) {
	case *ast.SearchCommand:
		return c.search(n)
	case *ast.DescribeCommand:
		c.noteSource(n.Source.Name)
		node := &ir.SourceSchema{Source: ir.Source{Name: n.Source.Name, Pattern: strings.ContainsRune(n.Source.Name, '*')}}
		return &pipe{node: node, origin: node}, nil
	case *ast.MakeResultsCommand:
		node, err := makeResults(n)
		if err != nil {
			return nil, err
		}
		return &pipe{node: node, origin: node}, nil
	case *ast.MultiSearchCommand:
		u := &ir.Union{Mode: ir.UnionByTime}
		for _, s := range n.Searches {
			sub, err := c.pipeline(s)
			if err != nil {
				return nil, err
			}
			u.Sources = append(u.Sources, sub)
		}
		return &pipe{node: u, origin: u}, nil
	case *ast.UnionCommand:
		node, err := c.union(nil, n)
		if err != nil {
			return nil, err
		}
		return &pipe{node: node, origin: node}, nil
	case *ast.UnsupportedCommand:
		return nil, c.unsupportedCommand(n)
	}
	return nil, semanticErr(cmd, "%s cannot start a query", cmd.Name())
}

func (c *compiler) union(input ir.Node, n *ast.UnionCommand) (ir.Node, error) {
	u := &ir.Union{}
	if input != nil {
		u.Sources = append(u.Sources, input)
	}
	for _, d := range n.Datasets {
		sub, err := c.pipeline(d)
		if err != nil {
			return nil, err
		}
		u.Sources = append(u.Sources, sub)
	}
	if len(u.Sources) < 2 {
		return nil, semanticErr(n, "union needs at least two datasets")
	}
	var out ir.Node = u
	if n.MaxOut > 0 {
		out = &ir.Limit{Input: u, Count: n.MaxOut}
	}
	return out, nil
}

func makeResults(n *ast.MakeResultsCommand) (ir.Node, error) {
	if n.Format == "" {
		count := n.Count
		if count < 0 {
			count = 0
		}
		if count > 5000 {
			return nil, semanticErr(n, "makeresults count must be at most 5000")
		}
		rows := make([][]ir.Expr, count)
		for i := range rows {
			rows[i] = []ir.Expr{}
		}
		vals := &ir.Values{Rows: rows}
		return &ir.Compute{Input: vals, Assigns: []ir.Assign{{Name: ir.TimeColumn, Expr: callIR("now")}}}, nil
	}
	if n.Data == "" {
		return nil, semanticErr(n, "makeresults format=%s needs data=<string>", n.Format)
	}
	switch n.Format {
	case "csv":
		return csvValues(n)
	case "json":
		return jsonValues(n)
	}
	return nil, semanticErr(n, "makeresults format must be csv or json")
}

func csvValues(n *ast.MakeResultsCommand) (ir.Node, error) {
	r := csv.NewReader(strings.NewReader(n.Data))
	r.TrimLeadingSpace = true
	recs, err := r.ReadAll()
	if err != nil || len(recs) == 0 {
		return nil, semanticErr(n, "makeresults: invalid CSV data")
	}
	out := &ir.Values{}
	types := make([]ir.Type, len(recs[0]))
	for i, h := range recs[0] {
		name, tname, typed := strings.Cut(strings.TrimSpace(h), ":")
		types[i] = ir.String
		if typed {
			t, ok := castTypes[strings.ToLower(tname)]
			if !ok {
				return nil, semanticErr(n, "makeresults: unknown column type %s", tname)
			}
			types[i] = t
		}
		out.Out.Columns = append(out.Out.Columns, ir.Column{Name: name, Type: types[i], Nullable: true})
	}
	for _, rec := range recs[1:] {
		row := make([]ir.Expr, len(types))
		for i := range types {
			if i >= len(rec) || rec[i] == "" {
				row[i] = ir.NullLit()
				continue
			}
			v, err := typedLiteral(rec[i], types[i])
			if err != nil {
				return nil, semanticErr(n, "makeresults: %v", err)
			}
			row[i] = v
		}
		out.Rows = append(out.Rows, row)
	}
	return out, nil
}

func typedLiteral(s string, t ir.Type) (*ir.Literal, error) {
	switch t.Kind {
	case ir.KindInt:
		v, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%q is not an int", s)
		}
		return ir.IntLit(v), nil
	case ir.KindFloat:
		v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		if err != nil {
			return nil, fmt.Errorf("%q is not a double", s)
		}
		return ir.FloatLit(v), nil
	case ir.KindBool:
		v, err := strconv.ParseBool(strings.TrimSpace(s))
		if err != nil {
			return nil, fmt.Errorf("%q is not a boolean", s)
		}
		return ir.BoolLit(v), nil
	case ir.KindTimestamp:
		v, ok := parseTimestamp(s)
		if !ok {
			return nil, fmt.Errorf("%q is not a timestamp", s)
		}
		return ir.TimestampLit(v), nil
	}
	return ir.StringLit(s), nil
}

func jsonValues(n *ast.MakeResultsCommand) (ir.Node, error) {
	var rows []map[string]any
	dec := json.NewDecoder(strings.NewReader(n.Data))
	dec.UseNumber()
	if err := dec.Decode(&rows); err != nil {
		var one map[string]any
		dec = json.NewDecoder(strings.NewReader(n.Data))
		dec.UseNumber()
		if err2 := dec.Decode(&one); err2 != nil {
			return nil, semanticErr(n, "makeresults: data must be a JSON object or an array of objects")
		}
		rows = []map[string]any{one}
	}
	var names []string
	seen := map[string]bool{}
	for _, r := range rows {
		keys := make([]string, 0, len(r))
		for k := range r {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if !seen[k] {
				seen[k] = true
				names = append(names, k)
			}
		}
	}
	out := &ir.Values{}
	types := make([]ir.Type, len(names))
	for i := range types {
		types[i] = ir.Null
	}
	for _, r := range rows {
		row := make([]ir.Expr, len(names))
		for i, name := range names {
			lit := jsonLiteral(r[name])
			row[i] = lit
			types[i] = ir.Unify(types[i], lit.T)
		}
		out.Rows = append(out.Rows, row)
	}
	for i, name := range names {
		t := types[i]
		if t.Kind == ir.KindNull {
			t = ir.Any
		}
		out.Out.Columns = append(out.Out.Columns, ir.Column{Name: name, Type: t, Nullable: true})
	}
	return out, nil
}

func jsonLiteral(v any) *ir.Literal {
	switch x := v.(type) {
	case nil:
		return ir.NullLit()
	case bool:
		return ir.BoolLit(x)
	case string:
		return ir.StringLit(x)
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return ir.IntLit(i)
		}
		f, _ := x.Float64()
		return ir.FloatLit(f)
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = jsonLiteral(e).V
		}
		return &ir.Literal{V: out, T: ir.ArrayOf(ir.Any)}
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = jsonLiteral(e).V
		}
		return &ir.Literal{V: out, T: ir.Object}
	}
	return ir.StringLit(fmt.Sprint(v))
}

func (c *compiler) command(st *pipe, cmd ast.Command) error {
	var err error
	switch n := cmd.(type) {
	case *ast.WhereCommand:
		err = c.where(st, n)
	case *ast.FieldsCommand:
		err = c.fields(st, n)
	case *ast.RenameCommand:
		err = c.rename(st, n)
	case *ast.ReplaceCommand:
		err = c.replace(st, n)
	case *ast.EvalCommand:
		err = c.eval(st, n)
	case *ast.StatsCommand:
		switch n.Command {
		case "stats":
			err = c.stats(st, n)
		default:
			err = c.windowStats(st, n)
		}
	case *ast.DedupCommand:
		err = c.dedup(st, n)
	case *ast.SortCommand:
		err = c.sort(st, n)
	case *ast.ReverseCommand:
		st.node = &ir.Reverse{Input: st.node}
	case *ast.HeadCommand:
		if n.Count < 0 || n.From < 0 {
			return semanticErr(n, "head size and offset must not be negative")
		}
		st.node = &ir.Limit{Input: st.node, Count: n.Count, Offset: n.From}
	case *ast.BinCommand:
		err = c.bin(st, n)
	case *ast.TopCommand:
		err = c.top(st, n)
	case *ast.ParseCommand:
		err = c.parse(st, n)
	case *ast.RexCommand:
		err = c.rex(st, n)
	case *ast.RegexCommand:
		err = c.regex(st, n)
	case *ast.SpathCommand:
		err = c.spath(st, n)
	case *ast.PatternsCommand:
		err = c.patterns(st, n)
	case *ast.LookupCommand:
		err = c.lookup(st, n)
	case *ast.FillnullCommand:
		err = c.fillnull(st, n)
	case *ast.ConvertCommand:
		err = c.convert(st, n)
	case *ast.TrendlineCommand:
		err = c.trendline(st, n)
	case *ast.FieldCommand:
		err = c.fieldCommand(st, n)
	case *ast.SubPipelineCommand:
		err = c.subPipelineCommand(st, n)
	case *ast.JoinCommand:
		err = c.join(st, n)
	case *ast.TotalsCommand:
		err = c.totals(st, n)
	case *ast.ChartCommand:
		err = c.chart(st, n)
	case *ast.TransposeCommand:
		if n.Rows < 1 || n.Rows > 10000 {
			return semanticErr(n, "transpose row count must be between 1 and 10000")
		}
		st.node = &ir.Transpose{Input: st.node, MaxRows: int(n.Rows), HeaderColumn: n.ColumnName}
	case *ast.XYSeriesCommand:
		err = c.xyseries(st, n)
	case *ast.UnionCommand:
		st.node, err = c.union(st.node, n)
	case *ast.UnsupportedCommand:
		return c.unsupportedCommand(n)
	default:
		return semanticErr(cmd, "%s is not valid after the first command", cmd.Name())
	}
	return err
}

func (c *compiler) where(st *pipe, n *ast.WhereCommand) error {
	x := c.ectx(st.schema())
	cond, err := x.expr(n.Cond)
	if err != nil {
		return err
	}
	if err := requireBool(n.Cond, cond); err != nil {
		return err
	}
	st.node = &ir.Filter{Input: st.node, Cond: cond}
	return nil
}

func (c *compiler) fields(st *pipe, n *ast.FieldsCommand) error {
	s := st.schema()
	names := make([]string, len(n.Fields))
	for i, f := range n.Fields {
		name := canonicalField(f)
		if !ir.IsNamePattern(name) {
			if _, ok := s.Resolve(name); !ok {
				return unknownField(n, name, s)
			}
		}
		names[i] = name
	}
	st.node = &ir.Project{Input: st.node, Names: names, Drop: n.Exclude}
	return nil
}

func (c *compiler) rename(st *pipe, n *ast.RenameCommand) error {
	s := st.schema()
	rn := &ir.RenameColumns{Input: st.node}
	for _, p := range n.Pairs {
		from, to := canonicalField(p.From), p.To
		if strings.Count(from, "*") != strings.Count(to, "*") {
			return semanticErr(n, "rename %s as %s: the source and target must have the same number of wildcards", from, to)
		}
		if !ir.IsNamePattern(from) {
			if _, ok := s.Resolve(from); !ok {
				return unknownField(n, from, s)
			}
		}
		rn.Renames = append(rn.Renames, ir.Rename{From: from, To: to})
		s = (&ir.RenameColumns{Input: &ir.Empty{Out: s}, Renames: []ir.Rename{{From: from, To: to}}}).Schema()
	}
	st.node = rn
	return nil
}

func (c *compiler) replace(st *pipe, n *ast.ReplaceCommand) error {
	s := st.schema()
	comp := &ir.Compute{Input: st.node}
	for _, f := range n.Fields {
		ref, err := resolve(f, s)
		if err != nil {
			return err
		}
		var e ir.Expr = ref
		for i, pat := range n.Patterns {
			repl := n.Replacements[i]
			if w := strings.Count(strings.ReplaceAll(repl, `\*`, ""), "*"); w != 0 && w != strings.Count(strings.ReplaceAll(pat, `\*`, ""), "*") {
				return semanticErr(n, "replace %q with %q: the replacement must have no wildcards or as many as the pattern", pat, repl)
			}
			e = callIR("wildcard_replace", e, ir.StringLit(pat), ir.StringLit(repl))
		}
		comp.Assigns = append(comp.Assigns, ir.Assign{Name: ref.Name, Expr: e})
	}
	st.node = comp
	return nil
}

func (c *compiler) eval(st *pipe, n *ast.EvalCommand) error {
	if n.Command == "fieldformat" {
		return c.unsupportedCommand(n)
	}
	comp := &ir.Compute{Input: st.node, Assigns: make([]ir.Assign, 0, len(n.Assigns))}
	var s ir.SchemaBuilder
	s.Init(st.schema(), len(n.Assigns))
	for _, a := range n.Assigns {
		x := c.ectx(s.Schema())
		v, err := x.expr(a.Expr)
		if err != nil {
			return err
		}
		if _, isAgg := v.(*ir.AggCall); isAgg {
			return semanticErr(a.Expr, "aggregations are not allowed in eval")
		}
		name := canonicalField(a.Field.Name())
		comp.Assigns = append(comp.Assigns, ir.Assign{Name: name, Expr: v})
		s.With(ir.Column{Name: name, Type: v.Type(), Nullable: true})
	}
	st.node = comp
	return nil
}

func boolArgValue(a ast.Arg) (bool, error) {
	if lit, ok := a.Value.(*ast.Literal); ok {
		switch strings.ToLower(lit.Text) {
		case "true", "t":
			return true, nil
		case "false", "f":
			return false, nil
		}
	}
	return false, semanticErr(a.Value, "%s must be true or false", a.Name)
}

func intArgValue(a ast.Arg) (int64, error) {
	if lit, ok := a.Value.(*ast.Literal); ok && lit.Kind == ast.LitInt {
		return strconv.ParseInt(lit.Text, 10, 64)
	}
	return 0, semanticErr(a.Value, "%s must be an integer", a.Name)
}

func stringArgValue(a ast.Arg) (string, error) {
	if lit, ok := a.Value.(*ast.Literal); ok && lit.Kind != ast.LitNull {
		return lit.Text, nil
	}
	return "", semanticErr(a.Value, "%s must be a string", a.Name)
}

// groupKeys compiles a by clause into named group expressions.
func (c *compiler) groupKeys(by *ast.ByClause, s ir.Schema) ([]ir.NamedExpr, error) {
	if by == nil {
		return nil, nil
	}
	var keys []ir.NamedExpr
	spanKey := func() error {
		sp := by.Span
		field := sp.Field
		if field == nil {
			field = &ast.Field{Parts: []string{ir.TimeColumn}}
		}
		ref, err := resolve(field, s)
		if err != nil {
			return err
		}
		spn, err := parseSpan(&ast.Field{Base: ast.Base{Span: sp.Span}}, sp.Value)
		if err != nil {
			return err
		}
		if spn.isTime() && ref.T.IsNumeric() {
			return typeErr(field, "a time span needs a timestamp field; %s is %s", ref.Name, ref.T)
		}
		name := sp.Alias
		if name == "" {
			name = fmt.Sprintf("span(%s,%s)", field.Name(), sp.Value)
		}
		keys = append(keys, ir.NamedExpr{Name: name, Expr: bucketExpr(ref, spn)})
		return nil
	}
	if by.Span != nil && by.SpanFirst {
		if err := spanKey(); err != nil {
			return nil, err
		}
	}
	for _, f := range by.Fields {
		ref, err := resolve(f, s)
		if err != nil {
			return nil, err
		}
		keys = append(keys, ir.NamedExpr{Name: ref.Name, Expr: ref})
	}
	if by.Span != nil && !by.SpanFirst {
		if err := spanKey(); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func (c *compiler) aggTerms(terms []ast.AggTerm, s ir.Schema, window bool, spanSeconds float64) ([]ir.NamedAgg, []ir.Assign, error) {
	x := c.ectx(s)
	var aggs []ir.NamedAgg
	var scaled []ir.Assign
	seen := map[string]bool{}
	for _, t := range terms {
		call, scale, err := x.aggCall(t, window, spanSeconds)
		if err != nil {
			return nil, nil, err
		}
		name := t.Alias
		if name == "" {
			name = t.Text
		}
		if seen[name] {
			return nil, nil, semanticErr(t.Func, "duplicate aggregation name %s; add an alias with as", name)
		}
		seen[name] = true
		aggs = append(aggs, ir.NamedAgg{Name: name, Agg: call})
		if scale != nil {
			ref := &ir.FieldRef{Name: name, T: ir.Float}
			scaled = append(scaled, ir.Assign{Name: name, Expr: &ir.Binary{Op: ir.OpMul, L: ref, R: scale, T: ir.Float}})
		}
	}
	return aggs, scaled, nil
}

func (c *compiler) stats(st *pipe, n *ast.StatsCommand) error {
	s := st.schema()
	agg := &ir.Aggregate{Input: st.node}
	for _, a := range n.Args {
		switch a.Name {
		case "bucket_nullable":
			v, err := boolArgValue(a)
			if err != nil {
				return err
			}
			agg.DropNullGroups = !v
		case "partitions", "allnum", "delim":
		case "dedup_splitvalues":
			return unsupportedFeature(n, "stats dedup_splitvalues is not supported")
		}
	}
	keys, err := c.groupKeys(n.By, s)
	if err != nil {
		return err
	}
	agg.Groups = keys
	aggs, scaled, err := c.aggTerms(n.Aggs, s, false, 0)
	if err != nil {
		return err
	}
	agg.Aggs = aggs
	var node ir.Node = agg
	if len(scaled) > 0 {
		node = &ir.Compute{Input: node, Assigns: scaled}
	}
	st.node = aggsFirst(node, agg)
	return nil
}

// aggsFirst reorders aggregate output to PPL's column order: aggregations
// first, then the group keys.
func aggsFirst(node ir.Node, agg *ir.Aggregate) ir.Node {
	if len(agg.Groups) == 0 {
		return node
	}
	var names []string
	for _, a := range agg.Aggs {
		names = append(names, a.Name)
	}
	for _, g := range agg.Groups {
		names = append(names, g.Name)
	}
	return &ir.Project{Input: node, Names: names}
}

func (c *compiler) windowStats(st *pipe, n *ast.StatsCommand) error {
	s := st.schema()
	w := &ir.Window{Input: st.node, Frame: ir.Frame{Kind: ir.FramePartition}}
	if n.Command == "streamstats" {
		w.Frame.Kind = ir.FrameRunning
	}
	global := true
	for _, a := range n.Args {
		switch a.Name {
		case "bucket_nullable":
			v, err := boolArgValue(a)
			if err != nil {
				return err
			}
			w.DropNullPartitions = !v
		case "current":
			v, err := boolArgValue(a)
			if err != nil {
				return err
			}
			w.Frame.ExcludeCurrent = !v
		case "window":
			v, err := intArgValue(a)
			if err != nil || v < 0 {
				return semanticErr(a.Value, "window must be a non-negative integer")
			}
			w.Frame.Rows = int(v)
		case "global":
			v, err := boolArgValue(a)
			if err != nil {
				return err
			}
			global = v
		case "reset_before", "reset_after":
			x := c.ectx(s)
			cond, err := x.expr(a.Value)
			if err != nil {
				return err
			}
			if a.Name == "reset_before" {
				w.Frame.ResetBefore = cond
			} else {
				w.Frame.ResetAfter = cond
			}
		case "time_window":
			return unsupportedFeature(a.Value, "streamstats time_window is not supported")
		}
	}
	keys, err := c.groupKeys(n.By, s)
	if err != nil {
		return err
	}
	if n.Command == "streamstats" && w.Frame.Rows > 0 && global && len(keys) > 0 {
		return unsupportedFeature(n, "streamstats window with global=true and a by clause is not supported; set global=false for per-group windows")
	}
	w.PartitionBy = keys
	aggs, _, err := c.aggTerms(n.Aggs, s, true, 0)
	if err != nil {
		return err
	}
	w.Funcs = aggs
	st.node = w
	return nil
}

func (c *compiler) dedup(st *pipe, n *ast.DedupCommand) error {
	if n.Count < 1 {
		return semanticErr(n, "dedup count must be greater than 0")
	}
	s := st.schema()
	d := &ir.Dedup{Input: st.node, Count: n.Count, KeepNull: n.KeepEmpty, Consecutive: n.Consecutive}
	for _, f := range n.Fields {
		ref, err := resolve(f, s)
		if err != nil {
			return err
		}
		d.Keys = append(d.Keys, ref)
	}
	st.node = d
	return nil
}

func (c *compiler) sortKeys(keys []ast.SortField, s ir.Schema) ([]ir.SortKey, error) {
	var out []ir.SortKey
	for _, k := range keys {
		ref, err := resolve(k.Field, s)
		if err != nil {
			return nil, err
		}
		var e ir.Expr = ref
		switch k.Cast {
		case "num":
			e = &ir.Cast{X: ref, To: ir.Float}
		case "str", "ip":
			e = &ir.Cast{X: ref, To: ir.String}
		}
		out = append(out, ir.SortKey{Expr: e, Desc: k.Desc, NullsFirst: !k.Desc})
	}
	return out, nil
}

func (c *compiler) sort(st *pipe, n *ast.SortCommand) error {
	keys, err := c.sortKeys(n.Keys, st.schema())
	if err != nil {
		return err
	}
	st.node = &ir.Sort{Input: st.node, Keys: keys}
	if n.Count > 0 {
		st.node = &ir.Limit{Input: st.node, Count: n.Count}
	}
	return nil
}
