package ppl

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/ppl/ast"
	"github.com/lynxbase/lynxdb/pkg/ppl/grok"
)

func (c *compiler) bin(st *pipe, n *ast.BinCommand) error {
	s := st.schema()
	ref, err := resolve(n.Field, s)
	if err != nil {
		return err
	}
	var spanText string
	for _, a := range n.Args {
		switch a.Name {
		case "span":
			v, _ := stringArgValue(a)
			if strings.Contains(v, "log") {
				return unsupportedFeature(a.Value, "bin with a logarithmic span is not supported")
			}
			spanText = v
		default:
			return unsupportedFeature(a.Value, "bin %s= is not supported; use span=", a.Name)
		}
	}
	if spanText == "" {
		return unsupportedFeature(n, "bin without span= is not supported; give the bucket width with span=")
	}
	spn, err := parseSpan(n, spanText)
	if err != nil {
		return err
	}
	name := ref.Name
	if n.Alias != "" {
		name = n.Alias
	}
	var e ir.Expr
	if spn.isTime() {
		e = bucketExpr(ref, spn)
	} else {
		lo := bucketExpr(ref, spn)
		var width ir.Expr = ir.FloatLit(spn.Count)
		if lit, ok := lo.(*ir.Call).Args[1].(*ir.Literal); ok {
			width = lit
		}
		hi := &ir.Binary{Op: ir.OpAdd, L: lo, R: width, T: lo.Type()}
		e = callIR("concat", &ir.Cast{X: lo, To: ir.String}, ir.StringLit("-"), &ir.Cast{X: hi, To: ir.String})
	}
	st.node = &ir.Compute{Input: st.node, Assigns: []ir.Assign{{Name: name, Expr: e}}}
	return nil
}

// top and rare count each combination of the fields, optionally within by
// groups, and keep the N most (top) or least (rare) frequent per group.
func (c *compiler) top(st *pipe, n *ast.TopCommand) error {
	s := st.schema()
	countField, percentField := "count", "percent"
	showCount, showPerc, useNull := true, false, true
	for _, a := range n.Args {
		var err error
		switch a.Name {
		case "countfield":
			countField, err = stringArgValue(a)
		case "percentfield":
			percentField, err = stringArgValue(a)
		case "showcount":
			showCount, err = boolArgValue(a)
		case "showperc":
			showPerc, err = boolArgValue(a)
		case "usenull":
			useNull, err = boolArgValue(a)
		}
		if err != nil {
			return err
		}
	}
	if n.Count < 0 {
		return semanticErr(n, "%s count must not be negative", n.Command)
	}
	var by, fields []ir.NamedExpr
	for _, f := range n.By {
		ref, err := resolve(f, s)
		if err != nil {
			return err
		}
		by = append(by, ir.NamedExpr{Name: ref.Name, Expr: ref})
	}
	for _, f := range n.Fields {
		ref, err := resolve(f, s)
		if err != nil {
			return err
		}
		fields = append(fields, ir.NamedExpr{Name: ref.Name, Expr: ref})
	}
	agg := &ir.Aggregate{Input: st.node, Groups: append(append([]ir.NamedExpr(nil), by...), fields...),
		Aggs: []ir.NamedAgg{{Name: countField, Agg: &ir.AggCall{Func: ir.MustFunc("count"), T: ir.Int}}}, DropNullGroups: !useNull}
	var node ir.Node = agg
	countRef := &ir.FieldRef{Name: countField, T: ir.Int}
	if showPerc {
		w := &ir.Window{Input: node, Frame: ir.Frame{Kind: ir.FramePartition}, PartitionBy: groupRefs(by),
			Funcs: []ir.NamedAgg{{Name: "__total", Agg: &ir.AggCall{Func: ir.MustFunc("sum"), Args: []ir.Expr{countRef}, T: ir.Int}}}}
		pct := &ir.Binary{Op: ir.OpMul, L: &ir.Binary{Op: ir.OpDiv, L: &ir.Cast{X: countRef, To: ir.Float}, R: &ir.FieldRef{Name: "__total", T: ir.Int}, T: ir.Float}, R: ir.FloatLit(100), T: ir.Float}
		node = &ir.Compute{Input: w, Assigns: []ir.Assign{{Name: percentField, Expr: callIR("round", pct, ir.IntLit(2))}}}
	}
	keys := []ir.SortKey{}
	for _, b := range by {
		keys = append(keys, ir.SortKey{Expr: &ir.FieldRef{Name: b.Name, T: b.Expr.Type()}, NullsFirst: true})
	}
	keys = append(keys, ir.SortKey{Expr: countRef, Desc: n.Command == "top", NullsFirst: n.Command != "top"})
	for _, f := range fields {
		keys = append(keys, ir.SortKey{Expr: &ir.FieldRef{Name: f.Name, T: f.Expr.Type()}, NullsFirst: true})
	}
	node = &ir.Sort{Input: node, Keys: keys}
	if n.Count > 0 {
		if len(by) == 0 {
			node = &ir.Limit{Input: node, Count: n.Count}
		} else {
			var dk []ir.Expr
			for _, b := range by {
				dk = append(dk, &ir.FieldRef{Name: b.Name, T: b.Expr.Type()})
			}
			node = &ir.Dedup{Input: node, Keys: dk, Count: n.Count, KeepNull: true}
		}
	}
	var names []string
	for _, b := range by {
		names = append(names, b.Name)
	}
	for _, f := range fields {
		names = append(names, f.Name)
	}
	if showCount {
		names = append(names, countField)
	}
	if showPerc {
		names = append(names, percentField)
	}
	st.node = &ir.Project{Input: node, Names: names}
	return nil
}

func groupRefs(keys []ir.NamedExpr) []ir.NamedExpr {
	out := make([]ir.NamedExpr, len(keys))
	for i, k := range keys {
		out[i] = ir.NamedExpr{Name: k.Name, Expr: &ir.FieldRef{Name: k.Name, T: k.Expr.Type()}}
	}
	return out
}

var namedGroup = regexp.MustCompile(`\(\?P?<([A-Za-z][A-Za-z0-9_]*)>`)

func regexOutputs(pattern string) []ir.Column {
	var out []ir.Column
	for _, m := range namedGroup.FindAllStringSubmatch(pattern, -1) {
		out = append(out, ir.Column{Name: m[1], Type: ir.String, Nullable: true})
	}
	return out
}

func (c *compiler) sourceText(e ast.Expr, s ir.Schema) (ir.Expr, error) {
	v, err := c.ectx(s).expr(e)
	if err != nil {
		return nil, err
	}
	switch v.Type().Kind {
	case ir.KindString, ir.KindAny, ir.KindNull:
		return v, nil
	}
	return nil, typeErr(e, "the field to parse must be a string, got %s", v.Type())
}

func (c *compiler) parse(st *pipe, n *ast.ParseCommand) error {
	s := st.schema()
	src, err := c.sourceText(n.Source, s)
	if err != nil {
		return err
	}
	ex := &ir.Extract{Input: st.node, Source: src, Pattern: n.Pattern, NoMatchEmpty: true}
	if n.Command == "grok" {
		g, err := grok.Compile(n.Pattern)
		if err != nil {
			return semanticErr(n, "%v", err)
		}
		if len(g.Fields) == 0 {
			return semanticErr(n, "the grok pattern captures no fields; use %%{PATTERN:name}")
		}
		ex.Method, ex.Pattern = ir.ExtractRegex, g.Regex
		for _, f := range g.Fields {
			ex.Outputs = append(ex.Outputs, ir.Column{Name: f, Type: ir.String, Nullable: true})
		}
	} else {
		if _, err := regexp.Compile(n.Pattern); err != nil {
			return semanticErr(n, "invalid regular expression: %v", err)
		}
		ex.Method = ir.ExtractRegex
		ex.Outputs = regexOutputs(n.Pattern)
		if len(ex.Outputs) == 0 {
			return semanticErr(n, "the parse pattern has no named groups; use (?<name>...)")
		}
	}
	st.node = ex
	return nil
}

func (c *compiler) rex(st *pipe, n *ast.RexCommand) error {
	if n.Field == nil {
		return semanticErr(n, "rex needs field=<field>")
	}
	s := st.schema()
	ref, err := resolve(n.Field, s)
	if err != nil {
		return err
	}
	mode, maxMatch := "extract", 1
	for _, a := range n.Args {
		switch a.Name {
		case "mode":
			v, _ := stringArgValue(a)
			mode = strings.ToLower(v)
		case "max_match":
			v, err := intArgValue(a)
			if err != nil || v < 0 {
				return semanticErr(a.Value, "max_match must be a non-negative integer")
			}
			maxMatch = int(v)
			if maxMatch == 0 {
				maxMatch = 10
			}
		case "offset_field":
			return unsupportedFeature(a.Value, "rex offset_field is not supported")
		}
	}
	if mode == "sed" {
		e, err := sedExpr(n, ref, n.Pattern)
		if err != nil {
			return err
		}
		st.node = &ir.Compute{Input: st.node, Assigns: []ir.Assign{{Name: ref.Name, Expr: e}}}
		return nil
	}
	if mode != "extract" {
		return semanticErr(n, "rex mode must be extract or sed")
	}
	if _, err := regexp.Compile(n.Pattern); err != nil {
		return semanticErr(n, "invalid regular expression: %v", err)
	}
	outs := regexOutputs(n.Pattern)
	if len(outs) == 0 {
		return semanticErr(n, "the rex pattern has no named groups; use (?<name>...)")
	}
	if maxMatch > 1 {
		for i := range outs {
			outs[i].Type = ir.ArrayOf(ir.String)
		}
	}
	st.node = &ir.Extract{Input: st.node, Source: ref, Method: ir.ExtractRegex, Pattern: n.Pattern, Outputs: outs, MaxMatches: maxMatch}
	return nil
}

// sedExpr compiles a sed s/regex/replacement/flags or y/from/to/ script.
func sedExpr(n ast.Node, ref ir.Expr, script string) (ir.Expr, error) {
	if len(script) < 2 || script[0] != 's' {
		return nil, unsupportedFeature(n, "rex sed mode supports only s/regex/replacement/[g] substitutions")
	}
	sep := string(script[1])
	parts := strings.Split(script[2:], sep)
	if len(parts) != 3 {
		return nil, semanticErr(n, "invalid sed expression %q", script)
	}
	if _, err := regexp.Compile(parts[0]); err != nil {
		return nil, semanticErr(n, "invalid regular expression: %v", err)
	}
	repl := regexp.MustCompile(`\\(\d)`).ReplaceAllString(parts[1], `$${$1}`)
	switch parts[2] {
	case "g":
		return callIR("regexp_replace", ref, ir.StringLit(parts[0]), ir.StringLit(repl)), nil
	case "":
		return callIR("regexp_replace", ref, ir.StringLit(`^((?s:.*?))`+parts[0]+`((?s:.*))$`), ir.StringLit("${1}"+repl+"${"+strconv.Itoa(2+strings.Count(parts[0], "("))+"}")), nil
	}
	return nil, unsupportedFeature(n, "sed flags %q are not supported; use g or none", parts[2])
}

func (c *compiler) regex(st *pipe, n *ast.RegexCommand) error {
	ref, err := resolve(n.Field, st.schema())
	if err != nil {
		return err
	}
	if _, err := regexp.Compile(n.Pattern); err != nil {
		return semanticErr(n, "invalid regular expression: %v", err)
	}
	var cond ir.Expr = callIR("regexp_match", toStringArg(ref), ir.StringLit(n.Pattern))
	if n.Negate {
		cond = &ir.Unary{Op: ir.OpNot, X: cond, T: ir.Bool}
	}
	st.node = &ir.Filter{Input: st.node, Cond: cond}
	return nil
}

// spathToJSONPath converts an spath path (a.b{0}.c) to a JSONPath.
func spathToJSONPath(p string) string {
	p = strings.ReplaceAll(strings.ReplaceAll(p, "{}", "[*]"), "{", "[")
	p = strings.ReplaceAll(p, "}", "]")
	if strings.HasPrefix(p, "$") {
		return p
	}
	return "$." + p
}

func (c *compiler) spath(st *pipe, n *ast.SpathCommand) error {
	s := st.schema()
	if n.Input == nil {
		n.Input = &ast.Field{Parts: []string{ir.RawColumn}}
	}
	in, err := resolve(n.Input, s)
	if err != nil {
		return err
	}
	if n.Path == "" {
		out := in.Name
		if n.Output != nil {
			out = n.Output.Name()
		}
		st.node = &ir.Compute{Input: st.node, Assigns: []ir.Assign{{Name: out, Expr: callIR("parse_json", in)}}}
		return nil
	}
	out := n.Path
	if n.Output != nil {
		out = n.Output.Name()
	}
	st.node = &ir.Compute{Input: st.node, Assigns: []ir.Assign{{Name: out, Expr: callIR("json_extract", in, ir.StringLit(spathToJSONPath(n.Path)))}}}
	return nil
}

func (c *compiler) patterns(st *pipe, n *ast.PatternsCommand) error {
	s := st.schema()
	src, err := c.sourceText(n.Source, s)
	if err != nil {
		return err
	}
	p := &ir.Patterns{Input: st.node, Source: src, Method: ir.PatternSimple, Output: "patterns_field", MaxSamples: 10, Regex: `[a-zA-Z0-9]+`}
	for _, f := range n.By {
		ref, err := resolve(f, s)
		if err != nil {
			return err
		}
		p.GroupBy = append(p.GroupBy, ref)
	}
	for _, a := range n.Args {
		v, _ := stringArgValue(a)
		switch a.Name {
		case "method":
			switch strings.ToLower(v) {
			case "simple_pattern":
			case "brain":
				return unsupportedFeature(a.Value, "patterns method=brain is not supported; use method=simple_pattern")
			default:
				return semanticErr(a.Value, "patterns method must be simple_pattern or brain")
			}
		case "mode":
			switch strings.ToLower(v) {
			case "label":
			case "aggregation":
				p.Aggregate = true
			default:
				return semanticErr(a.Value, "patterns mode must be label or aggregation")
			}
		case "max_sample_count":
			k, err := intArgValue(a)
			if err != nil || k < 0 {
				return semanticErr(a.Value, "max_sample_count must be a non-negative integer")
			}
			p.MaxSamples = int(k)
		case "new_field":
			p.Output = v
		case "pattern":
			if _, err := regexp.Compile(v); err != nil {
				return semanticErr(a.Value, "invalid regular expression: %v", err)
			}
			p.Regex = v
		case "frequency_threshold_percentage":
			f, err := strconv.ParseFloat(v, 64)
			if err != nil || f <= 0 || f > 1 {
				return semanticErr(a.Value, "frequency_threshold_percentage must be in (0, 1]")
			}
			p.Threshold = f
		case "buffer_limit", "variable_count_threshold":
		case "show_numbered_token":
			if b, err := boolArgValue(a); err != nil || b {
				return unsupportedFeature(a.Value, "show_numbered_token=true is not supported")
			}
		}
	}
	if len(p.GroupBy) > 0 && !p.Aggregate {
		return unsupportedFeature(n, "patterns by a group in label mode is not supported; use mode=aggregation")
	}
	st.node = p
	return nil
}

func (c *compiler) lookup(st *pipe, n *ast.LookupCommand) error {
	c.noteSource(n.Source.Name)
	s := st.schema()
	right := ir.Node(&ir.Scan{Sources: []ir.Source{{Name: n.Source.Name}}, Out: EventSchema()})
	rs := right.Schema()
	j := &ir.Join{Left: st.node, Kind: ir.JoinLeft, MaxMatches: 1}
	var keyNames []string
	for _, k := range n.Keys {
		lookupField := k.Field
		sourceField := k.Field
		if k.Alias != "" {
			sourceField = k.Alias
		}
		lref, err := resolveName(n, sourceField, s)
		if err != nil {
			return err
		}
		j.LeftKeys = append(j.LeftKeys, lref)
		j.RightKeys = append(j.RightKeys, &ir.FieldRef{Name: canonicalField(lookupField), T: ir.Any})
		keyNames = append(keyNames, canonicalField(lookupField))
	}
	if len(n.Outputs) == 0 {
		return unsupportedFeature(n, "lookup without an output list is not supported: name the copied fields with replace, append or output")
	}
	var assigns []ir.Assign
	var tmp []string
	var renames []ir.Rename
	for i, o := range n.Outputs {
		target := o.Field
		if o.Alias != "" {
			target = o.Alias
		}
		target = canonicalField(target)
		t := "__lookup_" + strconv.Itoa(i)
		renames = append(renames, ir.Rename{From: canonicalField(o.Field), To: t})
		tmp = append(tmp, t)
		tv := &ir.FieldRef{Name: t, T: ir.Any}
		matched := &ir.Unary{Op: ir.OpNot, X: callIR("is_null", &ir.FieldRef{Name: "__lookup_match", T: ir.Bool}), T: ir.Bool}
		var cur ir.Expr = &ir.FieldRef{Name: target, T: ir.Any}
		if _, ok := s.Resolve(target); !ok {
			cur = ir.NullLit()
		}
		var e ir.Expr
		if n.Mode == "append" {
			e = callIR("coalesce", cur, tv)
		} else {
			e = &ir.Case{Whens: []ir.When{{Cond: matched, Then: tv}}, Else: cur, T: ir.Any}
		}
		assigns = append(assigns, ir.Assign{Name: target, Expr: e})
	}
	right = &ir.Project{Input: right, Names: append(keyNames, outputFields(n)...)}
	right = &ir.RenameColumns{Input: right, Renames: renames}
	right = &ir.Compute{Input: right, Assigns: []ir.Assign{{Name: "__lookup_match", Expr: ir.BoolLit(true)}}}
	_ = rs
	j.Right = right
	j.Collision = ir.CollisionLeft
	var node ir.Node = &ir.Compute{Input: j, Assigns: assigns}
	node = &ir.Project{Input: node, Names: append(tmp, "__lookup_match"), Drop: true}
	st.node = node
	return nil
}

func outputFields(n *ast.LookupCommand) []string {
	out := make([]string, len(n.Outputs))
	for i, o := range n.Outputs {
		out[i] = canonicalField(o.Field)
	}
	return out
}

func (c *compiler) fillnull(st *pipe, n *ast.FillnullCommand) error {
	s := st.schema()
	x := c.ectx(s)
	f := &ir.FillNull{Input: st.node}
	if len(n.Pairs) > 0 {
		for _, p := range n.Pairs {
			ref, err := resolve(p.Field, s)
			if err != nil {
				return err
			}
			v, err := x.expr(p.Expr)
			if err != nil {
				return err
			}
			f.Fields = append(f.Fields, ir.Assign{Name: ref.Name, Expr: v})
		}
		st.node = f
		return nil
	}
	v, err := x.expr(n.Value)
	if err != nil {
		return err
	}
	if len(n.Fields) == 0 {
		f.Default = v
		st.node = f
		return nil
	}
	for _, fl := range n.Fields {
		ref, err := resolve(fl, s)
		if err != nil {
			return err
		}
		f.Fields = append(f.Fields, ir.Assign{Name: ref.Name, Expr: v})
	}
	st.node = f
	return nil
}

var convertFuncs = map[string]string{
	"auto": "convert_auto", "num": "convert_num", "rmcomma": "rmcomma", "rmunit": "rmunit", "memk": "memk",
	"dur2sec": "dur2sec", "mstime": "mstime",
}

func (c *compiler) convert(st *pipe, n *ast.ConvertCommand) error {
	s := st.schema()
	format := n.TimeFormat
	if format == "" {
		format = "%m/%d/%Y %H:%M:%S"
	}
	comp := &ir.Compute{Input: st.node}
	for _, f := range n.Funcs {
		ref, err := resolve(f.Field, s)
		if err != nil {
			return err
		}
		var e ir.Expr
		switch f.Func {
		case "none":
			e = ref
		case "ctime":
			e = callIR("ctime", ref, ir.StringLit(format))
		case "mktime":
			e = callIR("mktime", ref, ir.StringLit(format))
		default:
			fn, ok := convertFuncs[f.Func]
			if !ok {
				return errAt(ErrUnsupportedFunction, n, "unknown convert function %s", f.Func)
			}
			e = callIR(fn, ref)
		}
		name := ref.Name
		if f.Alias != "" {
			name = f.Alias
		}
		comp.Assigns = append(comp.Assigns, ir.Assign{Name: name, Expr: e})
	}
	st.node = comp
	return nil
}

func (c *compiler) trendline(st *pipe, n *ast.TrendlineCommand) error {
	s := st.schema()
	node := st.node
	if n.Sort != nil {
		keys, err := c.sortKeys([]ast.SortField{*n.Sort}, s)
		if err != nil {
			return err
		}
		node = &ir.Sort{Input: node, Keys: keys}
	}
	for _, t := range n.Terms {
		if t.N < 1 {
			return semanticErr(n, "the number of data points must be greater than zero")
		}
		ref, err := resolve(t.Field, s)
		if err != nil {
			return err
		}
		node = &ir.Filter{Input: node, Cond: &ir.Unary{Op: ir.OpNot, X: callIR("is_null", ref), T: ir.Bool}}
		name := t.Alias
		if name == "" {
			name = ref.Name + "_trendline"
		}
		fn := "moving_avg"
		if t.Kind == "wma" {
			fn = "weighted_moving_avg"
		}
		node = &ir.Window{Input: node, Frame: ir.Frame{Kind: ir.FrameRunning, Rows: int(t.N)},
			Funcs: []ir.NamedAgg{{Name: name, Agg: &ir.AggCall{Func: ir.MustFunc(fn), Args: []ir.Expr{ref}, T: ir.Float}}}}
		s = node.Schema()
	}
	st.node = node
	return nil
}

func (c *compiler) fieldCommand(st *pipe, n *ast.FieldCommand) error {
	s := st.schema()
	ref, err := resolve(n.Field, s)
	if err != nil {
		return err
	}
	switch n.Command {
	case "expand":
		st.node = &ir.Unnest{Input: st.node, Field: ref.Name, As: n.Alias}
	case "mvexpand":
		u := &ir.Unnest{Input: st.node, Field: ref.Name}
		for _, a := range n.Args {
			v, err := intArgValue(a)
			if err != nil || v < 1 {
				return semanticErr(a.Value, "mvexpand limit must be a positive integer")
			}
			u.Limit = int(v)
		}
		st.node = u
	case "flatten":
		st.node = &ir.FlattenObject{Input: st.node, Field: ref.Name, Aliases: n.Aliases}
	case "nomv":
		st.node = &ir.Compute{Input: st.node, Assigns: []ir.Assign{{Name: ref.Name, Expr: callIR("mvjoin", ref, ir.StringLit("\n"))}}}
	case "mvcombine":
		var node ir.Node = &ir.CombineValues{Input: st.node, Field: ref.Name}
		for _, a := range n.Args {
			delim, err := stringArgValue(a)
			if err != nil {
				return err
			}
			node = &ir.Compute{Input: node, Assigns: []ir.Assign{{Name: ref.Name, Expr: callIR("mvjoin", &ir.FieldRef{Name: ref.Name, T: ir.ArrayOf(ir.Any)}, ir.StringLit(delim))}}}
		}
		st.node = node
	}
	return nil
}

func (c *compiler) subPipelineCommand(st *pipe, n *ast.SubPipelineCommand) error {
	switch n.Command {
	case "append":
		if _, ok := n.Pipeline.Commands[0].(*ast.SearchCommand); !ok {
			return semanticErr(n, "the append subsearch must start with a search command")
		}
		sub, err := c.pipeline(n.Pipeline)
		if err != nil {
			return err
		}
		st.node = &ir.Union{Sources: []ir.Node{st.node, sub}}
	case "appendpipe":
		sub, err := c.commands(st.node, st.origin, n.Pipeline.Commands)
		if err != nil {
			return err
		}
		st.node = &ir.Union{Sources: []ir.Node{st.node, sub}}
	case "appendcol":
		override := false
		for _, a := range n.Args {
			v, err := boolArgValue(a)
			if err != nil {
				return err
			}
			override = v
		}
		origin := st.origin
		if origin == nil {
			origin = st.node
		}
		sub, err := c.commands(origin, origin, n.Pipeline.Commands)
		if err != nil {
			return err
		}
		st.node = &ir.ZipColumns{Left: st.node, Right: sub, Override: override}
	}
	return nil
}

func (c *compiler) totals(st *pipe, n *ast.TotalsCommand) error {
	s := st.schema()
	t := &ir.Totals{Input: st.node, RowColumn: "Total", LabelValue: "Total"}
	if n.Command == "addtotals" {
		t.RowTotals = true
	} else {
		t.ColumnTotals = true
	}
	for _, a := range n.Args {
		var err error
		switch a.Name {
		case "label":
			t.LabelValue, err = stringArgValue(a)
		case "labelfield":
			t.LabelColumn, err = stringArgValue(a)
		case "fieldname":
			t.RowColumn, err = stringArgValue(a)
		case "row":
			t.RowTotals, err = boolArgValue(a)
		case "col":
			t.ColumnTotals, err = boolArgValue(a)
		}
		if err != nil {
			return err
		}
	}
	for _, f := range n.Fields {
		ref, err := resolve(f, s)
		if err != nil {
			return err
		}
		t.Fields = append(t.Fields, ref.Name)
	}
	st.node = t
	return nil
}

func (c *compiler) xyseries(st *pipe, n *ast.XYSeriesCommand) error {
	s := st.schema()
	xref, err := resolve(n.X, s)
	if err != nil {
		return err
	}
	yref, err := resolve(n.Y, s)
	if err != nil {
		return err
	}
	sep, format := ": ", ""
	for _, a := range n.Args {
		v, err := stringArgValue(a)
		if err != nil {
			return err
		}
		if a.Name == "sep" {
			sep = v
		} else {
			format = v
		}
	}
	agg := &ir.Aggregate{Input: st.node, Groups: []ir.NamedExpr{{Name: xref.Name, Expr: xref}}}
	for _, d := range n.Data {
		dref, err := resolve(d, s)
		if err != nil {
			return err
		}
		for _, v := range n.Values {
			name := dref.Name + sep + v
			if format != "" {
				name = strings.ReplaceAll(strings.ReplaceAll(format, "$AGG$", dref.Name), "$VAL$", v)
			}
			cond := &ir.Binary{Op: ir.OpEq, L: &ir.Cast{X: yref, To: ir.String}, R: ir.StringLit(v), T: ir.Bool}
			agg.Aggs = append(agg.Aggs, ir.NamedAgg{Name: name, Agg: &ir.AggCall{Func: ir.MustFunc("first"), Args: []ir.Expr{dref}, Filter: cond, T: dref.T}})
		}
	}
	st.node = &ir.Sort{Input: agg, Keys: []ir.SortKey{{Expr: &ir.FieldRef{Name: xref.Name, T: xref.T}, NullsFirst: true}}}
	return nil
}
