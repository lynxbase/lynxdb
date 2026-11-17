package ppl

import (
	"strconv"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/ppl/ast"
)

type chartOptions struct {
	limit     int64
	bottom    bool
	useOther  bool
	useNull   bool
	nullStr   string
	otherStr  string
	span      string
	timeField string
}

// chart and timechart aggregate by a row key (the time bucket for
// timechart) and an optional series key. Series beyond the limit, ranked by
// the sum of the aggregate over all rows, merge into the OTHER series; the
// ranking runs as an uncorrelated subquery over the same input.
func (c *compiler) chart(st *pipe, n *ast.ChartCommand) error {
	s := st.schema()
	opt := chartOptions{limit: 10, useOther: true, useNull: true, nullStr: "NULL", otherStr: "OTHER", span: "1m", timeField: ir.TimeColumn}
	for _, a := range n.Args {
		v, _ := stringArgValue(a)
		var err error
		switch a.Name {
		case "limit":
			lv := strings.ToLower(v)
			switch {
			case strings.HasPrefix(lv, "top"):
				lv = strings.TrimPrefix(lv, "top")
			case strings.HasPrefix(lv, "bottom"):
				lv, opt.bottom = strings.TrimPrefix(lv, "bottom"), true
			}
			opt.limit, err = strconv.ParseInt(lv, 10, 64)
			if err != nil || opt.limit < 0 {
				return semanticErr(a.Value, "limit must be a non-negative integer, topN or bottomN")
			}
		case "useother":
			opt.useOther, err = boolArgValue(a)
		case "usenull":
			opt.useNull, err = boolArgValue(a)
		case "nullstr":
			opt.nullStr = v
		case "otherstr":
			opt.otherStr = v
		case "span":
			opt.span = v
		case "timefield":
			opt.timeField = canonicalField(v)
		case "bins", "minspan":
			return unsupportedFeature(a.Value, "%s %s= is not supported; use span=", n.Command, a.Name)
		}
		if err != nil {
			return err
		}
	}
	if opt.limit == 0 {
		opt.useOther = false
	}
	input := st.node
	var rowKeys []ir.NamedExpr
	spanSeconds := 0.0
	if n.Command == "timechart" {
		tref, err := resolveName(n, opt.timeField, s)
		if err != nil {
			return err
		}
		spn, err := parseSpan(n, opt.span)
		if err != nil {
			return err
		}
		if !spn.isTime() {
			return semanticErr(n, "timechart span needs a time unit, such as 5m")
		}
		spanSeconds = spn.seconds()
		rowKeys = append(rowKeys, ir.NamedExpr{Name: tref.Name, Expr: bucketExpr(tref, spn)})
	} else if n.Over != nil {
		k, err := c.splitKey(n.Over, n.OverArgs, s)
		if err != nil {
			return err
		}
		input = &ir.Filter{Input: input, Cond: &ir.Unary{Op: ir.OpNot, X: callIR("is_null", k.Expr), T: ir.Bool}}
		rowKeys = append(rowKeys, k)
	}
	aggs, scaled, err := c.aggTerms([]ast.AggTerm{n.Agg}, s, false, spanSeconds)
	if err != nil {
		return err
	}
	valueName := aggs[0].Name
	if n.By == nil {
		agg := &ir.Aggregate{Input: input, Groups: rowKeys, Aggs: aggs}
		var node ir.Node = agg
		if len(scaled) > 0 {
			node = &ir.Compute{Input: node, Assigns: scaled}
		}
		if n.Command == "timechart" {
			node = &ir.Sort{Input: node, Keys: []ir.SortKey{{Expr: &ir.FieldRef{Name: rowKeys[0].Name, T: ir.Timestamp}}}}
		}
		st.node = node
		return nil
	}
	series, err := c.splitKey(n.By, n.ByArgs, s)
	if err != nil {
		return err
	}
	seriesName := series.Name
	var label ir.Expr = &ir.Cast{X: series.Expr, To: ir.String}
	if opt.useNull {
		label = callIR("coalesce", label, ir.StringLit(opt.nullStr))
	} else {
		input = &ir.Filter{Input: input, Cond: &ir.Unary{Op: ir.OpNot, X: callIR("is_null", series.Expr), T: ir.Bool}}
	}
	labeled := ir.Node(&ir.Compute{Input: input, Assigns: []ir.Assign{{Name: seriesName, Expr: label}}})
	seriesRef := &ir.FieldRef{Name: seriesName, T: ir.String}
	if opt.limit > 0 {
		perRow := &ir.Aggregate{Input: labeled, Groups: append(append([]ir.NamedExpr(nil), rowKeys...), ir.NamedExpr{Name: seriesName, Expr: seriesRef}), Aggs: aggs}
		total := &ir.Aggregate{Input: perRow, Groups: []ir.NamedExpr{{Name: seriesName, Expr: seriesRef}},
			Aggs: []ir.NamedAgg{{Name: "__rank", Agg: &ir.AggCall{Func: ir.MustFunc("sum"), Args: []ir.Expr{&ir.FieldRef{Name: valueName, T: aggs[0].Agg.T}}, T: ir.Any}}}}
		ranked := &ir.TopN{Input: total, Count: opt.limit, Keys: []ir.SortKey{
			{Expr: &ir.FieldRef{Name: "__rank", T: ir.Any}, Desc: !opt.bottom},
			{Expr: seriesRef, NullsFirst: true},
		}}
		top := &ir.Project{Input: ranked, Names: []string{seriesName}}
		in := &ir.Subquery{Mode: ir.SubqueryIn, Plan: top, Args: []ir.Expr{seriesRef}, T: ir.Bool}
		if opt.useOther {
			labeled = &ir.Compute{Input: labeled, Assigns: []ir.Assign{{Name: seriesName,
				Expr: &ir.Case{Whens: []ir.When{{Cond: in, Then: seriesRef}}, Else: ir.StringLit(opt.otherStr), T: ir.String}}}}
		} else {
			labeled = &ir.Filter{Input: labeled, Cond: in}
		}
	}
	groups := append(append([]ir.NamedExpr(nil), rowKeys...), ir.NamedExpr{Name: seriesName, Expr: seriesRef})
	var node ir.Node = &ir.Aggregate{Input: labeled, Groups: groups, Aggs: aggs}
	if len(scaled) > 0 {
		node = &ir.Compute{Input: node, Assigns: scaled}
	}
	var keys []ir.SortKey
	for _, g := range groups {
		keys = append(keys, ir.SortKey{Expr: &ir.FieldRef{Name: g.Name, T: g.Expr.Type()}, NullsFirst: true})
	}
	st.node = &ir.Sort{Input: node, Keys: keys}
	return nil
}

// splitKey compiles a chart row or column split with its bin options.
func (c *compiler) splitKey(f *ast.Field, args []ast.Arg, s ir.Schema) (ir.NamedExpr, error) {
	ref, err := resolve(f, s)
	if err != nil {
		return ir.NamedExpr{}, err
	}
	for _, a := range args {
		if a.Name != "span" {
			return ir.NamedExpr{}, unsupportedFeature(a.Value, "chart split option %s= is not supported; use span=", a.Name)
		}
		v, _ := stringArgValue(a)
		spn, err := parseSpan(a.Value, v)
		if err != nil {
			return ir.NamedExpr{}, err
		}
		return ir.NamedExpr{Name: ref.Name, Expr: bucketExpr(ref, spn)}, nil
	}
	return ir.NamedExpr{Name: ref.Name, Expr: ref}, nil
}
