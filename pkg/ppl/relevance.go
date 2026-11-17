package ppl

import (
	"regexp"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/ppl/ast"
)

// The single-field relevance functions filter on the tokens of one column,
// case-insensitively, as the search command does on the raw text. LynxDB
// computes no scores, so they are plain filters and boost is accepted and
// ignored.
func init() {
	relevance("match", "SEARCH_WORD", func(x *ectx, n *ast.Call) (ir.Expr, error) {
		m, err := x.matchArgs(n, true)
		if err != nil {
			return nil, err
		}
		tokens := tokenize(m.text)
		if len(tokens) == 0 {
			return ir.BoolLit(m.zeroAll), nil
		}
		var out ir.Expr
		for _, tok := range tokens {
			s := &ir.Search{Mode: ir.SearchWord, Field: m.field, Pattern: tok}
			switch {
			case out == nil:
				out = s
			case m.and:
				out = &ir.Binary{Op: ir.OpAnd, L: out, R: s, T: ir.Bool}
			default:
				out = &ir.Binary{Op: ir.OpOr, L: out, R: s, T: ir.Bool}
			}
		}
		return out, nil
	})
	relevance("match_phrase", "SEARCH_PHRASE", func(x *ectx, n *ast.Call) (ir.Expr, error) {
		m, err := x.matchArgs(n, false)
		if err != nil {
			return nil, err
		}
		tokens := tokenize(m.text)
		switch {
		case len(tokens) == 0:
			return ir.BoolLit(m.zeroAll), nil
		case len(tokens) == 1 && strings.EqualFold(tokens[0], strings.TrimSpace(m.text)):
			return &ir.Search{Mode: ir.SearchWord, Field: m.field, Pattern: tokens[0]}, nil
		}
		return &ir.Search{Mode: ir.SearchPhrase, Field: m.field, Pattern: m.text}, nil
	})
	relevance("match_phrase_prefix", "SEARCH_PREFIX", func(x *ectx, n *ast.Call) (ir.Expr, error) {
		m, err := x.matchArgs(n, false)
		if err != nil {
			return nil, err
		}
		tokens := tokenize(m.text)
		switch len(tokens) {
		case 0:
			return ir.BoolLit(m.zeroAll), nil
		case 1:
			return &ir.Search{Mode: ir.SearchPrefix, Field: m.field, Pattern: tokens[0]}, nil
		}
		// The phrase starts on a token boundary and its last token may
		// continue, which no other search mode expresses.
		pattern := `(?i)(?:^|[^\p{L}\p{Nd}])` + regexp.QuoteMeta(strings.TrimSpace(m.text))
		return &ir.Search{Mode: ir.SearchRegex, Field: m.field, Pattern: pattern}, nil
	})
}

func relevance(name, construct string, f func(x *ectx, n *ast.Call) (ir.Expr, error)) {
	scalarCatalog[name] = &FuncSupport{Name: name, Lowering: construct, lower: f, namedArgs: true}
}

type matchCall struct {
	field   string
	text    string
	and     bool
	zeroAll bool
}

// matchArgs reads match(field, text [, option=value ...]).
func (x *ectx) matchArgs(n *ast.Call, operator bool) (matchCall, error) {
	var m matchCall
	if len(n.Args) != 2 {
		return m, typeErr(n, "%s expects 2 argument(s), got %d", n.Name, len(n.Args))
	}
	name, ok := relevanceText(n.Args[0])
	if lit, isLit := n.Args[0].(*ast.Literal); isLit && lit.Kind != ast.LitString {
		ok = false
	}
	if !ok {
		return m, semanticErr(n.Args[0], "%s: the first argument must be a field", n.Name)
	}
	ref, err := resolveName(n.Args[0], name, x.schema)
	if err != nil {
		return m, err
	}
	m.field = ref.Name
	if m.text, ok = relevanceText(n.Args[1]); !ok {
		return m, semanticErr(n.Args[1], "%s: the query must be a constant text", n.Name)
	}
	for _, a := range n.NamedArgs {
		v, ok := relevanceText(a.Value)
		switch {
		case a.Name == "boost":
		case a.Name == "operator" && operator && ok && (strings.EqualFold(v, "and") || strings.EqualFold(v, "or")):
			m.and = strings.EqualFold(v, "and")
		case a.Name == "operator" && operator:
			return m, semanticErr(a.Value, "%s: operator must be AND or OR", n.Name)
		case a.Name == "zero_terms_query" && ok && (strings.EqualFold(v, "none") || strings.EqualFold(v, "all")):
			m.zeroAll = strings.EqualFold(v, "all")
		case a.Name == "zero_terms_query":
			return m, semanticErr(a.Value, "%s: zero_terms_query must be none or all", n.Name)
		default:
			return m, unsupportedFeature(a.Value, "%s: the %s option is not supported", n.Name, a.Name)
		}
	}
	return m, nil
}

// relevanceText returns the text of a field name or a literal argument.
func relevanceText(e ast.Expr) (string, bool) {
	switch v := e.(type) {
	case *ast.Field:
		return v.Name(), true
	case *ast.Literal:
		return v.Text, v.Kind != ast.LitNull
	}
	return "", false
}
