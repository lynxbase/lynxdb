package ir

import "strings"

// Render draws the plan as an indented operator tree, root first, with
// subquery plans nested under the operator that evaluates them.
func Render(p *Plan) string {
	var b strings.Builder
	renderNode(&b, p.Root, "", "")
	return b.String()
}

func renderNode(b *strings.Builder, n Node, first, rest string) {
	b.WriteString(first + n.Label() + "\n")
	var subs []*Subquery
	for _, e := range NodeExprs(n) {
		WalkExpr(e, func(x Expr) bool {
			if sq, ok := x.(*Subquery); ok {
				subs = append(subs, sq)
			}
			return true
		})
	}
	children := n.Inputs()
	total := len(subs) + len(children)
	i := 0
	for _, sq := range subs {
		i++
		branch, cont := "├─ ", "│  "
		if i == total {
			branch, cont = "└─ ", "   "
		}
		b.WriteString(rest + branch + "subquery " + sq.String() + "\n")
		renderNode(b, sq.Plan, rest+cont+"└─ ", rest+cont+"   ")
	}
	for _, c := range children {
		i++
		if i == total {
			renderNode(b, c, rest+"└─ ", rest+"   ")
		} else {
			renderNode(b, c, rest+"├─ ", rest+"│  ")
		}
	}
}
