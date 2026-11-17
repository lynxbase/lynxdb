package ir

import (
	"strings"
	"testing"
	"time"
)

func scan() *Scan {
	return &Scan{Sources: []Source{{Name: "main"}}, Out: OpenSchema(
		Column{Name: TimeColumn, Type: Timestamp},
		Column{Name: RawColumn, Type: String},
	)}
}

func call(t *testing.T, name string, args ...Expr) *Call {
	t.Helper()
	f := MustFunc(name)
	types := make([]Type, len(args))
	for i, a := range args {
		types[i] = a.Type()
	}
	rt, err := f.Check(types)
	if err != nil {
		t.Fatalf("check %s: %v", name, err)
	}
	return &Call{Func: f, Args: args, T: rt}
}

func TestUnify(t *testing.T) {
	cases := []struct {
		a, b, want Type
	}{
		{Int, Int, Int},
		{Int, Float, Float},
		{Null, String, String},
		{String, Null, String},
		{String, Int, Any},
		{ArrayOf(Int), ArrayOf(Float), ArrayOf(Float)},
	}
	for _, c := range cases {
		if got := Unify(c.a, c.b); !got.Equal(c.want) {
			t.Errorf("Unify(%s, %s) = %s, want %s", c.a, c.b, got, c.want)
		}
	}
}

func TestParseTypeRoundTrip(t *testing.T) {
	for _, ty := range []Type{Any, Bool, Int, Float, String, Timestamp, Duration, Object, ArrayOf(String), ArrayOf(ArrayOf(Int))} {
		got, ok := ParseType(ty.String())
		if !ok || !got.Equal(ty) {
			t.Errorf("ParseType(%q) = %s, %v", ty.String(), got, ok)
		}
	}
	if _, ok := ParseType("decimal"); ok {
		t.Error("ParseType accepted an unknown type")
	}
}

func TestAccepts(t *testing.T) {
	if !Float.Accepts(Int) {
		t.Error("float must accept int")
	}
	if Int.Accepts(Float) {
		t.Error("int must not accept float")
	}
	if String.Accepts(Int) {
		t.Error("string must not accept int")
	}
	if !String.Accepts(Any) || !String.Accepts(Null) {
		t.Error("any and null are accepted everywhere")
	}
}

func TestFuncCheck(t *testing.T) {
	if _, err := MustFunc("abs").Check([]Type{String}); err == nil {
		t.Error("abs(string) must be a type error")
	}
	if rt, err := MustFunc("abs").Check([]Type{Int}); err != nil || !rt.Equal(Int) {
		t.Errorf("abs(int) = %s, %v", rt, err)
	}
	if _, err := MustFunc("substring").Check([]Type{String}); err == nil {
		t.Error("substring with one argument must be an arity error")
	}
	if _, err := MustFunc("concat").Check([]Type{String, Int, Any}); err != nil {
		t.Errorf("variadic concat: %v", err)
	}
	if rt, _ := MustFunc("coalesce").Check([]Type{Null, Int, Float}); !rt.Equal(Float) {
		t.Errorf("coalesce result = %s", rt)
	}
}

func TestRegistryInvariants(t *testing.T) {
	for _, f := range Funcs() {
		if f.Doc == "" {
			t.Errorf("%s has no doc", f.Name)
		}
		if f.Category == "" {
			t.Errorf("%s has no category", f.Name)
		}
		if f.Kind == AggregateFunc && f.Nulls != NullSkipped {
			t.Errorf("aggregate %s must skip nulls", f.Name)
		}
		if f.Name != strings.ToLower(f.Name) {
			t.Errorf("%s is not lower case", f.Name)
		}
	}
}

func TestSchemaDerivation(t *testing.T) {
	s := scan()
	c := &Compute{Input: s, Assigns: []Assign{{Name: "x", Expr: IntLit(1)}, {Name: "y", Expr: &Binary{Op: OpAdd, L: &FieldRef{Name: "x", T: Int}, R: IntLit(2), T: Int}}}}
	agg := &Aggregate{Input: c,
		Groups: []NamedExpr{{Name: "y", Expr: &FieldRef{Name: "y", T: Int}}},
		Aggs:   []NamedAgg{{Name: "count()", Agg: &AggCall{Func: MustFunc("count"), T: Int}}},
	}
	got := agg.Schema()
	if got.Open || len(got.Columns) != 2 || got.Columns[0].Name != "y" || got.Columns[1].Name != "count()" {
		t.Fatalf("aggregate schema = %+v", got)
	}
	proj := &Project{Input: agg, Names: []string{"count()"}}
	if names := proj.Schema().Names(); len(names) != 1 || names[0] != "count()" {
		t.Fatalf("project schema = %v", names)
	}
	if err := Validate(&Plan{Root: proj}); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRejectsUnknownColumnAfterAggregate(t *testing.T) {
	agg := &Aggregate{Input: scan(), Aggs: []NamedAgg{{Name: "c", Agg: &AggCall{Func: MustFunc("count"), T: Int}}}}
	f := &Filter{Input: agg, Cond: &Binary{Op: OpGt, L: Field("status"), R: IntLit(1), T: Bool}}
	err := Validate(&Plan{Root: f})
	if err == nil || !strings.Contains(err.Error(), "unknown column status") {
		t.Fatalf("err = %v", err)
	}
}

func TestValidateRejectsMisplacedAggregate(t *testing.T) {
	agg := &AggCall{Func: MustFunc("count"), T: Int}
	c := &Compute{Input: scan(), Assigns: []Assign{{Name: "x", Expr: agg}}}
	if err := Validate(&Plan{Root: c}); err == nil {
		t.Fatal("aggregate inside Compute must be rejected")
	}
	sc := &Compute{Input: scan(), Assigns: []Assign{{Name: "x", Expr: &Call{Func: MustFunc("count"), T: Int}}}}
	if err := Validate(&Plan{Root: sc}); err == nil {
		t.Fatal("aggregate function called as scalar must be rejected")
	}
}

func TestValidateLambdaScope(t *testing.T) {
	x := &LambdaParam{Name: "x", T: Any}
	arr := &ArrayExpr{Elems: []Expr{IntLit(1), IntLit(2)}, T: ArrayOf(Int)}
	good := call(t, "filter", arr, &Lambda{Params: []*LambdaParam{x}, Body: &Binary{Op: OpGt, L: x, R: IntLit(1), T: Bool}})
	if err := Validate(&Plan{Root: &Compute{Input: scan(), Assigns: []Assign{{Name: "a", Expr: good}}}}); err != nil {
		t.Fatal(err)
	}
	bad := &Compute{Input: scan(), Assigns: []Assign{{Name: "a", Expr: x}}}
	if err := Validate(&Plan{Root: bad}); err == nil {
		t.Fatal("free lambda parameter must be rejected")
	}
}

func TestRewriteExprIsCopyOnWrite(t *testing.T) {
	orig := &Binary{Op: OpAdd, L: IntLit(1), R: Field("a"), T: Any}
	out := RewriteExpr(orig, func(e Expr) Expr {
		if f, ok := e.(*FieldRef); ok && f.Name == "a" {
			return IntLit(2)
		}
		return e
	})
	if orig.R.(*FieldRef).Name != "a" {
		t.Fatal("original mutated")
	}
	if out.String() != "1 + 2" {
		t.Fatalf("rewritten = %s", out)
	}
	same := RewriteExpr(orig, func(e Expr) Expr { return e })
	if same != orig {
		t.Fatal("identity rewrite must return the same pointer")
	}
}

func TestRenameTarget(t *testing.T) {
	cases := []struct{ from, to, name, want string }{
		{"a", "b", "a", "b"},
		{"*_name", "name_*", "first_name", "name_first"},
		{"user.*", "u_*", "user.id", "u_id"},
	}
	for _, c := range cases {
		got, ok := RenameTarget(Rename{From: c.from, To: c.to}, c.name)
		if !ok || got != c.want {
			t.Errorf("RenameTarget(%s->%s, %s) = %q, %v", c.from, c.to, c.name, got, ok)
		}
	}
	if _, ok := RenameTarget(Rename{From: "*_name", To: "x_*"}, "age"); ok {
		t.Error("non-matching rename applied")
	}
}

func TestMatchName(t *testing.T) {
	if !MatchName("a*", "abc") || !MatchName("*c", "abc") || !MatchName("a*c", "abc") || MatchName("a*d", "abc") {
		t.Fatal("wildcard matching is wrong")
	}
	if !MatchName("*", "x.y") {
		t.Fatal("* must match dotted names")
	}
}

func TestTimeBoundResolve(t *testing.T) {
	now := time.Date(2026, 3, 18, 15, 42, 7, 0, time.UTC)
	cases := []struct {
		b    *TimeBound
		want time.Time
	}{
		{NowBound(), now},
		{&TimeBound{Steps: []TimeStep{{N: -7, Unit: UnitDay}}}, now.AddDate(0, 0, -7)},
		{&TimeBound{Steps: []TimeStep{{N: 1, Unit: UnitDay}, {Snap: true, Unit: UnitDay}}}, time.Date(2026, 3, 19, 0, 0, 0, 0, time.UTC)},
		{&TimeBound{Steps: []TimeStep{{N: -1, Unit: UnitMonth}, {Snap: true, Unit: UnitMonth}}}, time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)},
		{&TimeBound{Steps: []TimeStep{{Snap: true, Unit: UnitWeek, Weekday: 1}}}, time.Date(2026, 3, 16, 0, 0, 0, 0, time.UTC)},
		{&TimeBound{Steps: []TimeStep{{Snap: true, Unit: UnitQuarter}}}, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
	}
	for _, c := range cases {
		if got := c.b.Resolve(now); !got.Equal(c.want) {
			t.Errorf("%s resolved to %s, want %s", c.b, got, c.want)
		}
	}
}

func TestRender(t *testing.T) {
	f := &Filter{Input: scan(), Cond: &Search{Mode: SearchWord, Pattern: "error"}}
	l := &Limit{Input: f, Count: 10}
	want := "Limit(10)\n└─ Filter(SEARCH_WORD(_raw, \"error\"))\n   └─ Scan(main)\n"
	if got := Render(&Plan{Root: l}); got != want {
		t.Fatalf("Render =\n%s\nwant\n%s", got, want)
	}
}

func TestUnionSchemasNullability(t *testing.T) {
	a := ClosedSchema(Column{Name: "x", Type: Int}, Column{Name: "y", Type: String})
	b := ClosedSchema(Column{Name: "x", Type: Float})
	u := UnionSchemas(a, b)
	x, _ := u.Lookup("x")
	y, _ := u.Lookup("y")
	if !x.Type.Equal(Float) || x.Nullable || !y.Nullable {
		t.Fatalf("union = %+v", u)
	}
}
