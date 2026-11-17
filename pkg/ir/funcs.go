package ir

import (
	"fmt"
	"sort"
	"strings"
)

// FuncKind classifies registered functions.
type FuncKind uint8

const (
	// ScalarFunc maps one row's argument values to one value.
	ScalarFunc FuncKind = iota + 1
	// AggregateFunc folds the argument values of many rows into one value.
	AggregateFunc
	// WindowFunc computes a value per row from the rows of its frame and is
	// valid only in a Window operator.
	WindowFunc
)

func (k FuncKind) String() string {
	switch k {
	case ScalarFunc:
		return "scalar"
	case AggregateFunc:
		return "aggregate"
	case WindowFunc:
		return "window"
	}
	return "invalid"
}

// NullBehavior describes how a function treats NULL arguments.
type NullBehavior uint8

const (
	// NullStrict functions return NULL when any argument is NULL.
	NullStrict NullBehavior = iota
	// NullAware functions define their own NULL handling, documented on
	// the function.
	NullAware
	// NullSkipped aggregates ignore NULL input values; with no non-NULL
	// input they return NULL (count returns 0).
	NullSkipped
)

func (n NullBehavior) String() string {
	switch n {
	case NullStrict:
		return "strict"
	case NullAware:
		return "aware"
	case NullSkipped:
		return "skipped"
	}
	return "invalid"
}

// MergeKind describes whether an aggregate decomposes into partial states
// that workers compute independently and a coordinator merges.
type MergeKind uint8

const (
	// MergeNone aggregates must see all rows in one place.
	MergeNone MergeKind = iota
	// MergeExact aggregates merge partial states without loss.
	MergeExact
	// MergeApprox aggregates merge sketches; the result is an estimate with
	// the same error bound as a single-worker computation.
	MergeApprox
)

func (m MergeKind) String() string {
	switch m {
	case MergeExact:
		return "exact"
	case MergeApprox:
		return "approximate"
	}
	return "none"
}

// Param describes one function parameter.
type Param struct {
	Name string
	Type Type
	// Const requires the argument to be a literal.
	Const bool
	// Lambda, when positive, requires a lambda taking that many parameters.
	Lambda int
}

// ResultRule computes a function's static result type from its argument
// types, or reports a type error.
type ResultRule func(args []Type) (Type, error)

// Func is one entry of the function registry.
type Func struct {
	Name     string
	Kind     FuncKind
	Category string
	// Params lists the parameters; the first MinArgs are required.
	Params  []Param
	MinArgs int
	// Variadic repeats the last parameter without bound.
	Variadic bool
	Result   ResultRule
	Nulls    NullBehavior
	// Volatile functions may return a different value on every call.
	Volatile bool
	// QueryConstant functions return the same value for every row of one
	// query execution but differ between executions (the current time).
	QueryConstant bool
	// Merge describes partial aggregation support for aggregates.
	Merge MergeKind
	// OrderSensitive aggregates depend on input row order.
	OrderSensitive bool
	// Doc is a one-sentence description of the semantics.
	Doc string
	// Errors describes how invalid arguments are reported.
	Errors string
}

// Deterministic reports whether the function always returns the same value
// for the same arguments within and across executions.
func (f *Func) Deterministic() bool { return !f.Volatile && !f.QueryConstant }

// RowLocal reports whether a call can be evaluated from one row alone, so
// that it may run at scan time or be pushed into a storage filter.
func (f *Func) RowLocal() bool { return f.Kind == ScalarFunc && !f.Volatile }

// Foldable reports whether a call with literal arguments may be replaced by
// its value at planning time.
func (f *Func) Foldable() bool { return f.Kind == ScalarFunc && f.Deterministic() }

// MaxArgs returns the maximum argument count, or -1 when variadic.
func (f *Func) MaxArgs() int {
	if f.Variadic {
		return -1
	}
	return len(f.Params)
}

// Signature renders the parameter list for documentation and errors.
func (f *Func) Signature() string {
	parts := make([]string, 0, len(f.Params))
	for i, p := range f.Params {
		s := p.Name
		if p.Lambda > 0 {
			s += ": lambda"
		} else if p.Type.Kind != KindAny {
			s += ": " + p.Type.String()
		}
		if f.Variadic && i == len(f.Params)-1 {
			s += "..."
		}
		if i >= f.MinArgs && !(f.Variadic && i == len(f.Params)-1) {
			s = "[" + s + "]"
		}
		parts = append(parts, s)
	}
	return f.Name + "(" + strings.Join(parts, ", ") + ")"
}

// ParamFor returns the parameter that argument i binds to.
func (f *Func) ParamFor(i int) (Param, bool) {
	if i < len(f.Params) {
		return f.Params[i], true
	}
	if f.Variadic && len(f.Params) > 0 {
		return f.Params[len(f.Params)-1], true
	}
	return Param{}, false
}

// ArityError is returned when a call has the wrong number of arguments.
type ArityError struct {
	Func string
	Got  int
	Min  int
	Max  int
}

func (e *ArityError) Error() string {
	switch {
	case e.Max < 0:
		return fmt.Sprintf("%s expects at least %d argument(s), got %d", e.Func, e.Min, e.Got)
	case e.Min == e.Max:
		return fmt.Sprintf("%s expects %d argument(s), got %d", e.Func, e.Min, e.Got)
	default:
		return fmt.Sprintf("%s expects %d to %d arguments, got %d", e.Func, e.Min, e.Max, e.Got)
	}
}

// TypeError is returned when an argument's static type is not accepted.
type TypeError struct {
	Func     string
	Arg      int
	Param    string
	Expected Type
	Got      Type
}

func (e *TypeError) Error() string {
	return fmt.Sprintf("%s: argument %d (%s) must be %s, got %s", e.Func, e.Arg+1, e.Param, e.Expected, e.Got)
}

// Check validates argument count and static types and returns the result
// type.
func (f *Func) Check(args []Type) (Type, error) {
	n := len(args)
	if n < f.MinArgs || (!f.Variadic && n > len(f.Params)) {
		return Any, &ArityError{Func: f.Name, Got: n, Min: f.MinArgs, Max: f.MaxArgs()}
	}
	for i, t := range args {
		p, _ := f.ParamFor(i)
		if p.Lambda > 0 {
			continue
		}
		if !p.Type.Accepts(t) {
			return Any, &TypeError{Func: f.Name, Arg: i, Param: p.Name, Expected: p.Type, Got: t}
		}
	}
	if f.Result == nil {
		return Any, nil
	}
	return f.Result(args)
}

var funcRegistry = map[string]*Func{}

// RegisterFunc adds f to the registry; registering a name twice panics.
func RegisterFunc(f *Func) *Func {
	if _, dup := funcRegistry[f.Name]; dup {
		panic("ir: duplicate function " + f.Name)
	}
	if f.MinArgs > len(f.Params) {
		panic("ir: function " + f.Name + " requires more arguments than it declares")
	}
	funcRegistry[f.Name] = f
	return f
}

// LookupFunc returns the registered function with the given name.
func LookupFunc(name string) (*Func, bool) {
	f, ok := funcRegistry[name]
	return f, ok
}

// MustFunc returns the registered function with the given name and panics
// when there is none; it is meant for references fixed at compile time.
func MustFunc(name string) *Func {
	f, ok := funcRegistry[name]
	if !ok {
		panic("ir: unknown function " + name)
	}
	return f
}

// Funcs returns every registered function sorted by name.
func Funcs() []*Func {
	out := make([]*Func, 0, len(funcRegistry))
	for _, f := range funcRegistry {
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Result rules.

// Returns gives a fixed result type.
func Returns(t Type) ResultRule {
	return func([]Type) (Type, error) { return t, nil }
}

// SameAs gives the type of argument i.
func SameAs(i int) ResultRule {
	return func(args []Type) (Type, error) {
		if i < len(args) {
			if args[i].Kind == KindNull {
				return Any, nil
			}
			return args[i], nil
		}
		return Any, nil
	}
}

// UnifyArgs gives the common supertype of the arguments from index from on.
func UnifyArgs(from int) ResultRule {
	return func(args []Type) (Type, error) {
		t := Null
		for i := from; i < len(args); i++ {
			t = Unify(t, args[i])
		}
		if t.Kind == KindNull {
			return Any, nil
		}
		return t, nil
	}
}

// NumericResult gives int when every argument is int, float when any is
// float, and Any otherwise.
func NumericResult(args []Type) (Type, error) {
	allInt := true
	for _, a := range args {
		switch a.Kind {
		case KindInt:
		case KindFloat:
			allInt = false
		default:
			return Any, nil
		}
	}
	if allInt && len(args) > 0 {
		return Int, nil
	}
	return Float, nil
}

// ArrayOfArg gives an array of argument i's type.
func ArrayOfArg(i int) ResultRule {
	return func(args []Type) (Type, error) {
		if i < len(args) {
			return ArrayOf(args[i]), nil
		}
		return ArrayOf(Any), nil
	}
}
