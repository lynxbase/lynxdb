package exec

// Node is an operator of a logical plan: *Scan, *Aggregate, *Filter, *Project
// or *Sort. A plan is a chain that starts at a Scan, optionally aggregates and
// filters the groups, then optionally projects and sorts; a chain without an
// aggregate needs a projection to name its columns.
type Node interface{ node() }

// Scan reads the rows for which Filter is true, every row when it is nil.
type Scan struct {
	Filter *Expr
}

// Named is an expression and the name of the column it produces.
type Named struct {
	Name string
	Expr *Expr
}

// AggFunc identifies an aggregate function.
type AggFunc uint8

// The aggregate functions.
const (
	Count AggFunc = iota + 1
	CountDistinct
	Sum
	Min
	Max
	Avg
	// Var is the sample variance and Stdev its square root; both are null
	// for fewer than two values.
	Var
	Stdev
	// First and Last are the value of a column in the first and the last
	// row, in the order the table stores them, that holds one; Earliest and
	// Latest are its value in the row with the smallest and the largest
	// time that holds one.
	First
	Last
	Earliest
	Latest
	// ArgMin and ArgMax are the value of a column in the row where By is
	// the least and the greatest number, among the rows where the column
	// holds a value; of rows that tie, the first and the last in the order
	// the table stores them. Integers compare exactly unless By can also be
	// a float, and then as floats.
	ArgMin
	ArgMax
	// AnyValue is a value that a column holds in some row of the group.
	AnyValue
	// FirstRow is the value of a column in the first row of the group in
	// the order the table stores them, null where that row holds none;
	// every FirstRow of an aggregate reads the same row.
	FirstRow
	// ApproxDistinct estimates the number of distinct values of the
	// argument, telling kinds apart, in at most 4 KB per group: exactly up
	// to 383 values, and beyond with a standard error of 1.6 percent.
	ApproxDistinct
)

// Agg is one aggregate; Arg is nil for a Count of rows, and By orders the
// rows of an ArgMin or an ArgMax.
type Agg struct {
	Name string
	Func AggFunc
	Arg  *Expr
	By   *Expr
}

// Aggregate groups its input by Keys and computes Aggs per group; its
// columns are the keys followed by the aggregates. Without keys it produces
// one row.
type Aggregate struct {
	Input Node
	Keys  []Named
	Aggs  []Agg
}

// Filter keeps the rows of an aggregate for which Cond is true: an And, Or
// or Not of comparisons between expressions over the aggregate's columns. A
// comparison with a null, or between a number and a string, is false.
type Filter struct {
	Input Node
	Cond  *Expr
}

// Project computes Exprs over the columns of its input and replaces them.
type Project struct {
	Input Node
	Exprs []Named
}

// Order names a column to sort by; nulls sort last.
type Order struct {
	Column string
	Desc   bool
}

// Sort orders its input by By and keeps the first Limit rows, every row when
// Limit is zero; with no By it only limits.
type Sort struct {
	Input Node
	By    []Order
	Limit int
}

func (*Scan) node()      {}
func (*Aggregate) node() {}
func (*Filter) node()    {}
func (*Project) node()   {}
func (*Sort) node()      {}
