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
	// to 383 values, and beyond with a standard error of 1.6 percent. Over
	// a table sorted by the group keys and then by the argument, the order
	// of the rows counts the values, and the estimate is exact.
	ApproxDistinct
	// Percentile is the value below which P percent of the argument's
	// numbers lie, within one percent of that value: the number that has
	// floor(P/100*(n-1)) of the n numbers before it, read from a sketch of
	// at most 32 KB per group. Every Percentile of one argument shares the
	// sketch.
	Percentile
)

// Agg is one aggregate; Arg is nil for a Count of rows, By orders the rows
// of an ArgMin or an ArgMax, and P is the percentage of a Percentile, from 0
// to 100.
type Agg struct {
	Name string
	Func AggFunc
	Arg  *Expr
	By   *Expr
	P    float64
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
// With Rest, a projection of a scan is followed by every other field that
// its rows hold, one column per path in the order of the paths' names: a
// field is other when neither it nor an object it is in is named by Exprs or
// by Except. A row that does not hold a field has no value there, and one
// that holds a null has a value of the null kind. Above an aggregate, the
// last expression is a column that a FirstRow gives, and the fields are
// those of the rows it picked.
type Project struct {
	Input  Node
	Exprs  []Named
	Rest   bool
	Except []string
}

// Order names a column to sort by; nulls sort last, or first with
// NullsFirst.
type Order struct {
	Column     string
	Desc       bool
	NullsFirst bool
}

// Sort orders its input by By and keeps the first Limit rows, every row when
// Limit is zero; with no By it only limits. The columns named by Hide order
// the rows and are not in the result.
type Sort struct {
	Input Node
	By    []Order
	Limit int
	Hide  []string
}

func (*Scan) node()      {}
func (*Aggregate) node() {}
func (*Filter) node()    {}
func (*Project) node()   {}
func (*Sort) node()      {}
