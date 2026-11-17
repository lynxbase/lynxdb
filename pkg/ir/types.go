package ir

// Kind is the base kind of an IR type.
type Kind uint8

const (
	// KindAny is a value whose type is decided per row at execution time.
	KindAny Kind = iota
	// KindNull is the type of the NULL literal; it unifies with every type.
	KindNull
	KindBool
	// KindInt is a 64-bit signed integer.
	KindInt
	// KindFloat is a 64-bit IEEE 754 floating-point number.
	KindFloat
	KindString
	// KindTimestamp is an instant with nanosecond precision, UTC.
	KindTimestamp
	// KindDuration is a signed span of time with nanosecond precision.
	KindDuration
	// KindArray is an ordered list whose elements have type Elem.
	KindArray
	// KindObject is a string-keyed map of values.
	KindObject
)

var kindNames = [...]string{
	KindAny:       "any",
	KindNull:      "null",
	KindBool:      "bool",
	KindInt:       "int",
	KindFloat:     "float",
	KindString:    "string",
	KindTimestamp: "timestamp",
	KindDuration:  "duration",
	KindArray:     "array",
	KindObject:    "object",
}

func (k Kind) String() string {
	if int(k) < len(kindNames) {
		return kindNames[k]
	}
	return "invalid"
}

// Type is an IR data type. Array types carry their element type.
type Type struct {
	Kind Kind
	Elem *Type
}

var (
	Any       = Type{Kind: KindAny}
	Null      = Type{Kind: KindNull}
	Bool      = Type{Kind: KindBool}
	Int       = Type{Kind: KindInt}
	Float     = Type{Kind: KindFloat}
	String    = Type{Kind: KindString}
	Timestamp = Type{Kind: KindTimestamp}
	Duration  = Type{Kind: KindDuration}
	Object    = Type{Kind: KindObject}
)

// ArrayOf returns the array type with the given element type.
func ArrayOf(elem Type) Type {
	e := elem
	return Type{Kind: KindArray, Elem: &e}
}

// ElemType returns the element type of an array type, or Any.
func (t Type) ElemType() Type {
	if t.Kind == KindArray && t.Elem != nil {
		return *t.Elem
	}
	return Any
}

func (t Type) String() string {
	if t.Kind == KindArray {
		return "array<" + t.ElemType().String() + ">"
	}
	return t.Kind.String()
}

// Equal reports structural type equality.
func (t Type) Equal(o Type) bool {
	if t.Kind != o.Kind {
		return false
	}
	if t.Kind == KindArray {
		return t.ElemType().Equal(o.ElemType())
	}
	return true
}

// IsNumeric reports whether t is int or float.
func (t Type) IsNumeric() bool { return t.Kind == KindInt || t.Kind == KindFloat }

// IsDynamic reports whether the type is only known at execution time.
func (t Type) IsDynamic() bool { return t.Kind == KindAny }

// Accepts reports whether a value of static type v may be passed where t is
// expected without an explicit conversion. Any and Null are accepted
// everywhere and checked at execution time; an int is accepted where a float
// is expected (numeric widening).
func (t Type) Accepts(v Type) bool {
	switch {
	case t.Kind == KindAny, v.Kind == KindAny, v.Kind == KindNull:
		return true
	case t.Kind == KindFloat && v.Kind == KindInt:
		return true
	case t.Kind == KindArray && v.Kind == KindArray:
		return t.ElemType().Accepts(v.ElemType())
	default:
		return t.Kind == v.Kind
	}
}

// Unify returns the common supertype of a and b used for CASE branches,
// COALESCE arguments and UNION columns: equal types stay, NULL yields the
// other side, int and float widen to float, anything else is Any.
func Unify(a, b Type) Type {
	switch {
	case a.Kind == KindNull:
		return b
	case b.Kind == KindNull:
		return a
	case a.Equal(b):
		return a
	case a.IsNumeric() && b.IsNumeric():
		return Float
	case a.Kind == KindArray && b.Kind == KindArray:
		return ArrayOf(Unify(a.ElemType(), b.ElemType()))
	default:
		return Any
	}
}
