// Package vec defines typed column vectors and the kernels that run over them.
package vec

// Kind is the scalar type of every value in a column.
type Kind uint8

// The numeric values are part of the on-disk format.
const (
	KindNull    Kind = 1
	KindBool    Kind = 2
	KindInt     Kind = 3
	KindFloat   Kind = 4
	KindString  Kind = 5
	KindVariant Kind = 6
)

func (k Kind) String() string {
	switch k {
	case KindNull:
		return "null"
	case KindBool:
		return "bool"
	case KindInt:
		return "int"
	case KindFloat:
		return "float"
	case KindString:
		return "string"
	case KindVariant:
		return "variant"
	}

	return "invalid"
}

// Layout is the physical arrangement of a column's values.
type Layout uint8

const (
	// Flat stores one value per row.
	Flat Layout = iota
	// Dict stores one code per row into a sorted dictionary.
	Dict
	// Const stores a single value shared by every valid row.
	Const
)
