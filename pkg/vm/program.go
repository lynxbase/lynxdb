package vm

import (
	"github.com/lynxbase/lynxdb/pkg/event"
)

// Program holds compiled bytecode for one expression/predicate.
type Program struct {
	Instructions []byte
	Constants    []event.Value
	FieldNames   []string
	// GlobPatterns holds the lowercased glob patterns of OpHasTokenGlob.
	GlobPatterns []string

	// SubPrograms holds the lambda bodies that the array opcodes run; a
	// body has instructions of its own and shares the pools of its parent.
	SubPrograms []*Program
	// Params is the number of parameters of a lambda body; OpArrayMap
	// passes the element's 0-based index as the second.
	Params int

	// Natives holds the functions OpCallNative invokes.
	Natives []NativeFunc

	// NumericStrings makes the strict arithmetic, comparison and IN opcodes
	// convert a string operand to a number when the other operand is one.
	NumericStrings bool
}

// AddNative registers a native function and returns its index.
func (p *Program) AddNative(f NativeFunc) int {
	p.Natives = append(p.Natives, f)
	return len(p.Natives) - 1
}

// AddSubProgram appends a compiled sub-program and returns its index.
func (p *Program) AddSubProgram(sub *Program) int {
	p.SubPrograms = append(p.SubPrograms, sub)
	return len(p.SubPrograms) - 1
}

// AddConstant appends a constant and returns its index.
func (p *Program) AddConstant(v event.Value) int {
	p.Constants = append(p.Constants, v)

	return len(p.Constants) - 1
}

// AddFieldName appends a field name (deduplicating) and returns its index.
func (p *Program) AddFieldName(name string) int {
	for i, n := range p.FieldNames {
		if n == name {
			return i
		}
	}
	p.FieldNames = append(p.FieldNames, name)

	return len(p.FieldNames) - 1
}

// AddGlob appends a glob pattern (deduplicating) and returns its index.
func (p *Program) AddGlob(pattern string) int {
	for i, g := range p.GlobPatterns {
		if g == pattern {
			return i
		}
	}
	p.GlobPatterns = append(p.GlobPatterns, pattern)

	return len(p.GlobPatterns) - 1
}

// Emit appends raw bytes to the instruction stream.
func (p *Program) Emit(ins ...byte) int {
	pos := len(p.Instructions)
	p.Instructions = append(p.Instructions, ins...)

	return pos
}

// EmitOp appends a complete instruction (opcode + operands).
func (p *Program) EmitOp(op Opcode, operands ...int) int {
	pos := len(p.Instructions)
	p.Instructions = append(p.Instructions, Make(op, operands...)...)

	return pos
}

// PatchUint16 writes a uint16 value at the given offset in the instruction stream.
func (p *Program) PatchUint16(offset int, val uint16) {
	p.Instructions[offset] = byte(val >> 8)
	p.Instructions[offset+1] = byte(val)
}

// Len returns the current length of the instruction stream.
func (p *Program) Len() int {
	return len(p.Instructions)
}
