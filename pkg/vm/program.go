package vm

import "github.com/lynxbase/lynxdb/pkg/event"

// Program holds compiled bytecode for one expression/predicate.
type Program struct {
	Instructions  []byte
	Constants     []event.Value
	FieldNames    []string
	RegexPatterns []string
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

// AddRegex appends a regex pattern (deduplicating) and returns its index.
func (p *Program) AddRegex(pattern string) int {
	for i, r := range p.RegexPatterns {
		if r == pattern {
			return i
		}
	}
	p.RegexPatterns = append(p.RegexPatterns, pattern)

	return len(p.RegexPatterns) - 1
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
