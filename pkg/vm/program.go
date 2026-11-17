package vm

import (
	"fmt"
	"net"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// Program holds compiled bytecode for one expression/predicate.
type Program struct {
	Instructions  []byte
	Constants     []event.Value
	FieldNames    []string
	RegexPatterns []string
	// GlobPatterns holds raw glob patterns for OpGlobMatch (case-sensitive
	// whole-value) and OpHasTokenGlob (case-insensitive whole-token; the
	// compiler stores those lowercased). They are NOT regexes and must stay
	// out of RegexPatterns: ensureRegexCache compiles that pool, and a glob
	// like "*user*" is an invalid regex. ensureGlobCache compiles this pool
	// via internal/glob.
	GlobPatterns []string
	CIDRNets     []*net.IPNet // compiled CIDR networks for cidrmatch()

	// SubPrograms stores compiled lambda bodies. OpArrayAny/All/Filter/Map
	// reference sub-programs by index. Sub-programs share the parent's
	// Constants, FieldNames, and RegexPatterns pools (they use the parent
	// program for pool lookups). Each sub-program contains only its own
	// Instructions.
	SubPrograms []*Program

	// Natives holds the functions OpCallNative invokes.
	Natives []NativeFunc

	// NumericStrings makes the strict arithmetic, comparison and IN opcodes
	// convert a string operand to a number when the other operand is one.
	NumericStrings bool

	BSIHandledComparisons int
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

// AddCIDR parses a CIDR string, deduplicates, and returns its pool index.
func (p *Program) AddCIDR(cidr string) (int, error) {
	for i, n := range p.CIDRNets {
		if n.String() == cidr {
			return i, nil
		}
	}
	_, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		return 0, fmt.Errorf("vm.Program.AddCIDR: invalid CIDR %q: %w", cidr, err)
	}
	p.CIDRNets = append(p.CIDRNets, ipNet)

	return len(p.CIDRNets) - 1, nil
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
