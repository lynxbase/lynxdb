package vm

import (
	"encoding/binary"
	"fmt"
)

// Opcode represents a single VM instruction.
type Opcode byte

const (
	OpConstStr   Opcode = 0x12
	OpConstTrue  Opcode = 0x13
	OpConstFalse Opcode = 0x14
	OpConstNull  Opcode = 0x15

	// Mixed Arithmetic (auto-promote int->float).
	OpAdd Opcode = 0x3E
	OpSub Opcode = 0x3F
	OpMul Opcode = 0x40
	OpDiv Opcode = 0x41
	OpMod Opcode = 0x42

	// Comparison (pop 2, push bool).
	OpEq Opcode = 0x50
	OpLt Opcode = 0x52
	OpGt Opcode = 0x54

	// Logic.
	OpAnd Opcode = 0x60
	OpOr  Opcode = 0x61
	OpNot Opcode = 0x62
	OpXor Opcode = 0x63

	// Control Flow.
	OpJump        Opcode = 0x70
	OpJumpIfFalse Opcode = 0x71
	OpJumpIfTrue  Opcode = 0x72

	// Null Handling.
	OpCoalesce Opcode = 0xB0

	// OpCallNative has two 2-byte operands, a Program.Natives index and an
	// argument count N; it pops N values and pushes the function's result.
	OpCallNative Opcode = 0x8C
	OpIsNull     Opcode = 0xB1

	// Typed-value opcodes (duration, array, object).
	OpArrayBuild  Opcode = 0xE1 // 2-byte operand: element count N; pop N values, push array
	OpObjectBuild Opcode = 0xE2 // 2-byte operand: entry count N; pop 2N values (key,val pairs), push object
	OpIndex       Opcode = 0xE3 // pop index, pop container; arr[i] / obj["k"]; OOB/missing/null → null

	// Strict comparison opcodes.
	// Same-type: typed comparison. int vs float: cross-promote (both are "number").
	// Null/missing operand → null. Incompatible types → null + warning counter.
	OpEqStrict  Opcode = 0xE7 // pop 2, push bool/null; strict == (CS strings, no coercion)
	OpNeqStrict Opcode = 0xE8 // pop 2, push bool/null; strict !=
	OpLtStrict  Opcode = 0xE9 // pop 2, push bool/null; strict <
	OpLteStrict Opcode = 0xEA // pop 2, push bool/null; strict <=
	OpGtStrict  Opcode = 0xEB // pop 2, push bool/null; strict >
	OpGteStrict Opcode = 0xEC // pop 2, push bool/null; strict >=

	// Three-valued logic opcodes.
	OpNot3VL        Opcode = 0xED // pop 1, push bool/null; not(true)=false, not(false)=true, not(null)=null, not(non-bool)=null+warn
	OpJumpIfNull3VL Opcode = 0xEE // 2-byte operand: target; if TOS is null, pop and jump; else leave on stack
	OpAnd3VLNull    Opcode = 0xEF // pop 1 (right); left was null: if right=false→false, else→null
	OpOr3VLNull     Opcode = 0xF0 // pop 1 (right); left was null: if right=true→true, else→null

	// Strict arithmetic opcodes: int+int→int, int+float→float, string+string→concat
	// (+ only), division by zero→null, no string-to-number promotion.
	// Timestamp/duration algebra same as existing opcodes.
	OpAddStrict Opcode = 0xF1 // string+string=concat, int+int=int, int+float=float, ts/dur algebra; string+num→null+warn
	OpSubStrict Opcode = 0xF2 // no string sub; int-int=int, int-float=float, ts/dur algebra
	OpMulStrict Opcode = 0xF3 // int*int=int, int*float=float, dur*num, no string
	OpDivStrict Opcode = 0xF4 // int/int=TRUNCATING int, int/float=float, dur/dur=float, dur/num=dur, /0→null
	OpModStrict Opcode = 0xF5 // int%int only; non-int→null+warn; %0→null

	// Field-path and search opcodes.
	OpLoadPath     Opcode = 0xF6 // 2-byte operand: constant-pool index of dotted path string; flat column first, then object walk, no _raw fallback
	OpFieldMissing Opcode = 0xF7 // 2-byte operand: field name index; pushes true if field absent from row
	OpNegStrict    Opcode = 0xF8 // pop 1; negate int/float/duration; non-numeric→null+warn
	OpInStrict     Opcode = 0xF9 // 2-byte operand: count N; pop N items + 1 value; strict equality check; null-aware

	// Function opcodes.
	OpHasToken   Opcode = 0xFA // pop 2 (field, term); case-insensitive whole-token match
	OpContainsCI Opcode = 0xFB // pop 2 (field, substr); case-insensitive substring

	// Lambda and array/object function opcodes.
	OpArrayAny        Opcode = 0x03 // 2-byte operand: sub-program index; pop array, push bool (3VL)
	OpArrayAll        Opcode = 0x04 // 2-byte operand: sub-program index; pop array, push bool (3VL)
	OpArrayFilter     Opcode = 0x05 // 2-byte operand: sub-program index; pop array, push filtered array
	OpArrayMap        Opcode = 0x06 // 2-byte operand: sub-program index; pop array, push mapped array
	OpLoadLambdaParam Opcode = 0x07 // 2-byte operand: depth (0 = current lambda param)
	OpHasTokenGlob    Opcode = 0x19 // 2-byte operand: glob pool index; pop field; true if any token matches the glob (CI)
	OpArrayReduce     Opcode = 0x3D // 2-byte operand: sub-program index; pop array and init, push folded value

	OpReturn Opcode = 0xFF
)

// Definition describes an opcode's name and operand widths.
type Definition struct {
	Name          string
	OperandWidths []int // each entry is 1 or 2 bytes
}

var definitions = map[Opcode]*Definition{

	OpConstStr:   {"OpConstStr", []int{2}},
	OpConstTrue:  {"OpConstTrue", nil},
	OpConstFalse: {"OpConstFalse", nil},
	OpConstNull:  {"OpConstNull", nil},

	OpAdd: {"OpAdd", nil},
	OpSub: {"OpSub", nil},
	OpMul: {"OpMul", nil},
	OpDiv: {"OpDiv", nil},
	OpMod: {"OpMod", nil},

	OpHasTokenGlob: {"OpHasTokenGlob", []int{2}},

	OpEq: {"OpEq", nil},
	OpLt: {"OpLt", nil},
	OpGt: {"OpGt", nil},

	OpAnd: {"OpAnd", nil},
	OpOr:  {"OpOr", nil},
	OpNot: {"OpNot", nil},
	OpXor: {"OpXor", nil},

	OpJump:        {"OpJump", []int{2}},
	OpJumpIfFalse: {"OpJumpIfFalse", []int{2}},
	OpJumpIfTrue:  {"OpJumpIfTrue", []int{2}},

	OpCoalesce:   {"OpCoalesce", []int{2}},
	OpCallNative: {"OpCallNative", []int{2, 2}},
	OpIsNull:     {"OpIsNull", nil},

	OpArrayBuild:  {"OpArrayBuild", []int{2}},
	OpObjectBuild: {"OpObjectBuild", []int{2}},
	OpIndex:       {"OpIndex", nil},

	// Strict comparison
	OpEqStrict:  {"OpEqStrict", nil},
	OpNeqStrict: {"OpNeqStrict", nil},
	OpLtStrict:  {"OpLtStrict", nil},
	OpLteStrict: {"OpLteStrict", nil},
	OpGtStrict:  {"OpGtStrict", nil},
	OpGteStrict: {"OpGteStrict", nil},

	// Three-valued logic
	OpNot3VL:        {"OpNot3VL", nil},
	OpJumpIfNull3VL: {"OpJumpIfNull3VL", []int{2}},
	OpAnd3VLNull:    {"OpAnd3VLNull", nil},
	OpOr3VLNull:     {"OpOr3VLNull", nil},

	// Strict arithmetic
	OpAddStrict: {"OpAddStrict", nil},
	OpSubStrict: {"OpSubStrict", nil},
	OpMulStrict: {"OpMulStrict", nil},
	OpDivStrict: {"OpDivStrict", nil},
	OpModStrict: {"OpModStrict", nil},

	// Field paths and search
	OpLoadPath:     {"OpLoadPath", []int{2}},
	OpFieldMissing: {"OpFieldMissing", []int{2}},
	OpNegStrict:    {"OpNegStrict", nil},
	OpInStrict:     {"OpInStrict", []int{2}},

	// Functions
	OpHasToken:   {"OpHasToken", nil},
	OpContainsCI: {"OpContainsCI", nil},

	// Lambdas and arrays/objects
	OpArrayAny:        {"OpArrayAny", []int{2}},
	OpArrayAll:        {"OpArrayAll", []int{2}},
	OpArrayFilter:     {"OpArrayFilter", []int{2}},
	OpArrayMap:        {"OpArrayMap", []int{2}},
	OpLoadLambdaParam: {"OpLoadLambdaParam", []int{2}},
	OpArrayReduce:     {"OpArrayReduce", []int{2}},

	OpReturn: {"OpReturn", nil},
}

// Make creates a single encoded instruction from an opcode and operands.
func Make(op Opcode, operands ...int) []byte {
	def, ok := definitions[op]
	if !ok {
		return []byte{byte(op)}
	}
	instructionLen := 1
	for _, w := range def.OperandWidths {
		instructionLen += w
	}
	instruction := make([]byte, instructionLen)
	instruction[0] = byte(op)
	offset := 1
	for i, o := range operands {
		if i >= len(def.OperandWidths) {
			break
		}
		width := def.OperandWidths[i]
		switch width {
		case 1:
			instruction[offset] = byte(o)
		case 2:
			binary.BigEndian.PutUint16(instruction[offset:], uint16(o))
		}
		offset += width
	}

	return instruction
}

// ReadUint16 reads a big-endian uint16 from a byte slice.
func ReadUint16(ins []byte) uint16 {
	return binary.BigEndian.Uint16(ins)
}

func (op Opcode) String() string {
	if def, ok := definitions[op]; ok {
		return def.Name
	}

	return fmt.Sprintf("Unknown(0x%02x)", byte(op))
}
