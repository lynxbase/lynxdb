package vm

import (
	"encoding/binary"
	"fmt"
)

// Opcode represents a single VM instruction.
type Opcode byte

const (
	// Stack Manipulation.
	OpNop Opcode = 0x00
	OpPop Opcode = 0x01
	OpDup Opcode = 0x02

	// Constants (operand: 2-byte index into constant pool).
	OpConstInt   Opcode = 0x10
	OpConstFloat Opcode = 0x11
	OpConstStr   Opcode = 0x12
	OpConstTrue  Opcode = 0x13
	OpConstFalse Opcode = 0x14
	OpConstNull  Opcode = 0x15

	// Field Access (operand: 2-byte field name index).
	OpLoadField   Opcode = 0x20
	OpStoreField  Opcode = 0x21
	OpFieldExists Opcode = 0x22

	// Integer Arithmetic (pop 2, push 1).
	OpAddInt Opcode = 0x30
	OpSubInt Opcode = 0x31
	OpMulInt Opcode = 0x32
	OpDivInt Opcode = 0x33
	OpModInt Opcode = 0x34
	OpNegInt Opcode = 0x35

	// Float Arithmetic.
	OpAddFloat Opcode = 0x38
	OpSubFloat Opcode = 0x39
	OpMulFloat Opcode = 0x3A
	OpDivFloat Opcode = 0x3B
	OpNegFloat Opcode = 0x3C

	// Mixed Arithmetic (auto-promote int->float).
	OpAdd Opcode = 0x3E
	OpSub Opcode = 0x3F
	OpMul Opcode = 0x40
	OpDiv Opcode = 0x41
	OpMod Opcode = 0x42

	// String Operations.
	OpConcat    Opcode = 0x48
	OpStrLen    Opcode = 0x49
	OpSubstr    Opcode = 0x4A
	OpToLower   Opcode = 0x4B
	OpToUpper   Opcode = 0x4C
	OpStrMatch  Opcode = 0x4D
	OpGlobMatch Opcode = 0x4E
	OpReplace   Opcode = 0x4F
	OpSplit     Opcode = 0x57

	// Comparison (pop 2, push bool).
	OpEq     Opcode = 0x50
	OpNeq    Opcode = 0x51
	OpLt     Opcode = 0x52
	OpLte    Opcode = 0x53
	OpGt     Opcode = 0x54
	OpGte    Opcode = 0x55
	OpInList Opcode = 0x56
	OpLike   Opcode = 0x58

	// Logic.
	OpAnd Opcode = 0x60
	OpOr  Opcode = 0x61
	OpNot Opcode = 0x62

	// Control Flow.
	OpJump        Opcode = 0x70
	OpJumpIfFalse Opcode = 0x71
	OpJumpIfTrue  Opcode = 0x72

	// Type Conversion.
	OpToInt    Opcode = 0x80
	OpToFloat  Opcode = 0x81
	OpToString Opcode = 0x82
	OpToBool   Opcode = 0x83

	// Math Functions.
	OpRound Opcode = 0x90
	OpLn    Opcode = 0x91
	OpAbs   Opcode = 0x92
	OpCeil  Opcode = 0x93
	OpFloor Opcode = 0x94
	OpSqrt  Opcode = 0x95

	// Multivalue Operations.
	OpMvAppend Opcode = 0xA0
	OpMvJoin   Opcode = 0xA1
	OpMvDedup  Opcode = 0xA2
	OpMvCount  Opcode = 0xA3

	// Null Handling.
	OpCoalesce  Opcode = 0xB0
	OpIsNull    Opcode = 0xB1
	OpIsNotNull Opcode = 0xB2

	// Time Functions.
	OpStrftime Opcode = 0xC0

	// JSON Functions.
	OpJsonExtract  Opcode = 0xD0 // pop path, pop field, push extracted value
	OpJsonValid    Opcode = 0xD1 // pop field, push bool
	OpJsonKeys     Opcode = 0xD2 // pop path, pop field, push JSON array of keys
	OpJsonArrayLen Opcode = 0xD3 // pop path, pop field, push int length
	OpJsonObject   Opcode = 0xD4 // 2-byte operand: arg count; pop N values, push JSON object
	OpJsonArray    Opcode = 0xD5 // 2-byte operand: arg count; pop N values, push JSON array
	OpJsonType     Opcode = 0xD6 // pop path, pop field, push type string
	OpJsonSet      Opcode = 0xD7 // pop value, pop path, pop field, push modified JSON
	OpJsonRemove   Opcode = 0xD8 // pop path, pop field, push modified JSON
	OpJsonMerge    Opcode = 0xD9 // pop json2, pop json1, push merged JSON

	// Return.
	OpReturn Opcode = 0xFF
)

// Definition describes an opcode's name and operand widths.
type Definition struct {
	Name          string
	OperandWidths []int // each entry is 1 or 2 bytes
}

var definitions = map[Opcode]*Definition{
	OpNop: {"OpNop", nil},
	OpPop: {"OpPop", nil},
	OpDup: {"OpDup", nil},

	OpConstInt:   {"OpConstInt", []int{2}},
	OpConstFloat: {"OpConstFloat", []int{2}},
	OpConstStr:   {"OpConstStr", []int{2}},
	OpConstTrue:  {"OpConstTrue", nil},
	OpConstFalse: {"OpConstFalse", nil},
	OpConstNull:  {"OpConstNull", nil},

	OpLoadField:   {"OpLoadField", []int{2}},
	OpStoreField:  {"OpStoreField", []int{2}},
	OpFieldExists: {"OpFieldExists", []int{2}},

	OpAddInt: {"OpAddInt", nil},
	OpSubInt: {"OpSubInt", nil},
	OpMulInt: {"OpMulInt", nil},
	OpDivInt: {"OpDivInt", nil},
	OpModInt: {"OpModInt", nil},
	OpNegInt: {"OpNegInt", nil},

	OpAddFloat: {"OpAddFloat", nil},
	OpSubFloat: {"OpSubFloat", nil},
	OpMulFloat: {"OpMulFloat", nil},
	OpDivFloat: {"OpDivFloat", nil},
	OpNegFloat: {"OpNegFloat", nil},

	OpAdd: {"OpAdd", nil},
	OpSub: {"OpSub", nil},
	OpMul: {"OpMul", nil},
	OpDiv: {"OpDiv", nil},
	OpMod: {"OpMod", nil},

	OpConcat:    {"OpConcat", nil},
	OpStrLen:    {"OpStrLen", nil},
	OpSubstr:    {"OpSubstr", nil},
	OpToLower:   {"OpToLower", nil},
	OpToUpper:   {"OpToUpper", nil},
	OpStrMatch:  {"OpStrMatch", []int{2}},
	OpGlobMatch: {"OpGlobMatch", []int{2}},
	OpReplace:   {"OpReplace", []int{2}},
	OpSplit:     {"OpSplit", nil},

	OpEq:     {"OpEq", nil},
	OpNeq:    {"OpNeq", nil},
	OpLt:     {"OpLt", nil},
	OpLte:    {"OpLte", nil},
	OpGt:     {"OpGt", nil},
	OpGte:    {"OpGte", nil},
	OpInList: {"OpInList", []int{2}},
	OpLike:   {"OpLike", nil},

	OpAnd: {"OpAnd", nil},
	OpOr:  {"OpOr", nil},
	OpNot: {"OpNot", nil},

	OpJump:        {"OpJump", []int{2}},
	OpJumpIfFalse: {"OpJumpIfFalse", []int{2}},
	OpJumpIfTrue:  {"OpJumpIfTrue", []int{2}},

	OpToInt:    {"OpToInt", nil},
	OpToFloat:  {"OpToFloat", nil},
	OpToString: {"OpToString", nil},
	OpToBool:   {"OpToBool", nil},

	OpRound: {"OpRound", nil},
	OpLn:    {"OpLn", nil},
	OpAbs:   {"OpAbs", nil},
	OpCeil:  {"OpCeil", nil},
	OpFloor: {"OpFloor", nil},
	OpSqrt:  {"OpSqrt", nil},

	OpMvAppend: {"OpMvAppend", []int{2}},
	OpMvJoin:   {"OpMvJoin", nil},
	OpMvDedup:  {"OpMvDedup", nil},
	OpMvCount:  {"OpMvCount", nil},

	OpCoalesce:  {"OpCoalesce", []int{2}},
	OpIsNull:    {"OpIsNull", nil},
	OpIsNotNull: {"OpIsNotNull", nil},

	OpStrftime: {"OpStrftime", nil},

	OpJsonExtract:  {"OpJsonExtract", nil},
	OpJsonValid:    {"OpJsonValid", nil},
	OpJsonKeys:     {"OpJsonKeys", nil},
	OpJsonArrayLen: {"OpJsonArrayLen", nil},
	OpJsonObject:   {"OpJsonObject", []int{2}},
	OpJsonArray:    {"OpJsonArray", []int{2}},
	OpJsonType:     {"OpJsonType", nil},
	OpJsonSet:      {"OpJsonSet", nil},
	OpJsonRemove:   {"OpJsonRemove", nil},
	OpJsonMerge:    {"OpJsonMerge", nil},

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
