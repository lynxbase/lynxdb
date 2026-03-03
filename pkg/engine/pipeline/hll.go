package pipeline

import (
	"hash/fnv"
	"math"
	"math/bits"
)

// HyperLogLog implements the HyperLogLog algorithm for approximate cardinality estimation.
// Uses 14-bit precision (16384 registers) for ~0.8% standard error.
type HyperLogLog struct {
	registers [16384]uint8
	precision uint8
	m         uint32
}

// NewHyperLogLog creates a new HLL with 14-bit precision.
func NewHyperLogLog() *HyperLogLog {
	return &HyperLogLog{
		precision: 14,
		m:         16384, // 2^14
	}
}

// Add adds a value to the HLL sketch.
func (h *HyperLogLog) Add(value string) {
	hash := fnv.New64a()
	hash.Write([]byte(value))
	x := hash.Sum64()

	// Use first 'precision' bits as register index.
	idx := x >> (64 - h.precision)
	// Count leading zeros in the remaining bits.
	w := x<<h.precision | (1 << (h.precision - 1)) // ensure at least one bit set
	rho := uint8(bits.LeadingZeros64(w)) + 1

	if rho > h.registers[idx] {
		h.registers[idx] = rho
	}
}

// Count returns the estimated cardinality.
func (h *HyperLogLog) Count() int64 {
	// Compute harmonic mean of 2^(-register[i]).
	var sum float64
	zeros := 0
	for _, val := range &h.registers {
		sum += math.Pow(2.0, -float64(val))
		if val == 0 {
			zeros++
		}
	}

	m := float64(h.m)
	alpha := 0.7213 / (1.0 + 1.079/m)
	estimate := alpha * m * m / sum

	// Small range correction.
	if estimate <= 2.5*m && zeros > 0 {
		estimate = m * math.Log(m/float64(zeros))
	}

	return int64(estimate + 0.5)
}

// Merge merges another HLL into this one.
func (h *HyperLogLog) Merge(other *HyperLogLog) {
	for i := range h.registers {
		if other.registers[i] > h.registers[i] {
			h.registers[i] = other.registers[i]
		}
	}
}

// MarshalBinary serializes the HLL registers to a byte slice for spill persistence.
// Layout: [precision:1][registers:m].
func (h *HyperLogLog) MarshalBinary() []byte {
	buf := make([]byte, 1+len(h.registers))
	buf[0] = h.precision
	copy(buf[1:], h.registers[:])

	return buf
}

// UnmarshalHyperLogLog deserializes an HLL from bytes produced by MarshalBinary.
// Returns nil if data is too short or has unexpected precision.
func UnmarshalHyperLogLog(data []byte) *HyperLogLog {
	if len(data) < 2 {
		return nil
	}
	prec := data[0]
	m := uint32(1) << prec
	if uint32(len(data)-1) != m {
		return nil
	}
	h := &HyperLogLog{
		precision: prec,
		m:         m,
	}
	copy(h.registers[:], data[1:])

	return h
}
