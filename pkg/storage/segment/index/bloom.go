package index

import (
	"github.com/bits-and-blooms/bloom/v3"
)

// BloomFilter wraps a bloom filter for fast "term definitely not present" checks.
type BloomFilter struct {
	filter *bloom.BloomFilter
}

// NewBloomFilter creates a bloom filter sized for n expected items with the given
// false positive rate (e.g., 0.01 for 1%).
func NewBloomFilter(expectedItems uint, fpRate float64) *BloomFilter {
	return &BloomFilter{
		filter: bloom.NewWithEstimates(expectedItems, fpRate),
	}
}

// Add adds a term to the bloom filter.
func (bf *BloomFilter) Add(term string) {
	bf.filter.AddString(term)
}

// AddTokens tokenizes text and adds all tokens to the bloom filter.
func (bf *BloomFilter) AddTokens(text string) {
	for _, token := range TokenizeUnique(text) {
		bf.filter.AddString(token)
	}
}

// MayContain returns true if the term might be present (can have false positives),
// or false if the term is definitely not present.
func (bf *BloomFilter) MayContain(term string) bool {
	return bf.filter.TestString(term)
}

// MayContainAll returns true if all terms might be present.
// Returns false (with early exit) if any term is definitely absent.
func (bf *BloomFilter) MayContainAll(terms []string) bool {
	for _, t := range terms {
		if !bf.filter.TestString(t) {
			return false
		}
	}

	return true
}

// Union merges another bloom filter into this one (bitwise OR).
// After Union, MayContain returns true if either filter may contain the term.
// Returns an error if the filters are incompatible (different sizes or hash counts).
func (bf *BloomFilter) Union(other *BloomFilter) error {
	return bf.filter.Merge(other.filter)
}

// BitCount returns the size of the bloom filter's bit array.
func (bf *BloomFilter) BitCount() uint {
	return bf.filter.Cap()
}

// HashCount returns the number of hash functions used by the bloom filter.
func (bf *BloomFilter) HashCount() uint {
	return bf.filter.K()
}

// Encode serializes the bloom filter to bytes.
func (bf *BloomFilter) Encode() ([]byte, error) {
	return bf.filter.MarshalBinary()
}

// DecodeBloomFilter deserializes a bloom filter from bytes.
func DecodeBloomFilter(data []byte) (*BloomFilter, error) {
	f := &bloom.BloomFilter{}
	if err := f.UnmarshalBinary(data); err != nil {
		return nil, err
	}

	return &BloomFilter{filter: f}, nil
}
