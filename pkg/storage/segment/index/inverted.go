package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/vellum"
)

// InvertedIndex maps terms to posting lists (roaring bitmaps of event IDs).
// Built during segment creation and serialized for storage.
type InvertedIndex struct {
	// terms maps term -> roaring bitmap of event IDs.
	terms map[string]*roaring.Bitmap
}

// NewInvertedIndex creates an empty inverted index.
func NewInvertedIndex() *InvertedIndex {
	return &InvertedIndex{terms: make(map[string]*roaring.Bitmap)}
}

// Add indexes the tokens from the given text under the given event ID.
func (idx *InvertedIndex) Add(eventID uint32, text string) {
	tokens := TokenizeUnique(text)
	for _, token := range tokens {
		bm, ok := idx.terms[token]
		if !ok {
			bm = roaring.New()
			idx.terms[token] = bm
		}
		bm.Add(eventID)
	}
}

// AddField indexes a field:value pair under the given event ID.
// Uses composite key "field\x00value" for field-specific lookups.
func (idx *InvertedIndex) AddField(eventID uint32, field, value string) {
	key := field + "\x00" + value
	bm, ok := idx.terms[key]
	if !ok {
		bm = roaring.New()
		idx.terms[key] = bm
	}
	bm.Add(eventID)
}

// Search returns the event IDs matching the given term.
func (idx *InvertedIndex) Search(term string) *roaring.Bitmap {
	bm, ok := idx.terms[term]
	if !ok {
		return roaring.New()
	}

	return bm.Clone()
}

// Terms returns all indexed terms.
func (idx *InvertedIndex) Terms() []string {
	terms := make([]string, 0, len(idx.terms))
	for t := range idx.terms {
		terms = append(terms, t)
	}
	sort.Strings(terms)

	return terms
}

// Encode serializes the inverted index to bytes using FST for term lookup
// and roaring bitmaps for posting lists.
//
// Wire format:
//
//	[4B FST length] [FST data] [posting lists: 4B bitmap length + bitmap data ...]
//
// The FST maps each term to a uint64 offset into the posting list area.
func (idx *InvertedIndex) Encode() ([]byte, error) {
	terms := idx.Terms()
	if len(terms) == 0 {
		// Empty index: 4B zero FST length.
		return make([]byte, 4), nil
	}

	// Serialize all posting lists first to get offsets.
	var postingBuf bytes.Buffer
	offsets := make(map[string]uint64, len(terms))

	for _, term := range terms {
		bm := idx.terms[term]
		offsets[term] = uint64(postingBuf.Len())
		bmData, err := bm.ToBytes()
		if err != nil {
			return nil, fmt.Errorf("index: serialize bitmap for %q: %w", term, err)
		}
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(bmData)))
		postingBuf.Write(lenBuf[:])
		postingBuf.Write(bmData)
	}

	// Build FST: term -> offset into posting area.
	var fstBuf bytes.Buffer
	builder, err := vellum.New(&fstBuf, nil)
	if err != nil {
		return nil, fmt.Errorf("index: create FST builder: %w", err)
	}
	for _, term := range terms {
		if err := builder.Insert([]byte(term), offsets[term]); err != nil {
			return nil, fmt.Errorf("index: insert FST term %q: %w", term, err)
		}
	}
	if err := builder.Close(); err != nil {
		return nil, fmt.Errorf("index: close FST builder: %w", err)
	}

	// Assemble: [4B fst_len] [fst_data] [posting_data]
	fstData := fstBuf.Bytes()
	result := make([]byte, 0, 4+len(fstData)+postingBuf.Len())
	result = binary.LittleEndian.AppendUint32(result, uint32(len(fstData)))
	result = append(result, fstData...)
	result = append(result, postingBuf.Bytes()...)

	return result, nil
}

// SerializedIndex is a read-only inverted index decoded from bytes.
type SerializedIndex struct {
	fst         *vellum.FST
	postingData []byte
}

// DecodeInvertedIndex opens a serialized inverted index.
func DecodeInvertedIndex(data []byte) (*SerializedIndex, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("index: data too short")
	}

	fstLen := binary.LittleEndian.Uint32(data[0:4])
	if fstLen == 0 {
		return &SerializedIndex{}, nil
	}

	if 4+int(fstLen) > len(data) {
		return nil, fmt.Errorf("index: truncated FST data")
	}

	fstData := data[4 : 4+fstLen]
	fst, err := vellum.Load(fstData)
	if err != nil {
		return nil, fmt.Errorf("index: load FST: %w", err)
	}

	postingData := data[4+fstLen:]

	return &SerializedIndex{fst: fst, postingData: postingData}, nil
}

// Search returns event IDs matching the given term.
func (si *SerializedIndex) Search(term string) (*roaring.Bitmap, error) {
	if si.fst == nil {
		return roaring.New(), nil
	}

	offset, exists, err := si.fst.Get([]byte(term))
	if err != nil {
		return nil, fmt.Errorf("index: FST lookup %q: %w", term, err)
	}
	if !exists {
		return roaring.New(), nil
	}

	return si.readPosting(offset)
}

func (si *SerializedIndex) readPosting(offset uint64) (*roaring.Bitmap, error) {
	if int(offset)+4 > len(si.postingData) {
		return nil, fmt.Errorf("index: posting offset out of range")
	}
	bmLen := binary.LittleEndian.Uint32(si.postingData[offset : offset+4])
	start := offset + 4
	if int(start)+int(bmLen) > len(si.postingData) {
		return nil, fmt.Errorf("index: posting data truncated")
	}

	bm := roaring.New()
	if err := bm.UnmarshalBinary(si.postingData[start : start+uint64(bmLen)]); err != nil {
		return nil, fmt.Errorf("index: decode bitmap: %w", err)
	}

	return bm, nil
}

// SearchField returns event IDs matching field=value using composite key.
func (si *SerializedIndex) SearchField(field, value string) (*roaring.Bitmap, error) {
	key := field + "\x00" + value

	return si.Search(key)
}

// Contains returns true if the given term exists in the index.
func (si *SerializedIndex) Contains(term string) bool {
	if si.fst == nil {
		return false
	}
	_, exists, _ := si.fst.Get([]byte(term))

	return exists
}
