package pipeline

import (
	"context"
	"sort"
	"strings"
	"unicode"

	"github.com/lynxbase/lynxdb/pkg/event"
)

const (
	defaultDrainDepth     = 4
	defaultMaxTemplates   = 50
	defaultDrainBatchSize = 1024
)

// PatternsIterator is a blocking operator that extracts log templates using
// the Drain algorithm. It accumulates all rows, tokenizes the source field,
// clusters logs into templates, and emits pattern, count, percent, example.
type PatternsIterator struct {
	child        Iterator
	field        string
	maxTemplates int
	similarity   float64

	// Accumulation.
	totalCount int
	done       bool

	// Drain tree.
	tree *drainTree

	// Emission.
	output *Batch
	offset int
}

// NewPatternsIterator creates a new patterns iterator.
func NewPatternsIterator(child Iterator, field string, maxTemplates int, similarity float64) *PatternsIterator {
	if field == "" {
		field = "_raw"
	}
	if maxTemplates <= 0 {
		maxTemplates = defaultMaxTemplates
	}
	if similarity <= 0 || similarity > 1 {
		similarity = 0.4
	}

	return &PatternsIterator{
		child:        child,
		field:        field,
		maxTemplates: maxTemplates,
		similarity:   similarity,
		tree:         newDrainTree(defaultDrainDepth, similarity),
	}
}

func (p *PatternsIterator) Init(ctx context.Context) error {
	return p.child.Init(ctx)
}

func (p *PatternsIterator) Next(ctx context.Context) (*Batch, error) {
	// Accumulate and cluster all rows.
	if !p.done {
		p.done = true

		for {
			batch, err := p.child.Next(ctx)
			if err != nil {
				return nil, err
			}
			if batch == nil {
				break
			}
			for i := 0; i < batch.Len; i++ {
				val := batch.Value(p.field, i)
				line := val.AsString()
				if line != "" {
					p.tree.insert(line)
					p.totalCount++
				}
			}
		}

		// Collect templates sorted by count descending.
		p.output = p.buildOutput()
	}

	// Emit results.
	if p.output == nil || p.offset >= p.output.Len {
		return nil, nil
	}

	end := p.offset + defaultDrainBatchSize
	if end > p.output.Len {
		end = p.output.Len
	}

	result := p.output.Slice(p.offset, end)
	p.offset = end

	return result, nil
}

func (p *PatternsIterator) Close() error {
	return p.child.Close()
}

func (p *PatternsIterator) Schema() []FieldInfo {
	return []FieldInfo{
		{Name: "pattern", Type: "string"},
		{Name: "count", Type: "int"},
		{Name: "percent", Type: "float"},
		{Name: "example", Type: "string"},
	}
}

// buildOutput creates the output batch from collected templates.
func (p *PatternsIterator) buildOutput() *Batch {
	templates := p.tree.allTemplates()
	if len(templates) == 0 {
		return NewBatch(0)
	}

	// Sort by count descending.
	sort.Slice(templates, func(i, j int) bool {
		return templates[i].Count > templates[j].Count
	})

	// Truncate to maxTemplates.
	if len(templates) > p.maxTemplates {
		templates = templates[:p.maxTemplates]
	}

	b := NewBatch(len(templates))
	for _, tmpl := range templates {
		var percent float64
		if p.totalCount > 0 {
			percent = float64(tmpl.Count) / float64(p.totalCount) * 100
		}
		b.AddRow(map[string]event.Value{
			"pattern": event.StringValue(tmpl.Pattern),
			"count":   event.IntValue(tmpl.Count),
			"percent": event.FloatValue(percent),
			"example": event.StringValue(tmpl.Example),
		})
	}

	return b
}

// drainTree is a fixed-depth prefix tree for log template clustering.
type drainTree struct {
	depth      int
	root       *drainNode
	templates  []*logTemplate
	similarity float64
}

type drainNode struct {
	children    map[string]*drainNode
	templateIDs []int // indices into tree.templates
}

type logTemplate struct {
	ID      int
	Tokens  []string
	Pattern string
	Count   int64
	Example string
}

func newDrainTree(depth int, similarity float64) *drainTree {
	return &drainTree{
		depth:      depth,
		root:       &drainNode{children: make(map[string]*drainNode)},
		similarity: similarity,
	}
}

func (t *drainTree) allTemplates() []*logTemplate {
	return t.templates
}

// insert tokenizes a log line and inserts it into the Drain tree.
func (t *drainTree) insert(line string) {
	tokens := drainTokenize(line)
	if len(tokens) == 0 {
		return
	}

	// Walk the tree depth-first.
	node := t.root
	depth := 0
	for _, token := range tokens {
		if depth >= t.depth {
			break
		}
		child, ok := node.children[token]
		if !ok {
			// Try wildcard branch.
			child, ok = node.children["<*>"]
			if !ok {
				child = &drainNode{children: make(map[string]*drainNode)}
				node.children[token] = child
			}
		}
		node = child
		depth++
	}

	// At leaf depth, check existing templates for exact match.
	for _, tmplID := range node.templateIDs {
		tmpl := t.templates[tmplID]
		if templateMatches(tmpl.Tokens, tokens) {
			tmpl.Count++
			return
		}
	}

	// No exact match — try similarity threshold.
	bestSim := 0.0
	bestID := -1
	for _, tmplID := range node.templateIDs {
		tmpl := t.templates[tmplID]
		sim := templateSimilarity(tmpl.Tokens, tokens)
		if sim > bestSim {
			bestSim = sim
			bestID = tmplID
		}
	}
	if bestSim >= t.similarity && bestID >= 0 {
		tmpl := t.templates[bestID]
		tmpl.Tokens = mergeTokens(tmpl.Tokens, tokens)
		tmpl.Pattern = buildPattern(tmpl.Tokens)
		tmpl.Count++
		return
	}

	// No match — create new template.
	newTmpl := &logTemplate{
		ID:      len(t.templates),
		Tokens:  tokens,
		Pattern: buildPattern(tokens),
		Count:   1,
		Example: line,
	}
	t.templates = append(t.templates, newTmpl)
	node.templateIDs = append(node.templateIDs, newTmpl.ID)
}

// drainTokenize splits a log line into tokens, classifying variable tokens as <*>.
func drainTokenize(line string) []string {
	fields := strings.Fields(line)
	tokens := make([]string, 0, len(fields))

	for _, field := range fields {
		if isVariableToken(field) {
			tokens = append(tokens, "<*>")
		} else {
			tokens = append(tokens, field)
		}
	}

	return tokens
}

// isVariableToken returns true if a token should be treated as a variable.
func isVariableToken(token string) bool {
	if len(token) == 0 {
		return false
	}

	// Pure numeric (123, 45.67, -3.14).
	if isNumeric(token) {
		return true
	}

	// Hex (0x1a2b).
	if len(token) > 2 && token[0] == '0' && (token[1] == 'x' || token[1] == 'X') {
		return true
	}

	// UUID pattern (8-4-4-4-12).
	if len(token) == 36 && token[8] == '-' && token[13] == '-' && token[18] == '-' && token[23] == '-' {
		return true
	}

	// IP address pattern.
	if isIPAddress(token) {
		return true
	}

	return false
}

// isNumeric checks if a string is a number (integer or float, optionally negative).
func isNumeric(s string) bool {
	if len(s) == 0 {
		return false
	}
	start := 0
	if s[0] == '-' {
		start = 1
	}
	if start >= len(s) {
		return false
	}
	hasDigit := false
	hasDot := false
	for i := start; i < len(s); i++ {
		if s[i] >= '0' && s[i] <= '9' {
			hasDigit = true
		} else if s[i] == '.' && !hasDot {
			hasDot = true
		} else {
			return false
		}
	}

	return hasDigit
}

// isIPAddress checks if a string looks like an IPv4 address.
func isIPAddress(s string) bool {
	parts := strings.Split(s, ".")
	if len(parts) != 4 {
		return false
	}
	for _, part := range parts {
		if len(part) == 0 || len(part) > 3 {
			return false
		}
		for _, c := range part {
			if !unicode.IsDigit(c) {
				return false
			}
		}
	}

	return true
}

// templateMatches checks if a template pattern matches a tokenized line.
func templateMatches(templateTokens, lineTokens []string) bool {
	if len(templateTokens) != len(lineTokens) {
		return false
	}
	for i, tt := range templateTokens {
		if tt == "<*>" {
			continue // wildcard matches anything
		}
		if tt != lineTokens[i] {
			return false
		}
	}

	return true
}

// buildPattern joins tokens into a display pattern string.
func buildPattern(tokens []string) string {
	return strings.Join(tokens, " ")
}

// templateSimilarity computes the ratio of matching positions between two
// token slices. Positions where either token is "<*>" or both tokens are
// equal count as matches. Returns 0 if lengths differ.
func templateSimilarity(templateTokens, lineTokens []string) float64 {
	if len(templateTokens) != len(lineTokens) {
		return 0
	}
	if len(templateTokens) == 0 {
		return 1
	}
	matches := 0
	for i := range templateTokens {
		if templateTokens[i] == "<*>" || lineTokens[i] == "<*>" || templateTokens[i] == lineTokens[i] {
			matches++
		}
	}

	return float64(matches) / float64(len(templateTokens))
}

// mergeTokens merges two token slices by replacing mismatched positions
// with "<*>". Both slices must have equal length.
func mergeTokens(a, b []string) []string {
	merged := make([]string, len(a))
	for i := range a {
		if a[i] == b[i] {
			merged[i] = a[i]
		} else if a[i] == "<*>" || b[i] == "<*>" {
			merged[i] = "<*>"
		} else {
			merged[i] = "<*>"
		}
	}

	return merged
}
