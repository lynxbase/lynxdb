package spl2

import (
	"testing"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
)

// Glob Pattern Tests

func TestGlobToRegex(t *testing.T) {
	tests := []struct {
		pattern string
		input   string
		match   bool
	}{
		// Basic wildcards
		{"*-25-*", "0001580642-25-003049", true},
		{"*-25-*", "0001580642-24-003049", false},
		{"*-25-*", "-25-", true},
		{"*-25-*", "abc-25-xyz", true},

		// Prefix wildcard
		{"web*", "webserver", true},
		{"web*", "web01", true},
		{"web*", "web", true},
		{"web*", "myweb", false},

		// Suffix wildcard
		{"*.pdf", "report.pdf", true},
		{"*.pdf", "report.PDF", true}, // case-insensitive
		{"*.pdf", "report.txt", false},

		// Status code patterns
		{"4*", "400", true},
		{"4*", "404", true},
		{"4*", "4", true},
		{"4*", "500", false},
		{"4*", "304", false},

		// IP address pattern
		{"10.9.165.*", "10.9.165.1", true},
		{"10.9.165.*", "10.9.165.255", true},
		{"10.9.165.*", "10.9.166.1", false},

		// Multiple wildcards
		{"H*l*o", "Hello", true},
		{"H*l*o", "Halo", true},
		{"H*l*o", "HELLo", true}, // case-insensitive

		// Credit card pattern
		{"4232*1232", "4232001232", true},
		{"4232*1232", "4232999991232", true},
		{"4232*1232", "4232001233", false},

		// No wildcards — exact match
		{"error", "error", true},
		{"error", "Error", true}, // case-insensitive
		{"error", "errors", false},

		// Special regex characters in pattern
		{"file.txt", "file.txt", true},
		{"file.txt", "fileTtxt", false},
		{"(test)", "(test)", true},
		{"[data]", "[data]", true},
	}

	for _, tt := range tests {
		re := GlobToRegex(tt.pattern, true)
		got := re.MatchString(tt.input)
		if got != tt.match {
			t.Errorf("GlobToRegex(%q).Match(%q) = %v, want %v", tt.pattern, tt.input, got, tt.match)
		}
	}
}

// Search Expression Lexer Tests

func TestSearchLexer(t *testing.T) {
	tests := []struct {
		input  string
		expect []SearchTokenType
	}{
		{`error`, []SearchTokenType{STokWord, STokEOF}},
		{`"User Not Found"`, []SearchTokenType{STokQuoted, STokEOF}},
		{`host=localhost`, []SearchTokenType{STokWord, STokEq, STokWord, STokEOF}},
		{`status!=200`, []SearchTokenType{STokWord, STokNeq, STokWord, STokEOF}},
		{`status>=400`, []SearchTokenType{STokWord, STokGte, STokWord, STokEOF}},
		{`A AND B OR C`, []SearchTokenType{STokWord, STokAND, STokWord, STokOR, STokWord, STokEOF}},
		{`NOT error`, []SearchTokenType{STokNOT, STokWord, STokEOF}},
		{`status IN (400, 404)`, []SearchTokenType{STokWord, STokIN, STokLParen, STokWord, STokComma, STokWord, STokRParen, STokEOF}},
		{`CASE(Error)`, []SearchTokenType{STokCASE, STokEOF}},
		{`TERM(127.0.0.1)`, []SearchTokenType{STokTERM, STokEOF}},
		{`host=web*`, []SearchTokenType{STokWord, STokEq, STokWord, STokEOF}},
		{`"*-25-*"`, []SearchTokenType{STokQuoted, STokEOF}},
		{`src="10.9.165.*" OR dst="10.9.165.8"`, []SearchTokenType{
			STokWord, STokEq, STokQuoted, STokOR, STokWord, STokEq, STokQuoted, STokEOF,
		}},
	}

	for _, tt := range tests {
		lexer := NewSearchLexer(tt.input)
		tokens, err := lexer.Tokenize()
		if err != nil {
			t.Errorf("SearchLexer(%q): %v", tt.input, err)

			continue
		}

		if len(tokens) != len(tt.expect) {
			types := make([]SearchTokenType, len(tokens))
			for i, tok := range tokens {
				types[i] = tok.Type
			}
			t.Errorf("SearchLexer(%q): got %d tokens %v, want %d tokens %v",
				tt.input, len(tokens), types, len(tt.expect), tt.expect)

			continue
		}

		for i, tok := range tokens {
			if tok.Type != tt.expect[i] {
				t.Errorf("SearchLexer(%q): token[%d] = %s, want %s",
					tt.input, i, tok.Type, tt.expect[i])
			}
		}
	}
}

func TestSearchLexerCASEContent(t *testing.T) {
	lexer := NewSearchLexer(`CASE(Error)`)
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatal(err)
	}
	if tokens[0].Literal != "Error" {
		t.Errorf("CASE content = %q, want %q", tokens[0].Literal, "Error")
	}
}

func TestSearchLexerTERMContent(t *testing.T) {
	lexer := NewSearchLexer(`TERM(127.0.0.1)`)
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatal(err)
	}
	if tokens[0].Literal != "127.0.0.1" {
		t.Errorf("TERM content = %q, want %q", tokens[0].Literal, "127.0.0.1")
	}
}

// Search Expression Parser Tests

func TestSearchParserPrecedence(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		// OR binds tighter than AND
		{"A AND B OR C", "(A AND (B OR C))"},
		{"A B OR C", "(A AND (B OR C))"},

		// Explicit grouping overrides
		{"(A AND B) OR C", "((A AND B) OR C)"},
		{"(A B) OR C", "((A AND B) OR C)"},

		// NOT binds tightest
		{"NOT A AND B", "(NOT A AND B)"},
		{"NOT A OR B", "(NOT A OR B)"},

		// Complex
		{`host=localhost (UserNotFound OR sourcetype=webaccess)`,
			"(host=localhost AND (UserNotFound OR sourcetype=webaccess))"},

		// Field comparisons
		{`status=4* OR status=5*`, "(status=4* OR status=5*)"},

		// IN operator
		{`status IN (400, 403)`, "status IN (400, 403)"},

		// Mixed (OR is left-associative, so multiple ORs nest to the left)
		{`(code=10 OR code=29 OR code=43) host!="localhost" xqp>5`,
			"((((code=10 OR code=29) OR code=43) AND host!=localhost) AND xqp>5)"},
	}

	for _, tt := range tests {
		expr, err := ParseSearchExpression(tt.input)
		if err != nil {
			t.Errorf("ParseSearchExpression(%q): %v", tt.input, err)

			continue
		}
		got := expr.String()
		if got != tt.expected {
			t.Errorf("ParseSearchExpression(%q):\n  got  %s\n  want %s", tt.input, got, tt.expected)
		}
	}
}

func TestSearchParserKeyword(t *testing.T) {
	expr, err := ParseSearchExpression(`error`)
	if err != nil {
		t.Fatal(err)
	}
	kw, ok := expr.(*SearchKeywordExpr)
	if !ok {
		t.Fatalf("expected SearchKeywordExpr, got %T", expr)
	}
	if kw.Value != "error" || kw.HasWildcard || kw.CaseSensitive || kw.IsTermMatch {
		t.Errorf("unexpected keyword: %+v", kw)
	}
}

func TestSearchParserQuotedKeyword(t *testing.T) {
	expr, err := ParseSearchExpression(`"User Not Found"`)
	if err != nil {
		t.Fatal(err)
	}
	kw, ok := expr.(*SearchKeywordExpr)
	if !ok {
		t.Fatalf("expected SearchKeywordExpr, got %T", expr)
	}
	if kw.Value != "User Not Found" {
		t.Errorf("value = %q, want %q", kw.Value, "User Not Found")
	}
}

func TestSearchParserWildcardKeyword(t *testing.T) {
	expr, err := ParseSearchExpression(`"*-25-*"`)
	if err != nil {
		t.Fatal(err)
	}
	kw, ok := expr.(*SearchKeywordExpr)
	if !ok {
		t.Fatalf("expected SearchKeywordExpr, got %T", expr)
	}
	if !kw.HasWildcard {
		t.Error("expected HasWildcard=true")
	}
	if kw.Value != "*-25-*" {
		t.Errorf("Value: got %q, want %q", kw.Value, "*-25-*")
	}
}

func TestSearchParserFieldComparison(t *testing.T) {
	expr, err := ParseSearchExpression(`host=web*`)
	if err != nil {
		t.Fatal(err)
	}
	cmp, ok := expr.(*SearchCompareExpr)
	if !ok {
		t.Fatalf("expected SearchCompareExpr, got %T", expr)
	}
	if cmp.Field != "host" || cmp.Op != OpEq || cmp.Value != "web*" || !cmp.HasWildcard {
		t.Errorf("unexpected compare: %+v", cmp)
	}
}

func TestSearchParserIN(t *testing.T) {
	expr, err := ParseSearchExpression(`status IN (400, 404, 500)`)
	if err != nil {
		t.Fatal(err)
	}
	in, ok := expr.(*SearchInExpr)
	if !ok {
		t.Fatalf("expected SearchInExpr, got %T", expr)
	}
	if in.Field != "status" || len(in.Values) != 3 {
		t.Errorf("unexpected IN: %+v", in)
	}
	if in.Values[0].Value != "400" || in.Values[1].Value != "404" || in.Values[2].Value != "500" {
		t.Errorf("unexpected IN values: %+v", in.Values)
	}
}

func TestSearchParserINWithWildcards(t *testing.T) {
	expr, err := ParseSearchExpression(`status IN (4*, 5*)`)
	if err != nil {
		t.Fatal(err)
	}
	in, ok := expr.(*SearchInExpr)
	if !ok {
		t.Fatalf("expected SearchInExpr, got %T", expr)
	}
	if in.Field != "status" {
		t.Errorf("Field: got %q, want %q", in.Field, "status")
	}
	if len(in.Values) != 2 {
		t.Fatalf("Values count: got %d, want 2", len(in.Values))
	}
	if !in.Values[0].HasWildcard || !in.Values[1].HasWildcard {
		t.Error("expected wildcards in IN values")
	}
	if in.Values[0].Value != "4*" {
		t.Errorf("Values[0]: got %q, want %q", in.Values[0].Value, "4*")
	}
	if in.Values[1].Value != "5*" {
		t.Errorf("Values[1]: got %q, want %q", in.Values[1].Value, "5*")
	}
}

// Search Expression Evaluator Tests

func filterEvents(events []map[string]event.Value, eval *SearchEvaluator) []map[string]event.Value {
	var result []map[string]event.Value
	for _, row := range events {
		if eval.Evaluate(row) {
			result = append(result, row)
		}
	}

	return result
}

func TestSearchEvalKeyword(t *testing.T) {
	events := []map[string]event.Value{
		{"_raw": event.StringValue("ERROR occurred in module")},
		{"_raw": event.StringValue("Error occurred in module")},
		{"_raw": event.StringValue("info: all good")},
	}

	expr, _ := ParseSearchExpression(`error`)
	eval := NewSearchEvaluator(expr)
	results := filterEvents(events, eval)
	if len(results) != 2 {
		t.Errorf("got %d results, want 2", len(results))
	}
}

func TestSearchEvalGlobOnRaw(t *testing.T) {
	events := []map[string]event.Value{
		{"_raw": event.StringValue("filing-25-003049.htm")},
		{"_raw": event.StringValue("filing-24-003049.htm")},
		{"_raw": event.StringValue("report-25-data.txt")},
	}

	expr, _ := ParseSearchExpression(`"*-25-*"`)
	eval := NewSearchEvaluator(expr)
	results := filterEvents(events, eval)
	if len(results) != 2 {
		t.Errorf("got %d results, want 2", len(results))
	}
}

func TestSearchEvalFieldComparison(t *testing.T) {
	events := []map[string]event.Value{
		{"_raw": event.StringValue(""), "status": event.StringValue("200"), "host": event.StringValue("web01")},
		{"_raw": event.StringValue(""), "status": event.StringValue("404"), "host": event.StringValue("web02")},
		{"_raw": event.StringValue(""), "status": event.StringValue("500"), "host": event.StringValue("db01")},
		{"_raw": event.StringValue(""), "host": event.StringValue("web03")}, // no status field
	}

	// search status>=400
	expr, _ := ParseSearchExpression(`status>=400`)
	eval := NewSearchEvaluator(expr)
	results := filterEvents(events, eval)
	if len(results) != 2 {
		t.Errorf("status>=400: got %d results, want 2", len(results))
	}

	// search host=web*
	expr, _ = ParseSearchExpression(`host=web*`)
	eval = NewSearchEvaluator(expr)
	results = filterEvents(events, eval)
	if len(results) != 3 {
		t.Errorf("host=web*: got %d results, want 3", len(results))
	}

	// search status!=200
	expr, _ = ParseSearchExpression(`status!=200`)
	eval = NewSearchEvaluator(expr)
	results = filterEvents(events, eval)
	if len(results) != 2 {
		t.Errorf("status!=200: got %d results, want 2 (only events WHERE status EXISTS and != 200)", len(results))
	}

	// search NOT status=200
	expr, _ = ParseSearchExpression(`NOT status=200`)
	eval = NewSearchEvaluator(expr)
	results = filterEvents(events, eval)
	if len(results) != 3 {
		t.Errorf("NOT status=200: got %d results, want 3 (404, 500, AND missing status)", len(results))
	}
}

func TestSearchEvalINOperator(t *testing.T) {
	events := []map[string]event.Value{
		{"_raw": event.StringValue(""), "status": event.StringValue("200")},
		{"_raw": event.StringValue(""), "status": event.StringValue("400")},
		{"_raw": event.StringValue(""), "status": event.StringValue("404")},
		{"_raw": event.StringValue(""), "status": event.StringValue("500")},
	}

	// search status IN (400, 404, 500)
	expr, _ := ParseSearchExpression(`status IN (400, 404, 500)`)
	eval := NewSearchEvaluator(expr)
	results := filterEvents(events, eval)
	if len(results) != 3 {
		t.Errorf("status IN (400,404,500): got %d results, want 3", len(results))
	}

	// search status IN (4*, 5*)
	expr, _ = ParseSearchExpression(`status IN (4*, 5*)`)
	eval = NewSearchEvaluator(expr)
	results = filterEvents(events, eval)
	if len(results) != 3 {
		t.Errorf("status IN (4*,5*): got %d results, want 3", len(results))
	}
}

func TestSearchEvalNOTvsNotEqual(t *testing.T) {
	events := []map[string]event.Value{
		{"_raw": event.StringValue(""), "src": event.StringValue("10.0.0.1")},
		{"_raw": event.StringValue(""), "src": event.StringValue("127.0.0.1")},
		{"_raw": event.StringValue(""), "dst": event.StringValue("10.0.0.2")}, // no src field
	}

	// src!="127.0.0.1" — only events WITH src field that isn't 127.0.0.1
	expr, _ := ParseSearchExpression(`src!="127.0.0.1"`)
	eval := NewSearchEvaluator(expr)
	results := filterEvents(events, eval)
	if len(results) != 1 {
		t.Errorf("src!=\"127.0.0.1\": got %d results, want 1", len(results))
	}

	// NOT src="127.0.0.1" — events without src OR src != 127.0.0.1
	expr, _ = ParseSearchExpression(`NOT src="127.0.0.1"`)
	eval = NewSearchEvaluator(expr)
	results = filterEvents(events, eval)
	if len(results) != 2 {
		t.Errorf("NOT src=\"127.0.0.1\": got %d results, want 2", len(results))
	}
}

func TestSearchEvalCASEDirective(t *testing.T) {
	events := []map[string]event.Value{
		{"_raw": event.StringValue("Error occurred"), "host": event.StringValue("LOCALHOST")},
		{"_raw": event.StringValue("error occurred"), "host": event.StringValue("localhost")},
	}

	// search CASE(Error) — case-sensitive on _raw
	expr, _ := ParseSearchExpression(`CASE(Error)`)
	eval := NewSearchEvaluator(expr)
	results := filterEvents(events, eval)
	if len(results) != 1 {
		t.Errorf("CASE(Error): got %d results, want 1", len(results))
	}
	if results[0]["_raw"].AsString() != "Error occurred" {
		t.Errorf("wrong event matched")
	}

	// search host=CASE(LOCALHOST)
	expr, _ = ParseSearchExpression(`host=CASE(LOCALHOST)`)
	eval = NewSearchEvaluator(expr)
	results = filterEvents(events, eval)
	if len(results) != 1 {
		t.Errorf("host=CASE(LOCALHOST): got %d results, want 1", len(results))
	}
}

func TestSearchEvalTERMDirective(t *testing.T) {
	events := []map[string]event.Value{
		{"_raw": event.StringValue("_time=1549931073,127.0.0.1,test,foo,bar")},      // TERM match
		{"_raw": event.StringValue("_time=1549931073,host=127.0.0.1,test,foo,bar")}, // NO match
		{"_raw": event.StringValue("src 127.0.0.1 connected")},                      // TERM match
	}

	expr, _ := ParseSearchExpression(`TERM(127.0.0.1)`)
	eval := NewSearchEvaluator(expr)
	results := filterEvents(events, eval)
	if len(results) != 2 {
		t.Errorf("TERM(127.0.0.1): got %d results, want 2", len(results))
	}
}

func TestSearchEvalImpliedAND(t *testing.T) {
	events := []map[string]event.Value{
		{"_raw": event.StringValue("error warning critical")},
		{"_raw": event.StringValue("error info")},
		{"_raw": event.StringValue("warning info")},
	}

	// search error warning — implied AND
	expr, _ := ParseSearchExpression(`error warning`)
	eval := NewSearchEvaluator(expr)
	results := filterEvents(events, eval)
	if len(results) != 1 {
		t.Errorf("got %d results, want 1", len(results))
	}
}

func TestSearchEvalPrecedenceORbeforeAND(t *testing.T) {
	events := []map[string]event.Value{
		{"_raw": event.StringValue("A B C")},
		{"_raw": event.StringValue("A C")},
		{"_raw": event.StringValue("B C")},
		{"_raw": event.StringValue("A B")},
		{"_raw": event.StringValue("C")},
	}

	// search A B OR C → A AND (B OR C)
	expr, _ := ParseSearchExpression(`A B OR C`)
	eval := NewSearchEvaluator(expr)
	results := filterEvents(events, eval)
	if len(results) != 3 { // A B C, A C, A B
		t.Errorf("A B OR C: got %d results, want 3", len(results))
	}

	// search (A B) OR C → (A AND B) OR C
	expr, _ = ParseSearchExpression(`(A B) OR C`)
	eval = NewSearchEvaluator(expr)
	results = filterEvents(events, eval)
	if len(results) != 5 { // all match because all have A&B or C
		t.Errorf("(A B) OR C: got %d results, want 5", len(results))
	}
}

func TestSearchEvalCaseInsensitiveDefault(t *testing.T) {
	events := []map[string]event.Value{
		{"_raw": event.StringValue("ERROR occurred")},
		{"_raw": event.StringValue("Error occurred")},
		{"_raw": event.StringValue("error occurred")},
	}

	expr, _ := ParseSearchExpression(`error`)
	eval := NewSearchEvaluator(expr)
	results := filterEvents(events, eval)
	if len(results) != 3 {
		t.Errorf("got %d results, want 3 (case-insensitive)", len(results))
	}
}

func TestSearchEvalFieldExists(t *testing.T) {
	events := []map[string]event.Value{
		{"_raw": event.StringValue(""), "status": event.StringValue("200")},
		{"_raw": event.StringValue("")}, // no status
	}

	// search status=*  — field exists check
	expr, _ := ParseSearchExpression(`status=*`)
	eval := NewSearchEvaluator(expr)
	results := filterEvents(events, eval)
	if len(results) != 1 {
		t.Errorf("status=*: got %d results, want 1", len(results))
	}

	// search NOT status=*  — field absence check
	expr, _ = ParseSearchExpression(`NOT status=*`)
	eval = NewSearchEvaluator(expr)
	results = filterEvents(events, eval)
	if len(results) != 1 {
		t.Errorf("NOT status=*: got %d results, want 1", len(results))
	}
}

func TestSearchEvalComplexQuery(t *testing.T) {
	events := []map[string]event.Value{
		{"_raw": event.StringValue("GET /api"), "host": event.StringValue("webserver01"), "status": event.StringValue("200")},
		{"_raw": event.StringValue("GET /api"), "host": event.StringValue("webserver02"), "status": event.StringValue("404")},
		{"_raw": event.StringValue("POST /api"), "host": event.StringValue("webserver01"), "status": event.StringValue("500")},
		{"_raw": event.StringValue("GET /api"), "host": event.StringValue("dbserver01"), "status": event.StringValue("200")},
	}

	// search host=webserver* (status=4* OR status=5*)
	expr, _ := ParseSearchExpression(`host=webserver* (status=4* OR status=5*)`)
	eval := NewSearchEvaluator(expr)
	results := filterEvents(events, eval)
	if len(results) != 2 {
		t.Errorf("got %d results, want 2", len(results))
	}

	// search host=webserver* status IN(4*, 5*)
	expr, _ = ParseSearchExpression(`host=webserver* status IN (4*, 5*)`)
	eval = NewSearchEvaluator(expr)
	results = filterEvents(events, eval)
	if len(results) != 2 {
		t.Errorf("got %d results, want 2", len(results))
	}
}

func TestSearchEvalSplunkExample2(t *testing.T) {
	events := []map[string]event.Value{
		{"_raw": event.StringValue(""), "code": event.StringValue("10"), "host": event.StringValue("server1"), "xqp": event.StringValue("10")},
		{"_raw": event.StringValue(""), "code": event.StringValue("29"), "host": event.StringValue("localhost"), "xqp": event.StringValue("3")},
		{"_raw": event.StringValue(""), "code": event.StringValue("43"), "host": event.StringValue("server2"), "xqp": event.StringValue("8")},
		{"_raw": event.StringValue(""), "code": event.StringValue("50"), "host": event.StringValue("server3"), "xqp": event.StringValue("1")},
	}

	// search (code=10 OR code=29 OR code=43) host!="localhost" xqp>5
	expr, _ := ParseSearchExpression(`(code=10 OR code=29 OR code=43) host!="localhost" xqp>5`)
	eval := NewSearchEvaluator(expr)
	results := filterEvents(events, eval)
	if len(results) != 2 {
		t.Errorf("got %d results, want 2", len(results))
	}

	// Same with IN: search code IN(10, 29, 43) host!="localhost" xqp>5
	expr, _ = ParseSearchExpression(`code IN (10, 29, 43) host!="localhost" xqp>5`)
	eval = NewSearchEvaluator(expr)
	results = filterEvents(events, eval)
	if len(results) != 2 {
		t.Errorf("IN version: got %d results, want 2", len(results))
	}
}

func TestSearchEvalSplunkExample1(t *testing.T) {
	events := []map[string]event.Value{
		{"_raw": event.StringValue(""), "src": event.StringValue("10.9.165.5"), "dst": event.StringValue("192.168.1.1")},
		{"_raw": event.StringValue(""), "src": event.StringValue("10.9.165.10"), "dst": event.StringValue("10.9.165.8")},
		{"_raw": event.StringValue(""), "src": event.StringValue("192.168.1.1"), "dst": event.StringValue("10.9.165.8")},
		{"_raw": event.StringValue(""), "src": event.StringValue("172.16.0.1"), "dst": event.StringValue("172.16.0.2")},
	}

	// search src="10.9.165.*" OR dst="10.9.165.8"
	expr, _ := ParseSearchExpression(`src="10.9.165.*" OR dst="10.9.165.8"`)
	eval := NewSearchEvaluator(expr)
	results := filterEvents(events, eval)
	if len(results) != 3 {
		t.Errorf("got %d results, want 3", len(results))
	}
}

// Pipeline Integration Test

func TestSearchParseInPipeline(t *testing.T) {
	// Test that search commands parse correctly in full SPL2 queries.
	tests := []struct {
		query string
		ok    bool
	}{
		{`FROM idx_test | search "*-25-*"`, true},
		{`FROM idx_test | search host=web*`, true},
		{`FROM idx_test | search status=4* OR status=5*`, true},
		{`FROM idx_test | search NOT error`, true},
		{`FROM idx_test | search status IN (400, 404, 500)`, true},
		{`FROM idx_test | search src="10.9.165.*" OR dst="10.9.165.8"`, true},
		{`FROM idx_test | search host=webserver* (status=4* OR status=5*)`, true},
		{`FROM idx_test | search CASE(Error)`, true},
		{`FROM idx_test | search TERM(127.0.0.1)`, true},
		{`FROM idx_test | search (code=10 OR code=29 OR code=43) host!="localhost" xqp>5`, true},
		{`FROM idx_test | search error | stats count`, true},
		{`FROM idx_test | search host=web* status>=400 | head 10`, true},
	}

	for _, tt := range tests {
		_, err := Parse(tt.query)
		if (err == nil) != tt.ok {
			t.Errorf("Parse(%q): err=%v, want ok=%v", tt.query, err, tt.ok)
		}
	}
}

func TestSearchCommandHasExpression(t *testing.T) {
	q, err := Parse(`FROM idx_test | search host=web* status>=400`)
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Commands) != 1 {
		t.Fatalf("expected 1 command, got %d", len(q.Commands))
	}
	sc, ok := q.Commands[0].(*SearchCommand)
	if !ok {
		t.Fatalf("expected SearchCommand, got %T", q.Commands[0])
	}
	if sc.Expression == nil {
		t.Fatal("expected SearchCommand.Expression to be set")
	}
	// Verify the expression is an AND of two comparisons
	and, ok := sc.Expression.(*SearchAndExpr)
	if !ok {
		t.Fatalf("expected SearchAndExpr, got %T", sc.Expression)
	}
	if _, ok := and.Left.(*SearchCompareExpr); !ok {
		t.Errorf("left should be SearchCompareExpr, got %T", and.Left)
	}
	if _, ok := and.Right.(*SearchCompareExpr); !ok {
		t.Errorf("right should be SearchCompareExpr, got %T", and.Right)
	}
}

// TERM Matching Tests

func TestMatchTerm(t *testing.T) {
	tests := []struct {
		text  string
		term  string
		match bool
	}{
		// Bounded by commas (major breakers)
		{"_time=1549931073,127.0.0.1,test,foo,bar", "127.0.0.1", true},
		// "host=127.0.0.1" is the full term between major breakers
		{"_time=1549931073,host=127.0.0.1,test,foo,bar", "127.0.0.1", false},
		// Bounded by spaces
		{"src 127.0.0.1 connected", "127.0.0.1", true},
		// At start of string
		{"127.0.0.1 connected", "127.0.0.1", true},
		// At end of string
		{"connected to 127.0.0.1", "127.0.0.1", true},
		// Entire string
		{"127.0.0.1", "127.0.0.1", true},
		// Not present
		{"192.168.1.1", "127.0.0.1", false},
	}

	for _, tt := range tests {
		got := MatchTerm(tt.text, tt.term)
		if got != tt.match {
			t.Errorf("MatchTerm(%q, %q) = %v, want %v", tt.text, tt.term, got, tt.match)
		}
	}
}

// Hint Extraction Tests

func TestSearchHintExtraction(t *testing.T) {
	prog, err := ParseProgram(`FROM idx_test | search error warning`)
	if err != nil {
		t.Fatal(err)
	}
	hints := ExtractQueryHints(prog)
	if len(hints.SearchTerms) == 0 {
		t.Fatal("expected search terms to be extracted")
	}
	termSet := map[string]bool{}
	for _, term := range hints.SearchTerms {
		termSet[term] = true
	}
	if !termSet["error"] {
		t.Error("missing search term 'error'")
	}
	if !termSet["warning"] {
		t.Error("missing search term 'warning'")
	}
}

// Numeric Comparison Tests

func TestNumericOrLexCompare(t *testing.T) {
	tests := []struct {
		field, search string
		op            CompareOp
		expected      bool
	}{
		{"500", "400", OpGt, true},
		{"500", "400", OpGte, true},
		{"200", "400", OpLt, true},
		{"400", "400", OpGte, true},
		{"400", "400", OpLte, true},
		{"399", "400", OpGte, false},
		// Lexicographic when non-numeric
		{"beta", "alpha", OpGt, true},
		{"alpha", "beta", OpLt, true},
	}

	for _, tt := range tests {
		got := numericOrLexCompare(tt.field, tt.search, tt.op)
		if got != tt.expected {
			t.Errorf("numericOrLexCompare(%q, %q, %s) = %v, want %v",
				tt.field, tt.search, tt.op, got, tt.expected)
		}
	}
}

// Fast Path Tests

func TestExtractStarLiteralStar(t *testing.T) {
	if lit, ok := extractStarLiteralStar("*-25-*"); !ok || lit != "-25-" {
		t.Errorf("got %q, %v", lit, ok)
	}
	if _, ok := extractStarLiteralStar("*a*b*"); ok {
		t.Error("should not match pattern with inner *")
	}
	if _, ok := extractStarLiteralStar("hello"); ok {
		t.Error("should not match pattern without *")
	}
}

func TestExtractLiteralStar(t *testing.T) {
	if lit, ok := extractLiteralStar("web*"); !ok || lit != "web" {
		t.Errorf("got %q, %v", lit, ok)
	}
	if _, ok := extractLiteralStar("*web"); ok {
		t.Error("should not match *prefix")
	}
}

func TestExtractStarLiteral(t *testing.T) {
	if lit, ok := extractStarLiteral("*.pdf"); !ok || lit != ".pdf" {
		t.Errorf("got %q, %v", lit, ok)
	}
	if _, ok := extractStarLiteral("pdf*"); ok {
		t.Error("should not match suffix*")
	}
}

// ─── BUG-4: source alias in search evaluator ─────────────────────────────────

func TestSearchEval_SourceAliasInCompare(t *testing.T) {
	// Pipeline rows store "source" under "_source" (physical column name).
	// Search expressions use "source" (user-facing name).
	// The evaluator must resolve source → _source.
	row := map[string]event.Value{
		"_raw":    event.StringValue("test event"),
		"_source": event.StringValue("nginx"),
		"status":  event.IntValue(200),
	}

	// source=nginx should match via _source column.
	expr, err := ParseSearchExpression(`source=nginx`)
	if err != nil {
		t.Fatal(err)
	}
	eval := NewSearchEvaluator(expr)
	if !eval.Evaluate(row) {
		t.Error("source=nginx should match row with _source=nginx")
	}

	// source=postgres should NOT match.
	expr2, err := ParseSearchExpression(`source=postgres`)
	if err != nil {
		t.Fatal(err)
	}
	eval2 := NewSearchEvaluator(expr2)
	if eval2.Evaluate(row) {
		t.Error("source=postgres should not match row with _source=nginx")
	}
}

func TestSearchEval_SourceAliasInIn(t *testing.T) {
	row := map[string]event.Value{
		"_raw":    event.StringValue("test event"),
		"_source": event.StringValue("nginx"),
	}

	// source IN ("nginx", "api") should match via _source column.
	expr, err := ParseSearchExpression(`source IN ("nginx", "api")`)
	if err != nil {
		t.Fatal(err)
	}
	eval := NewSearchEvaluator(expr)
	if !eval.Evaluate(row) {
		t.Error("source IN (nginx, api) should match row with _source=nginx")
	}

	// source IN ("postgres", "redis") should NOT match.
	expr2, err := ParseSearchExpression(`source IN ("postgres", "redis")`)
	if err != nil {
		t.Fatal(err)
	}
	eval2 := NewSearchEvaluator(expr2)
	if eval2.Evaluate(row) {
		t.Error("source IN (postgres, redis) should not match row with _source=nginx")
	}
}

func TestSearchEval_IndexFieldNotAliased(t *testing.T) {
	// "index" is a real physical column — NOT aliased to _source.
	// The evaluator should look it up directly from the row map.
	row := map[string]event.Value{
		"_raw":    event.StringValue("test event"),
		"_source": event.StringValue("nginx-access"),
		"index":   event.StringValue("web-logs"),
	}

	// index=web-logs should match the "index" column directly.
	expr, err := ParseSearchExpression(`index=web-logs`)
	if err != nil {
		t.Fatal(err)
	}
	eval := NewSearchEvaluator(expr)
	if !eval.Evaluate(row) {
		t.Error("index=web-logs should match row with index=web-logs")
	}

	// index=nginx-access should NOT match (that's the _source value).
	expr2, err := ParseSearchExpression(`index=nginx-access`)
	if err != nil {
		t.Fatal(err)
	}
	eval2 := NewSearchEvaluator(expr2)
	if eval2.Evaluate(row) {
		t.Error("index=nginx-access should NOT match (index != _source)")
	}
}

func TestSearchEval_SourceGlobAlias(t *testing.T) {
	row := map[string]event.Value{
		"_raw":    event.StringValue("test event"),
		"_source": event.StringValue("nginx-access"),
	}

	// source=nginx* should match _source=nginx-access.
	expr, err := ParseSearchExpression(`source=nginx*`)
	if err != nil {
		t.Fatal(err)
	}
	eval := NewSearchEvaluator(expr)
	if !eval.Evaluate(row) {
		t.Error("source=nginx* should match _source=nginx-access")
	}
}
