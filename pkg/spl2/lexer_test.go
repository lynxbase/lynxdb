package spl2

import (
	"testing"
)

func TestLexer_SimpleTokens(t *testing.T) {
	input := `| , ( ) = != < <= > >= *`
	lexer := NewLexer(input)
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatalf("Tokenize: %v", err)
	}

	expected := []TokenType{
		TokenPipe, TokenComma, TokenLParen, TokenRParen,
		TokenEq, TokenNeq, TokenLt, TokenLte, TokenGt, TokenGte, TokenStar,
		TokenEOF,
	}

	if len(tokens) != len(expected) {
		t.Fatalf("got %d tokens, want %d", len(tokens), len(expected))
	}

	for i, exp := range expected {
		if tokens[i].Type != exp {
			t.Errorf("token[%d]: got %s, want %s", i, tokens[i].Type, exp)
		}
	}
}

func TestLexer_Keywords(t *testing.T) {
	input := `FROM where SEARCH stats eval sort head tail timechart rex fields table dedup by as and or not`
	lexer := NewLexer(input)
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatalf("Tokenize: %v", err)
	}

	expected := []TokenType{
		TokenFrom, TokenWhere, TokenSearch, TokenStats, TokenEval,
		TokenSort, TokenHead, TokenTail, TokenTimechart, TokenRex,
		TokenFields, TokenTable, TokenDedup, TokenBy, TokenAs,
		TokenAnd, TokenOr, TokenNot, TokenEOF,
	}

	if len(tokens) != len(expected) {
		t.Fatalf("got %d tokens, want %d", len(tokens), len(expected))
	}

	for i, exp := range expected {
		if tokens[i].Type != exp {
			t.Errorf("token[%d]: got %s, want %s (literal=%q)", i, tokens[i].Type, exp, tokens[i].Literal)
		}
	}
}

func TestLexer_Strings(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{`"hello"`, "hello"},
		{`"hello world"`, "hello world"},
		{`"escaped \"quote\""`, `escaped "quote"`},
		{`"line\nnewline"`, "line\nnewline"},
		{`""`, ""},
	}

	for _, tt := range tests {
		lexer := NewLexer(tt.input)
		tokens, err := lexer.Tokenize()
		if err != nil {
			t.Errorf("Tokenize(%q): %v", tt.input, err)

			continue
		}
		if tokens[0].Type != TokenString {
			t.Errorf("expected STRING, got %s", tokens[0].Type)
		}
		if tokens[0].Literal != tt.expected {
			t.Errorf("Tokenize(%q): got %q, want %q", tt.input, tokens[0].Literal, tt.expected)
		}
	}
}

func TestLexer_Numbers(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"42", "42"},
		{"0", "0"},
		{"3.14", "3.14"},
		{"-1", "-1"},
		{"-0.5", "-0.5"},
		{"100", "100"},
	}

	for _, tt := range tests {
		lexer := NewLexer(tt.input)
		tokens, err := lexer.Tokenize()
		if err != nil {
			t.Errorf("Tokenize(%q): %v", tt.input, err)

			continue
		}
		if tokens[0].Type != TokenNumber {
			t.Errorf("expected NUMBER, got %s for %q", tokens[0].Type, tt.input)
		}
		if tokens[0].Literal != tt.expected {
			t.Errorf("got %q, want %q", tokens[0].Literal, tt.expected)
		}
	}
}

func TestLexer_Identifiers(t *testing.T) {
	tests := []struct {
		input   string
		tokType TokenType
		literal string
	}{
		{"host", TokenIdent, "host"},
		{"_raw", TokenIdent, "_raw"},
		{"response_time", TokenIdent, "response_time"},
		{"web-*", TokenGlob, "web-*"},
	}

	for _, tt := range tests {
		lexer := NewLexer(tt.input)
		tokens, err := lexer.Tokenize()
		if err != nil {
			t.Errorf("Tokenize(%q): %v", tt.input, err)

			continue
		}
		if tokens[0].Type != tt.tokType {
			t.Errorf("%q: got %s, want %s", tt.input, tokens[0].Type, tt.tokType)
		}
		if tokens[0].Literal != tt.literal {
			t.Errorf("%q: got %q, want %q", tt.input, tokens[0].Literal, tt.literal)
		}
	}
}

func TestLexer_FullQuery(t *testing.T) {
	input := `FROM main WHERE host="web-*" | search "error" | stats count() by host | sort -count | head 20`
	lexer := NewLexer(input)
	tokens, err := lexer.Tokenize()
	if err != nil {
		t.Fatalf("Tokenize: %v", err)
	}

	// Verify it tokenizes without error, ends with EOF, and produces a reasonable token count.
	if tokens[len(tokens)-1].Type != TokenEOF {
		t.Error("expected EOF at end")
	}
	if len(tokens) < 15 {
		t.Errorf("expected at least 15 tokens for full query, got %d", len(tokens))
	}
}

func TestLexer_UnterminatedString(t *testing.T) {
	lexer := NewLexer(`"unterminated`)
	_, err := lexer.Tokenize()
	if err == nil {
		t.Error("expected error for unterminated string")
	}
}
