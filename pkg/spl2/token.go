package spl2

import "fmt"

// TokenType represents the type of a lexer token.
type TokenType int

const (
	TokenEOF TokenType = iota
	TokenPipe
	TokenComma
	TokenLParen
	TokenRParen
	TokenLBracket
	TokenRBracket
	TokenSemicolon
	TokenDollar
	TokenEq
	TokenNeq
	TokenLt
	TokenLte
	TokenGt
	TokenGte
	TokenStar
	TokenPlus
	TokenMinus
	TokenSlash

	// Literals.
	TokenIdent  // identifier (may contain dots, hyphens, underscores)
	TokenString // "quoted string"
	TokenNumber // integer or float
	TokenGlob   // wildcard pattern like web-*

	// Keywords.
	TokenFrom
	TokenWhere
	TokenSearch
	TokenStats
	TokenEval
	TokenSort
	TokenHead
	TokenTail
	TokenTimechart
	TokenRex
	TokenFields
	TokenTable
	TokenDedup
	TokenRename
	TokenBin
	TokenStreamstats
	TokenEventstats
	TokenJoin
	TokenAppend
	TokenMultisearch
	TokenTransaction
	TokenXyseries
	TokenTop
	TokenRare
	TokenFillnull
	TokenBy
	TokenAs
	TokenAnd
	TokenOr
	TokenNot
	TokenIn
	TokenSpan
	TokenTrue
	TokenFalse
	TokenLike
	TokenTypeKeyword // "type" used in JOIN type=inner
	TokenWindow
	TokenCurrent
	TokenMaxspan
	TokenStartswith
	TokenEndswith
	TokenMaterialize
	TokenViews
	TokenDropview
)

var tokenNames = map[TokenType]string{
	TokenEOF:         "EOF",
	TokenPipe:        "PIPE",
	TokenComma:       "COMMA",
	TokenLParen:      "LPAREN",
	TokenRParen:      "RPAREN",
	TokenLBracket:    "LBRACKET",
	TokenRBracket:    "RBRACKET",
	TokenSemicolon:   "SEMICOLON",
	TokenDollar:      "DOLLAR",
	TokenEq:          "EQ",
	TokenNeq:         "NEQ",
	TokenLt:          "LT",
	TokenLte:         "LTE",
	TokenGt:          "GT",
	TokenGte:         "GTE",
	TokenStar:        "STAR",
	TokenPlus:        "PLUS",
	TokenMinus:       "MINUS",
	TokenSlash:       "SLASH",
	TokenIdent:       "IDENT",
	TokenString:      "STRING",
	TokenNumber:      "NUMBER",
	TokenGlob:        "GLOB",
	TokenFrom:        "FROM",
	TokenWhere:       "WHERE",
	TokenSearch:      "SEARCH",
	TokenStats:       "STATS",
	TokenEval:        "EVAL",
	TokenSort:        "SORT",
	TokenHead:        "HEAD",
	TokenTail:        "TAIL",
	TokenTimechart:   "TIMECHART",
	TokenRex:         "REX",
	TokenFields:      "FIELDS",
	TokenTable:       "TABLE",
	TokenDedup:       "DEDUP",
	TokenRename:      "RENAME",
	TokenBin:         "BIN",
	TokenStreamstats: "STREAMSTATS",
	TokenEventstats:  "EVENTSTATS",
	TokenJoin:        "JOIN",
	TokenAppend:      "APPEND",
	TokenMultisearch: "MULTISEARCH",
	TokenTransaction: "TRANSACTION",
	TokenXyseries:    "XYSERIES",
	TokenTop:         "TOP",
	TokenRare:        "RARE",
	TokenFillnull:    "FILLNULL",
	TokenBy:          "BY",
	TokenAs:          "AS",
	TokenAnd:         "AND",
	TokenOr:          "OR",
	TokenNot:         "NOT",
	TokenIn:          "IN",
	TokenSpan:        "SPAN",
	TokenTrue:        "TRUE",
	TokenFalse:       "FALSE",
	TokenLike:        "LIKE",
	TokenMaterialize: "MATERIALIZE",
	TokenViews:       "VIEWS",
	TokenDropview:    "DROPVIEW",
}

func (t TokenType) String() string {
	if s, ok := tokenNames[t]; ok {
		return s
	}

	return fmt.Sprintf("TOKEN(%d)", t)
}

// Token represents a single lexer token.
type Token struct {
	Type    TokenType
	Literal string
	Pos     int // byte offset in input
}

// String implements fmt.Stringer for debug/error output.
func (t Token) String() string {
	return fmt.Sprintf("{%s %q @%d}", t.Type, t.Literal, t.Pos)
}

var keywords = map[string]TokenType{
	"from":        TokenFrom,
	"where":       TokenWhere,
	"search":      TokenSearch,
	"stats":       TokenStats,
	"eval":        TokenEval,
	"sort":        TokenSort,
	"head":        TokenHead,
	"tail":        TokenTail,
	"timechart":   TokenTimechart,
	"rex":         TokenRex,
	"fields":      TokenFields,
	"table":       TokenTable,
	"dedup":       TokenDedup,
	"rename":      TokenRename,
	"bin":         TokenBin,
	"streamstats": TokenStreamstats,
	"eventstats":  TokenEventstats,
	"join":        TokenJoin,
	"append":      TokenAppend,
	"multisearch": TokenMultisearch,
	"transaction": TokenTransaction,
	"xyseries":    TokenXyseries,
	"top":         TokenTop,
	"rare":        TokenRare,
	"fillnull":    TokenFillnull,
	"by":          TokenBy,
	"as":          TokenAs,
	"and":         TokenAnd,
	"or":          TokenOr,
	"not":         TokenNot,
	"in":          TokenIn,
	"span":        TokenSpan,
	"true":        TokenTrue,
	"false":       TokenFalse,
	"like":        TokenLike,
	"materialize": TokenMaterialize,
	"views":       TokenViews,
	"dropview":    TokenDropview,
}

func lookupKeyword(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}

	return TokenIdent
}
