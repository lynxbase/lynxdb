import { StreamLanguage, StringStream } from "@codemirror/language";
import { tags, Tag } from "@lezer/highlight";

const COMMANDS = new Set([
  "from", "search", "where", "group", "order", "take", "let", "parse",
  "keep", "omit", "rename", "dedup", "join", "append", "fillnull", "table",
  "top", "bottom", "rare", "sort", "head", "tail", "explode", "pack",
  "materialize", "every", "bucket", "running", "enrich", "rank", "select",
  "stats", "eval", "rex", "bin", "timechart", "streamstats", "eventstats",
  "transaction", "xyseries", "multisearch", "fields", "limit",
]);

const CLAUSES = new Set([
  "by", "as", "compute", "using", "extract", "if_missing",
  "desc", "asc", "span", "inner", "outer", "left", "right",
]);

const FUNCTIONS = new Set([
  "count", "sum", "avg", "min", "max", "dc", "values", "stdev",
  "perc50", "perc75", "perc90", "perc95", "perc99", "earliest", "latest",
  "if", "case", "match", "coalesce", "tonumber", "tostring", "round",
  "substr", "lower", "upper", "len", "ln", "isnotnull", "isnull",
  "strftime", "mvjoin", "mvappend", "mvdedup",
]);

const OPERATORS = new Set(["AND", "OR", "NOT", "IN", "LIKE"]);
const BOOLEANS = new Set(["true", "false"]);

/**
 * Custom tag for clause keywords (secondary keywords like "by", "as").
 * Mapped as a sub-tag of keyword so highlight styles can target it.
 */
export const clauseTag = Tag.define(tags.keyword);

export const lynxflowLanguage = StreamLanguage.define({
  token(stream: StringStream): string | null {
    // Skip whitespace
    if (stream.eatSpace()) return null;

    // Comments: -- or //
    if (stream.match("--") || stream.match("//")) {
      stream.skipToEnd();
      return "comment";
    }

    // Multi-char operators first
    if (stream.match(">=") || stream.match("<=") || stream.match("!=")) {
      return "operator";
    }

    // Single-char operators
    if (stream.match("=") || stream.match(">") || stream.match("<")) {
      return "operator";
    }

    // Pipe
    if (stream.eat("|")) {
      return "punctuation";
    }

    // Double-quoted strings
    if (stream.eat('"')) {
      while (!stream.eol()) {
        const ch = stream.next();
        if (ch === "\\") {
          stream.next(); // skip escaped char
        } else if (ch === '"') {
          break;
        }
      }
      return "string";
    }

    // Single-quoted strings
    if (stream.eat("'")) {
      while (!stream.eol()) {
        const ch = stream.next();
        if (ch === "\\") {
          stream.next();
        } else if (ch === "'") {
          break;
        }
      }
      return "string";
    }

    // Numbers
    if (stream.match(/^-?\d+(\.\d+)?/)) {
      return "number";
    }

    // Words: identifiers, keywords, functions, etc.
    if (stream.match(/^[a-zA-Z_]\w*/)) {
      const word = stream.current();
      const lower = word.toLowerCase();

      if (OPERATORS.has(word)) return "operator";
      if (BOOLEANS.has(lower)) return "atom";
      if (COMMANDS.has(lower)) return "keyword";
      if (CLAUSES.has(lower)) return "definitionKeyword";
      if (FUNCTIONS.has(lower)) return "function(variableName)";

      return "variableName";
    }

    // Parentheses, commas, etc.
    if (stream.eat("(") || stream.eat(")") || stream.eat(",")) {
      return "punctuation";
    }

    // Wildcards and other chars
    if (stream.eat("*")) {
      return "variableName";
    }

    // Consume any other character
    stream.next();
    return null;
  },
});
