import { StreamLanguage, StringStream } from "@codemirror/language";
import { tags, Tag } from "@lezer/highlight";
import { CLAUSES, COMMANDS, FUNCTIONS, OPERATORS } from "./lynxflow-catalog";

const COMMAND_SET = new Set(COMMANDS);
const CLAUSE_SET = new Set(CLAUSES);
const FUNCTION_SET = new Set(FUNCTIONS);
const OPERATOR_SET = new Set(OPERATORS.map((op) => op.toLowerCase()));
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

    // Comments: --
    if (stream.match("--")) {
      stream.skipToEnd();
      return "comment";
    }

    // Multi-char operators first
    if (
      stream.match(">=") ||
      stream.match("<=") ||
      stream.match("!=") ||
      stream.match("==") ||
      stream.match("=~") ||
      stream.match("!~") ||
      stream.match("??") ||
      stream.match("?.") ||
      stream.match("..")
    ) {
      return "operator";
    }

    // Single-char operators
    if (
      stream.match("=") ||
      stream.match(">") ||
      stream.match("<") ||
      stream.match("+") ||
      stream.match("-") ||
      stream.match("/") ||
      stream.match("%") ||
      stream.match("@")
    ) {
      return "operator";
    }

    // Pipe
    if (stream.eat("|")) {
      return "punctuation";
    }

    // F-strings
    if (stream.match("f\"")) {
      while (!stream.eol()) {
        const ch = stream.next();
        if (ch === "\\") {
          stream.next();
        } else if (ch === '"') {
          break;
        }
      }
      return "string";
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

    // Single-quoted identifiers
    if (stream.eat("'")) {
      while (!stream.eol()) {
        const ch = stream.next();
        if (ch === "\\") {
          stream.next();
        } else if (ch === "'") {
          break;
        }
      }
      return "variableName";
    }

    // Relative durations and numbers
    if (stream.match(/^[+-]?\d+[smhdwMy](@[smhdwMy]|@w[0-6])?/)) {
      return "number";
    }
    if (stream.match(/^-?\d+(\.\d+)?([eE][+-]?\d+)?/)) {
      return "number";
    }

    // Words: identifiers, keywords, functions, etc.
    if (stream.match(/^[a-zA-Z_][a-zA-Z0-9_.:-]*/)) {
      const word = stream.current();
      const lower = word.toLowerCase();

      if (OPERATOR_SET.has(lower)) return "operator";
      if (BOOLEANS.has(lower)) return "atom";
      if (COMMAND_SET.has(lower)) return "keyword";
      if (CLAUSE_SET.has(lower)) return "definitionKeyword";
      if (FUNCTION_SET.has(lower)) return "function(variableName)";

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
