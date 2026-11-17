// Package ir defines LynxDB's canonical intermediate representation: the
// typed, language-independent contract between query-language frontends and
// the optimizer, physical planner and execution engine.
//
// A frontend parses its own syntax, resolves names and types, and emits a
// Plan: a tree of logical operators (Node) whose scalar computations are
// typed expressions (Expr) referring to functions from the central registry
// (Funcs). Nothing in this package knows about any query language's syntax,
// keywords or parser nodes, and nothing in it knows about storage formats.
//
// Value model. Every expression has a static Type. Columns of schema-on-read
// sources whose type is not known before execution have type Any, and
// operations on Any values follow the runtime coercion rules documented on
// each function. Every value may be NULL. A reference to a column that is
// absent from a row evaluates to NULL; the IsMissing predicate is the only
// construct that observes absence directly.
//
// Three-valued logic. Comparisons and boolean operators follow SQL
// three-valued logic: a comparison with a NULL operand is NULL, AND/OR/NOT
// use Kleene truth tables, and a Filter keeps a row only when its condition
// is TRUE.
package ir
