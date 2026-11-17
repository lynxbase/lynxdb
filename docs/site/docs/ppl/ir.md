---
title: IR specification
sidebar_label: IR specification
description: The canonical intermediate representation between query languages and the LynxDB engine.
---

# IR specification

The canonical IR (`pkg/ir`) is the contract between query-language
frontends and the LynxDB engine. A plan is a tree of logical operators
(`ir.Node`); scalar computations are typed expressions (`ir.Expr`). The IR
names semantic database operations, not the syntax of any language, and it
does not depend on storage formats.

## Types

| IR type | Values |
| --- | --- |
| `any` | A value whose type is decided per row (schema-on-read columns). |
| `null` | The type of the NULL literal; unifies with every type. |
| `bool` | TRUE, FALSE. |
| `int` | 64-bit signed integer. |
| `float` | 64-bit IEEE 754 float. |
| `string` | UTF-8 text. |
| `timestamp` | An instant with nanosecond precision, UTC. |
| `duration` | A signed span of time; also used for times of day. |
| `array<T>` | Ordered list. |
| `object` | String-keyed map. |

`Unify` gives the common type of CASE branches, COALESCE arguments and
UNION columns: equal types stay, NULL yields the other type, int and float
widen to float, everything else becomes `any`. A float parameter accepts an
int argument; `any` and `null` arguments are accepted everywhere and checked
at run time.

## Schemas

Every operator exposes its output schema: columns with name, type,
nullability and optional metadata. A closed schema lists every column; an
open schema lists the columns known at planning time and allows further
columns of type `any`.

## NULL, missing and three-valued logic

- A reference to a column absent from a row evaluates to NULL.
  `IsMissing(col)` is TRUE only for an absent column; `is_null(x)` is TRUE
  for NULL or absent.
- Comparisons and arithmetic with a NULL operand yield NULL.
- AND, OR, XOR and NOT use Kleene logic.
- `Filter` keeps a row only when its condition is TRUE.
- `InList` is TRUE on a match, NULL when there is no match and the tested
  value or an element is NULL, FALSE otherwise.
- Integer division and modulo by zero yield NULL.

## Run-time conversions

Values of type `any` meet each other at run time. The binary operators
apply these rules:

- int with int is int; int with float is float.
- Adding two strings concatenates them.
- A string meeting a number in arithmetic or a comparison is converted to a
  number; it yields NULL when it does not parse.
- A string compared with a timestamp is converted to a timestamp; it yields
  NULL when it does not parse.
- timestamp - timestamp is a duration; timestamp ± duration is a timestamp.
- Other incompatible combinations yield NULL.

`Cast` converts explicitly; a value that cannot be converted yields NULL.
Casting a float to int truncates toward zero; casting a string to bool
accepts true and false in any case; casting a number to a timestamp reads
epoch seconds.

## Expressions

| Expression | Meaning |
| --- | --- |
| `Literal` | A constant. |
| `FieldRef` | A column; a dotted name reads the column with exactly that name, else walks object members. |
| `Unary`, `Binary` | Negation, NOT; arithmetic, comparisons, AND, OR, XOR. |
| `Call` | A registered scalar function. |
| `Case` | First branch whose condition is TRUE, else the ELSE value or NULL. |
| `Cast` | Explicit conversion. |
| `ArrayExpr`, `ObjectExpr`, `Index` | Construction and element access. |
| `Lambda`, `LambdaParam` | Anonymous functions, only as arguments of higher-order functions. |
| `InList` | Membership test. |
| `IsMissing` | Column absence. |
| `RelativeTime` | An instant relative to the query's reference time (`now-1d@d`). |
| `Search` | Index-aware text predicate; see below. |
| `Subquery` | Uncorrelated EXISTS, IN or scalar subquery. |
| `AggCall` | Aggregate or window function application, only in `Aggregate` and `Window`. |

### Search predicates

`Search{Mode, Field, Pattern, CaseSensitive}` keeps the distinctions storage
indexes care about. `Field` empty means the event's raw text.

| Mode | Matches | Index use |
| --- | --- | --- |
| word | every token of the pattern as a whole token | term index lookup |
| prefix | a token starting with the pattern | term-index range |
| glob | a token matching `*`/`?` wildcards | bounded term expansion |
| phrase | the pattern at token boundaries | term index on the phrase's tokens, verified per row |
| substring | the pattern anywhere | verified per row |
| regex | an RE2 expression | verified per row |

## Operators

| Operator | Semantics |
| --- | --- |
| `Scan` | Reads events of sources (names or `*` patterns) within an exact time range. Carries hints: pruning predicates, required columns, a row limit, descending order. Hints never change results. |
| `Values` | Literal rows. |
| `Empty` | No rows. |
| `SourceSchema` | One row per column of a source. |
| `Filter` | Keeps rows whose condition is TRUE. |
| `Project` | Keeps (in listed order) or drops columns; names may contain `*`. |
| `RenameColumns` | Renames columns, with `*` captures. |
| `Compute` | Adds or replaces columns; assignments see earlier ones. |
| `Aggregate` | Groups by expressions and computes aggregates; output is groups then aggregates; one row without groups. `DropNullGroups` drops rows with NULL group values. `Phase` splits partial and final aggregation. |
| `Window` | Aggregates per row over the whole partition or a running frame (row bound, current-row exclusion). |
| `Sort`, `TopN`, `Limit`, `Reverse` | Ordering with explicit NULL placement; bounded heap top-N; offset and count; reversal. |
| `Dedup` | First N rows per key, optional NULL keys, optional consecutive mode. |
| `Join` | inner, left, right, full, semi, anti, cross; equality keys plus residual condition; per-row match cap; collision rule; optional qualified copies of each side's columns. |
| `Union` | Concatenation by column name, or newest-first merge. |
| `ZipColumns` | Positional pairing of two inputs. |
| `Extract` | Regex captures or JSON members from a string expression. |
| `Patterns` | Log pattern labeling or aggregation. |
| `Unnest` | One row per array element. |
| `FlattenObject` | Object members as columns. |
| `FillNull` | NULL replacement per column or for all columns. |
| `Transpose`, `Totals`, `CombineValues` | Reshaping operators. |

## Function registry

`ir.Funcs()` lists every function with its kind (scalar, aggregate,
window), category, parameters (type, constant-only, lambda arity), result
rule, NULL behaviour (strict, aware, skipped), volatility (volatile, query
constant), merge kind for partial aggregation (exact, approximate, none),
order sensitivity and documentation. `Foldable` functions with constant
arguments are folded at planning time; `RowLocal` functions may run at scan
time. Every scalar function has a VM implementation.

## Validation

`ir.Validate` checks operator shapes, column resolution against closed
input schemas, function kinds, arity and static argument types, constant
arguments, aggregate placement, lambda scoping and subquery shapes.

## Optimizer

`pkg/ir/opt` applies, to a fixed point:

- constant folding of deterministic calls, through an evaluator the caller
  supplies;
- merging adjacent filters and removing `TRUE` filters;
- copying indexable conjuncts (raw-text searches, column equals string,
  disjunctions of words) into `Scan.Prune`, keeping the filter;
- narrowing `Scan.Time` with constant comparisons of the time column;
- `Sort` + `Limit` into `TopN`;
- pushing a row bound into `Scan.Limit` through `Compute`, `Project` and
  `RenameColumns`;
- recording the required columns on each scan.

See [IR optimizer](./optimizer.md) for each rule.

## Storage hints

`physical.PlanHints` (`pkg/engine/physical/hints.go`) converts the scans of
an optimized plan into the storage engine's hints: sources, the resolved
time range, search terms and token globs from raw-text `Search` pruning
predicates, a term tree for disjunctions of words, field predicates for
columns equal to strings, required columns, the row limit and the scan
order. A plan with one scan gets all of its hints; a plan with several scans
gets only the union of their sources and time ranges. The server's planner
(`pkg/planner`) attaches these hints to every query, and the storage engine
uses them to skip segments and select rows; the operators above the scan
still evaluate every predicate.
