---
title: Adding functions
sidebar_label: Adding functions
description: How to add a scalar or aggregation function to LynxDB's IR registry and PPL catalogs.
---

# Adding functions

Functions live in two places: the language-independent IR registry, which
the engine implements, and the PPL catalogs, which map PPL names onto it.

## Scalar functions

1. **Register** the function in `pkg/ir/funcs_catalog.go` with
   `scalar(category, name, minArgs, params, result, doc)`. Parameters carry a
   type (`p`), constant-only flag (`pc`) or lambda arity (`pl`); the result
   rule (`Returns`, `SameAs`, `UnifyArgs`, `NumericResult`, ...) computes
   the static result type. Wrap the call to set the remaining properties:
   `aware` for functions that handle NULL arguments themselves (the default
   is strict: NULL in, NULL out), `variadic`, `volatile` (never folded, not
   row-local), `queryConst` (the same value for every row of one execution,
   such as `now`), and `errs` to document how invalid arguments are
   reported. The `Doc` sentence appears in the generated function table and
   in `GET /api/v1/catalog`.
2. **Implement** it in `pkg/vm/ir_funcs*.go`.
   `TestIRRegistryHasImplementations` fails until every registered scalar
   function has an implementation. Invalid values should produce NULL
   rather than an error.
3. **Map the PPL name** in `pkg/ppl/functions.go`: `direct(name, irName)`
   for a one-to-one mapping or an alias, `lowered(name, construct, f)` for a
   function compiled to IR operators, or `unsupportedFunc(reason, names...)`
   for a PPL function LynxDB does not execute. Functions with named
   arguments are registered like the relevance functions in
   `pkg/ppl/relevance.go`.
4. **Test** evaluation in `pkg/vm/ir_compiler_test.go` and, when the PPL
   mapping is not direct, add a compile golden under
   `pkg/ppl/testdata/compile`.
5. **Regenerate** the reference tables with `go run ./internal/docgen`.

Foldable functions (scalar, not volatile, not query-constant) are evaluated
at planning time when all arguments are literals, so they must be pure.

## Aggregation functions

1. Register the aggregate in `pkg/ir/funcs_catalog.go` with
   `aggregate(name, minArgs, params, result, merge, doc)`. The merge kind
   (`exact`, `approximate`, none) states whether partial results combine;
   mark order-dependent aggregates with `ordered`.
2. Map it to a pipeline aggregator in `pkg/engine/physical/operators.go`
   (`aggregateNames`) and implement the aggregator in `pkg/engine/pipeline`.
3. Add the PPL name to `aggregateCatalog` in `pkg/ppl/aggs.go`, with a note
   for any deviation from OpenSearch. Special argument handling (such as
   `percentile`'s constant percent) goes in `ectx.aggCall`.
4. Test it in `pkg/query/query_test.go` and regenerate the tables.

Window-only functions (`row_number`, `rank`, ...) are listed in
`windowOnlyFuncs` and fail with `UNSUPPORTED_FUNCTION`.
