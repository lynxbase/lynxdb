---
title: Adding commands
sidebar_label: Adding commands
description: How to add a PPL command or command option to LynxDB.
---

# Adding commands

A command touches the parser, the compiler and the catalog. It reaches the
engine through IR operators, so most commands need no engine change.

1. **Syntax.** Add the command's node to `pkg/ppl/ast/commands.go` and a
   parse function to `pkg/ppl/parser/commands.go`, dispatched from
   `parser.command` (or `parser.sourceCommand` for a source command). Match
   keywords with `atKw`/`acceptKw` so they stay usable as field names, read
   options with `args`, and report errors through `fail` so they carry a
   position and the expected tokens.
2. **Lowering.** Handle the node in `compiler.command` (`pkg/ppl/commands.go`)
   and lower it in `pkg/ppl/commands*.go` to existing IR operators. Resolve
   every field against the input schema (`resolve`), compile expressions
   with `ectx`, and return `semanticErr`, `typeErr` or `unsupportedFeature`
   at the offending node. Reject options LynxDB does not execute with
   `unsupportedFeature`; never ignore them silently.
3. **New operators, only when needed.** When no composition of operators
   expresses the semantics, add an operator to `pkg/ir/plan.go` with its
   schema derivation and label, its validation in `pkg/ir/validate.go`, its
   expression listing in `pkg/ir/walk.go`, its column handling in
   `pkg/ir/opt/prune.go` if it should not stop column pruning, and its row
   operator in `pkg/engine/physical` and `pkg/engine/pipeline`. When the
   columnar executor should run it too, translate it in
   `pkg/colstore/exec/irplan`; an operator that the translation refuses is
   run by the row operators.
4. **Catalog.** Record the support level and notes in
   `pkg/ppl/catalog.go`. Notes state the limitations and deviations from
   OpenSearch PPL; for an unsupported command they state the reason.
5. **Tests.** Add a compile golden pair under `pkg/ppl/testdata/compile`
   (`name.ppl` and the expected `name.ir`) and an execution case in
   `pkg/database/execute_test.go`. Parser-only cases go in
   `pkg/ppl/parser/corpus_test.go`.
6. **Documentation.** Regenerate the reference tables with
   `go run ./internal/docgen` and describe the command in the
   [command guide](./command-guide.md) when it is commonly used.

An unsupported command still needs a parse rule (often skipping to the next
`|`) so that it fails with `UNSUPPORTED_COMMAND` and its catalog reason
rather than a syntax error.
