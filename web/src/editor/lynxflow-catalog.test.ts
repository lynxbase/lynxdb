import { describe, it, expect } from "vitest";
import {
  COMMANDS,
  COMMAND_DOCS,
  AGG_FUNCTIONS,
  EVAL_FUNCTIONS,
} from "./lynxflow-catalog";

describe("lynxflow catalog", () => {
  it("has no duplicate command names", () => {
    expect(new Set(COMMANDS).size).toBe(COMMANDS.length);
  });

  it("documents the core SPL2 commands", () => {
    for (const cmd of ["from", "search", "where", "stats", "eval", "sort"]) {
      expect(COMMANDS).toContain(cmd);
      expect(COMMAND_DOCS[cmd]).toBeTypeOf("string");
    }
  });

  it("lists aggregation and eval functions in call form", () => {
    expect(AGG_FUNCTIONS).toContain("count()");
    expect(AGG_FUNCTIONS).toContain("avg()");
    expect(EVAL_FUNCTIONS).toContain("if()");
    expect(EVAL_FUNCTIONS).toContain("coalesce()");
  });
});
