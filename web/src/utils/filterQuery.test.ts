import { describe, it, expect } from "vitest";
import { appendFilter } from "./filterQuery";

describe("appendFilter", () => {
  it("creates a leading clause for an empty query", () => {
    expect(appendFilter("", "level", "ERROR", false)).toBe(
      '| where level="ERROR"',
    );
  });

  it("appends to an existing query and trims it", () => {
    expect(appendFilter("  search foo  ", "level", "ERROR", false)).toBe(
      'search foo | where level="ERROR"',
    );
  });

  it("uses != when excluding", () => {
    expect(appendFilter("search foo", "status", "500", true)).toBe(
      'search foo | where status!="500"',
    );
  });

  it("escapes embedded double quotes", () => {
    expect(appendFilter("", "msg", 'he said "hi"', false)).toBe(
      '| where msg="he said \\"hi\\""',
    );
  });

  it("returns the query unchanged for a null value", () => {
    expect(
      appendFilter("search foo", "f", null as unknown as string, false),
    ).toBe("search foo");
  });
});
