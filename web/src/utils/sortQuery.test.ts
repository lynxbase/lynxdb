import { describe, it, expect } from "vitest";
import { parseSortFromQuery, updateSortInQuery } from "./sortQuery";

describe("parseSortFromQuery", () => {
  it("returns null without a sort clause", () => {
    expect(parseSortFromQuery("search foo")).toBeNull();
  });

  it("parses ascending sort", () => {
    expect(parseSortFromQuery("search foo | sort bar")).toEqual({
      field: "bar",
      direction: "asc",
    });
  });

  it("treats a + prefix as ascending", () => {
    expect(parseSortFromQuery("search foo | sort +bar")).toEqual({
      field: "bar",
      direction: "asc",
    });
  });

  it("parses descending sort with a - prefix", () => {
    expect(parseSortFromQuery("search foo | sort -bar")).toEqual({
      field: "bar",
      direction: "desc",
    });
  });

  it("supports dotted field names", () => {
    expect(parseSortFromQuery("foo | sort -a.b.c")).toEqual({
      field: "a.b.c",
      direction: "desc",
    });
  });
});

describe("updateSortInQuery", () => {
  it("appends an ascending sort", () => {
    expect(updateSortInQuery("search foo", "bar", "asc")).toBe(
      "search foo | sort bar",
    );
  });

  it("appends a descending sort", () => {
    expect(updateSortInQuery("search foo", "bar", "desc")).toBe(
      "search foo | sort -bar",
    );
  });

  it("replaces an existing sort clause", () => {
    expect(updateSortInQuery("search foo | sort -old", "new", "asc")).toBe(
      "search foo | sort new",
    );
  });

  it("removes the sort clause when direction is null", () => {
    expect(updateSortInQuery("search foo | sort -old", "new", null)).toBe(
      "search foo",
    );
  });
});
