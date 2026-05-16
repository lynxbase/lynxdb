import { describe, it, expect } from "vitest";
import { uiBase, uiPath } from "./base";

// Under Vitest, import.meta.env.BASE_URL defaults to "/", so uiBase resolves
// to an empty string and uiPath produces root-relative paths.
describe("base", () => {
  it("strips the trailing slash from the base", () => {
    expect(uiBase).toBe("");
  });

  it("returns / for the root path", () => {
    expect(uiPath("/")).toBe("/");
    expect(uiPath("")).toBe("/");
  });

  it("prefixes a leading slash when missing", () => {
    expect(uiPath("status")).toBe("/status");
    expect(uiPath("/status")).toBe("/status");
  });
});
