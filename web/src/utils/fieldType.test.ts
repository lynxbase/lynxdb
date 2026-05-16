import { describe, it, expect } from "vitest";
import { typeAbbrev } from "./fieldType";

describe("typeAbbrev", () => {
  it("returns empty for missing type", () => {
    expect(typeAbbrev()).toBe("");
    expect(typeAbbrev("")).toBe("");
  });

  it("maps known types and is case-insensitive", () => {
    expect(typeAbbrev("string")).toBe("str");
    expect(typeAbbrev("INTEGER")).toBe("int");
    expect(typeAbbrev("int")).toBe("int");
    expect(typeAbbrev("float")).toBe("flt");
    expect(typeAbbrev("number")).toBe("flt");
    expect(typeAbbrev("boolean")).toBe("bool");
    expect(typeAbbrev("datetime")).toBe("ts");
    expect(typeAbbrev("timestamp")).toBe("ts");
  });

  it("truncates unknown types to three chars", () => {
    expect(typeAbbrev("geopoint")).toBe("geo");
  });
});
