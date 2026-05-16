import { describe, it, expect, vi, afterEach } from "vitest";
import {
  generateFilename,
  generateCSV,
  generateJSON,
  downloadFile,
} from "./export";

describe("generateFilename", () => {
  it("produces a timestamped name with the given extension", () => {
    expect(generateFilename("csv")).toMatch(
      /^lynxdb-results-\d{8}-\d{6}\.csv$/,
    );
  });
});

describe("generateCSV", () => {
  it("emits a header row then body rows", () => {
    const csv = generateCSV(
      ["a", "b"],
      [
        { a: 1, b: 2 },
        { a: 3, b: 4 },
      ],
    );
    expect(csv).toBe("a,b\n1,2\n3,4");
  });

  it("quotes fields containing commas, quotes or newlines", () => {
    const csv = generateCSV(
      ["x"],
      [{ x: "a,b" }, { x: 'he "said"' }, { x: "line\nbreak" }],
    );
    expect(csv).toBe('x\n"a,b"\n"he ""said"""\n"line\nbreak"');
  });

  it("renders missing values as empty", () => {
    expect(generateCSV(["a", "b"], [{ a: 1 }])).toBe("a,b\n1,");
  });
});

describe("generateJSON", () => {
  it("pretty-prints with two-space indent", () => {
    expect(generateJSON([{ a: 1 }])).toBe('[\n  {\n    "a": 1\n  }\n]');
  });
});

describe("downloadFile", () => {
  afterEach(() => vi.restoreAllMocks());

  it("creates an anchor, clicks it, and revokes the object URL", () => {
    const createURL = vi
      .spyOn(URL, "createObjectURL")
      .mockReturnValue("blob:mock");
    const revokeURL = vi
      .spyOn(URL, "revokeObjectURL")
      .mockImplementation(() => {});
    const click = vi
      .spyOn(HTMLAnchorElement.prototype, "click")
      .mockImplementation(() => {});

    downloadFile("hello", "out.txt", "text/plain");

    expect(createURL).toHaveBeenCalledOnce();
    expect(click).toHaveBeenCalledOnce();
    expect(revokeURL).toHaveBeenCalledWith("blob:mock");
    expect(document.querySelector("a")).toBeNull();
  });
});
