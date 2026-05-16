import { test, expect, request as apiRequest } from "@playwright/test";

const API = "http://127.0.0.1:3100";
const MARKER = "hello smoke parity";

// Seed a queryable event before the UI run, retrying until the async batcher
// has flushed it so "run a query, see results" is deterministic.
test.beforeAll(async () => {
  const ctx = await apiRequest.newContext();
  await ctx.post(`${API}/api/v1/ingest`, {
    data: [{ event: MARKER, source: "smoke", fields: { level: "error" } }],
  });

  await expect
    .poll(
      async () => {
        const res = await ctx.post(`${API}/api/v1/query`, {
          data: { q: "level=error", from: "-1h", to: "now" },
        });
        if (!res.ok()) return 0;
        return JSON.stringify(await res.json()).includes(MARKER) ? 1 : 0;
      },
      { timeout: 30_000, intervals: [500, 1000, 2000] },
    )
    .toBe(1);
  await ctx.dispose();
});

test("app shell loads with the query editor and run control", async ({
  page,
}) => {
  await page.goto("/ui/");
  await expect(page.locator(".cm-editor")).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Run query" }),
  ).toBeVisible();
});

test("running a query shows results", async ({ page }) => {
  await page.goto("/ui/#q=level%3Derror&from=-1h");
  await expect(page.locator(".cm-content")).toContainText("level=error");
  await page.getByRole("button", { name: "Run query" }).click();
  await expect(page.getByText(MARKER).first()).toBeVisible({
    timeout: 15_000,
  });
});

test("theme toggle switches between light and dark", async ({ page }) => {
  await page.goto("/ui/");
  const html = page.locator("html");
  await expect(html).not.toHaveClass(/dark/);
  await page.getByTitle("Switch to dark mode").click();
  await expect(html).toHaveClass(/dark/);
  await page.getByTitle("Switch to light mode").click();
  await expect(html).not.toHaveClass(/dark/);
});

test("command palette opens and closes", async ({ page }) => {
  await page.goto("/ui/");
  await page.keyboard.press("Control+k");
  const palette = page.getByPlaceholder("Type a command...");
  await expect(palette).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(palette).toBeHidden();
});

test("live tail can be started and stopped", async ({ page }) => {
  await page.goto("/ui/");
  await page.getByRole("button", { name: "Start live tail" }).click();
  await expect(
    page.getByRole("button", { name: "Stop live tail" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Stop live tail" }).click();
  await expect(
    page.getByRole("button", { name: "Start live tail" }),
  ).toBeVisible();
});
