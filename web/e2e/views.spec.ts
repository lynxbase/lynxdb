import { test, expect } from "@playwright/test";

// Regression coverage for the redesigned routes and theme persistence.
// Each route renders its header shell even while data loads or errors,
// so asserting the heading proves the view mounts without crashing.

test("status view route renders", async ({ page }) => {
  await page.goto("/ui/status");
  await expect(
    page.getByRole("heading", { name: "Server Status" }),
  ).toBeVisible();
});

test("saved queries view route renders", async ({ page }) => {
  await page.goto("/ui/queries");
  await expect(
    page.getByRole("heading", { name: "Saved Queries" }),
  ).toBeVisible();
});

test("settings view route renders", async ({ page }) => {
  await page.goto("/ui/settings");
  await expect(
    page.getByRole("heading", { name: "Settings" }),
  ).toBeVisible();
});

test("theme choice persists across reload", async ({ page }) => {
  await page.goto("/ui/");
  const html = page.locator("html");
  await expect(html).not.toHaveClass(/dark/);
  await page.getByTitle("Switch to dark mode").click();
  await expect(html).toHaveClass(/dark/);
  await page.reload();
  await expect(html).toHaveClass(/dark/);
});
