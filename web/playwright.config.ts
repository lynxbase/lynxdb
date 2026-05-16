import { defineConfig, devices } from "@playwright/test";

// The smoke suite is the migration acceptance gate: it drives the real app
// served by the Go binary with the freshly built UI embedded, exactly as it
// ships. baseURL points at the embedded SPA mount (/ui/).
const baseURL = process.env.E2E_BASE_URL ?? "http://127.0.0.1:3100/ui/";

export default defineConfig({
  testDir: "e2e",
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  workers: 1,
  reporter: process.env.CI ? "github" : "list",
  timeout: 30_000,
  use: {
    baseURL,
    trace: "on-first-retry",
  },
  projects: [
    { name: "chromium", use: { ...devices["Desktop Chrome"] } },
  ],
  webServer: {
    command: "bash e2e/serve.sh",
    url: "http://127.0.0.1:3100/health",
    reuseExistingServer: !process.env.CI,
    timeout: 240_000,
    stdout: "pipe",
    stderr: "pipe",
  },
});
