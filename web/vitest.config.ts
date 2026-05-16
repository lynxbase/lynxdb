import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "jsdom",
    setupFiles: ["src/test/setup.ts"],
    include: ["src/**/*.{test,spec}.{ts,tsx}"],
    coverage: {
      provider: "v8",
      include: [
        "src/utils/**",
        "src/api/streaming.ts",
        "src/editor/lynxflow-catalog.ts",
      ],
    },
  },
});
