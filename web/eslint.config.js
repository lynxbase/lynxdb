import js from "@eslint/js";
import tseslint from "typescript-eslint";
import reactHooks from "eslint-plugin-react-hooks";
import reactRefresh from "eslint-plugin-react-refresh";
import jsxA11y from "eslint-plugin-jsx-a11y";
import globals from "globals";

export default tseslint.config(
  {
    ignores: [
      "dist/**",
      "node_modules/**",
      ".vite/**",
      "coverage/**",
      "playwright-report/**",
      "test-results/**",
    ],
  },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  jsxA11y.flatConfigs.recommended,
  {
    files: ["**/*.{ts,tsx}"],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: "module",
      globals: { ...globals.browser },
      parserOptions: { ecmaFeatures: { jsx: true } },
    },
    plugins: {
      "react-hooks": reactHooks,
      "react-refresh": reactRefresh,
    },
    rules: {
      "react-hooks/rules-of-hooks": "error",
      "react-hooks/exhaustive-deps": "warn",
      "react-refresh/only-export-components": [
        "warn",
        { allowConstantExport: true },
      ],
      "@typescript-eslint/no-unused-vars": [
        "error",
        { argsIgnorePattern: "^_", varsIgnorePattern: "^_" },
      ],
      "@typescript-eslint/no-explicit-any": "warn",
      // Deprecated and superseded by label-has-associated-control (which
      // passes here): it false-positives on correct htmlFor+id labels.
      "jsx-a11y/label-has-for": "off",
      // Autofocus is used intentionally on the sole input of single-purpose
      // forms (auth screen, create-query dialog) to support the keyboard-first
      // workflow; not a hard WCAG failure in these scoped cases.
      "jsx-a11y/no-autofocus": "off",
    },
  },
  {
    // Vendored shadcn/ui primitives: accessibility is maintained upstream
    // (Radix). We do not hand-author these, so do not lint them for a11y.
    files: ["src/components/ui/**"],
    rules: {
      "jsx-a11y/anchor-has-content": "off",
      "jsx-a11y/heading-has-content": "off",
      "jsx-a11y/anchor-is-valid": "off",
    },
  },
  {
    files: ["**/*.{test,spec}.{ts,tsx}", "tests/**", "e2e/**"],
    languageOptions: { globals: { ...globals.node } },
  },
);
