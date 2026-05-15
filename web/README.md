# LynxDB Web UI

The single-page app served at `/ui/`. It is built and embedded into the
LynxDB Go binary (`internal/webui`), so it must work fully offline: no CDN,
no runtime network beyond the LynxDB API itself.

## Stack

- **React 19** + **react-router 7** (renderer + routing, `basename="/ui"`)
- **Zustand** for state (`src/stores/*`, plus the overlay store in
  `src/utils/keyboard.ts`)
- **Tailwind CSS v4** (CSS-first, `@tailwindcss/vite`) + **shadcn/ui**
  (new-york, vendored in `src/components/ui/`, Radix primitives)
- **CodeMirror 6** for the SPL2/LynxFlow editor, **uPlot** for the histogram
  (both vanilla, wrapped in thin React components)
- Self-hosted fonts via `@fontsource` (Inter, JetBrains Mono) — no Google CDN
- **Vite 6** build; **Vitest** + Testing Library; **Playwright** e2e

## Scripts

| Script | Purpose |
| --- | --- |
| `bun run dev` | Vite dev server (proxies `/api` and `/health` to :3100) |
| `bun run build` | Production build into `dist/` |
| `bun run typecheck` | `tsc --noEmit` (strict, `noUncheckedIndexedAccess`) |
| `bun run lint` | ESLint flat config (react-hooks, jsx-a11y as error) |
| `bun run format` | Prettier write |
| `bun run test` | Vitest unit suite |
| `bun run e2e` | Playwright suite (builds the Go binary, serves embedded UI) |
| `bun run bundle:check` | Fails if first-load JS gzip exceeds the 175 KB budget |

`make webui` (repo root) runs install + build and copies `dist/` into
`internal/webui/dist` for embedding.

## Design system

Visual identity is Grafana-flavored: dark-first, data-dense, 2px radius,
13px base, no shadows (elevation via border/`muted`). Tokens live in
`src/styles/globals.css` (`:root` / `.dark`) mapped to shadcn semantic
names; chart and syntax colors are tokenized so the histogram and editor
track the theme. Theme state is the single source `useThemeStore`
(`src/stores/ui.ts`), which toggles `.dark` and persists to
`localStorage('lynxdb_theme')`; the inline script in `index.html` prevents
FOUC.

## Conventions

- One source of truth for state: Zustand stores; components subscribe to
  the narrowest slice. Non-React code uses `Store.getState()`.
- Every async surface has explicit loading / empty / error states
  (Skeleton / empty copy / `Alert`); global failures use Sonner toasts.
- No `*.module.css` — components use Tailwind utilities + shadcn tokens.
  `src/components/ui/*` is vendored shadcn (a11y maintained upstream; not
  hand-edited except `sonner.tsx`, rewired to `useThemeStore`).
- Keep CodeMirror/uPlot logic framework-agnostic; only the wrapper is React.
- CI gates every change: typecheck, lint, unit tests, build, bundle budget,
  and the Playwright suite against the real embedded binary.
