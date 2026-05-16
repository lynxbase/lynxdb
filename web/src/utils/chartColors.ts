import { useThemeStore } from "../stores/ui";

/** Cache of resolved CSS custom property values, keyed by theme + property name. */
let cachedTheme: string | null = null;
const tokenCache = new Map<string, string>();

/** Invalidate the token cache when the theme changes. */
useThemeStore.subscribe((state) => {
  if (state.theme !== cachedTheme) {
    tokenCache.clear();
    cachedTheme = state.theme;
  }
});

// Initialize the cached theme to the current value.
cachedTheme = useThemeStore.getState().theme;

/**
 * Resolve a CSS custom property from the document root.
 * Results are cached per theme — avoids repeated getComputedStyle calls
 * (which force style recalculation / reflow).
 */
export function cssVar(name: string): string {
  const cached = tokenCache.get(name);
  if (cached !== undefined) return cached;

  const value = getComputedStyle(document.documentElement)
    .getPropertyValue(name)
    .trim();
  tokenCache.set(name, value);
  return value;
}

export function chartAxisFont(): string {
  return '11px Inter, "Helvetica Neue", Arial, sans-serif';
}
