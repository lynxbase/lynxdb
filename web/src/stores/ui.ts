import { create } from "zustand";

type Theme = "light" | "dark";

const STORAGE_KEY = "lynxdb_theme";

function getInitialTheme(): Theme {
  const stored = localStorage.getItem(STORAGE_KEY);
  if (stored === "light" || stored === "dark") return stored;
  return window.matchMedia("(prefers-color-scheme: dark)").matches
    ? "dark"
    : "light";
}

interface ThemeState {
  theme: Theme;
}

export const useThemeStore = create<ThemeState>(() => ({
  theme: getInitialTheme(),
}));

/** Apply theme class to <html> and persist to localStorage. */
function applyTheme(t: Theme): void {
  document.documentElement.classList.toggle("dark", t === "dark");
  localStorage.setItem(STORAGE_KEY, t);
}

applyTheme(useThemeStore.getState().theme);
useThemeStore.subscribe((state) => applyTheme(state.theme));

export function toggleTheme(): void {
  useThemeStore.setState((s) => ({
    theme: s.theme === "light" ? "dark" : "light",
  }));
}

/**
 * Follow OS theme changes when the user has not explicitly set a preference.
 * If localStorage has no stored value, match the system theme automatically.
 */
const mq = window.matchMedia("(prefers-color-scheme: dark)");
mq.addEventListener("change", (e) => {
  const stored = localStorage.getItem(STORAGE_KEY);
  if (!stored) {
    useThemeStore.setState({ theme: e.matches ? "dark" : "light" });
  }
});
