import { create } from "zustand";

const STORAGE_KEY = "lynxdb_query_history";
const MAX_HISTORY = 100;

function loadHistory(): string[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return raw ? JSON.parse(raw) : [];
  } catch {
    return [];
  }
}

interface HistoryState {
  /** Query history list, most recent first. */
  queryHistory: string[];
}

export const useQueryHistoryStore = create<HistoryState>(() => ({
  queryHistory: loadHistory(),
}));

/** Current navigation index: -1 = not navigating, 0+ = position in history */
let historyIndex = -1;
/** Saved draft of what the user was typing before navigating history */
let draft = "";

/**
 * Push a query to history. Deduplicates and caps at MAX_HISTORY.
 * Resets navigation position.
 */
export function pushHistory(query: string): void {
  const trimmed = query.trim();
  if (!trimmed) return;

  const current = useQueryHistoryStore.getState().queryHistory;
  const next = [trimmed, ...current.filter((q) => q !== trimmed)];
  if (next.length > MAX_HISTORY) next.length = MAX_HISTORY;

  useQueryHistoryStore.setState({ queryHistory: next });
  localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
  historyIndex = -1;
}

/**
 * Navigate through query history.
 * Returns the query to display, or null if no movement possible.
 * Preserves the current draft when first entering history navigation.
 */
export function navigateHistory(
  direction: "up" | "down",
  currentQuery: string,
): string | null {
  const history = useQueryHistoryStore.getState().queryHistory;
  if (history.length === 0) return null;

  if (direction === "up") {
    if (historyIndex === -1) {
      draft = currentQuery;
    }
    if (historyIndex < history.length - 1) {
      historyIndex++;
      return history[historyIndex] ?? null;
    }
    return null; // already at oldest entry
  } else {
    if (historyIndex > 0) {
      historyIndex--;
      return history[historyIndex] ?? null;
    } else if (historyIndex === 0) {
      historyIndex = -1;
      return draft; // restore saved draft
    }
    return null; // not navigating
  }
}

/**
 * Reset history navigation state. Call when user manually edits
 * (docChanged but not from history navigation).
 */
export function resetHistoryNavigation(): void {
  historyIndex = -1;
  draft = "";
}
