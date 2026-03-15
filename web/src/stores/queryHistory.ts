import { signal } from "@preact/signals";

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

/** Signal-backed query history list, most recent first. */
export const queryHistory = signal<string[]>(loadHistory());

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

  // Deduplicate: remove existing occurrence, add to front
  const next = [trimmed, ...queryHistory.value.filter((q) => q !== trimmed)];
  if (next.length > MAX_HISTORY) next.length = MAX_HISTORY;

  queryHistory.value = next;
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
  const history = queryHistory.value;
  if (history.length === 0) return null;

  if (direction === "up") {
    if (historyIndex === -1) {
      // Entering history navigation -- save current draft
      draft = currentQuery;
    }
    if (historyIndex < history.length - 1) {
      historyIndex++;
      return history[historyIndex];
    }
    return null; // already at oldest entry
  } else {
    // direction === "down"
    if (historyIndex > 0) {
      historyIndex--;
      return history[historyIndex];
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
