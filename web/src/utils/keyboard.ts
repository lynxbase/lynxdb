import { create } from "zustand";

/**
 * Central keyboard shortcut definitions, platform detection, and overlay state.
 *
 * This module is the single source of truth for all keyboard shortcuts.
 * Components import SHORTCUTS for definitions, formatShortcut() for
 * platform-adaptive display, and the overlay store for open/close state.
 */

const IS_MAC = /Mac|iPhone|iPad|iPod/.test(navigator.platform);

export type ShortcutDef = {
  key: string;
  mod?: boolean;
  shift?: boolean;
  alt?: boolean;
  label: string;
};

export const SHORTCUTS = {
  runQuery: { key: "Enter", mod: true, label: "Run query" },
  focusEditor: { key: "L", mod: true, label: "Focus editor" },
  toggleTail: { key: "T", mod: true, shift: true, label: "Toggle live tail" },
  toggleSidebar: { key: "F", mod: true, shift: true, label: "Toggle sidebar" },
  openPalette: { key: "K", mod: true, label: "Command palette" },
  closePanel: { key: "Escape", label: "Close panel" },
  openHelp: { key: "?", label: "Keyboard shortcuts" },
  focusSearch: { key: "/", label: "Focus editor" },
  historyUp: { key: "↑", mod: true, label: "Previous query" },
  historyDown: { key: "↓", mod: true, label: "Next query" },
} as const;

/**
 * Format a shortcut definition into a platform-adaptive display string.
 *
 * On macOS: uses symbols (Cmd, Shift, Opt) joined without separators.
 * On other platforms: uses text (Ctrl, Shift, Alt) joined with "+".
 */
export function formatShortcut(def: ShortcutDef): string {
  const parts: string[] = [];
  if (def.mod) parts.push(IS_MAC ? "⌘" : "Ctrl");
  if (def.shift) parts.push(IS_MAC ? "⇧" : "Shift");
  if (def.alt) parts.push(IS_MAC ? "⌥" : "Alt");
  parts.push(def.key);
  return IS_MAC ? parts.join("") : parts.join("+");
}

/** Returns true if the current platform is macOS/iOS. */
export function isMac(): boolean {
  return IS_MAC;
}

interface OverlayState {
  /** Command palette open state. */
  paletteOpen: boolean;
  /** Help overlay open state. */
  helpOverlayOpen: boolean;
  /**
   * Query passed from the command palette to SearchView. When non-null,
   * SearchView loads it into the editor and executes. Lives here (not in
   * CommandPalette) to avoid a view-imports-from-component anti-pattern.
   */
  paletteQuery: string | null;
}

export const useOverlayStore = create<OverlayState>(() => ({
  paletteOpen: false,
  helpOverlayOpen: false,
  paletteQuery: null,
}));

export function setPaletteOpen(open: boolean): void {
  useOverlayStore.setState({ paletteOpen: open });
}

export function togglePalette(): void {
  useOverlayStore.setState((s) => ({ paletteOpen: !s.paletteOpen }));
}

export function setHelpOverlayOpen(open: boolean): void {
  useOverlayStore.setState({ helpOverlayOpen: open });
}

export function toggleHelpOverlay(): void {
  useOverlayStore.setState((s) => ({ helpOverlayOpen: !s.helpOverlayOpen }));
}

export function setPaletteQuery(query: string | null): void {
  useOverlayStore.setState({ paletteQuery: query });
}
