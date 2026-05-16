import { useLayoutEffect, useRef } from "react";
import {
  useOverlayStore,
  setPaletteOpen,
  setHelpOverlayOpen,
} from "../utils/keyboard";

interface Shortcuts {
  onFocusEditor?: () => void;
  onToggleTail?: () => void;
  onToggleSidebar?: () => void;
  onClosePanel?: () => void;
  onOpenPalette?: () => void;
  onOpenHelp?: () => void;
}

function isInputFocused(): boolean {
  const el = document.activeElement;
  if (!el) return false;
  if (el instanceof HTMLInputElement || el instanceof HTMLTextAreaElement) {
    return true;
  }
  // CodeMirror's content area
  if (el.classList?.contains("cm-content")) return true;
  return false;
}

/**
 * Registers global keyboard shortcuts for the search view.
 *
 * - Ctrl/Cmd+K -> open command palette
 * - Ctrl/Cmd+L -> focus editor
 * - Ctrl/Cmd+Shift+T -> toggle live tail
 * - Ctrl/Cmd+Shift+F -> toggle sidebar
 * - "/" (when not in an input) -> focus editor
 * - "?" (when not in an input) -> open help overlay
 * - Escape -> layered close (palette > help overlay > onClosePanel callback)
 */
export function useKeyboardShortcuts(shortcuts: Shortcuts): void {
  // Use a ref so the effect closure always sees the latest callbacks
  // without needing to re-register the listener.
  const ref = useRef(shortcuts);
  ref.current = shortcuts;

  useLayoutEffect(() => {
    function handler(e: KeyboardEvent) {
      const s = ref.current;
      const modKey = e.ctrlKey || e.metaKey;

      // Ctrl/Cmd+K -> open command palette (prevents browser address bar)
      if (e.key === "k" && modKey && !e.shiftKey) {
        e.preventDefault();
        s.onOpenPalette?.();
        return;
      }

      // Ctrl/Cmd+L -> focus editor (prevents browser address bar)
      if (e.key === "l" && modKey && !e.shiftKey) {
        e.preventDefault();
        s.onFocusEditor?.();
        return;
      }

      // Ctrl/Cmd+Shift+T -> toggle live tail (prevents browser reopen tab)
      if (e.key === "T" && modKey && e.shiftKey) {
        e.preventDefault();
        s.onToggleTail?.();
        return;
      }

      // Ctrl/Cmd+Shift+F -> toggle sidebar
      if (e.key === "f" && modKey && e.shiftKey) {
        e.preventDefault();
        s.onToggleSidebar?.();
        return;
      }

      // "/" when not in an input -> focus editor
      if (e.key === "/" && !modKey && !e.shiftKey && !isInputFocused()) {
        e.preventDefault();
        s.onFocusEditor?.();
        return;
      }

      // "?" when not in an input -> open help overlay
      // Must come after modifier-key shortcuts to avoid conflicts (Pitfall 6)
      if (e.key === "?" && !modKey && !isInputFocused()) {
        e.preventDefault();
        s.onOpenHelp?.();
        return;
      }

      // Escape -> layered close: palette > help overlay > app panels
      if (e.key === "Escape") {
        if (useOverlayStore.getState().paletteOpen) {
          setPaletteOpen(false);
          return;
        }
        if (useOverlayStore.getState().helpOverlayOpen) {
          setHelpOverlayOpen(false);
          return;
        }
        s.onClosePanel?.();
      }
    }

    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, []);
}
