import { useEffect, useRef } from "preact/hooks";

interface Shortcuts {
  onFocusEditor?: () => void;
  onToggleTail?: () => void;
  onToggleSidebar?: () => void;
  onClosePanel?: () => void;
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
 * - Ctrl/Cmd+K or "/" (when not in an input) -> focus editor
 * - Ctrl/Cmd+L -> toggle live tail
 * - Ctrl/Cmd+Shift+F -> toggle sidebar
 * - Escape -> close panel
 */
export function useKeyboardShortcuts(shortcuts: Shortcuts): void {
  // Use a ref so the effect closure always sees the latest callbacks
  // without needing to re-register the listener.
  const ref = useRef(shortcuts);
  ref.current = shortcuts;

  useEffect(() => {
    function handler(e: KeyboardEvent) {
      const s = ref.current;
      const modKey = e.ctrlKey || e.metaKey;

      // Ctrl/Cmd+K -> focus editor
      if (e.key === "k" && modKey) {
        e.preventDefault();
        s.onFocusEditor?.();
        return;
      }

      // "/" when not in an input -> focus editor
      if (e.key === "/" && !modKey && !e.shiftKey && !isInputFocused()) {
        e.preventDefault();
        s.onFocusEditor?.();
        return;
      }

      // Ctrl/Cmd+L -> toggle live tail
      if (e.key === "l" && modKey) {
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

      // Escape -> close panel
      if (e.key === "Escape") {
        s.onClosePanel?.();
      }
    }

    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, []);
}
