import React, { useRef, useEffect, useCallback } from "react";
import { Compartment, EditorState } from "@codemirror/state";
import { EditorView, keymap, placeholder, lineNumbers } from "@codemirror/view";
import { defaultKeymap } from "@codemirror/commands";
import { acceptCompletion, completionStatus } from "@codemirror/autocomplete";
import { linter } from "@codemirror/lint";
import { lynxflowLanguage } from "./lynxflow-lang";
import { lynxThemeFor, lynxHighlighting } from "./theme";
import { lynxflowAutocompletion } from "./autocomplete";
import {
  navigateHistory,
  resetHistoryNavigation,
} from "../stores/queryHistory";
import { useThemeStore } from "../stores/ui";

interface QueryEditorProps {
  value: string;
  onChange: (value: string) => void;
  onExecute: () => void;
  /** Optional ref callback so the parent can call focus() on the editor. */
  editorRef?: (handle: QueryEditorHandle | null) => void;
}

export interface QueryEditorHandle {
  focus: () => void;
  getView: () => EditorView | null;
}

// Compartment for dynamically toggling line numbers based on line count
const lineNumberCompartment = new Compartment();
// Compartment for swapping the light/dark editor theme on theme change
const themeCompartment = new Compartment();

export function QueryEditor({
  value,
  onChange,
  onExecute,
  editorRef,
}: QueryEditorProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const wrapRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);
  const onChangeRef = useRef(onChange);
  const onExecuteRef = useRef(onExecute);

  // Manual height ref: null = auto-expand mode, number = user set explicit height via drag
  const manualHeightRef = useRef<number | null>(null);
  // Track current line number state to avoid infinite reconfigure loop (Pitfall 1)
  const hasLineNumbersRef = useRef(false);
  // Track the height at drag start for computing delta
  const dragStartHeightRef = useRef<number>(0);
  // Track whether a doc change is from history navigation to avoid resetting historyIndex
  const isHistoryNavigationRef = useRef(false);

  // Keep callback refs current without recreating the editor
  onChangeRef.current = onChange;
  onExecuteRef.current = onExecute;

  useEffect(() => {
    if (!containerRef.current) return;

    const runQuery = keymap.of([
      {
        key: "Mod-Enter",
        run: () => {
          onExecuteRef.current();
          return true;
        },
      },
    ]);

    const state = EditorState.create({
      doc: value,
      extensions: [
        runQuery,
        keymap.of(defaultKeymap),
        lynxflowLanguage,
        themeCompartment.of(
          lynxThemeFor(useThemeStore.getState().theme === "dark"),
        ),
        lynxHighlighting,
        lynxflowAutocompletion(),
        // Shift+Enter for newline: placed AFTER autocomplete to avoid Pitfall 5
        keymap.of([
          {
            key: "Shift-Enter",
            run: (view) => {
              view.dispatch(view.state.replaceSelection("\n"));
              return true;
            },
          },
        ]),
        // Ctrl+Up/Down for query history navigation
        keymap.of([
          {
            key: "Ctrl-ArrowUp",
            run: (view) => {
              const result = navigateHistory("up", view.state.doc.toString());
              if (result !== null) {
                isHistoryNavigationRef.current = true;
                view.dispatch({
                  changes: {
                    from: 0,
                    to: view.state.doc.length,
                    insert: result,
                  },
                });
                isHistoryNavigationRef.current = false;
              }
              return true;
            },
          },
          {
            key: "Ctrl-ArrowDown",
            run: (view) => {
              const result = navigateHistory("down", view.state.doc.toString());
              if (result !== null) {
                isHistoryNavigationRef.current = true;
                view.dispatch({
                  changes: {
                    from: 0,
                    to: view.state.doc.length,
                    insert: result,
                  },
                });
                isHistoryNavigationRef.current = false;
              }
              return true;
            },
          },
        ]),
        // No-op linter sets up diagnostic display infrastructure (Pitfall 6).
        // Actual diagnostics are dispatched via setDiagnostics from the parent.
        linter(() => [], { delay: 0 }),
        placeholder(
          'from main | where level="error" | group by _source compute count()',
        ),
        // Dynamic line numbers via Compartment: starts with no line numbers (single line)
        lineNumberCompartment.of([]),
        EditorView.updateListener.of((update) => {
          if (update.docChanged) {
            onChangeRef.current(update.state.doc.toString());

            // Reset history navigation when user manually edits (not via Ctrl+Up/Down)
            if (!isHistoryNavigationRef.current) {
              resetHistoryNavigation();
            }

            // Toggle line numbers based on line count (Pitfall 1: guard with comparison)
            const lineCount = update.state.doc.lines;
            const shouldHaveNumbers = lineCount >= 2;

            if (shouldHaveNumbers !== hasLineNumbersRef.current) {
              hasLineNumbersRef.current = shouldHaveNumbers;
              update.view.dispatch({
                effects: lineNumberCompartment.reconfigure(
                  shouldHaveNumbers ? lineNumbers() : [],
                ),
              });
            }
          }
        }),
        // Enter: accept completion if open, otherwise run query
        keymap.of([
          {
            key: "Enter",
            run: (view) => {
              if (completionStatus(view.state) === "active") {
                return acceptCompletion(view);
              }
              onExecuteRef.current();
              return true;
            },
          },
        ]),
        // Tab accepts the current completion when the panel is open
        keymap.of([
          {
            key: "Tab",
            run: (view) => {
              if (completionStatus(view.state) === "active") {
                return acceptCompletion(view);
              }
              return false;
            },
          },
        ]),
        EditorView.contentAttributes.of({ "aria-label": "Query editor" }),
      ],
    });

    const view = new EditorView({
      state,
      parent: containerRef.current,
    });

    viewRef.current = view;

    // Expose handle to parent
    if (editorRef) {
      editorRef({
        focus: () => view.focus(),
        getView: () => viewRef.current,
      });
    }

    return () => {
      view.destroy();
      viewRef.current = null;
      hasLineNumbersRef.current = false;
      if (editorRef) editorRef(null);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- intentionally run once
  }, []);

  // Sync external value changes into the editor
  useEffect(() => {
    const view = viewRef.current;
    if (!view) return;
    const current = view.state.doc.toString();
    if (current !== value) {
      view.dispatch({
        changes: { from: 0, to: current.length, insert: value },
      });

      // Also check line numbers for externally set values
      const lineCount = view.state.doc.lines;
      const shouldHaveNumbers = lineCount >= 2;
      if (shouldHaveNumbers !== hasLineNumbersRef.current) {
        hasLineNumbersRef.current = shouldHaveNumbers;
        view.dispatch({
          effects: lineNumberCompartment.reconfigure(
            shouldHaveNumbers ? lineNumbers() : [],
          ),
        });
      }
    }
  }, [value]);

  // Swap the editor theme when the app theme changes, preserving editor
  // state (scroll/selection) by reconfiguring the compartment.
  useEffect(() => {
    const sync = (dark: boolean) => {
      viewRef.current?.dispatch({
        effects: themeCompartment.reconfigure(lynxThemeFor(dark)),
      });
    };
    return useThemeStore.subscribe((state, prev) => {
      if (state.theme !== prev.theme) sync(state.theme === "dark");
    });
  }, []);

  // Drag handle pointer event handlers
  const handlePointerDown = useCallback((e: React.PointerEvent<HTMLDivElement>) => {
    e.preventDefault();
    const target = e.currentTarget;
    target.setPointerCapture(e.pointerId);

    // Capture the current height of the wrap element at drag start
    const wrap = wrapRef.current;
    if (!wrap) return;
    dragStartHeightRef.current = wrap.getBoundingClientRect().height;
    const startY = e.clientY;

    const onMove = (moveEvent: PointerEvent) => {
      const deltaY = moveEvent.clientY - startY;
      const maxHeight = window.innerHeight * 0.5; // 50vh cap
      const newHeight = Math.max(
        32,
        Math.min(dragStartHeightRef.current + deltaY, maxHeight),
      );
      manualHeightRef.current = newHeight;
      if (wrapRef.current) {
        wrapRef.current.style.height = `${newHeight}px`;
      }
    };

    const onUp = () => {
      target.removeEventListener("pointermove", onMove);
      target.removeEventListener("pointerup", onUp);
    };

    target.addEventListener("pointermove", onMove);
    target.addEventListener("pointerup", onUp);
  }, []);

  return (
    <div className="flex flex-1 flex-col relative min-w-0">
      <div
        ref={wrapRef}
        className="min-h-8 border border-border rounded-sm overflow-hidden transition-colors duration-150 focus-within:border-primary motion-reduce:transition-none"
      >
        <div ref={containerRef} />
      </div>
      <div
        className="absolute bottom-0 inset-x-0 h-1 cursor-ns-resize bg-transparent z-[2] transition-colors duration-150 hover:bg-accent motion-reduce:transition-none"
        onPointerDown={handlePointerDown}
        role="separator"
        aria-orientation="horizontal"
        aria-label="Resize query editor"
      />
    </div>
  );
}

// Default export so the heavy CodeMirror bundle can be lazy-loaded.
export default QueryEditor;
