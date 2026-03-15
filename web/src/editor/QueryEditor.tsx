import { useRef, useEffect } from "preact/hooks";
import { EditorState } from "@codemirror/state";
import { EditorView, keymap, placeholder } from "@codemirror/view";
import { defaultKeymap } from "@codemirror/commands";
import { acceptCompletion, completionStatus } from "@codemirror/autocomplete";
import { lynxflowLanguage } from "./lynxflow-lang";
import { lynxTheme, lynxHighlighting } from "./theme";
import { lynxflowAutocompletion } from "./autocomplete";
import styles from "./QueryEditor.module.css";

interface QueryEditorProps {
  value: string;
  onChange: (value: string) => void;
  onExecute: () => void;
  /** Optional ref callback so the parent can call focus() on the editor. */
  editorRef?: (handle: QueryEditorHandle | null) => void;
}

export interface QueryEditorHandle {
  focus: () => void;
}

export function QueryEditor({ value, onChange, onExecute, editorRef }: QueryEditorProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);
  const onChangeRef = useRef(onChange);
  const onExecuteRef = useRef(onExecute);

  // Keep callback refs current without recreating the editor
  onChangeRef.current = onChange;
  onExecuteRef.current = onExecute;

  useEffect(() => {
    if (!containerRef.current) return;

    const runQuery = keymap.of([{
      key: "Mod-Enter",
      run: () => {
        onExecuteRef.current();
        return true;
      },
    }]);

    const state = EditorState.create({
      doc: value,
      extensions: [
        runQuery,
        keymap.of(defaultKeymap),
        lynxflowLanguage,
        lynxTheme,
        lynxHighlighting,
        lynxflowAutocompletion(),
        placeholder('from main | where level="error" | group by _source compute count()'),
        EditorView.updateListener.of((update) => {
          if (update.docChanged) {
            onChangeRef.current(update.state.doc.toString());
          }
        }),
        // Enter: accept completion if open, otherwise run query
        keymap.of([{
          key: "Enter",
          run: (view) => {
            if (completionStatus(view.state) === "active") {
              return acceptCompletion(view);
            }
            onExecuteRef.current();
            return true;
          },
        }]),
        // Tab accepts the current completion when the panel is open
        keymap.of([{
          key: "Tab",
          run: (view) => {
            if (completionStatus(view.state) === "active") {
              return acceptCompletion(view);
            }
            return false;
          },
        }]),
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
      });
    }

    return () => {
      view.destroy();
      viewRef.current = null;
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
    }
  }, [value]);

  return <div ref={containerRef} class={styles.editorWrap} />;
}
