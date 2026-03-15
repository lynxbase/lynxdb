import { useState, useEffect, useCallback } from "preact/hooks";
import type { JSX } from "preact";
import styles from "./EventDetail.module.css";

interface EventDetailProps {
  event: Record<string, unknown> | null;
  onClose: () => void;
}

type TabId = "fields" | "json";

/** Render a JSON value as colored spans */
function colorizeJson(obj: unknown, indent = 0): JSX.Element[] {
  const pad = "  ".repeat(indent);
  const elements: JSX.Element[] = [];

  if (obj === null) {
    elements.push(<span class={styles.jsonNull}>null</span>);
    return elements;
  }

  if (typeof obj === "string") {
    elements.push(<span class={styles.jsonString}>"{obj}"</span>);
    return elements;
  }

  if (typeof obj === "number") {
    elements.push(<span class={styles.jsonNumber}>{String(obj)}</span>);
    return elements;
  }

  if (typeof obj === "boolean") {
    elements.push(<span class={styles.jsonBool}>{String(obj)}</span>);
    return elements;
  }

  if (Array.isArray(obj)) {
    if (obj.length === 0) {
      elements.push(<span class={styles.jsonPunct}>[]</span>);
      return elements;
    }
    elements.push(<span class={styles.jsonPunct}>{"["}</span>);
    elements.push(<span>{"\n"}</span>);
    obj.forEach((item, i) => {
      elements.push(<span>{pad}{"  "}</span>);
      elements.push(...colorizeJson(item, indent + 1));
      if (i < obj.length - 1) elements.push(<span class={styles.jsonPunct}>,</span>);
      elements.push(<span>{"\n"}</span>);
    });
    elements.push(<span>{pad}</span>);
    elements.push(<span class={styles.jsonPunct}>{"]"}</span>);
    return elements;
  }

  if (typeof obj === "object") {
    const entries = Object.entries(obj as Record<string, unknown>);
    if (entries.length === 0) {
      elements.push(<span class={styles.jsonPunct}>{"{}"}</span>);
      return elements;
    }
    elements.push(<span class={styles.jsonPunct}>{"{"}</span>);
    elements.push(<span>{"\n"}</span>);
    entries.forEach(([key, val], i) => {
      elements.push(<span>{pad}{"  "}</span>);
      elements.push(<span class={styles.jsonKey}>"{key}"</span>);
      elements.push(<span class={styles.jsonPunct}>: </span>);
      elements.push(...colorizeJson(val, indent + 1));
      if (i < entries.length - 1) elements.push(<span class={styles.jsonPunct}>,</span>);
      elements.push(<span>{"\n"}</span>);
    });
    elements.push(<span>{pad}</span>);
    elements.push(<span class={styles.jsonPunct}>{"}"}</span>);
    return elements;
  }

  elements.push(<span>{String(obj)}</span>);
  return elements;
}

export function EventDetail({ event, onClose }: EventDetailProps) {
  const [tab, setTab] = useState<TabId>("fields");

  // Close on Escape
  useEffect(() => {
    function onKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") {
        onClose();
      }
    }
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, [onClose]);

  const handleCopy = useCallback(() => {
    if (event) {
      navigator.clipboard.writeText(JSON.stringify(event, null, 2)).catch(() => {
        // clipboard write can fail in non-HTTPS contexts; silently ignore
      });
    }
  }, [event]);

  if (!event) return null;

  const entries = Object.entries(event);

  return (
    <div class={styles.panel} role="complementary" aria-label="Event details">
      <div class={styles.toolbar}>
        <button
          type="button"
          class={`${styles.tab} ${tab === "fields" ? styles.tabActive : ""}`}
          onClick={() => setTab("fields")}
        >
          Fields
        </button>
        <button
          type="button"
          class={`${styles.tab} ${tab === "json" ? styles.tabActive : ""}`}
          onClick={() => setTab("json")}
        >
          JSON
        </button>
        <div class={styles.spacer} />
        <button type="button" class={styles.copyBtn} onClick={handleCopy}>
          Copy JSON
        </button>
        <button
          type="button"
          class={styles.closeBtn}
          onClick={onClose}
          aria-label="Close detail panel"
        >
          &times;
        </button>
      </div>
      <div class={styles.body}>
        {tab === "fields" ? (
          <table class={styles.fieldsTable}>
            <tbody>
              {entries.map(([key, value]) => (
                <tr key={key}>
                  <td class={styles.fieldKey}>{key}</td>
                  <td class={styles.fieldValue}>{value == null ? "" : String(value)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <pre class={styles.jsonPre}>
            {colorizeJson(event)}
          </pre>
        )}
      </div>
    </div>
  );
}
