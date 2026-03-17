import { useState } from "preact/hooks";
import type { DashboardPanel } from "../../api/client";
import styles from "./PanelEditForm.module.css";

const PANEL_TYPES = [
  "timechart",
  "bar",
  "line",
  "area",
  "table",
  "stat",
  "pie",
] as const;

interface PanelEditFormProps {
  panel?: DashboardPanel;
  onSave: (panel: DashboardPanel) => void;
  onCancel: () => void;
}

export function PanelEditForm({ panel, onSave, onCancel }: PanelEditFormProps) {
  const isEdit = !!panel;

  const [title, setTitle] = useState(panel?.title ?? "New Panel");
  const [type, setType] = useState(panel?.type ?? "timechart");
  const [query, setQuery] = useState(panel?.q ?? "* | timechart count span=5m");
  const [x, setX] = useState(panel?.position.x ?? 0);
  const [y, setY] = useState(panel?.position.y ?? 0);
  const [w, setW] = useState(panel?.position.w ?? 6);
  const [h, setH] = useState(panel?.position.h ?? 4);

  const [errors, setErrors] = useState<{ title?: string; query?: string }>({});

  function handleSave() {
    const newErrors: { title?: string; query?: string } = {};
    if (!title.trim()) newErrors.title = "Title is required";
    if (!query.trim()) newErrors.query = "Query is required";
    if (Object.keys(newErrors).length > 0) {
      setErrors(newErrors);
      return;
    }
    setErrors({});

    const saved: DashboardPanel = {
      id: panel?.id ?? crypto.randomUUID(),
      title: title.trim(),
      type,
      q: query.trim(),
      position: {
        x: Math.max(0, Math.min(11, x)),
        y: Math.max(0, y),
        w: Math.max(1, Math.min(12, w)),
        h: Math.max(1, Math.min(20, h)),
      },
    };
    onSave(saved);
  }

  function handleOverlayClick(e: MouseEvent) {
    if ((e.target as HTMLElement).classList.contains(styles.overlay)) {
      onCancel();
    }
  }

  return (
    <div class={styles.overlay} onClick={handleOverlayClick}>
      <div class={styles.form}>
        <div class={styles.formTitle}>
          {isEdit ? "Edit Panel" : "Add Panel"}
        </div>

        <div class={styles.field}>
          <label class={styles.label}>Title</label>
          <input
            class={`${styles.input} ${errors.title ? styles.inputError : ""}`}
            type="text"
            value={title}
            onInput={(e) => {
              setTitle((e.target as HTMLInputElement).value);
              if (errors.title) setErrors((prev) => ({ ...prev, title: undefined }));
            }}
            placeholder="Panel title"
          />
          {errors.title && <div class={styles.errorMsg}>{errors.title}</div>}
        </div>

        <div class={styles.field}>
          <label class={styles.label}>Type</label>
          <select
            class={styles.select}
            value={type}
            onChange={(e) => setType((e.target as HTMLSelectElement).value)}
          >
            {PANEL_TYPES.map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </select>
        </div>

        <div class={styles.field}>
          <label class={styles.label}>Query (SPL2)</label>
          <textarea
            class={`${styles.textarea} ${errors.query ? styles.inputError : ""}`}
            rows={4}
            value={query}
            onInput={(e) => {
              setQuery((e.target as HTMLTextAreaElement).value);
              if (errors.query) setErrors((prev) => ({ ...prev, query: undefined }));
            }}
            placeholder="* | timechart count span=5m"
          />
          {errors.query && <div class={styles.errorMsg}>{errors.query}</div>}
        </div>

        <div class={styles.field}>
          <span class={styles.posLabel}>Position</span>
          <div class={styles.posGrid}>
            <div class={styles.posField}>
              <span class={styles.posFieldLabel}>X</span>
              <input
                class={styles.input}
                type="number"
                min={0}
                max={11}
                value={x}
                onInput={(e) =>
                  setX(Number((e.target as HTMLInputElement).value))
                }
              />
            </div>
            <div class={styles.posField}>
              <span class={styles.posFieldLabel}>Y</span>
              <input
                class={styles.input}
                type="number"
                min={0}
                value={y}
                onInput={(e) =>
                  setY(Number((e.target as HTMLInputElement).value))
                }
              />
            </div>
            <div class={styles.posField}>
              <span class={styles.posFieldLabel}>W</span>
              <input
                class={styles.input}
                type="number"
                min={1}
                max={12}
                value={w}
                onInput={(e) =>
                  setW(Number((e.target as HTMLInputElement).value))
                }
              />
            </div>
            <div class={styles.posField}>
              <span class={styles.posFieldLabel}>H</span>
              <input
                class={styles.input}
                type="number"
                min={1}
                max={20}
                value={h}
                onInput={(e) =>
                  setH(Number((e.target as HTMLInputElement).value))
                }
              />
            </div>
          </div>
        </div>

        <div class={styles.actions}>
          <button type="button" class={styles.btnCancel} onClick={onCancel}>
            Cancel
          </button>
          <button type="button" class={styles.btnPrimary} onClick={handleSave}>
            {isEdit ? "Update Panel" : "Add Panel"}
          </button>
        </div>
      </div>
    </div>
  );
}
