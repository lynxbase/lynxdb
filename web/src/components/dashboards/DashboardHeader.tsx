import { useState, useRef } from "preact/hooks";
import { Edit2 } from "lucide-preact";
import styles from "./DashboardHeader.module.css";

const TIME_PRESETS: { label: string; value: string }[] = [
  { label: "Last 15m", value: "-15m" },
  { label: "Last 1h", value: "-1h" },
  { label: "Last 4h", value: "-4h" },
  { label: "Last 24h", value: "-24h" },
  { label: "Last 7d", value: "-7d" },
  { label: "Last 30d", value: "-30d" },
];

const REFRESH_OPTIONS: { label: string; value: number }[] = [
  { label: "Off", value: 0 },
  { label: "5s", value: 5000 },
  { label: "10s", value: 10000 },
  { label: "30s", value: 30000 },
  { label: "1m", value: 60000 },
  { label: "5m", value: 300000 },
  { label: "15m", value: 900000 },
];

interface DashboardHeaderProps {
  name: string;
  editable?: boolean;
  onNameChange?: (name: string) => void;
  from: string;
  onTimeChange: (from: string) => void;
  refreshInterval: number;
  onRefreshIntervalChange: (ms: number) => void;
  editMode?: boolean;
  onToggleEdit?: () => void;
  onSave?: () => void;
  onDiscard?: () => void;
  saveDisabled?: boolean;
  onDelete?: () => void;
  onToggleVariables?: () => void;
  showVariableEditor?: boolean;
}

export function DashboardHeader({
  name,
  editable,
  onNameChange,
  from,
  onTimeChange,
  refreshInterval,
  onRefreshIntervalChange,
  editMode,
  onToggleEdit,
  onSave,
  onDiscard,
  saveDisabled,
  onDelete,
  onToggleVariables,
  showVariableEditor,
}: DashboardHeaderProps) {
  const [editing, setEditing] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  function handleNameClick() {
    if (editable && editMode) {
      setEditing(true);
      setTimeout(() => inputRef.current?.focus(), 0);
    }
  }

  function handleNameBlur() {
    setEditing(false);
    const val = inputRef.current?.value.trim();
    if (val && val !== name && onNameChange) {
      onNameChange(val);
    }
  }

  function handleNameKeyDown(e: KeyboardEvent) {
    if (e.key === "Enter") {
      (e.target as HTMLInputElement).blur();
    }
  }

  return (
    <div class={styles.dashHeader}>
      {editing ? (
        <input
          ref={inputRef}
          class={styles.nameInput}
          type="text"
          defaultValue={name}
          onBlur={handleNameBlur}
          onKeyDown={handleNameKeyDown}
        />
      ) : (
        <span
          class={styles.dashName}
          onClick={handleNameClick}
          style={editable && editMode ? { cursor: "pointer" } : undefined}
        >
          {name || "Untitled Dashboard"}
        </span>
      )}

      <div class={styles.spacer} />

      <div class={styles.controls}>
        <select
          class={styles.select}
          value={from}
          onChange={(e) =>
            onTimeChange((e.target as HTMLSelectElement).value)
          }
        >
          {TIME_PRESETS.map((p) => (
            <option key={p.value} value={p.value}>
              {p.label}
            </option>
          ))}
        </select>

        <div class={styles.refreshWrap}>
          <select
            class={styles.select}
            value={refreshInterval}
            onChange={(e) =>
              onRefreshIntervalChange(
                Number((e.target as HTMLSelectElement).value),
              )
            }
          >
            {REFRESH_OPTIONS.map((r) => (
              <option key={r.value} value={r.value}>
                {r.label}
              </option>
            ))}
          </select>
          {refreshInterval > 0 && <span class={styles.refreshDot} />}
        </div>

        {!editMode && onToggleEdit && (
          <button
            type="button"
            class={styles.iconBtn}
            onClick={onToggleEdit}
            title="Edit dashboard"
          >
            <Edit2 size={14} />
          </button>
        )}

        {editMode && (
          <>
            {onToggleVariables && (
              <button
                type="button"
                class={showVariableEditor ? styles.btnPrimary : styles.btnSecondary}
                onClick={onToggleVariables}
              >
                Variables
              </button>
            )}
            {onSave && (
              <button
                type="button"
                class={styles.btnPrimary}
                onClick={onSave}
                disabled={saveDisabled}
              >
                Save
              </button>
            )}
            {onDiscard && (
              <button
                type="button"
                class={styles.btnSecondary}
                onClick={onDiscard}
              >
                Discard
              </button>
            )}
            {onDelete && (
              <button
                type="button"
                class={styles.btnDanger}
                onClick={onDelete}
              >
                Delete
              </button>
            )}
          </>
        )}
      </div>
    </div>
  );
}
