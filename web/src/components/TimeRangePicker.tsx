import { useRef, useEffect, useCallback } from "preact/hooks";
import { signal } from "@preact/signals";
import type { Signal } from "@preact/signals";
import styles from "./TimeRangePicker.module.css";

interface Preset {
  label: string;
  value: string;
}

const PRESETS: Preset[] = [
  { label: "Last 15m", value: "-15m" },
  { label: "Last 1h", value: "-1h" },
  { label: "Last 4h", value: "-4h" },
  { label: "Last 24h", value: "-24h" },
  { label: "Last 7d", value: "-7d" },
  { label: "Last 30d", value: "-30d" },
];

function labelForValue(value: string): string {
  const preset = PRESETS.find((p) => p.value === value);
  return preset ? preset.label : value;
}

interface TimeRangePickerProps {
  from: Signal<string>;
  to: Signal<string | undefined>;
}

const open = signal(false);

export function TimeRangePicker({ from, to }: TimeRangePickerProps) {
  const wrapperRef = useRef<HTMLDivElement>(null);

  const handleSelect = useCallback((value: string) => {
    from.value = value;
    to.value = undefined;
    open.value = false;
  }, [from, to]);

  // Close dropdown on outside click
  useEffect(() => {
    function onPointerDown(e: PointerEvent) {
      if (wrapperRef.current && !wrapperRef.current.contains(e.target as Node)) {
        open.value = false;
      }
    }
    document.addEventListener("pointerdown", onPointerDown, true);
    return () => document.removeEventListener("pointerdown", onPointerDown, true);
  }, []);

  return (
    <div class={styles.wrapper} ref={wrapperRef}>
      <button
        type="button"
        class={styles.trigger}
        onClick={() => { open.value = !open.value; }}
        aria-haspopup="listbox"
        aria-expanded={open.value}
      >
        <span class={styles.triggerIcon}>&#9202;</span>
        {labelForValue(from.value)}
      </button>
      {open.value && (
        <div class={styles.dropdown} role="listbox" aria-label="Time range">
          {PRESETS.map((preset) => (
            <button
              key={preset.value}
              type="button"
              role="option"
              aria-selected={from.value === preset.value}
              class={`${styles.option} ${from.value === preset.value ? styles.optionActive : ""}`}
              onClick={() => handleSelect(preset.value)}
            >
              {preset.label}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
