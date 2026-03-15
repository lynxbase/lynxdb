import { useEffect, useRef, useCallback } from "preact/hooks";
import styles from "./flow.module.css";

interface FieldCommandMenuProps {
  field: string;
  fieldType?: string;
  anchorRect: DOMRect;
  onInsertCommand: (template: string) => void;
  onClose: () => void;
}

interface CommandEntry {
  label: string;
  template: string;
}

function getCommands(field: string, fieldType?: string): CommandEntry[] {
  const f = field;
  const isNumeric = fieldType === "int" || fieldType === "float" || fieldType === "flt";

  if (isNumeric) {
    return [
      { label: `where ${f}>0`, template: `| where ${f}>0` },
      { label: `stats avg(${f})`, template: `| stats avg(${f})` },
      { label: `stats sum(${f})`, template: `| stats sum(${f})` },
      { label: `stats max(${f})`, template: `| stats max(${f})` },
      { label: `sort -${f}`, template: `| sort -${f}` },
      { label: `bin ${f}`, template: `| bin ${f}` },
      { label: `table ${f}`, template: `| table ${f}` },
    ];
  }

  return [
    { label: `where ${f}="..."`, template: `| where ${f}=""` },
    { label: `stats count by ${f}`, template: `| stats count by ${f}` },
    { label: `top ${f}`, template: `| top ${f}` },
    { label: `rex field=${f}`, template: `| rex field=${f} "(?P<val>\\S+)"` },
    { label: `dedup ${f}`, template: `| dedup ${f}` },
    { label: `sort ${f}`, template: `| sort ${f}` },
    { label: `table ${f}`, template: `| table ${f}` },
  ];
}

export function FieldCommandMenu({
  field,
  fieldType,
  anchorRect,
  onInsertCommand,
  onClose,
}: FieldCommandMenuProps) {
  const menuRef = useRef<HTMLDivElement>(null);
  const commands = getCommands(field, fieldType);

  // Position: below anchor, flip up if near bottom
  const top = anchorRect.bottom + 4;
  const left = anchorRect.left;
  const viewportHeight = window.innerHeight;
  const flipUp = top + 200 > viewportHeight;

  const style: Record<string, string> = {
    position: "fixed",
    left: `${Math.max(4, left)}px`,
    zIndex: "100",
  };
  if (flipUp) {
    style.bottom = `${viewportHeight - anchorRect.top + 4}px`;
  } else {
    style.top = `${top}px`;
  }

  // Close on outside click or Escape
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        onClose();
      }
    }
    function handleKey(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    document.addEventListener("mousedown", handleClick, true);
    document.addEventListener("keydown", handleKey, true);
    return () => {
      document.removeEventListener("mousedown", handleClick, true);
      document.removeEventListener("keydown", handleKey, true);
    };
  }, [onClose]);

  const handleItemClick = useCallback(
    (template: string) => {
      onInsertCommand(template);
      onClose();
    },
    [onInsertCommand, onClose],
  );

  return (
    <div ref={menuRef} class={styles.commandMenu} style={style}>
      {commands.map((cmd) => (
        <button
          key={cmd.template}
          type="button"
          class={styles.commandMenuItem}
          onClick={() => handleItemClick(cmd.template)}
        >
          {cmd.label}
        </button>
      ))}
    </div>
  );
}
