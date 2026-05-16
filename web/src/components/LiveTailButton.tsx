import { formatShortcut, SHORTCUTS } from "../utils/keyboard";
import { Button } from "./ui/button";

interface LiveTailButtonProps {
  active: boolean;
  onToggle: () => void;
}

/**
 * Toggle button that activates / deactivates the live tail SSE stream.
 *
 * When active, renders with a green pulsing dot indicator.
 */
export function LiveTailButton({ active, onToggle }: LiveTailButtonProps) {
  return (
    <Button
      type="button"
      variant="outline"
      size="sm"
      className={
        active
          ? "gap-1.5 shrink-0 whitespace-nowrap border-[var(--success)] bg-[var(--success)]/10 text-[var(--success)] hover:bg-[var(--success)]/15 hover:text-[var(--success)]"
          : "gap-1.5 shrink-0 whitespace-nowrap text-muted-foreground"
      }
      onClick={onToggle}
      aria-pressed={active}
      aria-label={active ? "Stop live tail" : "Start live tail"}
      title={
        active
          ? `Stop live tail (${formatShortcut(SHORTCUTS.toggleTail)})`
          : `Start live tail (${formatShortcut(SHORTCUTS.toggleTail)})`
      }
    >
      {active && (
        <span
          className="inline-block size-2 shrink-0 rounded-full bg-[var(--success)] animate-[pulse_1.5s_ease-in-out_infinite] motion-reduce:animate-none"
          aria-hidden="true"
        />
      )}
      Live Tail
    </Button>
  );
}
