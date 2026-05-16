import {
  SHORTCUTS,
  formatShortcut,
  useOverlayStore,
  setHelpOverlayOpen,
} from "../utils/keyboard";
import type { ShortcutDef } from "../utils/keyboard";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from "./ui/dialog";
import { Kbd } from "./ui/kbd";

type ShortcutRow = {
  def: ShortcutDef;
  label: string;
};

type ShortcutGroup = {
  title: string;
  items: ShortcutRow[];
};

const GROUPS: ShortcutGroup[] = [
  {
    title: "General",
    items: [
      { def: SHORTCUTS.openPalette, label: "Command palette" },
      { def: SHORTCUTS.openHelp, label: "Keyboard shortcuts" },
    ],
  },
  {
    title: "Query",
    items: [
      { def: SHORTCUTS.runQuery, label: "Run query" },
      { def: SHORTCUTS.focusEditor, label: "Focus editor" },
      { def: SHORTCUTS.focusSearch, label: "Focus editor (alt)" },
      { def: SHORTCUTS.historyUp, label: "Previous query" },
      { def: SHORTCUTS.historyDown, label: "Next query" },
    ],
  },
  {
    title: "Navigation",
    items: [
      { def: SHORTCUTS.toggleSidebar, label: "Toggle field sidebar" },
      { def: SHORTCUTS.toggleTail, label: "Toggle live tail" },
    ],
  },
  {
    title: "Panels",
    items: [{ def: SHORTCUTS.closePanel, label: "Close topmost panel" }],
  },
];

export function HelpOverlay() {
  const helpOverlayOpen = useOverlayStore((s) => s.helpOverlayOpen);

  return (
    <Dialog
      open={helpOverlayOpen}
      onOpenChange={(open) => {
        if (!open) setHelpOverlayOpen(false);
      }}
    >
      <DialogContent className="max-w-[600px] rounded-md sm:max-w-[600px]">
        <DialogHeader>
          <DialogTitle>Keyboard Shortcuts</DialogTitle>
          <DialogDescription className="sr-only">
            A list of all available keyboard shortcuts.
          </DialogDescription>
        </DialogHeader>
        <div className="grid grid-cols-1 gap-6 sm:grid-cols-2">
          {GROUPS.map((group) => (
            <div key={group.title} className="flex flex-col gap-1">
              <div className="mb-1 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                {group.title}
              </div>
              {group.items.map((item) => (
                <div
                  key={item.label}
                  className="flex items-center justify-between py-1"
                >
                  <span className="text-[0.8125rem] text-foreground">
                    {item.label}
                  </span>
                  <Kbd>{formatShortcut(item.def)}</Kbd>
                </div>
              ))}
            </div>
          ))}
        </div>
      </DialogContent>
    </Dialog>
  );
}
