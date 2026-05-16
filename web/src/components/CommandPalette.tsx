import { useCallback } from "react";
import { useNavigate } from "react-router";
import type { ComponentType } from "react";
import {
  Search,
  BookmarkCheck,
  Activity,
  Settings,
  Play,
  Repeat,
  Sun,
  Moon,
  PanelLeftClose,
  Keyboard,
  Clock,
} from "lucide-react";
import { useThemeStore, toggleTheme } from "../stores/ui";
import { useQueryHistoryStore } from "../stores/queryHistory";
import {
  SHORTCUTS,
  formatShortcut,
  useOverlayStore,
  setPaletteOpen,
  setHelpOverlayOpen,
  setPaletteQuery,
} from "../utils/keyboard";
import type { ShortcutDef } from "../utils/keyboard";
import {
  CommandDialog,
  CommandInput,
  CommandList,
  CommandEmpty,
  CommandGroup,
  CommandItem,
  CommandShortcut,
} from "./ui/command";

type PaletteItem = {
  id: string;
  label: string;
  section: "navigation" | "commands" | "recent";
  icon: ComponentType<{ size?: string | number; className?: string }>;
  shortcut?: ShortcutDef;
  action: () => void;
};

function truncate(text: string, max: number): string {
  return text.length > max ? text.slice(0, max) + "…" : text;
}

export function CommandPalette() {
  const navigate = useNavigate();

  const paletteOpen = useOverlayStore((s) => s.paletteOpen);
  const theme = useThemeStore((s) => s.theme);
  const queryHistory = useQueryHistoryStore((s) => s.queryHistory);

  const close = useCallback(() => setPaletteOpen(false), []);

  const navigationItems: PaletteItem[] = [
    {
      id: "nav-search",
      label: "Search",
      section: "navigation",
      icon: Search,
      shortcut: SHORTCUTS.focusSearch,
      action: () => navigate("/"),
    },
    {
      id: "nav-queries",
      label: "Saved Queries",
      section: "navigation",
      icon: BookmarkCheck,
      action: () => navigate("/queries"),
    },
    {
      id: "nav-status",
      label: "Status",
      section: "navigation",
      icon: Activity,
      action: () => navigate("/status"),
    },
    {
      id: "nav-settings",
      label: "Settings",
      section: "navigation",
      icon: Settings,
      action: () => navigate("/settings"),
    },
  ];

  const commandItems: PaletteItem[] = [
    {
      id: "cmd-run",
      label: "Run query",
      section: "commands",
      icon: Play,
      shortcut: SHORTCUTS.runQuery,
      action: () => navigate("/"),
    },
    {
      id: "cmd-tail",
      label: "Toggle live tail",
      section: "commands",
      icon: Repeat,
      shortcut: SHORTCUTS.toggleTail,
      action: () => navigate("/"),
    },
    {
      id: "cmd-theme",
      label: `Toggle theme (${theme === "light" ? "dark" : "light"})`,
      section: "commands",
      icon: theme === "light" ? Moon : Sun,
      action: () => toggleTheme(),
    },
    {
      id: "cmd-sidebar",
      label: "Toggle sidebar",
      section: "commands",
      icon: PanelLeftClose,
      shortcut: SHORTCUTS.toggleSidebar,
      action: () => navigate("/"),
    },
    {
      id: "cmd-help",
      label: "Keyboard shortcuts",
      section: "commands",
      icon: Keyboard,
      shortcut: SHORTCUTS.openHelp,
      action: () => {
        setPaletteOpen(false);
        setHelpOverlayOpen(true);
      },
    },
  ];

  const recentItems: PaletteItem[] = queryHistory
    .slice(0, 10)
    .map((q: string, i: number) => ({
      id: `recent-${i}`,
      label: truncate(q, 60),
      section: "recent" as const,
      icon: Clock,
      action: () => {
        setPaletteQuery(q);
        navigate("/");
      },
    }));

  const handleSelect = useCallback(
    (id: string) => {
      const allItems = [...navigationItems, ...commandItems, ...recentItems];
      const item = allItems.find((i) => i.id === id);
      if (item) {
        item.action();
        close();
      }
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [theme, queryHistory, close],
  );

  return (
    <CommandDialog
      open={paletteOpen}
      onOpenChange={(open) => {
        if (!open) close();
      }}
      title="Command Palette"
      description="Search for a command to run..."
      showCloseButton={false}
    >
      <CommandInput placeholder="Type a command..." />
      <CommandList>
        <CommandEmpty>No matches</CommandEmpty>

        <CommandGroup heading="Navigation">
          {navigationItems.map((item) => (
            <CommandItem
              key={item.id}
              value={item.id + " " + item.label}
              onSelect={() => handleSelect(item.id)}
            >
              <item.icon className="size-4" />
              <span>{item.label}</span>
              {item.shortcut && (
                <CommandShortcut>
                  {formatShortcut(item.shortcut)}
                </CommandShortcut>
              )}
            </CommandItem>
          ))}
        </CommandGroup>

        <CommandGroup heading="Commands">
          {commandItems.map((item) => (
            <CommandItem
              key={item.id}
              value={item.id + " " + item.label}
              onSelect={() => handleSelect(item.id)}
            >
              <item.icon className="size-4" />
              <span>{item.label}</span>
              {item.shortcut && (
                <CommandShortcut>
                  {formatShortcut(item.shortcut)}
                </CommandShortcut>
              )}
            </CommandItem>
          ))}
        </CommandGroup>

        {recentItems.length > 0 && (
          <CommandGroup heading="Recent Queries">
            {recentItems.map((item) => (
              <CommandItem
                key={item.id}
                value={item.id + " " + item.label}
                onSelect={() => handleSelect(item.id)}
              >
                <item.icon className="size-4" />
                <span className="truncate">{item.label}</span>
              </CommandItem>
            ))}
          </CommandGroup>
        )}
      </CommandList>
    </CommandDialog>
  );
}
