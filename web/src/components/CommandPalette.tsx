import { useRef, useEffect, useState } from "preact/hooks";
import { batch } from "@preact/signals";
import { route } from "preact-router";
import type { ComponentType } from "preact";
import {
  Search,
  BookmarkCheck,
  Settings,
  Play,
  Repeat,
  Sun,
  Moon,
  PanelLeftClose,
  Keyboard,
  Clock,
} from "lucide-preact";
import { toggleTheme, theme } from "../stores/ui";
import { queryHistory } from "../stores/queryHistory";
import {
  SHORTCUTS,
  formatShortcut,
  paletteOpen,
  helpOverlayOpen,
  paletteQuery,
} from "../utils/keyboard";
import type { ShortcutDef } from "../utils/keyboard";
import styles from "./CommandPalette.module.css";

type PaletteItem = {
  id: string;
  label: string;
  section: "navigation" | "commands" | "recent";
  icon: ComponentType<{ size?: string | number }>;
  shortcut?: ShortcutDef;
  action: () => void;
};

const SECTION_LABELS: Record<PaletteItem["section"], string> = {
  navigation: "Navigation",
  commands: "Commands",
  recent: "Recent Queries",
};

const SECTION_ORDER: PaletteItem["section"][] = ["navigation", "commands", "recent"];

function filterItems(items: PaletteItem[], q: string): PaletteItem[] {
  if (!q.trim()) return items;
  const lower = q.toLowerCase();
  return items
    .map((item) => {
      const label = item.label.toLowerCase();
      if (label.startsWith(lower)) return { item, score: 3 };
      if (label.split(/\s+/).some((w) => w.startsWith(lower)))
        return { item, score: 2 };
      if (label.includes(lower)) return { item, score: 1 };
      return null;
    })
    .filter((x): x is { item: PaletteItem; score: number } => x !== null)
    .sort((a, b) => b.score - a.score)
    .map((x) => x.item);
}

function truncate(text: string, max: number): string {
  return text.length > max ? text.slice(0, max) + "\u2026" : text;
}

export function CommandPalette() {
  const inputRef = useRef<HTMLInputElement>(null);
  const [search, setSearch] = useState("");
  const [selected, setSelected] = useState(0);

  const navigationItems: PaletteItem[] = [
    {
      id: "nav-search",
      label: "Search",
      section: "navigation",
      icon: Search,
      shortcut: SHORTCUTS.focusSearch,
      action: () => route("/"),
    },
    {
      id: "nav-queries",
      label: "Saved Queries",
      section: "navigation",
      icon: BookmarkCheck,
      action: () => route("/queries"),
    },
    {
      id: "nav-settings",
      label: "Settings",
      section: "navigation",
      icon: Settings,
      action: () => route("/settings"),
    },
  ];

  const commandItems: PaletteItem[] = [
    {
      id: "cmd-run",
      label: "Run query",
      section: "commands",
      icon: Play,
      shortcut: SHORTCUTS.runQuery,
      action: () => route("/"),
    },
    {
      id: "cmd-tail",
      label: "Toggle live tail",
      section: "commands",
      icon: Repeat,
      shortcut: SHORTCUTS.toggleTail,
      action: () => route("/"),
    },
    {
      id: "cmd-theme",
      label: `Toggle theme (${theme.value === "light" ? "dark" : "light"})`,
      section: "commands",
      icon: theme.value === "light" ? Moon : Sun,
      action: () => toggleTheme(),
    },
    {
      id: "cmd-sidebar",
      label: "Toggle sidebar",
      section: "commands",
      icon: PanelLeftClose,
      shortcut: SHORTCUTS.toggleSidebar,
      action: () => route("/"),
    },
    {
      id: "cmd-help",
      label: "Keyboard shortcuts",
      section: "commands",
      icon: Keyboard,
      shortcut: SHORTCUTS.openHelp,
      action: () => {
        batch(() => {
          paletteOpen.value = false;
          helpOverlayOpen.value = true;
        });
      },
    },
  ];

  const recentItems: PaletteItem[] = queryHistory.value.slice(0, 10).map(
    (q, i) => ({
      id: `recent-${i}`,
      label: truncate(q, 60),
      section: "recent" as const,
      icon: Clock,
      action: () => {
        paletteQuery.value = q;
        route("/");
      },
    }),
  );

  const allItems = [...navigationItems, ...commandItems, ...recentItems];
  const filtered = filterItems(allItems, search);

  // Reset state on open
  useEffect(() => {
    if (paletteOpen.value) {
      setSearch("");
      setSelected(0);
      // Auto-focus the input after rendering
      requestAnimationFrame(() => {
        inputRef.current?.focus();
      });
    }
  }, [paletteOpen.value]);

  // Clamp selected index when filtered list changes
  useEffect(() => {
    if (selected >= filtered.length) {
      setSelected(Math.max(0, filtered.length - 1));
    }
  }, [filtered.length, selected]);

  if (!paletteOpen.value) return null;

  const handleKeyDown = (e: KeyboardEvent) => {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setSelected((prev) => (prev + 1) % filtered.length);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setSelected((prev) => (prev - 1 + filtered.length) % filtered.length);
    } else if (e.key === "Enter") {
      e.preventDefault();
      if (filtered[selected]) {
        filtered[selected].action();
        paletteOpen.value = false;
      }
    } else if (e.key === "Escape") {
      e.preventDefault();
      paletteOpen.value = false;
    }
  };

  const handleBackdropClick = () => {
    paletteOpen.value = false;
  };

  // Group filtered items by section (preserving section order)
  const grouped = SECTION_ORDER.map((section) => ({
    section,
    label: SECTION_LABELS[section],
    items: filtered.filter((item) => item.section === section),
  })).filter((g) => g.items.length > 0);

  // Compute flat index offset for each group
  let flatIndex = 0;

  return (
    <div class={styles.backdrop} onClick={handleBackdropClick}>
      <div
        class={styles.palette}
        onClick={(e: Event) => e.stopPropagation()}
        onKeyDown={handleKeyDown}
      >
        <input
          ref={inputRef}
          type="text"
          class={styles.searchInput}
          placeholder="Type a command..."
          value={search}
          onInput={(e: Event) => {
            setSearch((e.target as HTMLInputElement).value);
            setSelected(0);
          }}
        />
        <div class={styles.results}>
          {filtered.length === 0 && (
            <div class={styles.empty}>No matches</div>
          )}
          {grouped.map((group) => {
            const groupStartIndex = flatIndex;
            const groupItems = group.items.map((item, i) => {
              const itemIndex = groupStartIndex + i;
              return (
                <div
                  key={item.id}
                  class={`${styles.item}${itemIndex === selected ? ` ${styles.selected}` : ""}`}
                  onClick={() => {
                    item.action();
                    paletteOpen.value = false;
                  }}
                  onMouseEnter={() => setSelected(itemIndex)}
                >
                  <item.icon size={16} />
                  <span class={styles.itemLabel}>{item.label}</span>
                  {item.shortcut && (
                    <span class={styles.itemShortcut}>
                      {formatShortcut(item.shortcut)}
                    </span>
                  )}
                </div>
              );
            });
            flatIndex += group.items.length;
            return (
              <div key={group.section}>
                <div class={styles.sectionHeader}>{group.label}</div>
                {groupItems}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
