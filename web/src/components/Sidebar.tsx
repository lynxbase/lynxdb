import type { ComponentType } from "react";
import { Link, useLocation } from "react-router";
import {
  Search,
  BookmarkCheck,
  Activity,
  Settings,
  Sun,
  Moon,
  LogOut,
  HelpCircle,
} from "lucide-react";
import { useThemeStore, toggleTheme } from "../stores/ui";
import { useAuthStore, clearToken } from "../stores/auth";
import { setPaletteOpen, setHelpOverlayOpen } from "../utils/keyboard";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "./ui/tooltip";
import { cn } from "@/lib/utils";

const NAV_ITEMS = [
  { path: "/", icon: Search, label: "Search" },
  { path: "/queries", icon: BookmarkCheck, label: "Queries" },
  { path: "/status", icon: Activity, label: "Status" },
  { path: "/settings", icon: Settings, label: "Settings" },
] as const;

function RailButton({
  icon: Icon,
  label,
  active,
  onClick,
  to,
}: {
  icon: ComponentType<{ className?: string }>;
  label: string;
  active?: boolean;
  onClick?: () => void;
  to?: string;
}) {
  const cls = cn(
    "flex size-9 items-center justify-center rounded-md text-muted-foreground transition-colors",
    "hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
    "focus-visible:outline-2 focus-visible:outline-offset-1 focus-visible:outline-ring",
    active && "bg-sidebar-accent text-primary",
  );
  const inner = <Icon className="size-[18px]" />;
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        {to ? (
          <Link to={to} className={cls} aria-label={label}>
            {inner}
          </Link>
        ) : (
          <button
            type="button"
            className={cls}
            onClick={onClick}
            aria-label={label}
            title={label}
          >
            {inner}
          </button>
        )}
      </TooltipTrigger>
      <TooltipContent side="right">{label}</TooltipContent>
    </Tooltip>
  );
}

export function Sidebar() {
  const url = useLocation().pathname;
  const theme = useThemeStore((s) => s.theme);
  const token = useAuthStore((s) => s.token);
  const isDark = theme === "dark";

  return (
    <aside className="flex h-full w-14 shrink-0 flex-col items-center border-r border-sidebar-border bg-sidebar py-3">
      <Link
        to="/"
        aria-label="LynxDB"
        className="mb-3 flex size-9 items-center justify-center rounded-md focus-visible:outline-2 focus-visible:outline-ring"
      >
        <img
          src={`${import.meta.env.BASE_URL || "/"}lynxdb-icon.png`}
          alt="LynxDB"
          className="size-6 object-contain"
        />
      </Link>

      <nav className="flex flex-1 flex-col items-center gap-1">
        {NAV_ITEMS.map(({ path, icon, label }) => (
          <RailButton
            key={path}
            icon={icon}
            label={label}
            to={path}
            active={url === path}
          />
        ))}
      </nav>

      <div className="flex flex-col items-center gap-1">
        <RailButton
          icon={isDark ? Sun : Moon}
          label={isDark ? "Switch to light mode" : "Switch to dark mode"}
          onClick={toggleTheme}
        />
        <RailButton
          icon={HelpCircle}
          label="Keyboard shortcuts"
          onClick={() => {
            setPaletteOpen(false);
            setHelpOverlayOpen(true);
          }}
        />
        {token && (
          <RailButton icon={LogOut} label="Sign out" onClick={clearToken} />
        )}
      </div>
    </aside>
  );
}
