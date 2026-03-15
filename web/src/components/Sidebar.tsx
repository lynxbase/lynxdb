import { useRouter } from "preact-router";
import {
  Search,
  LayoutDashboard,
  Bell,
  BookmarkCheck,
  Settings,
  LogOut,
  Sun,
  Moon,
} from "lucide-preact";
import { theme, toggleTheme } from "../stores/ui";
import { token, clearToken } from "../stores/auth";
import styles from "./Sidebar.module.css";

const NAV_ITEMS = [
  { path: "/", icon: Search, label: "Search" },
  { path: "/dashboards", icon: LayoutDashboard, label: "Dashboards" },
  { path: "/alerts", icon: Bell, label: "Alerts" },
  { path: "/queries", icon: BookmarkCheck, label: "Saved Queries" },
  { path: "/settings", icon: Settings, label: "Settings" },
] as const;

function isActive(url: string, path: string): boolean {
  // Exact match for leaf routes
  if (path === "/" || path === "/queries" || path === "/settings") {
    return url === path;
  }
  // Prefix match for routes with sub-paths
  return url === path || url.startsWith(path + "/");
}

export function Sidebar() {
  const [routerState] = useRouter();
  const url = routerState?.url ?? "/";

  return (
    <nav class={styles.sidebar}>
      <div class={styles.top}>
        <a href="/" class={styles.logo}>
          <img src="/favicon.svg" alt="" class={styles.logoIcon} />
          <span class={styles.logoText}>LynxDB</span>
        </a>
        {NAV_ITEMS.map(({ path, icon: Icon, label }) => (
          <a
            key={path}
            href={path}
            class={`${styles.navItem} ${isActive(url, path) ? styles.active : ""}`}
            title={label}
          >
            <Icon size={20} />
            <span class={styles.navLabel}>{label}</span>
          </a>
        ))}
      </div>
      <div class={styles.bottom}>
        <button
          type="button"
          class={styles.navItem}
          onClick={toggleTheme}
          title={theme.value === "dark" ? "Switch to light mode" : "Switch to dark mode"}
        >
          {theme.value === "dark" ? <Sun size={20} /> : <Moon size={20} />}
          <span class={styles.navLabel}>
            {theme.value === "dark" ? "Light mode" : "Dark mode"}
          </span>
        </button>
        {token.value && (
          <button
            type="button"
            class={styles.navItem}
            onClick={clearToken}
            title="Sign out"
          >
            <LogOut size={20} />
            <span class={styles.navLabel}>Sign out</span>
          </button>
        )}
      </div>
    </nav>
  );
}
