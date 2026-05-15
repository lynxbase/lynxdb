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
import {
  useOverlayStore,
  setPaletteOpen,
  setHelpOverlayOpen,
} from "../utils/keyboard";
import {
  Sidebar as SidebarPrimitive,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarSeparator,
} from "./ui/sidebar";

const NAV_ITEMS = [
  { path: "/", icon: Search, label: "Search" },
  { path: "/queries", icon: BookmarkCheck, label: "Queries" },
  { path: "/status", icon: Activity, label: "Status" },
  { path: "/settings", icon: Settings, label: "Settings" },
] as const;

export function Sidebar() {
  const location = useLocation();
  const url = location.pathname;
  const theme = useThemeStore((s) => s.theme);
  const token = useAuthStore((s) => s.token);
  // Subscribe to re-render on overlay changes
  useOverlayStore();

  return (
    <SidebarPrimitive
      collapsible="icon"
      className="border-sidebar-border"
    >
      <SidebarHeader>
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton size="lg" asChild tooltip="LynxDB">
              <Link to="/">
                <img
                  src={`${import.meta.env.BASE_URL || "/"}lynxdb-icon.png`}
                  alt="LynxDB"
                  className="size-5 shrink-0 object-contain"
                />
                <span className="truncate font-semibold tracking-tight">
                  LynxDB
                </span>
              </Link>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupContent>
            <SidebarMenu>
              {NAV_ITEMS.map(({ path, icon: Icon, label }) => (
                <SidebarMenuItem key={path}>
                  <SidebarMenuButton
                    asChild
                    isActive={url === path}
                    tooltip={label}
                  >
                    <Link to={path}>
                      <Icon className="size-4" />
                      <span>{label}</span>
                    </Link>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
      <SidebarFooter>
        <SidebarSeparator />
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton
              onClick={toggleTheme}
              tooltip={
                theme === "dark"
                  ? "Switch to light mode"
                  : "Switch to dark mode"
              }
              title={
                theme === "dark"
                  ? "Switch to light mode"
                  : "Switch to dark mode"
              }
            >
              {theme === "dark" ? (
                <Sun className="size-4" />
              ) : (
                <Moon className="size-4" />
              )}
              <span>
                {theme === "dark" ? "Light mode" : "Dark mode"}
              </span>
            </SidebarMenuButton>
          </SidebarMenuItem>
          <SidebarMenuItem>
            <SidebarMenuButton
              onClick={() => {
                setPaletteOpen(false);
                setHelpOverlayOpen(true);
              }}
              tooltip="Keyboard shortcuts (?)"
            >
              <HelpCircle className="size-4" />
              <span>Shortcuts</span>
            </SidebarMenuButton>
          </SidebarMenuItem>
          {token && (
            <SidebarMenuItem>
              <SidebarMenuButton
                onClick={clearToken}
                tooltip="Sign out"
              >
                <LogOut className="size-4" />
                <span>Sign out</span>
              </SidebarMenuButton>
            </SidebarMenuItem>
          )}
        </SidebarMenu>
      </SidebarFooter>
    </SidebarPrimitive>
  );
}
