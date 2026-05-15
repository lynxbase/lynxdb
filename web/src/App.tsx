import { lazy, Suspense } from "react";
import { BrowserRouter, Routes, Route } from "react-router";
import { Sidebar } from "./components/Sidebar";
import { AuthGate } from "./components/AuthGate";
import { CommandPalette } from "./components/CommandPalette";
import { HelpOverlay } from "./components/HelpOverlay";
import { uiBase } from "./utils/base";
import { SidebarProvider, SidebarInset } from "./components/ui/sidebar";
import { Toaster } from "./components/ui/sonner";
import { useKeyboardShortcuts } from "./hooks/useKeyboardShortcuts";
import {
  useOverlayStore,
  setPaletteOpen,
  setHelpOverlayOpen,
} from "./utils/keyboard";

const SearchView = lazy(() =>
  import("./views/SearchView").then((m) => ({ default: m.SearchView })),
);
const QueriesView = lazy(() => import("./views/QueriesView"));
const SettingsView = lazy(() => import("./views/SettingsView"));
const StatusView = lazy(() =>
  import("./views/StatusView").then((m) => ({ default: m.StatusView })),
);

/**
 * App-shell keyboard shortcuts (command palette, help). Registered here so
 * they work regardless of the active route or lazy-loaded chunk.
 */
function ShellShortcuts() {
  useKeyboardShortcuts({
    onOpenPalette: () => {
      setHelpOverlayOpen(false);
      setPaletteOpen(!useOverlayStore.getState().paletteOpen);
    },
    onOpenHelp: () => {
      setPaletteOpen(false);
      setHelpOverlayOpen(!useOverlayStore.getState().helpOverlayOpen);
    },
  });
  return null;
}

export function App() {
  return (
    <AuthGate>
      <BrowserRouter basename={uiBase || "/"}>
        <SidebarProvider defaultOpen={false}>
          <ShellShortcuts />
          <Sidebar />
          <SidebarInset>
            <Suspense fallback={null}>
              <Routes>
                <Route path="/" element={<SearchView />} />
                <Route path="/queries" element={<QueriesView />} />
                <Route path="/status" element={<StatusView />} />
                <Route path="/settings" element={<SettingsView />} />
              </Routes>
            </Suspense>
          </SidebarInset>
          <CommandPalette />
          <HelpOverlay />
          <Toaster position="bottom-right" richColors closeButton />
        </SidebarProvider>
      </BrowserRouter>
    </AuthGate>
  );
}
