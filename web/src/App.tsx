import Router from "preact-router";
import { Sidebar } from "./components/Sidebar";
import { AuthGate } from "./components/AuthGate";
import { CommandPalette } from "./components/CommandPalette";
import { HelpOverlay } from "./components/HelpOverlay";
import { SearchView } from "./views/SearchView";
import { lazy } from "./utils/lazy";
import styles from "./App.module.css";

const QueriesView = lazy(() => import("./views/QueriesView"));
const SettingsView = lazy(() => import("./views/SettingsView"));

export function App() {
  return (
    <AuthGate>
      <div class={styles.shell}>
        <Sidebar />
        <main class={styles.content}>
          <Router>
            <SearchView path="/" />
            <QueriesView path="/queries" />
            <SettingsView path="/settings" />
          </Router>
        </main>
        <CommandPalette />
        <HelpOverlay />
      </div>
    </AuthGate>
  );
}
