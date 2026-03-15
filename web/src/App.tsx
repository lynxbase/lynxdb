import Router from "preact-router";
import { Sidebar } from "./components/Sidebar";
import { AuthGate } from "./components/AuthGate";
import { SearchView } from "./views/SearchView";
import { lazy } from "./utils/lazy";
import styles from "./App.module.css";

const DashboardsView = lazy(() => import("./views/DashboardsView"));
const AlertsView = lazy(() => import("./views/AlertsView"));
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
            <DashboardsView path="/dashboards/:rest*" />
            <AlertsView path="/alerts/:rest*" />
            <QueriesView path="/queries" />
            <SettingsView path="/settings" />
          </Router>
        </main>
      </div>
    </AuthGate>
  );
}
