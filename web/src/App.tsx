import Router from "preact-router";
import { Layout } from "./components/Layout";
import { AuthGate } from "./components/AuthGate";
import { SearchView } from "./views/SearchView";
import { StatusView } from "./views/StatusView";

export function App() {
  return (
    <AuthGate>
      <Layout>
        <Router>
          <SearchView path="/" />
          <StatusView path="/status" />
        </Router>
      </Layout>
    </AuthGate>
  );
}
