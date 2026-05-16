import { createRoot } from "react-dom/client";
import { App } from "./App";
import "./styles/globals.css";

async function bootstrap() {
  if (import.meta.env.VITE_DEMO === "1") {
    const { installDemo } = await import("./demo/install");
    installDemo();
  }
  // No StrictMode yet: it double-invokes effects, which would change behavior
  // while the module-scope query lifecycle still has known issues.
  createRoot(document.getElementById("app")!).render(<App />);
}

void bootstrap();
