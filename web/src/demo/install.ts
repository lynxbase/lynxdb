/* eslint-disable @typescript-eslint/no-explicit-any */
/**
 * Demo network layer. Patches fetch + EventSource so the whole UI runs
 * against in-memory data with zero backend. Loaded only when VITE_DEMO=1.
 */
import * as M from "./mock";

let SAVED = [...M.SAVED_QUERIES];

function json(body: unknown, init?: ResponseInit): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "Content-Type": "application/json" },
    ...init,
  });
}

async function route(
  url: string,
  method: string,
  body: any,
): Promise<Response | null> {
  const u = new URL(url, location.origin);
  const p = u.pathname;
  const qp = u.searchParams;

  if (p === "/health") return json({ status: "ok" });
  if (p === "/api/v1/status") return json({ data: M.status() });
  if (p === "/api/v1/fields") return json({ data: M.fieldCatalog() });
  if (/^\/api\/v1\/fields\/.+\/values$/.test(p)) {
    return json({
      data: {
        values: [
          { value: "error", count: 84 },
          { value: "warn", count: 142 },
          { value: "info", count: 160 },
          { value: "debug", count: 34 },
        ],
      },
    });
  }
  if (p === "/api/v1/indexes") return json({ data: { indexes: M.INDEXES } });
  if (p === "/api/v1/views" && method === "GET")
    return json({ data: { views: M.VIEWS } });
  if (p.startsWith("/api/v1/views/"))
    return json({ data: { ...M.VIEWS[0], columns: [], retention: "90d" } });
  if (p === "/api/v1/histogram") {
    return json({
      data: M.histogram(qp.get("group_by") != null),
    });
  }
  if (p === "/api/v1/query/explain")
    return json({ data: M.explain(qp.get("q") ?? "") });

  if (p === "/api/v1/query" && method === "POST") {
    const r = M.runQuery(body ?? {});
    return json(r);
  }
  if (p === "/api/v1/query/stream" && method === "POST") {
    const r = M.runQuery({ ...(body ?? {}), limit: 5000 });
    const rows =
      (r.data as any).type === "events"
        ? (r.data as any).events
        : (r.data as any).rows;
    const lines =
      rows.map((x: unknown) => JSON.stringify(x)).join("\n") +
      `\n${JSON.stringify({ __meta: r.meta })}\n`;
    return new Response(lines, {
      status: 200,
      headers: { "Content-Type": "application/x-ndjson" },
    });
  }

  if (p === "/api/v1/queries" && method === "GET")
    return json({ data: SAVED });
  if (p === "/api/v1/queries" && method === "POST") {
    const sq = {
      id: `sq_${Date.now()}`,
      name: body?.name ?? "Untitled",
      q: body?.q ?? "",
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    };
    SAVED = [sq, ...SAVED];
    return json({ data: sq }, { status: 201 });
  }
  if (p.startsWith("/api/v1/queries/") && method === "DELETE") {
    const id = p.split("/").pop();
    SAVED = SAVED.filter((s) => s.id !== id);
    return new Response(null, { status: 204 });
  }

  if (p === "/api/v1/config" && method === "GET")
    return json({ data: M.CONFIG });
  if (p === "/api/v1/config" && method === "PATCH")
    return json({ data: { config: { ...M.CONFIG, ...body }, restart_required: [] } });

  if (p.startsWith("/api/v1/")) return json({ data: {} });
  return null;
}

export function installDemo(): void {
  const realFetch = window.fetch.bind(window);
  window.fetch = async (input: any, init?: RequestInit) => {
    const url = typeof input === "string" ? input : input.url;
    const method = (init?.method ?? "GET").toUpperCase();
    let body: any;
    try {
      body = init?.body ? JSON.parse(init.body as string) : undefined;
    } catch {
      body = undefined;
    }
    if (url.includes("/api/v1/") || url.endsWith("/health")) {
      const r = await route(url, method, body);
      if (r) {
        await new Promise((res) => setTimeout(res, 120));
        return r;
      }
    }
    return realFetch(input, init);
  };

  class DemoEventSource extends EventTarget {
    onopen: ((e: Event) => void) | null = null;
    onerror: ((e: Event) => void) | null = null;
    url: string;
    readyState = 1;
    private timer: number | undefined;
    constructor(url: string) {
      super();
      this.url = url;
      setTimeout(() => {
        if (url.includes("/api/v1/tail")) this.startTail();
      }, 100);
    }
    private emit(type: string, data: unknown) {
      this.dispatchEvent(
        new MessageEvent(type, { data: JSON.stringify(data) }),
      );
    }
    private startTail() {
      for (let i = 0; i < 12; i++) this.emit("result", M.tailEvent());
      this.emit("catchup_done", { count: 12 });
      this.timer = window.setInterval(
        () => this.emit("result", M.tailEvent()),
        1500,
      );
    }
    close() {
      this.readyState = 2;
      if (this.timer) clearInterval(this.timer);
    }
  }
  (window as any).EventSource = DemoEventSource;
}
