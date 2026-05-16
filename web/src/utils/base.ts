const rawBase = import.meta.env.BASE_URL || "/";

export const uiBase = rawBase.endsWith("/") ? rawBase.slice(0, -1) : rawBase;

export function uiPath(path = "/"): string {
  if (path === "" || path === "/") {
    return uiBase ? `${uiBase}/` : "/";
  }
  return `${uiBase}${path.startsWith("/") ? path : `/${path}`}`;
}
