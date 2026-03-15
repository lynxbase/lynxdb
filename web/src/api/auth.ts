import { signal } from "@preact/signals";

const STORAGE_KEY = "lynxdb_token";

/** Reactive token signal. Empty string = not authenticated. */
export const token = signal(localStorage.getItem(STORAGE_KEY) ?? "");

/** Whether a 401 was received (triggers login prompt even with a stored token). */
export const authRequired = signal(false);

export function setToken(value: string): void {
  token.value = value;
  authRequired.value = false;
  if (value) {
    localStorage.setItem(STORAGE_KEY, value);
  } else {
    localStorage.removeItem(STORAGE_KEY);
  }
}

export function clearToken(): void {
  setToken("");
  authRequired.value = true;
}

/** Build headers object with auth token if available. */
export function authHeaders(): Record<string, string> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };
  if (token.value) {
    headers["Authorization"] = `Bearer ${token.value}`;
  }
  return headers;
}

/**
 * Handle a fetch response -- if 401, mark auth as required.
 * Returns true if the response is a 401 (caller should stop processing).
 */
export function handleAuthError(resp: Response): boolean {
  if (resp.status === 401) {
    authRequired.value = true;
    return true;
  }
  return false;
}
