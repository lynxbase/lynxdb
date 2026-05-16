import { create } from "zustand";

const STORAGE_KEY = "lynxdb_token";

interface AuthState {
  /** API token. Empty string = not authenticated. */
  token: string;
  /** Whether a 401 was received (triggers login prompt even with a token). */
  authRequired: boolean;
}

export const useAuthStore = create<AuthState>(() => ({
  token: localStorage.getItem(STORAGE_KEY) ?? "",
  authRequired: false,
}));

export function setToken(value: string): void {
  if (value) {
    localStorage.setItem(STORAGE_KEY, value);
  } else {
    localStorage.removeItem(STORAGE_KEY);
  }
  useAuthStore.setState({ token: value, authRequired: false });
}

export function clearToken(): void {
  localStorage.removeItem(STORAGE_KEY);
  useAuthStore.setState({ token: "", authRequired: true });
}

/** Build headers object with auth token if available. */
export function authHeaders(): Record<string, string> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };
  const { token } = useAuthStore.getState();
  if (token) {
    headers["Authorization"] = `Bearer ${token}`;
  }
  return headers;
}

/**
 * Handle a fetch response -- if 401, mark auth as required.
 * Returns true if the response is a 401 (caller should stop processing).
 */
export function handleAuthError(resp: Response): boolean {
  if (resp.status === 401) {
    useAuthStore.setState({ authRequired: true });
    return true;
  }
  return false;
}
