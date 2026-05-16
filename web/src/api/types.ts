/**
 * Shared API response types.
 * This module is the canonical location for TypeScript interfaces
 * used across the API layer.
 */

/** Standard error shape returned by LynxDB API. */
export interface APIError {
  code: string;
  message: string;
  suggestion?: string;
}

/** Error envelope as parsed from a non-OK API response body. */
export interface APIErrorResponse {
  error?: Partial<APIError>;
  data?: { error?: string };
}

/** Extract the most specific human-readable message from an error response. */
export function apiErrorMessage(
  body: APIErrorResponse,
  fallback: string,
): string {
  return body.error?.message || body.data?.error || fallback;
}
