export function cssVar(name: string): string {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

export function chartAxisFont(): string {
  return '11px Inter, "Helvetica Neue", Arial, sans-serif';
}
