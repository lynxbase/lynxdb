/** Abbreviate a field type for compact display in the fields UI. */
export function typeAbbrev(t?: string): string {
  if (!t) return "";
  switch (t.toLowerCase()) {
    case "string":
      return "str";
    case "integer":
    case "int":
      return "int";
    case "float":
    case "number":
      return "flt";
    case "boolean":
    case "bool":
      return "bool";
    case "datetime":
    case "timestamp":
      return "ts";
    default:
      return t.slice(0, 3);
  }
}
