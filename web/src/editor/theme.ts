import { EditorView } from "@codemirror/view";
import { HighlightStyle, syntaxHighlighting } from "@codemirror/language";
import { tags } from "@lezer/highlight";

function makeTheme(dark: boolean) {
  return EditorView.theme(
    {
      "&": {
        backgroundColor: "var(--bg-primary)",
        color: "var(--text-primary)",
        fontSize: "14px",
        fontFamily: "var(--font-mono)",
        maxHeight: "50vh",
      },
      ".cm-scroller": {
        overflow: "auto",
      },
      ".cm-content": {
        caretColor: "var(--primary)",
        padding: "6px 12px",
        minHeight: "18px",
      },
      ".cm-cursor": {
        borderLeftColor: "var(--primary)",
      },
      "&.cm-focused .cm-selectionBackground, .cm-selectionBackground": {
        backgroundColor: dark
          ? "rgba(110, 159, 255, 0.28)"
          : "rgba(50, 116, 217, 0.14)",
      },
      ".cm-activeLine": {
        backgroundColor: "transparent",
      },
      ".cm-gutters": {
        backgroundColor: "var(--bg-secondary)",
        borderRight: "1px solid var(--border)",
        color: "var(--text-muted)",
        fontSize: "12px",
      },
      "&.cm-focused": {
        outline: "none",
      },
      ".cm-placeholder": {
        color: "var(--text-muted)",
      },
      /* Autocomplete tooltip styling */
      ".cm-tooltip.cm-tooltip-autocomplete": {
        backgroundColor: "var(--bg-primary)",
        border: "1px solid var(--border)",
        borderRadius: "var(--radius)",
        boxShadow: "none",
        overflow: "hidden",
      },
      ".cm-tooltip-autocomplete ul": {
        fontFamily: "var(--font-mono)",
        fontSize: "13px",
      },
      ".cm-tooltip-autocomplete ul li": {
        padding: "3px 8px",
        color: "var(--text-primary)",
      },
      ".cm-tooltip-autocomplete ul li[aria-selected]": {
        backgroundColor: "var(--bg-hover)",
        color: "var(--text-primary)",
      },
      ".cm-completionLabel": {
        color: "var(--text-primary)",
      },
      ".cm-completionDetail": {
        color: "var(--text-muted)",
        fontStyle: "normal",
        marginLeft: "8px",
      },
      /* Completion icon styling: colored circle-dot per type */
      ".cm-completionIcon": {
        fontSize: "0",
        width: "16px",
        height: "16px",
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
        marginRight: "4px",
        opacity: "1",
      },
      ".cm-completionIcon::after": {
        content: '""',
        display: "block",
        width: "8px",
        height: "8px",
        borderRadius: "50%",
      },
      ".cm-completionIcon-keyword::after": {
        backgroundColor: "var(--syntax-keyword)",
      },
      ".cm-completionIcon-property::after": {
        backgroundColor: "var(--syntax-number)",
      },
      ".cm-completionIcon-function::after": {
        backgroundColor: "var(--syntax-function)",
      },
      ".cm-completionIcon-text::after": {
        backgroundColor: "var(--chart-axis)",
      },
      ".cm-completionIcon-variable::after": {
        backgroundColor: "var(--syntax-keyword)",
      },
      /* Diagnostic (lint) styling for syntax error underlines and tooltips */
      ".cm-diagnostic-error": {
        borderBottom: "2px solid var(--destructive)",
        paddingBottom: "1px",
      },
      ".cm-tooltip-lint": {
        backgroundColor: "var(--bg-primary)",
        border: "1px solid var(--border)",
        borderRadius: "var(--radius)",
        padding: "4px 8px",
        fontSize: "13px",
        color: "var(--text-primary)",
      },
    },
    { dark },
  );
}

export const lynxLightTheme = makeTheme(false);
export const lynxDarkTheme = makeTheme(true);

export const lynxHighlighting = syntaxHighlighting(
  HighlightStyle.define([
    { tag: tags.keyword, color: "var(--syntax-keyword)" },
    { tag: tags.definitionKeyword, color: "var(--syntax-keyword)" },
    { tag: tags.function(tags.variableName), color: "var(--syntax-function)" },
    { tag: tags.operator, color: "var(--syntax-operator)" },
    { tag: tags.string, color: "var(--syntax-string)" },
    { tag: tags.number, color: "var(--syntax-number)" },
    { tag: tags.bool, color: "var(--syntax-bool)" },
    { tag: tags.comment, color: "var(--text-muted)", fontStyle: "italic" },
    { tag: tags.punctuation, color: "var(--text-secondary)" },
    { tag: tags.name, color: "var(--text-primary)" },
  ]),
);

/** Theme extension for the current mode, used inside a Compartment. */
export function lynxThemeFor(dark: boolean) {
  return dark ? lynxDarkTheme : lynxLightTheme;
}
