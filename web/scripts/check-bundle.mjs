/* global process, console */

/**
 * Bundle budget check.
 *
 * Reads dist/.vite/manifest.json, finds the entry chunk (isEntry: true),
 * walks its STATIC import graph (the "imports" arrays, NOT "dynamicImports"),
 * gzip-compresses each referenced .js file, sums the gzipped bytes, and
 * exits 1 if the total exceeds 175 KB.
 *
 * Usage: node scripts/check-bundle.mjs
 * Assumes a prior `vite build` has been run.
 */

import { readFileSync } from "node:fs";
import { join } from "node:path";
import { gzipSync } from "node:zlib";

const BUDGET_BYTES = 175 * 1024; // 175 KB
const DIST_DIR = join(process.cwd(), "dist");
const MANIFEST_PATH = join(DIST_DIR, ".vite", "manifest.json");

// Read manifest
let manifest;
try {
  manifest = JSON.parse(readFileSync(MANIFEST_PATH, "utf-8"));
} catch {
  console.error(`Failed to read manifest at ${MANIFEST_PATH}`);
  console.error("Run 'bun run build' first.");
  process.exit(1);
}

// Find entry chunk(s)
const entries = Object.values(manifest).filter((chunk) => chunk.isEntry);
if (entries.length === 0) {
  console.error("No entry chunk found in manifest.");
  process.exit(1);
}

// Walk static imports starting from the entry
const visited = new Set();
const queue = [];

for (const entry of entries) {
  queue.push(entry);
}

while (queue.length > 0) {
  const chunk = queue.pop();
  const file = chunk.file;
  if (visited.has(file)) continue;
  visited.add(file);

  // Walk static imports (NOT dynamicImports)
  if (chunk.imports) {
    for (const importKey of chunk.imports) {
      const importedChunk = manifest[importKey];
      if (importedChunk && !visited.has(importedChunk.file)) {
        queue.push(importedChunk);
      }
    }
  }
}

// Compute gzipped sizes for all .js files in the static graph
const breakdown = [];
let totalGzip = 0;

for (const file of [...visited].sort()) {
  if (!file.endsWith(".js")) continue;

  const filePath = join(DIST_DIR, file);
  let raw;
  try {
    raw = readFileSync(filePath);
  } catch {
    console.warn(`  Warning: could not read ${filePath}`);
    continue;
  }

  const gzipped = gzipSync(raw);
  const gzSize = gzipped.length;
  totalGzip += gzSize;
  breakdown.push({ file, rawSize: raw.length, gzSize });
}

// Print breakdown
console.log("Static JS bundle breakdown (gzip):");
console.log("─".repeat(70));
for (const { file, rawSize, gzSize } of breakdown) {
  const rawKB = (rawSize / 1024).toFixed(2);
  const gzKB = (gzSize / 1024).toFixed(2);
  console.log(`  ${file.padEnd(45)} ${rawKB.padStart(8)} KB  → ${gzKB.padStart(8)} KB gz`);
}
console.log("─".repeat(70));

const totalKB = (totalGzip / 1024).toFixed(2);
const budgetKB = (BUDGET_BYTES / 1024).toFixed(2);
console.log(`  Total static JS (gzip): ${totalKB} KB / ${budgetKB} KB budget`);

if (totalGzip > BUDGET_BYTES) {
  console.log(`\n  FAIL: exceeds budget by ${((totalGzip - BUDGET_BYTES) / 1024).toFixed(2)} KB`);
  process.exit(1);
} else {
  const headroom = ((BUDGET_BYTES - totalGzip) / 1024).toFixed(2);
  console.log(`\n  PASS: ${headroom} KB headroom`);
  process.exit(0);
}
