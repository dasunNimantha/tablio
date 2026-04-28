/**
 * Helpers for parsing and serialising the persisted "sub-tab" string
 * that lives on each TabInfo. Pulled out of TableView.tsx so it can be
 * unit tested without React.
 *
 * The sub-tab string supports three logical shapes:
 *   - "data"            -> Data mode
 *   - "schema"          -> Schema mode, no auto-scroll
 *   - "schema:<anchor>" -> Schema mode, deep-link to a section
 *
 * Legacy tabs persisted before the mode-switch refactor used bare anchor
 * names ("columns", "indexes", "stats", "partitions", ...). Those decay
 * to schema mode anchored at that section so old tabs still land where
 * the user expected.
 */

export type TableMode = "data" | "schema";

export type SchemaAnchor =
  | "columns"
  | "constraints"
  | "indexes"
  | "fks"
  | "refs"
  | "triggers"
  | "partitions"
  | "stats";

export const SCHEMA_ANCHORS: SchemaAnchor[] = [
  "columns",
  "constraints",
  "indexes",
  "fks",
  "refs",
  "triggers",
  "partitions",
  "stats",
];

export interface ParsedSubTab {
  mode: TableMode;
  anchor?: SchemaAnchor;
}

export function parseSubTab(s: string | undefined | null): ParsedSubTab {
  if (!s || s === "data") return { mode: "data" };
  if (s === "schema") return { mode: "schema" };
  if (s.startsWith("schema:")) {
    const anchor = s.slice("schema:".length) as SchemaAnchor;
    return {
      mode: "schema",
      anchor: SCHEMA_ANCHORS.includes(anchor) ? anchor : undefined,
    };
  }
  // Legacy: bare anchor names from the previous design.
  if (SCHEMA_ANCHORS.includes(s as SchemaAnchor)) {
    return { mode: "schema", anchor: s as SchemaAnchor };
  }
  return { mode: "data" };
}

export function serializeSubTab(mode: TableMode, anchor?: SchemaAnchor): string {
  if (mode === "data") return "data";
  return anchor ? `schema:${anchor}` : "schema";
}
