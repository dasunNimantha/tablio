/** How often schema metadata the user is looking at is re-read in the
 *  background, so changes made outside the app (migrations, other
 *  clients) show up without a manual refresh. */
export const SCHEMA_POLL_INTERVAL_MS = 30_000;

/** Minimal shape of a sidebar tree node needed to detect structural change. */
export interface StructuralNode {
  id: string;
  type: string;
  tableType?: string;
  partitionStrategy?: string | null;
  preloadedChildren?: StructuralNode[];
}

/** Signature of a node list that changes only when objects are added,
 *  removed, renamed or change kind — not when row counts or sizes move. */
export function structureSignature(nodes: StructuralNode[]): string {
  return nodes
    .map(
      (n) =>
        `${n.id}|${n.type}|${n.tableType ?? ""}|${n.partitionStrategy ?? ""}` +
        (n.preloadedChildren ? `[${structureSignature(n.preloadedChildren)}]` : "")
    )
    .join(";");
}
