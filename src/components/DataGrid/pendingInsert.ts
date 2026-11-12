import type { NewRow } from "../../lib/tauri";

/**
 * A row the user added but hasn't saved yet. Its values live with the
 * record itself (not at a position in the fetched page), so paging,
 * sorting or refreshing can never make Save insert a different row.
 * `touched[i]` marks columns the user (or the test-data generator)
 * set explicitly; untouched columns are omitted from the INSERT so the
 * column DEFAULT applies, while an explicit NULL is sent as NULL.
 */
export interface PendingInsert {
  tempId: number;
  values: unknown[];
  touched: boolean[];
}

/** Build the INSERT payload for a pending row: only touched columns. */
export function pendingInsertToNewRow(ins: PendingInsert, columnNames: string[]): NewRow {
  const values: [string, unknown][] = [];
  columnNames.forEach((name, ci) => {
    if (ins.touched[ci]) values.push([name, ins.values[ci] ?? null]);
  });
  return { values };
}
