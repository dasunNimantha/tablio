/** Cell renderer for a real SQL NULL: a pill, distinct from the text "NULL". */
export function NullCell() {
  return <span className="cell-null-pill">NULL</span>;
}

/** `cellRendererSelector` that uses {@link NullCell} for NULL values only. */
export function nullCellSelector(params: { value: unknown; valueFormatted?: string | null }) {
  const isNull = params.value === null || params.value === undefined;
  // A formatted placeholder ("auto" / "default") is not a NULL.
  if (!isNull || (params.valueFormatted && params.valueFormatted !== "NULL")) return undefined;
  return { component: NullCell };
}
