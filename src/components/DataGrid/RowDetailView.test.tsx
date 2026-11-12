import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent, act } from "@testing-library/react";
import { RowDetailView, prepareEditValue } from "./RowDetailView";
import type { ColumnInfo } from "../../lib/tauri";

const col = (name: string, data_type: string, is_nullable = true, is_primary_key = false): ColumnInfo => ({
  name,
  data_type,
  is_nullable,
  is_primary_key,
  default_value: null,
  ordinal_position: 0,
  is_auto_generated: false,
});

describe("prepareEditValue", () => {
  it("stores an empty string for nullable text columns", () => {
    expect(prepareEditValue("", col("note", "text"))).toEqual({ ok: true, value: "" });
  });

  it("stores the literal string Null for text columns", () => {
    expect(prepareEditValue("Null", col("note", "text"))).toEqual({ ok: true, value: "Null" });
  });

  it("does not trim text", () => {
    expect(prepareEditValue("  hi  ", col("note", "varchar(10)"))).toEqual({ ok: true, value: "  hi  " });
  });

  it("enforces varchar length on the untrimmed value", () => {
    expect(prepareEditValue("abcd ", col("code", "varchar(4)")).ok).toBe(false);
  });

  it("maps empty input to NULL for nullable numeric columns", () => {
    expect(prepareEditValue("", col("n", "integer"))).toEqual({ ok: true, value: null });
    expect(prepareEditValue("", col("n", "integer", false)).ok).toBe(false);
  });

  it("keeps big integers and exact decimals as strings", () => {
    expect(prepareEditValue("9223372036854775807", col("id", "bigint")))
      .toEqual({ ok: true, value: "9223372036854775807" });
    expect(prepareEditValue("1234567890123456789.0123456789", col("amt", "numeric(38,10)")))
      .toEqual({ ok: true, value: "1234567890123456789.0123456789" });
  });

  it("rejects malformed numbers instead of truncating them", () => {
    expect(prepareEditValue("12abc", col("n", "integer")).ok).toBe(false);
  });

  it("checks integer ranges", () => {
    expect(prepareEditValue("40000", col("n", "smallint")).ok).toBe(false);
    expect(prepareEditValue("3000000000", col("n", "integer")).ok).toBe(false);
    expect(prepareEditValue("3000000000", col("n", "bigint")).ok).toBe(true);
  });
});

describe("RowDetailView editing", () => {
  const columns = [col("id", "integer", false, true), col("note", "text")];

  function setup(row: unknown[]) {
    const onCellChange = vi.fn();
    render(
      <RowDetailView columns={columns} row={row} rowIndex={0} onClose={() => {}} onCellChange={onCellChange} />,
    );
    return { onCellChange };
  }

  function startEditingNote() {
    const values = document.querySelectorAll(".json-value-inline");
    fireEvent.doubleClick(values[1]);
    return document.querySelector("textarea") as HTMLTextAreaElement;
  }

  it("does not record an edit when a NULL field loses focus unchanged", async () => {
    vi.useFakeTimers();
    try {
      setup([1, null]);
      const ta = startEditingNote();
      fireEvent.blur(ta);
      await act(async () => { vi.runAllTimers(); });
      expect(screen.queryByText(/Apply/)).toBeNull();
    } finally {
      vi.useRealTimers();
    }
  });

  it("sets an explicit NULL via the NULL button", () => {
    const { onCellChange } = setup([1, "hello"]);
    startEditingNote();
    fireEvent.mouseDown(screen.getByRole("button", { name: "NULL" }));
    fireEvent.click(screen.getByText(/Apply 1 edit/));
    expect(onCellChange).toHaveBeenCalledWith(1, null);
  });

  it("stores an empty string when the text is cleared", () => {
    const { onCellChange } = setup([1, "hello"]);
    const ta = startEditingNote();
    fireEvent.change(ta, { target: { value: "" } });
    fireEvent.keyDown(ta, { key: "Enter" });
    fireEvent.click(screen.getByText(/Apply 1 edit/));
    expect(onCellChange).toHaveBeenCalledWith(1, "");
  });
});
