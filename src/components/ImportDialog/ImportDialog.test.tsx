import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, screen, fireEvent, waitFor, cleanup } from "@testing-library/react";
import { ImportDialog, MAX_XLSX_BYTES, duplicateTargets } from "./ImportDialog";
import { api, type ColumnInfo, type XlsxWorkbookPreview, type XlsxSheetPayload } from "../../lib/tauri";

const tableColumns: ColumnInfo[] = [
  { name: "id", data_type: "int", is_nullable: false, default_value: null, is_primary_key: true, ordinal_position: 1, is_auto_generated: false },
  { name: "name", data_type: "text", is_nullable: true, default_value: null, is_primary_key: false, ordinal_position: 2, is_auto_generated: false },
  { name: "active", data_type: "bool", is_nullable: true, default_value: null, is_primary_key: false, ordinal_position: 3, is_auto_generated: false },
];

function makeWorkbookPreview(sheetCount: number): XlsxWorkbookPreview {
  const sheetNames = Array.from({ length: sheetCount }, (_, i) => `Sheet${i + 1}`);
  return {
    sheet_names: sheetNames,
    default_sheet: sheetNames[0],
    sheet: {
      name: sheetNames[0],
      headers: ["id", "name", "active"],
      preview_rows: [
        [1, "Alice", true],
        [2, "Bob", false],
      ],
      total_data_rows: 2,
      inferred_types: ["number", "string", "bool"],
      error_cells: [],
      error_cell_count: 0,
    },
  };
}

function makeSheetPayload(name: string): XlsxSheetPayload {
  return {
    name,
    headers: ["id", "name", "active"],
    rows: [
      [1, "Alice", true],
      [2, "Bob", false],
    ],
    inferred_types: ["number", "string", "bool"],
    error_cells: [],
    error_cell_count: 0,
  };
}

function makeFile(name: string, size: number, content = "stub"): File {
  // jsdom honors `size` only when the underlying chunks add up; build
  // a Blob of the requested length so file.size works.
  const data = new Uint8Array(size);
  // Fill with a known byte so it's not all zero (helps debugging).
  data.fill(0x41);
  if (content) {
    const enc = new TextEncoder();
    const head = enc.encode(content);
    data.set(head.subarray(0, Math.min(head.length, size)));
  }
  return new File([data], name, { type: "application/octet-stream" });
}

describe("ImportDialog", () => {
  beforeEach(() => {
    vi.spyOn(api, "listColumns").mockResolvedValue(tableColumns);
    vi.spyOn(api, "importData").mockResolvedValue(2);
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  function mount() {
    return render(
      <ImportDialog
        connectionId="test"
        database="db"
        schema="public"
        tableName="users"
        onClose={vi.fn()}
        onImported={vi.fn()}
      />,
    );
  }

  it('renders the engine-agnostic "Import Data" header', async () => {
    mount();
    expect(await screen.findByText("Import Data")).toBeInTheDocument();
    // The file input accepts CSV + Excel.
    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    expect(input.accept).toContain(".csv");
    expect(input.accept).toContain(".xlsx");
    expect(input.accept).toContain(".xls");
  });

  it("rejects unsupported extensions before parsing", async () => {
    mount();
    await screen.findByText("Import Data");
    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    fireEvent.change(input, { target: { files: [makeFile("notes.txt", 64)] } });
    expect(
      await screen.findByText(/unsupported file type/i),
    ).toBeInTheDocument();
  });

  it("rejects oversized files before invoking IPC", async () => {
    const parseSpy = vi
      .spyOn(api, "parseXlsxWorkbook")
      .mockResolvedValue(makeWorkbookPreview(1));
    mount();
    await screen.findByText("Import Data");
    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    // 100MB + 1 byte triggers the cap. We don't actually allocate that
    // much in the test (jsdom handles File.size synthetically); pass
    // an explicit size via a sparse File instead.
    const oversized = new File([new Uint8Array(0)], "huge.xlsx", { type: "x" });
    Object.defineProperty(oversized, "size", { value: 200 * 1024 * 1024 });
    fireEvent.change(input, { target: { files: [oversized] } });
    expect(
      await screen.findByText(/file is too large/i),
    ).toBeInTheDocument();
    expect(parseSpy).not.toHaveBeenCalled();
  });

  it("parses xlsx and seeds column mapping by name match", async () => {
    vi.spyOn(api, "parseXlsxWorkbook").mockResolvedValue(makeWorkbookPreview(1));
    vi.spyOn(api, "parseXlsxSheet").mockResolvedValue(makeSheetPayload("Sheet1"));

    mount();
    await screen.findByText("Import Data");
    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    fireEvent.change(input, {
      target: { files: [makeFile("data.xlsx", 1024)] },
    });

    // Column mapping renders all three source columns. Each one
    // auto-maps to the same-named target column because the
    // case-insensitive match logic kicks in for both CSV and XLSX.
    await waitFor(() => {
      expect(screen.getByText(/column mapping/i)).toBeInTheDocument();
    });
    const mappingSelects = document.querySelectorAll(
      ".import-mapping-row select",
    ) as NodeListOf<HTMLSelectElement>;
    expect(mappingSelects).toHaveLength(3);
    expect(Array.from(mappingSelects).map((s) => s.value)).toEqual([
      "id",
      "name",
      "active",
    ]);
  });

  it("renders inferred type badges next to xlsx column names", async () => {
    vi.spyOn(api, "parseXlsxWorkbook").mockResolvedValue(makeWorkbookPreview(1));
    vi.spyOn(api, "parseXlsxSheet").mockResolvedValue(makeSheetPayload("Sheet1"));

    mount();
    await screen.findByText("Import Data");
    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    fireEvent.change(input, {
      target: { files: [makeFile("data.xlsx", 1024)] },
    });

    await waitFor(() => {
      const badges = document.querySelectorAll(".import-mapping-type");
      expect(badges).toHaveLength(3);
      expect(Array.from(badges).map((b) => b.textContent)).toEqual([
        "number",
        "string",
        "bool",
      ]);
    });
  });

  it("hides the sheet picker when the workbook has only one sheet", async () => {
    vi.spyOn(api, "parseXlsxWorkbook").mockResolvedValue(makeWorkbookPreview(1));
    vi.spyOn(api, "parseXlsxSheet").mockResolvedValue(makeSheetPayload("Sheet1"));

    mount();
    await screen.findByText("Import Data");
    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    fireEvent.change(input, {
      target: { files: [makeFile("data.xlsx", 1024)] },
    });

    await waitFor(() => {
      expect(screen.getByText(/column mapping/i)).toBeInTheDocument();
    });
    expect(document.querySelector(".import-sheet-select")).toBeNull();
  });

  it("shows the sheet picker for multi-sheet workbooks and reloads on change", async () => {
    vi.spyOn(api, "parseXlsxWorkbook").mockResolvedValue(makeWorkbookPreview(3));
    const sheetSpy = vi
      .spyOn(api, "parseXlsxSheet")
      .mockImplementation(async (_b, sheet) => makeSheetPayload(sheet));

    mount();
    await screen.findByText("Import Data");
    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    fireEvent.change(input, {
      target: { files: [makeFile("data.xlsx", 1024)] },
    });

    const select = (await waitFor(() => {
      const el = document.querySelector(".import-sheet-select") as HTMLSelectElement | null;
      expect(el).not.toBeNull();
      return el!;
    })) as HTMLSelectElement;
    expect(Array.from(select.options).map((o) => o.value)).toEqual([
      "Sheet1",
      "Sheet2",
      "Sheet3",
    ]);
    expect(select.value).toBe("Sheet1");

    fireEvent.change(select, { target: { value: "Sheet2" } });
    await waitFor(() => {
      expect(sheetSpy).toHaveBeenCalledWith(expect.any(Uint8Array), "Sheet2");
    });
  });

  it("falls back to CSV parser for .csv files", async () => {
    const xlsxSpy = vi.spyOn(api, "parseXlsxWorkbook").mockResolvedValue(
      makeWorkbookPreview(1),
    );

    mount();
    await screen.findByText("Import Data");
    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    const csv = new File(["id,name\n1,Alice\n2,Bob"], "data.csv", { type: "text/csv" });
    fireEvent.change(input, { target: { files: [csv] } });

    await waitFor(() => {
      expect(screen.getByText(/column mapping/i)).toBeInTheDocument();
    });
    // CSV path doesn't touch the xlsx command.
    expect(xlsxSpy).not.toHaveBeenCalled();
    // Type badges aren't rendered for CSV (no inferred_types from the
    // legacy parser).
    expect(document.querySelectorAll(".import-mapping-type")).toHaveLength(0);
  });

  it("rejects an xlsx above the Excel limit before reading the file", async () => {
    const parseSpy = vi
      .spyOn(api, "parseXlsxWorkbook")
      .mockResolvedValue(makeWorkbookPreview(1));
    mount();
    await screen.findByText("Import Data");
    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    const file = new File([new Uint8Array(0)], "big.xlsx", { type: "x" });
    Object.defineProperty(file, "size", { value: MAX_XLSX_BYTES + 1 });
    const readSpy = vi.spyOn(file, "arrayBuffer");
    fireEvent.change(input, { target: { files: [file] } });
    const msg = await screen.findByText(/file is too large/i);
    expect(msg.textContent).toMatch(/Excel imports are limited to 25 MB/);
    expect(msg.textContent).toMatch(/CSV/);
    expect(readSpy).not.toHaveBeenCalled();
    expect(parseSpy).not.toHaveBeenCalled();
  });

  it("accepts a CSV larger than the Excel limit", async () => {
    mount();
    await screen.findByText("Import Data");
    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    const csv = new File(["id,name\n1,Alice"], "data.csv", { type: "text/csv" });
    Object.defineProperty(csv, "size", { value: MAX_XLSX_BYTES + 1 });
    fireEvent.change(input, { target: { files: [csv] } });
    await waitFor(() => {
      expect(screen.getByText(/column mapping/i)).toBeInTheDocument();
    });
    expect(screen.queryByText(/file is too large/i)).toBeNull();
  });

  it("warns about Excel error cells with the count and a few examples", async () => {
    vi.spyOn(api, "parseXlsxWorkbook").mockResolvedValue(makeWorkbookPreview(1));
    vi.spyOn(api, "parseXlsxSheet").mockResolvedValue({
      ...makeSheetPayload("Sheet1"),
      error_cells: ["B2: #N/A", "C3: #DIV/0!", "A4: #REF!", "B5: #VALUE!", "C6: #NAME?", "A7: #NUM!"],
      error_cell_count: 42,
    });
    mount();
    await screen.findByText("Import Data");
    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    fireEvent.change(input, { target: { files: [makeFile("data.xlsx", 1024)] } });
    const warning = await screen.findByTestId("import-error-cells");
    expect(warning.textContent).toContain("42 cells contain an Excel error");
    expect(warning.textContent).toContain("imported as NULL");
    expect(warning.textContent).toContain("B2: #N/A, C3: #DIV/0!, A4: #REF!, B5: #VALUE!, C6: #NAME?");
    expect(warning.textContent).not.toContain("A7: #NUM!");
    expect(warning.textContent).toContain("…");
  });

  it("uses the singular and no ellipsis for a single error cell", async () => {
    vi.spyOn(api, "parseXlsxWorkbook").mockResolvedValue(makeWorkbookPreview(1));
    vi.spyOn(api, "parseXlsxSheet").mockResolvedValue({
      ...makeSheetPayload("Sheet1"),
      error_cells: ["B2: #N/A"],
      error_cell_count: 1,
    });
    mount();
    await screen.findByText("Import Data");
    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    fireEvent.change(input, { target: { files: [makeFile("data.xlsx", 1024)] } });
    const warning = await screen.findByTestId("import-error-cells");
    expect(warning.textContent).toContain("1 cell contains an Excel error");
    expect(warning.textContent).not.toContain("…");
  });

  it("shows no error-cell warning for a clean sheet", async () => {
    vi.spyOn(api, "parseXlsxWorkbook").mockResolvedValue(makeWorkbookPreview(1));
    vi.spyOn(api, "parseXlsxSheet").mockResolvedValue(makeSheetPayload("Sheet1"));
    mount();
    await screen.findByText("Import Data");
    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    fireEvent.change(input, { target: { files: [makeFile("data.xlsx", 1024)] } });
    await waitFor(() => {
      expect(screen.getByText(/column mapping/i)).toBeInTheDocument();
    });
    expect(screen.queryByTestId("import-error-cells")).toBeNull();
  });

  async function loadCsv(text: string, props: Partial<Parameters<typeof ImportDialog>[0]> = {}) {
    const onImported = vi.fn();
    const onClose = vi.fn();
    render(
      <ImportDialog
        connectionId="test"
        database="db"
        schema="public"
        tableName="users"
        onClose={onClose}
        onImported={onImported}
        {...props}
      />,
    );
    await screen.findByText("Import Data");
    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    fireEvent.change(input, { target: { files: [new File([text], "data.csv", { type: "text/csv" })] } });
    await waitFor(() => expect(screen.getByText(/column mapping/i)).toBeInTheDocument());
    return { onImported, onClose };
  }

  it("imports a quoted field containing a comma without shifting columns", async () => {
    await loadCsv('id,name,active\n1,"Smith, John",true\n2,"multi\nline",false');
    fireEvent.click(screen.getByRole("button", { name: "Import" }));
    await waitFor(() => expect(api.importData).toHaveBeenCalled());
    expect(vi.mocked(api.importData).mock.calls[0][0]).toMatchObject({
      columns: ["id", "name", "active"],
      rows: [
        ["1", "Smith, John", "true"],
        ["2", "multi\nline", "false"],
      ],
    });
  });

  it("stays open on a success state with Done after importing", async () => {
    const { onImported, onClose } = await loadCsv("id,name\n1,a\n2,b");
    fireEvent.click(screen.getByRole("button", { name: "Import" }));
    const ok = await screen.findByTestId("import-success");
    expect(ok).toHaveTextContent("2 rows imported into public.users");
    expect(onImported).toHaveBeenCalledWith(2);
    expect(onClose).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: "Done" }));
    expect(onClose).toHaveBeenCalled();
  });

  it("blocks import when two source columns map to the same table column", async () => {
    await loadCsv("id,name,Name\n1,a,b");
    // "Name" was de-duplicated to "Name_2" and both match "name"? Only the
    // exact (case-insensitive) match is seeded; map the second one by hand.
    const selects = screen.getAllByRole("combobox", { name: /Table column for/ });
    fireEvent.change(selects[2], { target: { value: "name" } });
    expect(await screen.findByRole("alert")).toHaveTextContent(/More than one source column is mapped to "name"/);
    expect(screen.getByRole("button", { name: "Import" })).toBeDisabled();
  });

  it("uses the chosen delimiter and gives blank headers a name", async () => {
    await loadCsv("id;;name\n1;x;y");
    expect(screen.getByLabelText("Table column for column_2")).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText("Delimiter"), { target: { value: "," } });
    await waitFor(() => expect(screen.getByLabelText("Table column for id;;name")).toBeInTheDocument());
  });
});

describe("duplicateTargets", () => {
  it("lists table columns used more than once and ignores skipped ones", () => {
    expect(duplicateTargets({ a: "id", b: "", c: "id", d: "name", e: "" })).toEqual(["id"]);
    expect(duplicateTargets({ a: "id", b: "name" })).toEqual([]);
  });
});
