import { describe, it, expect, beforeEach } from "vitest";
import {
  useAlterTableDraftStore,
  draftKey,
  type AlterTableDraft,
} from "./alterTableDraftStore";

function emptyDraft(overrides: Partial<AlterTableDraft> = {}): AlterTableDraft {
  return {
    tableNameLocal: "",
    operations: [],
    pendingNewColumns: [],
    showPreview: false,
    columnFilter: "",
    ...overrides,
  };
}

describe("draftKey", () => {
  it("composes a stable key from (connection, db, schema, table)", () => {
    expect(draftKey("c1", "db", "public", "users")).toBe(
      "c1:db:public:users",
    );
  });

  it("different connections produce different keys for the same table", () => {
    expect(draftKey("c1", "db", "public", "users")).not.toBe(
      draftKey("c2", "db", "public", "users"),
    );
  });
});

describe("alterTableDraftStore", () => {
  beforeEach(() => {
    // Hard reset between tests so persisted state from a prior run
    // doesn't leak.
    useAlterTableDraftStore.setState({ drafts: {} });
    sessionStorage.clear();
  });

  describe("getDraft / setDraft", () => {
    it("returns undefined for an unknown key", () => {
      expect(useAlterTableDraftStore.getState().getDraft("nope")).toBeUndefined();
    });

    it("stores a draft and returns it on getDraft", () => {
      const key = draftKey("c1", "db", "public", "users");
      useAlterTableDraftStore.getState().setDraft(key, {
        tableNameLocal: "users",
        operations: [{ op: "drop_column", column_name: "legacy" }],
      });
      const got = useAlterTableDraftStore.getState().getDraft(key);
      expect(got).toBeDefined();
      expect(got?.tableNameLocal).toBe("users");
      expect(got?.operations).toHaveLength(1);
    });

    it("setDraft is a shallow merge — unspecified fields stay at their previous value", () => {
      const key = draftKey("c1", "db", "public", "users");
      useAlterTableDraftStore.getState().setDraft(key, {
        tableNameLocal: "users",
        operations: [{ op: "drop_column", column_name: "x" }],
      });
      // Subsequent patch only flips showPreview — operations and
      // tableNameLocal must survive.
      useAlterTableDraftStore.getState().setDraft(key, { showPreview: true });
      const got = useAlterTableDraftStore.getState().getDraft(key)!;
      expect(got.tableNameLocal).toBe("users");
      expect(got.operations).toHaveLength(1);
      expect(got.showPreview).toBe(true);
    });

    it("setDraft on an unknown key fills the rest with the documented defaults", () => {
      // Drafts have to be fully-shaped objects so consumers can do
      // `draft.operations.length` without null-checks. Setting a
      // partial patch against a never-seen key must produce a
      // complete draft — empty arrays / false / "".
      const key = draftKey("c1", "db", "public", "users");
      useAlterTableDraftStore.getState().setDraft(key, {
        tableNameLocal: "users",
      });
      expect(useAlterTableDraftStore.getState().getDraft(key)).toEqual(
        emptyDraft({ tableNameLocal: "users" }),
      );
    });

    it("drafts are isolated per (connection, db, schema, table)", () => {
      const aKey = draftKey("c1", "db", "public", "users");
      const bKey = draftKey("c1", "db", "public", "orders");
      useAlterTableDraftStore
        .getState()
        .setDraft(aKey, { tableNameLocal: "users" });
      useAlterTableDraftStore
        .getState()
        .setDraft(bKey, { tableNameLocal: "orders" });
      expect(useAlterTableDraftStore.getState().getDraft(aKey)?.tableNameLocal)
        .toBe("users");
      expect(useAlterTableDraftStore.getState().getDraft(bKey)?.tableNameLocal)
        .toBe("orders");
    });
  });

  describe("clearDraft", () => {
    it("removes a stored draft", () => {
      const key = draftKey("c1", "db", "public", "users");
      useAlterTableDraftStore.getState().setDraft(key, { tableNameLocal: "x" });
      useAlterTableDraftStore.getState().clearDraft(key);
      expect(useAlterTableDraftStore.getState().getDraft(key)).toBeUndefined();
    });

    it("is a no-op when the key doesn't exist", () => {
      // Important because the editor calls clearDraft on Apply
      // success, even if the user hadn't edited anything (e.g. they
      // typed in the rename input then erased it).
      expect(() =>
        useAlterTableDraftStore.getState().clearDraft("never:seen:key"),
      ).not.toThrow();
    });
  });

  describe("pruneStaleConnections", () => {
    it("removes drafts whose connection id is not in the valid set", () => {
      useAlterTableDraftStore
        .getState()
        .setDraft(draftKey("c1", "db", "public", "users"), {
          tableNameLocal: "users",
        });
      useAlterTableDraftStore
        .getState()
        .setDraft(draftKey("c2", "db", "public", "orders"), {
          tableNameLocal: "orders",
        });

      useAlterTableDraftStore
        .getState()
        .pruneStaleConnections(new Set(["c1"]));

      const all = useAlterTableDraftStore.getState().drafts;
      expect(Object.keys(all)).toEqual(["c1:db:public:users"]);
    });

    it("leaves the store unchanged when every connection is valid (reference-identity stable)", () => {
      const key = draftKey("c1", "db", "public", "users");
      useAlterTableDraftStore.getState().setDraft(key, { tableNameLocal: "x" });
      const before = useAlterTableDraftStore.getState().drafts;
      useAlterTableDraftStore
        .getState()
        .pruneStaleConnections(new Set(["c1", "c2"]));
      const after = useAlterTableDraftStore.getState().drafts;
      // Stable reference avoids spuriously bumping subscribers.
      expect(after).toBe(before);
    });

    it("clears everything when no connections are valid", () => {
      useAlterTableDraftStore
        .getState()
        .setDraft(draftKey("c1", "db", "public", "users"), {
          tableNameLocal: "x",
        });
      useAlterTableDraftStore
        .getState()
        .setDraft(draftKey("c2", "db", "public", "orders"), {
          tableNameLocal: "y",
        });
      useAlterTableDraftStore.getState().pruneStaleConnections(new Set());
      expect(
        Object.keys(useAlterTableDraftStore.getState().drafts),
      ).toHaveLength(0);
    });
  });
});
