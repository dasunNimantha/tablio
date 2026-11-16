import { describe, expect, it } from "vitest";
import { getMatcher, NO_FILTER, type TreeFilter, type TreeNode } from "./treeModel";

const node = (id: string, type: TreeNode["type"], label = id, extra: Partial<TreeNode> = {}): TreeNode => ({
  id,
  label,
  type,
  connectionId: "c",
  connectionColor: "#000",
  ...extra,
});

const filter = (q: string, types: Partial<TreeFilter> = {}): TreeFilter => ({
  ...NO_FILTER,
  active: true,
  q,
  ...types,
});

describe("getMatcher", () => {
  const tables = node("c:db:s:tables", "table-group", "Tables");
  const views = node("c:db:s:views", "view-group", "Views");
  const users = node("c:db:s:users", "table");
  const orders = node("c:db:s:orders", "table");
  const partition = node("c:db:s:orders:partitions:orders_2024", "table", "orders_2024");
  const partitioned = node("c:db:s:events", "table", "events", {
    preloadedChildren: [node("c:db:s:events:partitions", "partition-group", "Partitions", { preloadedChildren: [partition] })],
  });
  const childrenMap: Record<string, TreeNode[]> = {
    [tables.id]: [users, orders, partitioned],
    [views.id]: [node("c:db:s:active_users", "view", "active_users")],
  };

  it("matches objects by name and branches through loaded descendants", () => {
    const match = getMatcher(childrenMap, filter("user"));
    expect(match(users)).toBe(true);
    expect(match(orders)).toBe(false);
    expect(match(tables)).toBe(true);
    expect(match(views)).toBe(true);
  });

  it("does not match a branch by its own name, nor an unloaded branch", () => {
    const match = getMatcher(childrenMap, filter("tables"));
    expect(match(tables)).toBe(false);
    expect(match(node("c:db:other:functions", "function-group", "Functions"))).toBe(false);
  });

  it("finds partitions inside preloaded children", () => {
    expect(getMatcher(childrenMap, filter("2024"))(tables)).toBe(true);
  });

  it("applies the type filter", () => {
    const match = getMatcher(childrenMap, filter("", { views: false }));
    expect(match(users)).toBe(true);
    expect(match(views)).toBe(false);
    expect(match(node("c:db:other:views", "view-group", "Views"))).toBe(false);
    // Without text, unloaded branches stay visible.
    expect(match(node("c:db2", "database", "db2"))).toBe(true);
  });

  it("is cached per filter value and children map", () => {
    const f = filter("user");
    expect(getMatcher(childrenMap, f)).toBe(getMatcher(childrenMap, f));
    expect(getMatcher({ ...childrenMap }, f)).not.toBe(getMatcher(childrenMap, f));
  });

  it("survives a node whose id collides with its parent's", () => {
    const group = node("c:db:s:tables", "table-group", "Tables");
    const clash = node("c:db:s:tables", "table", "tables");
    const match = getMatcher({ [group.id]: [clash] }, filter("zzz"));
    expect(match(group)).toBe(false);
  });
});
