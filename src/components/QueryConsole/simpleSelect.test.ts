import { describe, it, expect } from "vitest";
import { parseSimpleSelect } from "./simpleSelect";

describe("parseSimpleSelect — editable simple selects", () => {
  it.each([
    ["SELECT * FROM users", { schema: "", table: "users" }],
    ["SELECT id, name FROM users WHERE id > 5 ORDER BY name LIMIT 10;", { schema: "", table: "users" }],
    ["select u.id, u.email from public.users u where u.id in (select user_id from orders)", { schema: "public", table: "users" }],
    ['SELECT "id", "Name" FROM "app"."Users" AS x', { schema: "app", table: "Users" }],
    ["SELECT `id` FROM `shop`.`orders` LIMIT 10, 20", { schema: "shop", table: "orders" }],
    ["SELECT [id] FROM [dbo].[t] ORDER BY id OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", { schema: "dbo", table: "t" }],
    ["SELECT users.* FROM users", { schema: "", table: "users" }],
    ["SELECT id AS id FROM users", { schema: "", table: "users" }],
    ["-- comment\nSELECT * FROM users /* trailing */ ;", { schema: "", table: "users" }],
    ["SELECT * FROM users WHERE name = 'a;b' AND note = 'x FROM y'", { schema: "", table: "users" }],
  ])("%s", (sql, expected) => {
    expect(parseSimpleSelect(sql)).toEqual(expected);
  });
});

describe("parseSimpleSelect — read-only results", () => {
  it("rejects a subquery in the select list", () => {
    expect(parseSimpleSelect(
      "SELECT id, (SELECT count(*) FROM orders o WHERE o.user_id = u.id) n FROM users u",
    )).toBeNull();
  });

  it("rejects comma joins with aliases", () => {
    expect(parseSimpleSelect("SELECT * FROM users u, orders o WHERE u.id = o.user_id")).toBeNull();
    expect(parseSimpleSelect("SELECT * FROM users, orders")).toBeNull();
  });

  it("rejects aliased columns that would write into another column", () => {
    expect(parseSimpleSelect("SELECT id, name AS email FROM users")).toBeNull();
    expect(parseSimpleSelect("SELECT id, name email FROM users")).toBeNull();
  });

  it("rejects multiple statements", () => {
    expect(parseSimpleSelect("SELECT * FROM a; SELECT * FROM b")).toBeNull();
    expect(parseSimpleSelect("SELECT * FROM a; DELETE FROM a")).toBeNull();
  });

  it("rejects joins, set operations, grouping, distinct and CTEs", () => {
    for (const sql of [
      "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
      "SELECT * FROM users NATURAL JOIN orders",
      "SELECT * FROM users CROSS JOIN orders",
      "SELECT id FROM a UNION SELECT id FROM b",
      "SELECT id FROM a INTERSECT SELECT id FROM b",
      "SELECT id FROM a EXCEPT SELECT id FROM b",
      "SELECT status FROM orders GROUP BY status",
      "SELECT DISTINCT id FROM users",
      "SELECT TOP 10 * FROM users",
      "WITH cte AS (SELECT * FROM users) SELECT * FROM cte",
      "SELECT * FROM ONLY users",
      "SELECT * FROM (SELECT * FROM users) s",
    ]) {
      expect(parseSimpleSelect(sql)).toBeNull();
    }
  });

  it("rejects expressions, functions and literals in the select list", () => {
    for (const sql of [
      "SELECT count(*) FROM users",
      "SELECT id, 1 FROM users",
      "SELECT id, 'x' FROM users",
      "SELECT id + 1 FROM users",
      "SELECT lower(name) FROM users",
    ]) {
      expect(parseSimpleSelect(sql)).toBeNull();
    }
  });

  it("rejects qualifiers that don't match the table", () => {
    expect(parseSimpleSelect("SELECT o.id FROM users u")).toBeNull();
  });

  it("rejects non-select statements and empty input", () => {
    expect(parseSimpleSelect("")).toBeNull();
    expect(parseSimpleSelect("UPDATE users SET name = 'x'")).toBeNull();
    expect(parseSimpleSelect("SELECT * FROM users WHERE name = 'unterminated")).toBeNull();
  });
});
