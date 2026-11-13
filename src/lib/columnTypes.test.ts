import { describe, it, expect } from "vitest";
import {
  buildCreateTableSql,
  columnTypesFor,
  defaultColumnType,
  defaultIdType,
  supportsNullability,
  validateColumnNames,
  type DdlColumn,
} from "./columnTypes";
import type { DbType } from "./sqlDialect";

const ALL: DbType[] = ["postgres", "cockroachdb", "mysql", "mariadb", "tidb", "mssql", "sqlite", "cassandra"];

const col = (name: string, data_type: string, extra: Partial<DdlColumn> = {}): DdlColumn => ({
  name,
  data_type,
  is_nullable: true,
  is_primary_key: false,
  default_value: "",
  ...extra,
});

describe("columnTypesFor", () => {
  it("offers each dialect's default id and new-column types", () => {
    for (const db of ALL) {
      const types = columnTypesFor(db);
      expect(types).toContain(defaultIdType(db));
      expect(types).toContain(defaultColumnType(db));
      expect(new Set(types).size).toBe(types.length);
    }
  });

  it("does not offer Postgres-only types to other engines", () => {
    for (const db of ["mysql", "mssql", "sqlite", "cassandra"] as DbType[]) {
      const types = columnTypesFor(db).map((t) => t.toLowerCase());
      expect(types).not.toContain("serial");
      expect(types).not.toContain("jsonb");
      expect(types).not.toContain("bytea");
    }
  });

  it("falls back to the Postgres catalog for an unknown dialect", () => {
    expect(columnTypesFor(undefined)).toEqual(columnTypesFor("postgres"));
  });
});

describe("defaultIdType", () => {
  it("uses the idiomatic auto-generated key per engine", () => {
    expect(defaultIdType("postgres")).toBe("serial");
    expect(defaultIdType("cockroachdb")).toBe("SERIAL");
    expect(defaultIdType("mysql")).toBe("INT AUTO_INCREMENT");
    expect(defaultIdType("mariadb")).toBe("INT AUTO_INCREMENT");
    expect(defaultIdType("tidb")).toBe("INT AUTO_INCREMENT");
    expect(defaultIdType("mssql")).toBe("INT IDENTITY(1,1)");
    expect(defaultIdType("sqlite")).toBe("INTEGER");
    expect(defaultIdType("cassandra")).toBe("uuid");
  });
});

describe("buildCreateTableSql", () => {
  const cols = [
    col("id", "INT IDENTITY(1,1)", { is_nullable: false, is_primary_key: true }),
    col("status", "NVARCHAR(20)", { default_value: "'new'" }),
    col("", "INT"),
  ];

  it("quotes MSSQL identifiers with brackets and skips unnamed rows", () => {
    expect(buildCreateTableSql("mssql", "db", "dbo", "orders", cols)).toBe(
      "CREATE TABLE [dbo].[orders] (\n" +
        "    [id] INT IDENTITY(1,1) NOT NULL,\n" +
        "    [status] NVARCHAR(20) DEFAULT 'new',\n" +
        "    PRIMARY KEY ([id])\n);",
    );
  });

  it("qualifies MySQL tables with the database using backticks", () => {
    const sql = buildCreateTableSql("mysql", "shop", "shop", "t", [
      col("id", "INT AUTO_INCREMENT", { is_nullable: false, is_primary_key: true }),
    ]);
    expect(sql).toContain("CREATE TABLE `shop`.`t`");
    expect(sql).toContain("`id` INT AUTO_INCREMENT NOT NULL");
    expect(sql).toContain("PRIMARY KEY (`id`)");
  });

  it("drops the schema on SQLite and uses double quotes on Postgres", () => {
    expect(buildCreateTableSql("sqlite", "main", "main", "t", [col("a", "TEXT")])).toMatch(/^CREATE TABLE "t" \(/);
    expect(buildCreateTableSql("postgres", "db", "public", "t", [col("a", "text")])).toMatch(
      /^CREATE TABLE "public"\."t" \(/,
    );
  });

  it("keyspace-qualifies Cassandra and omits NOT NULL / DEFAULT", () => {
    const sql = buildCreateTableSql("cassandra", "ks", "ks", "t", [
      col("id", "uuid", { is_nullable: false, is_primary_key: true, default_value: "x" }),
    ]);
    expect(sql).toBe('CREATE TABLE "ks"."t" (\n    "id" uuid,\n    PRIMARY KEY ("id")\n);');
    expect(supportsNullability("cassandra")).toBe(false);
  });
});

describe("validateColumnNames", () => {
  it("flags case-insensitive duplicates", () => {
    const r = validateColumnNames([{ name: "id" }, { name: "name" }, { name: " ID " }]);
    expect([...r.duplicates].sort()).toEqual([0, 2]);
    expect(r.message).toBe("Duplicate column name: id, ID");
  });

  it("ignores blank names and passes unique names", () => {
    const r = validateColumnNames([{ name: "id" }, { name: "" }, { name: "  " }, { name: "b" }]);
    expect(r.duplicates.size).toBe(0);
    expect(r.message).toBeNull();
  });
});
