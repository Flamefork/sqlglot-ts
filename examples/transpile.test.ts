import { describe, test } from "node:test"
import { deepStrictEqual, strictEqual } from "node:assert/strict"
import { transpile, transpileOne } from "sqlglot-ts"
import "sqlglot-ts/dialects/duckdb"
import "sqlglot-ts/dialects/postgres"

describe("transpile", () => {
  test("cross-dialect: DuckDB → Postgres", () => {
    const sql = transpileOne("SELECT EPOCH(ts)", {
      read: "duckdb",
      write: "postgres",
    })

    strictEqual(sql, "SELECT DATE_PART('epoch', ts)")
  })

  test("transpile splits multiple statements", () => {
    const results = transpile("SELECT 1; SELECT 2")

    deepStrictEqual(results, ["SELECT 1", "SELECT 2"])
  })

  test("pretty option adds newlines", () => {
    const sql = transpileOne("SELECT a FROM t WHERE x = 1", { pretty: true })

    strictEqual(sql, "SELECT\n  a\nFROM t\nWHERE x = 1")
  })

  test("identity transpile (no dialect specified)", () => {
    const sql = transpileOne("SELECT a, b FROM t")

    strictEqual(sql, "SELECT a, b FROM t")
  })
})
