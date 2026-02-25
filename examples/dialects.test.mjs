import { describe, test } from "node:test"
import { strictEqual } from "node:assert/strict"
import { parseOne, Dialect } from "sqlglot-ts"
import { DuckDB, Postgres, DuckDBDialect } from "sqlglot-ts/dialects"

describe("dialects", () => {
  test("dialect singletons are instances", () => {
    strictEqual(DuckDB instanceof DuckDBDialect, true)
  })

  test("Dialect.get() resolves by string", () => {
    const d = Dialect.get("duckdb")
    strictEqual(d instanceof DuckDBDialect, true)
  })

  test("dialect .generate() produces SQL", () => {
    const ast = parseOne("SELECT 1")

    const sql = Postgres.generate(ast)
    strictEqual(sql, "SELECT 1")
  })

  test(".sql() with dialect option", () => {
    const ast = parseOne("SELECT CAST(x AS INT)")
    const sql = ast.sql({ dialect: "postgres" })
    strictEqual(sql, "SELECT CAST(x AS INT)")
  })

  test("per-dialect import (tree-shaking friendly)", async () => {
    const mod = await import("sqlglot-ts/dialects/duckdb")
    strictEqual(typeof mod.DuckDBDialect, "function")
  })
})
