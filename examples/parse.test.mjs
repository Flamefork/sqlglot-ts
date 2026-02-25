import { describe, test } from "node:test"
import { deepStrictEqual, strictEqual, throws } from "node:assert/strict"
import { parse, parseOne } from "sqlglot-ts"
import "sqlglot-ts/dialects/duckdb"

describe("parse", () => {
  test("round-trip: parseOne → .sql()", () => {
    const ast = parseOne("SELECT a, b FROM t WHERE x = 1")
    const sql = ast.sql()

    strictEqual(sql, "SELECT a, b FROM t WHERE x = 1")
  })

  test("parse splits multiple statements", () => {
    const stmts = parse("SELECT 1; SELECT 2")

    deepStrictEqual(
      stmts.map((s) => s.sql()),
      ["SELECT 1", "SELECT 2"],
    )
  })

  test("parseOne throws on multiple statements", () => {
    throws(() => parseOne("SELECT 1; SELECT 2"), /Expected exactly one/)
  })

  test("dialect-specific syntax (DuckDB list literal)", () => {
    const ast = parseOne("SELECT [1, 2, 3]", { dialect: "duckdb" })
    const sql = ast.sql({ dialect: "duckdb" })

    strictEqual(sql, "SELECT [1, 2, 3]")
  })

  test("complex SQL round-trip", () => {
    const input =
      "SELECT a, SUM(b) FROM t JOIN u ON t.id = u.id WHERE a > 1 GROUP BY a HAVING SUM(b) > 10 ORDER BY a LIMIT 5"
    const sql = parseOne(input).sql()

    strictEqual(sql, input)
  })
})
