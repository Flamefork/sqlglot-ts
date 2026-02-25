import { describe, test } from "node:test"
import { strictEqual } from "node:assert/strict"
import { select, from_ } from "sqlglot-ts"

describe("builder", () => {
  test("select().from()", () => {
    const sql = select("a", "b").from("t").sql()

    strictEqual(sql, "SELECT a, b FROM t")
  })

  test("from_().select() — reverse order, same result", () => {
    const sql = from_("t").select("a", "b").sql()

    strictEqual(sql, "SELECT a, b FROM t")
  })

  test("where + limit chaining", () => {
    const sql = select("a").from("t").where("a > 1").limit(10).sql()

    strictEqual(sql, "SELECT a FROM t WHERE a > 1 LIMIT 10")
  })

  test("multiple .where() calls are ANDed", () => {
    const sql = select("a").from("t").where("a > 1").where("b < 5").sql()

    strictEqual(sql, "SELECT a FROM t WHERE a > 1 AND b < 5")
  })

  test(".select() appends columns", () => {
    const sql = select("a").from("t").select("b").sql()

    strictEqual(sql, "SELECT a, b FROM t")
  })
})
