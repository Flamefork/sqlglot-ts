import { describe, test } from "node:test"
import { strictEqual, ok } from "node:assert/strict"
import { parseOne, dump, load } from "sqlglot-ts"

describe("serde", () => {
  test("dump/load round-trip preserves SQL", () => {
    const ast = parseOne("SELECT a, b FROM t WHERE x = 1")
    const payload = dump(ast)
    const restored = load(payload)

    strictEqual(restored.sql(), "SELECT a, b FROM t WHERE x = 1")
  })

  test("payload survives JSON serialization", () => {
    const ast = parseOne("SELECT 1 + 2")
    const json = JSON.stringify(dump(ast))
    const restored = load(JSON.parse(json))

    strictEqual(restored.sql(), "SELECT 1 + 2")
  })

  test("dump returns plain objects (no class instances)", () => {
    const payload = dump(parseOne("SELECT 1"))

    ok(Array.isArray(payload))
    ok(payload.length > 0)
  })

  test("load(null) returns undefined", () => {
    strictEqual(load(null), undefined)
  })
})
