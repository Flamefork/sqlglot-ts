import { describe, test } from "node:test"
import { deepStrictEqual, strictEqual, ok } from "node:assert/strict"
import { parseOne } from "sqlglot-ts"
import { Column, Table, Where, Select } from "sqlglot-ts/expressions"

describe("AST", () => {
  const ast = parseOne("SELECT a, b FROM t WHERE x = 1")

  test("walk() iterates all nodes", () => {
    const nodes = Array.from(ast.walk())

    ok(nodes.length > 5)
  })

  test("findAll(Column) returns matching nodes", () => {
    const names = ast.findAll(Column).map((c) => c.name)

    deepStrictEqual(names, ["a", "b", "x"])
  })

  test("find(Table) returns first match", () => {
    const table = ast.find(Table)

    strictEqual(table?.name, "t")
  })

  test("sub-tree generates its own SQL", () => {
    const where = ast.find(Where)

    strictEqual(where?.sql(), "WHERE x = 1")
  })

  test("root is a Select expression", () => {
    ok(ast instanceof Select)
  })
})
