import { existsSync, readFileSync } from "node:fs"
import { describe, test } from "node:test"
import { ok } from "node:assert/strict"

function assertNoMatch(path, pattern, message) {
  const content = readFileSync(path, "utf8")
  const match = content.match(pattern)
  if (!match || match.index === undefined) {
    return
  }

  const before = content.slice(0, match.index)
  const line = before.split("\n").length
  ok(false, `${message}\n  ${path}:${line}: ${match[0]}`)
}

describe("fluent API architecture", () => {
  test("no module augmentation", () => {
    assertNoMatch(
      "src/expressions.ts",
      /declare module "\.\/(expression-base|expressions\.generated)\.js"/,
      "fluent API must not rely on module augmentation",
    )
  })

  for (const path of ["src/expressions.ts", "src/expressions.generated.ts"]) {
    test(`no prototype patching in ${path}`, () => {
      assertNoMatch(
        path,
        /Object\.defineProperty\(/,
        "fluent API must not rely on prototype patching",
      )
      assertNoMatch(
        path,
        /[A-Za-z][A-Za-z0-9_]*\.prototype\./,
        "fluent API must not rely on prototype patching",
      )
    })
  }

  test("no declaration fixer", () => {
    ok(
      !existsSync("tools/fix_dts_module_specifiers.mjs"),
      "build must not rely on declaration fixer (tools/)",
    )
    ok(
      !existsSync("tools/checks/fix_dts_module_specifiers.mjs"),
      "build must not rely on declaration fixer (tools/checks/)",
    )
    assertNoMatch(
      "package.json",
      /fix_dts_module_specifiers/,
      "build script must not reference declaration fixer",
    )
  })
})
