import { existsSync, readFileSync } from "node:fs"

function fail(message, details) {
  if (details) {
    process.stderr.write(`${details}\n`)
  }
  process.stderr.write(`${message}\n`)
  process.exit(1)
}

function assertNoMatch(path, pattern, message) {
  const content = readFileSync(path, "utf8")
  const match = content.match(pattern)
  if (!match || match.index === undefined) {
    return
  }

  const before = content.slice(0, match.index)
  const line = before.split("\n").length
  fail(message, `${path}:${line}: ${match[0]}`)
}

assertNoMatch(
  "src/expressions.ts",
  /declare module "\.\/(expression-base|expressions\.generated)\.js"/,
  "fluent API must not rely on module augmentation",
)

for (const path of ["src/expressions.ts", "src/expressions.generated.ts"]) {
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
}

if (existsSync("tools/fix_dts_module_specifiers.mjs")) {
  fail("build must not rely on declaration fixer")
}

assertNoMatch(
  "package.json",
  /fix_dts_module_specifiers/,
  "build script must not reference declaration fixer",
)
