import { readFileSync } from "node:fs"
import { resolve } from "node:path"
import { describe, test } from "node:test"
import { ok } from "node:assert/strict"

const checksDir = import.meta.dirname
const repoRoot = resolve(checksDir, "../..")

const RESERVED = new Set([
  "array",
  "delete",
  "false",
  "true",
  "null",
  "case",
  "var",
])

function toCamel(name) {
  const trail =
    name.endsWith("_") &&
    !name.startsWith("_") &&
    !name.slice(0, -1).endsWith("_")
      ? "_"
      : ""
  const base = name.replace(/_+$/, "")
  const parts = base.split("_")
  const result =
    parts[0] +
    parts
      .slice(1)
      .map((p) => p.charAt(0).toUpperCase() + p.slice(1))
      .join("")
  return result + trail
}

async function getTsSurface(classNames) {
  const mod = await import(resolve(repoRoot, "dist/index.mjs"))
  const expMod = await import(resolve(repoRoot, "dist/expressions.mjs"))

  const result = {}

  const topLevel = Object.entries(mod)
    .filter(
      ([name, value]) =>
        typeof value === "function" &&
        name[0] === name[0].toLowerCase() &&
        name[0] !== "_",
    )
    .map(([name]) => name)
    .sort()
  result.top_level = new Set(topLevel)

  for (const className of classNames) {
    const cls = expMod[className] || mod[className]
    if (!cls) {
      result[className] = new Set()
      continue
    }

    const own = new Set()
    const proto = cls.prototype
    if (proto) {
      for (const name of Object.getOwnPropertyNames(proto)) {
        if (!name.startsWith("_") && name !== "constructor") own.add(name)
      }
    }
    for (const name of Object.getOwnPropertyNames(cls)) {
      if (
        !name.startsWith("_") &&
        !["length", "name", "prototype"].includes(name)
      )
        own.add(name)
    }
    try {
      const instance = new cls({})
      for (const name of Object.getOwnPropertyNames(instance)) {
        if (!name.startsWith("_") && name !== "constructor") own.add(name)
      }
    } catch {}
    result[className] = own
  }

  return result
}

function checkMatch(pyName, tsNames) {
  if (tsNames.has(pyName)) return true
  if (tsNames.has(toCamel(pyName))) return true
  return RESERVED.has(pyName) && tsNames.has(pyName + "_")
}

// Use top-level await for dynamic imports
const pySurface = JSON.parse(
  readFileSync(resolve(checksDir, "api_surface_python.json"), "utf8"),
)
const excludes = JSON.parse(
  readFileSync(resolve(checksDir, "api_surface_excludes.json"), "utf8"),
)
const classNames = Object.keys(pySurface).filter((k) => k !== "top_level")
const tsSurface = await getTsSurface(classNames)

describe("API surface parity", () => {
  for (const section of ["top_level", ...classNames]) {
    const pyNames = pySurface[section] || []
    const tsNames = tsSurface[section] || new Set()
    const sectionExcludes = excludes[section] || {}

    test(
      section === "top_level" ? "top-level functions" : `${section} methods`,
      () => {
        const missing = pyNames.filter(
          (name) => !(name in sectionExcludes) && !checkMatch(name, tsNames),
        )
        ok(
          missing.length === 0,
          `Missing ${missing.length} item(s) in ${section}: ${missing.join(", ")}`,
        )
      },
    )
  }
})
