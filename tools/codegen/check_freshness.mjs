import { execFileSync } from "node:child_process"
import { test } from "node:test"
import { deepStrictEqual } from "node:assert/strict"

function trackedDirtyPaths() {
  const output = execFileSync("git", ["status", "--porcelain=v1"], {
    encoding: "utf8",
  })

  return new Set(
    output
      .split("\n")
      .filter((line) => line && !line.startsWith("?? "))
      .map((line) => line.slice(3).split(" -> ").at(-1))
      .filter((path) => typeof path === "string"),
  )
}

test("codegen produces no unstaged changes", () => {
  const before = trackedDirtyPaths()

  execFileSync(
    "uv",
    ["run", "--directory", "tools", "python", "codegen/generate.py"],
    {
      stdio: "inherit",
    },
  )

  const after = trackedDirtyPaths()
  const newlyDirty = [...after].filter((path) => !before.has(path))

  deepStrictEqual(
    newlyDirty,
    [],
    `codegen produced unstaged tracked changes:\n${newlyDirty.map((p) => `- ${p}`).join("\n")}`,
  )
})
