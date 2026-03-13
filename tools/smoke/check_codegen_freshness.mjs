import { execFileSync } from "node:child_process"

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

const before = trackedDirtyPaths()

execFileSync("npm", ["run", "generate"], { stdio: "inherit" })

const after = trackedDirtyPaths()
const newlyDirty = [...after].filter((path) => !before.has(path))

if (newlyDirty.length > 0) {
  process.stderr.write("codegen produced unstaged tracked changes:\n")
  for (const path of newlyDirty) {
    process.stderr.write(`- ${path}\n`)
  }
  process.exit(1)
}
