import { execFileSync } from "node:child_process"
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { after, before, describe, test } from "node:test"

const repoRoot = resolve(import.meta.dirname, "../..")

describe("packaging", () => {
  let projDir
  let tarball

  before(() => {
    if (!existsSync(join(repoRoot, "dist"))) {
      throw new Error("dist/ is missing; run 'just build' first")
    }

    const packDir = mkdtempSync(join(tmpdir(), "sqlglot-pack-"))
    projDir = mkdtempSync(join(tmpdir(), "sqlglot-proj-"))

    execFileSync("npm", ["pack", "--pack-destination", packDir], {
      cwd: repoRoot,
    })

    const files = execFileSync("ls", [packDir], { encoding: "utf8" })
      .trim()
      .split("\n")
    const tarballs = files.filter(
      (f) => f.startsWith("sqlglot-ts-") && f.endsWith(".tgz"),
    )
    if (tarballs.length !== 1) {
      throw new Error(
        `Expected exactly one tarball, found: ${tarballs.join(", ")}`,
      )
    }
    tarball = join(packDir, tarballs[0])

    writeFileSync(join(projDir, "package.json"), '{ "type": "module" }')
    execFileSync("npm", ["install", tarball, "--save"], { cwd: projDir })
  })

  after(() => {
    if (projDir) {
      rmSync(projDir, { recursive: true, force: true })
    }
  })

  test("runtime smoke", () => {
    const testScript = `
import { parse, parseOne, transpile, transpileOne, dump, load, select, from_ } from 'sqlglot-ts';
import { DuckDB, Postgres } from 'sqlglot-ts/dialects';
import { Column, Table, Select } from 'sqlglot-ts/expressions';

const exprs = parse('SELECT 1; SELECT 2');
if (exprs.length !== 2) throw new Error('parse failed');

const ast = parseOne('SELECT a FROM t');
if (ast.sql() !== 'SELECT a FROM t') throw new Error('round-trip failed');

const results = transpile('SELECT 1; SELECT 2');
if (results.length !== 2) throw new Error('transpile failed');

const sql = DuckDB.generate(ast);
if (typeof sql !== 'string') throw new Error('dialect generate failed');

const table = ast.find(Table);
if (table?.name !== 't') throw new Error('expressions import failed');

const built = select('a').from_('t').sql();
if (built !== 'SELECT a FROM t') throw new Error('builder failed');

const restored = load(dump(ast));
if (restored.sql() !== ast.sql()) throw new Error('serde failed');
`
    writeFileSync(join(projDir, "test.mjs"), testScript)
    execFileSync("node", ["test.mjs"], { cwd: projDir, stdio: "pipe" })
  })

  test("type check", () => {
    const testTs = `
import { parseOne, select } from "sqlglot-ts"
import * as exp from "sqlglot-ts/expressions"

select("x").from_("tbl").where("x = 1")
parseOne("SELECT x").assertIs(exp.Select).select("y")
parseOne("SELECT x").assertIs(exp.Column).desc()

const ordered = parseOne("SELECT x FROM tbl ORDER BY x DESC")
  .find(exp.Ordered)
  ?.assertIs(exp.Ordered)

if (!ordered) {
  throw new Error("expected ordered expression")
}

ordered.desc()
// @ts-expect-error Ordered no longer exposes nullsFirst convenience getter
ordered.nullsFirst
`
    const tsConfig = JSON.stringify({
      compilerOptions: {
        module: "nodenext",
        moduleResolution: "nodenext",
        target: "es2022",
        strict: true,
        skipLibCheck: false,
      },
      include: ["test.ts"],
    })

    writeFileSync(join(projDir, "test.ts"), testTs)
    writeFileSync(join(projDir, "tsconfig.json"), tsConfig)

    const tscPath = join(repoRoot, "node_modules/typescript/bin/tsc")
    execFileSync("node", [tscPath, "--noEmit", "-p", "tsconfig.json"], {
      cwd: projDir,
      stdio: "pipe",
    })
  })
})
