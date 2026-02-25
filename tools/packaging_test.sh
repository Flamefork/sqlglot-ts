#!/usr/bin/env bash
set -euo pipefail

echo "=== Smoke test: npm pack → temp install → import ==="

PACK_DIR=$(mktemp -d)
PROJ_DIR=$(mktemp -d)
trap 'rm -rf "$PACK_DIR" "$PROJ_DIR"' EXIT

npm run build
npm pack --pack-destination "$PACK_DIR"

TARBALLS=("$PACK_DIR"/sqlglot-ts-*.tgz)
if [[ ${#TARBALLS[@]} -ne 1 ]]; then
  echo "Expected exactly one tarball, found: ${TARBALLS[*]}" >&2
  exit 1
fi
TARBALL="${TARBALLS[0]}"
echo "Packed: $TARBALL"

cd "$PROJ_DIR"
cat > package.json << 'EOF'
{ "type": "module" }
EOF

npm install "$TARBALL" --save

cat > test.mjs << 'SCRIPT'
import { parse, parseOne, transpile, transpileOne, dump, load, select, from_ } from 'sqlglot-ts';
import { DuckDB, Postgres } from 'sqlglot-ts/dialects';
import { Column, Table, Select } from 'sqlglot-ts/expressions';

// parse
const exprs = parse('SELECT 1; SELECT 2');
if (exprs.length !== 2) throw new Error('parse failed');

// parseOne round-trip
const ast = parseOne('SELECT a FROM t');
if (ast.sql() !== 'SELECT a FROM t') throw new Error('round-trip failed');

// transpile
const results = transpile('SELECT 1; SELECT 2');
if (results.length !== 2) throw new Error('transpile failed');

// dialect singletons
const sql = DuckDB.generate(ast);
if (typeof sql !== 'string') throw new Error('dialect generate failed');

// expressions subpath
const table = ast.find(Table);
if (table?.name !== 't') throw new Error('expressions import failed');

// builder
const built = select('a').from('t').sql();
if (built !== 'SELECT a FROM t') throw new Error('builder failed');

// serde
const restored = load(dump(ast));
if (restored.sql() !== ast.sql()) throw new Error('serde failed');

console.log('All smoke tests passed!');
SCRIPT

node test.mjs
echo "=== Smoke test passed ==="
