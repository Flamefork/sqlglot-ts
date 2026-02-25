# sqlglot-ts

TypeScript port of [SQLGlot](https://github.com/tobymao/sqlglot) — a SQL parser and transpiler.

**Browser-first implementation** focusing on parsing and transpilation. Zero runtime dependencies.

> **Early release.** DuckDB dialect is fully tested against Python SQLGlot's test suite (500+ test cases). Other dialects are included and functional but have varying levels of coverage — expect gaps in edge cases. Contributions and bug reports welcome.

## Installation

```bash
npm install sqlglot-ts
```

## Usage

### Parse SQL

```typescript
import { parse, parseOne } from 'sqlglot-ts';

// Parse multiple statements
const statements = parse('SELECT 1; SELECT 2');

// Parse a single statement
const ast = parseOne('SELECT * FROM users WHERE id = 1');
```

### Transpile between dialects

```typescript
import { transpileOne } from 'sqlglot-ts';
import 'sqlglot-ts/dialects/duckdb';
import 'sqlglot-ts/dialects/postgres';

const sql = transpileOne(
  'SELECT TRY_CAST(x AS INT)',
  { read: 'duckdb', write: 'postgres' }
);
// => 'SELECT CAST(x AS INT)'
```

### Use dialect objects

```typescript
import { parseOne } from 'sqlglot-ts';
import { Postgres, BigQuery, DuckDB } from 'sqlglot-ts/dialects';

const ast = parseOne('SELECT TRY_CAST(x AS INT)');

console.log(BigQuery.generate(ast));
console.log(Postgres.generate(ast));
console.log(DuckDB.generate(ast));
```

### Work with the AST

```typescript
import { parseOne } from 'sqlglot-ts';
import * as exp from 'sqlglot-ts/expressions';

const ast = parseOne('SELECT a, b FROM t WHERE x > 1');

// Traverse the AST
for (const node of ast.walk()) {
  console.log(node.key);
}

// Find specific node types
const columns = ast.findAll(exp.Column);
columns.forEach(col => console.log(col.name));

// Generate SQL back
console.log(ast.sql());
```

## Dialect imports

The core `sqlglot-ts` entry point does **not** load any dialect — only standard SQL parsing works out of the box. Import the dialects you need:

```typescript
// Only what you need (recommended)
import 'sqlglot-ts/dialects/duckdb';
import 'sqlglot-ts/dialects/postgres';

// Or all dialects at once
import 'sqlglot-ts/dialects';
```

Each dialect file registers itself on import via `Dialect.register()`.

## Supported Dialects

**Fully tested:** DuckDB

**Included (partial coverage):** Athena, BigQuery, ClickHouse, Databricks, Doris, Dremio, Drill, Druid, Dune, Exasol, Fabric, Hive, Materialize, MySQL, Oracle, PostgreSQL, Presto, PRQL, Redshift, RisingWave, SingleStore, Snowflake, Solr, Spark, SQLite, StarRocks, Tableau, Teradata, Trino, T-SQL.

## API Reference

### Functions

- `parse(sql, options?)` — Parse SQL into AST array
- `parseOne(sql, options?)` — Parse a single SQL statement
- `transpile(sql, options?)` — Transpile SQL between dialects (returns array)
- `transpileOne(sql, options?)` — Transpile a single SQL statement

### Classes

- `Dialect` — Base dialect class with registry
- `Parser` — SQL parser
- `Generator` — SQL generator
- `Tokenizer` — SQL tokenizer
- `Expression` — Base AST node class

## Bundle Size

The library uses unbundled ESM — each dialect is a separate file. Core parsing (`sqlglot-ts`) without any dialects is the smallest import. Adding dialects increases the total proportionally.

Typical sizes (gzipped):
- Core only: ~80KB
- Core + one dialect: ~100–120KB
- All dialects: ~150KB

## Examples

Runnable examples that double as tests live in [`examples/`](./examples/) — they import from `sqlglot-ts` just like user code and cover the full public API: parsing, transpilation, dialects, AST traversal, builder, and serialization.

```bash
npm run test:examples  # Run all examples
```

## Development

```bash
npm install            # Install dependencies
npm run build          # Build
npm test               # format:check + lint + typecheck + examples + compat DuckDB
npm run test:compat    # Full compat run (all dialects)
npm run test:packaging # npm pack → temp install → verify exports
npm run fix            # Format + lint fix
npm run generate       # Generate TS expressions from Python SQLGlot
npm run release -- 0.2.0  # Bump version, commit, tag, push → CI publishes to npm
```

## Differences from Python SQLGlot

**Included:**
- Tokenizer, Parser, Generator
- All 31 dialects
- Expression AST classes
- Cross-dialect transpilation

**Not included:**
- Optimizer (qualify, annotate_types, simplify, etc.)
- Executor
- Lineage analysis
- Schema module

## License

MIT
