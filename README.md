# sqlglot-ts

TypeScript port of [SQLGlot](https://github.com/tobymao/sqlglot) - a SQL parser, transpiler, optimizer, and engine.

**Note:** This is a browser-first implementation focusing on parsing and transpilation. The optimizer and executor modules are not included.

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

### Transpile SQL

```typescript
import { transpile, transpileOne } from 'sqlglot-ts';

// Transpile from PostgreSQL to BigQuery
const sql = transpileOne(
  'SELECT CAST(x AS INT)',
  { read: 'postgres', write: 'bigquery' }
);
// => 'SELECT SAFE_CAST(x AS INT)'
```

### Use Dialect Objects

```typescript
import { parseOne } from 'sqlglot-ts';
import { Postgres, MySQL, BigQuery, Snowflake, DuckDB } from 'sqlglot-ts/dialects';

const ast = parseOne('SELECT TRY_CAST(x AS INT)');

console.log(BigQuery.generate(ast));   // SELECT SAFE_CAST(x AS INT)
console.log(Snowflake.generate(ast));  // SELECT TRY_CAST(x AS INT)
console.log(DuckDB.generate(ast));     // SELECT TRY_CAST(x AS INT)
```

### Work with AST

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

## Supported Dialects

- PostgreSQL (`postgres`)
- MySQL (`mysql`)
- BigQuery (`bigquery`)
- Snowflake (`snowflake`)
- DuckDB (`duckdb`)

## API Reference

### Functions

- `parse(sql, options?)` - Parse SQL into AST array
- `parseOne(sql, options?)` - Parse a single SQL statement
- `transpile(sql, options?)` - Transpile SQL between dialects (returns array)
- `transpileOne(sql, options?)` - Transpile a single SQL statement

### Classes

- `Dialect` - Base dialect class with registry
- `Parser` - SQL parser
- `Generator` - SQL generator
- `Tokenizer` - SQL tokenizer
- `Expression` - Base AST node class

## Development

```bash
# Install dependencies
npm install

# Build
npm run build

# Run tests
npm test

# Run Pyodide bridge tests
npm run test:bridge

# Type check
npm run typecheck
```

## Bundle Size

The library is designed to be tree-shakeable. Importing only what you need keeps bundle size small:

```typescript
// Full import (~85KB gzipped)
import * as sqlglot from 'sqlglot-ts';

// Selective import (smaller)
import { parseOne, transpileOne } from 'sqlglot-ts';

// Single dialect (smallest)
import { Postgres } from 'sqlglot-ts/dialects';
```

## Differences from Python SQLGlot

This TypeScript port focuses on core parsing and generation functionality:

**Included:**
- Tokenizer
- Parser
- Generator
- 5 dialects (PostgreSQL, MySQL, BigQuery, Snowflake, DuckDB)
- Expression AST classes
- Basic dialect transpilation

**Not included:**
- Optimizer (qualify, annotate_types, simplify, etc.)
- Executor (runs SQL on dataframes)
- Lineage analysis
- Schema module
- 29 additional dialects

## License

MIT
