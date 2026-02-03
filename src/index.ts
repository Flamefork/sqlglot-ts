/**
 * sqlglot-ts - SQL parser and transpiler for TypeScript
 * Port of SQLGlot (https://github.com/tobymao/sqlglot)
 */

export { TokenType, Token, Tokenizer } from "./tokens.js"
export { Expression, indexOffsetLogs, select, from_ } from "./expressions.js"
export { Parser } from "./parser.js"
export { Generator } from "./generator.js"
export type { GenerateOptions } from "./generator.js"
export { Dialect, type DialectOptions } from "./dialect.js"
export { formatTime } from "./time.js"
export { annotateTypes } from "./optimizer/annotate_types.js"
export { ErrorLevel, ParseError } from "./errors.js"
export type { ParseErrorDetail } from "./errors.js"
export { dump, load } from "./serde.js"

import { Dialect } from "./dialect.js"
import { Expression } from "./expressions.js"
import type { GenerateOptions } from "./generator.js"

// Import dialects to trigger registration
import "./dialects/athena.js"
import "./dialects/bigquery.js"
import "./dialects/clickhouse.js"
import "./dialects/databricks.js"
import "./dialects/doris.js"
import "./dialects/dremio.js"
import "./dialects/drill.js"
import "./dialects/druid.js"
import "./dialects/duckdb.js"
import "./dialects/dune.js"
import "./dialects/exasol.js"
import "./dialects/fabric.js"
import "./dialects/hive.js"
import "./dialects/materialize.js"
import "./dialects/mysql.js"
import "./dialects/oracle.js"
import "./dialects/postgres.js"
import "./dialects/presto.js"
import "./dialects/prql.js"
import "./dialects/redshift.js"
import "./dialects/risingwave.js"
import "./dialects/singlestore.js"
import "./dialects/snowflake.js"
import "./dialects/solr.js"
import "./dialects/spark.js"
import "./dialects/sqlite.js"
import "./dialects/starrocks.js"
import "./dialects/tableau.js"
import "./dialects/teradata.js"
import "./dialects/trino.js"
import "./dialects/tsql.js"

// Initialize Expression.dump/load methods
import { dump as _dump, load as _load } from "./serde.js"
Expression.setSerdeImpl(_dump, (payloads) => _load(payloads)!)

// Initialize Expression.parseImpl() method
Expression.setParseImpl((sql, options) => {
  const dialect = options?.dialect
  return parseOne(sql, dialect ? { dialect } : {})
})

// Initialize Expression.sql() method
Expression.setSqlImpl((expr, options) => {
  const dialect = Dialect.get(options?.dialect as string | Dialect | undefined)
  const genOptions: GenerateOptions = {}
  if (options?.pretty !== undefined) {
    genOptions.pretty = options.pretty
  }
  const ul = options?.unsupportedLevel
  if (ul === "IGNORE" || ul === "WARN" || ul === "RAISE") {
    genOptions.unsupportedLevel = ul
  }
  return dialect.generate(expr, genOptions)
})

export interface ParseOptions {
  dialect?: string | Dialect
}

export interface TranspileOptions {
  read?: string | Dialect
  write?: string | Dialect
  pretty?: boolean
}

/**
 * Parse SQL string into AST
 */
export function parse(sql: string, options: ParseOptions = {}): Expression[] {
  const dialect = Dialect.get(options.dialect)
  return dialect.parse(sql)
}

/**
 * Parse a single SQL statement
 */
export function parseOne(sql: string, options: ParseOptions = {}): Expression {
  const expressions = parse(sql, options)
  const [first] = expressions
  if (expressions.length !== 1 || !first) {
    throw new Error(
      `Expected exactly one expression, got ${expressions.length}`,
    )
  }
  return first
}

/**
 * Transpile SQL from one dialect to another
 */
export function transpile(
  sql: string,
  options: TranspileOptions = {},
): string[] {
  const readDialect = Dialect.get(options.read)
  const writeDialect = Dialect.get(options.write)

  const expressions = readDialect.parse(sql)
  const genOptions: GenerateOptions = {}
  if (options.pretty !== undefined) {
    genOptions.pretty = options.pretty
  }

  return expressions.map((expr: Expression) =>
    writeDialect.generate(expr, genOptions),
  )
}

/**
 * Transpile a single SQL statement
 */
export function transpileOne(
  sql: string,
  options: TranspileOptions = {},
): string {
  const results = transpile(sql, options)
  const [first] = results
  if (results.length !== 1 || !first) {
    throw new Error(`Expected exactly one statement, got ${results.length}`)
  }
  return first
}
