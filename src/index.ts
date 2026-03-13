/**
 * sqlglot-ts - SQL parser and transpiler for TypeScript
 * Port of SQLGlot (https://github.com/tobymao/sqlglot)
 */

export { Dialect, type DialectOptions } from "./dialect.js"
export type { ParseErrorDetail } from "./errors.js"
export { ErrorLevel, ParseError } from "./errors.js"
export {
  alias_,
  and_,
  array_,
  case_,
  cast,
  column,
  columnTableNames,
  condition,
  convert,
  delete_,
  Expression,
  except_,
  false_,
  from_,
  func,
  insert,
  intersect,
  merge,
  not_,
  null_,
  or_,
  paren,
  parseIdentifier,
  renameColumn,
  renameTable,
  replacePlaceholders,
  select,
  splitNumWords,
  subquery,
  table_,
  tableName,
  toBool,
  toColumn,
  toIdentifier,
  toInterval,
  toTable,
  true_,
  tuple_,
  union,
  update,
  values,
  var_,
  xor,
} from "./expressions.js"
export type { GenerateOptions } from "./generator.js"
export { Generator } from "./generator.js"
export { annotateTypes } from "./optimizer/annotate_types.js"
export { Parser } from "./parser.js"
export { dump, load } from "./serde.js"
export { formatTime } from "./time.js"
export { Token, Tokenizer, TokenType } from "./tokens.js"

import { Dialect } from "./dialect.js"
import {
  Expression,
  type ExpressionClass,
  type ExpressionConstructor,
} from "./expressions.js"
import type { GenerateOptions } from "./generator.js"

// Initialize Expression.dump/load methods
import { dump as _dump, load as _load } from "./serde.js"

Expression.setSerdeImpl(_dump, (payloads) => _load(payloads)!)

// Initialize Expression.parseImpl() method
Expression.setParseImpl((sql, options) => {
  const opts: ParseOptions = {}
  if (options?.dialect) {
    opts.dialect = options.dialect
  }
  if (options?.into) {
    opts.into = options.into
  }
  return parseOne(sql, opts)
})

// Initialize Expression.sql() method
Expression.setSqlImpl((expr, options) => {
  const dialect = Dialect.get(options?.dialect as string | Dialect | undefined)
  const genOptions: GenerateOptions = {}
  if (options?.pretty !== undefined) {
    genOptions.pretty = options.pretty
  }
  if (options?.identify !== undefined) {
    genOptions.identify = options.identify
  }
  const ul = options?.unsupportedLevel
  if (ul === "IGNORE" || ul === "WARN" || ul === "RAISE") {
    genOptions.unsupportedLevel = ul
  }
  return dialect.generate(expr, genOptions)
})

export interface ParseOptions {
  dialect?: string | Dialect
  into?:
    | ExpressionConstructor
    | ExpressionConstructor[]
    | ExpressionClass
    | ExpressionClass[]
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
export function parseOne<T extends Expression>(
  sql: string,
  options: ParseOptions & { into: ExpressionClass<T> },
): T
export function parseOne(sql: string, options?: ParseOptions): Expression
export function parseOne(sql: string, options: ParseOptions = {}): Expression {
  const dialect = Dialect.get(options.dialect)
  let expressions: Expression[]
  if (options.into) {
    expressions = dialect.parseInto(
      options.into as ExpressionClass | ExpressionClass[],
      sql,
    )
  } else {
    expressions = dialect.parse(sql)
  }
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
