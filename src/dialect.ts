/**
 * Dialect system for SQL parsing and generation
 */

import type { Expression } from "./expressions.js"
import * as exp from "./expressions.js"
import { type GenerateOptions, Generator } from "./generator.js"
import { Parser, type ParserOptions } from "./parser.js"
import { formatTime } from "./time.js"
import { Tokenizer, type TokenizerOptions } from "./tokens.js"
import { type Trie, newTrie } from "./trie.js"

export interface DialectOptions {
  tokenizer?: TokenizerOptions
  parser?: ParserOptions
  generator?: GenerateOptions
}

// Registry of dialects
const DIALECTS: Map<string, Dialect> = new Map()

export class Dialect {
  static readonly name: string = "sqlglot"

  // Inner class references (static for dialect inheritance)
  protected static TokenizerClass: typeof Tokenizer = Tokenizer
  protected static ParserClass: typeof Parser = Parser
  protected static GeneratorClass: typeof Generator = Generator

  // Feature flags (matching Python's patterns)
  static STRICT_STRING_CONCAT = true
  static CONCAT_COALESCE = false
  static LOG_BASE_FIRST: boolean | null = true
  static ARRAY_AGG_INCLUDES_NULLS: boolean | null = null
  static LEAST_GREATEST_IGNORES_NULLS = false
  static INDEX_OFFSET = 0
  static NULL_ORDERING:
    | "nulls_are_small"
    | "nulls_are_large"
    | "nulls_are_last" = "nulls_are_small"
  static HEX_STRING_IS_INTEGER_TYPE = false
  static HEX_LOWERCASE = false
  static BYTE_STRING_IS_BYTES_TYPE = false
  static PRESERVE_ORIGINAL_NAMES = false
  static TYPED_DIVISION = false
  static SAFE_DIVISION = false

  // String literal delimiters (extracted from tokenizer config)
  static BIT_START: string | null = null
  static BIT_END: string | null = null
  static HEX_START: string | null = null
  static HEX_END: string | null = null
  static BYTE_START: string | null = null
  static BYTE_END: string | null = null

  // Time format mappings (dialect format -> strftime)
  static TIME_MAPPING: Map<string, string> = new Map()
  // Cached inverse mapping (strftime -> dialect format)
  static INVERSE_TIME_MAPPING: Map<string, string> = new Map()

  // Default time format strings (matching Python)
  static DATE_FORMAT = "'%Y-%m-%d'"
  static TIME_FORMAT = "'%Y-%m-%d %H:%M:%S'"

  // Cached tries (built lazily from TIME_MAPPING / INVERSE_TIME_MAPPING)
  private static _TIME_TRIE: Trie | null = null
  private static _INVERSE_TIME_TRIE: Trie | null = null

  static get TIME_TRIE(): Trie {
    if (!this._TIME_TRIE) {
      this._TIME_TRIE = newTrie([...this.TIME_MAPPING.keys()])
    }
    return this._TIME_TRIE
  }

  static get INVERSE_TIME_TRIE(): Trie {
    if (!this._INVERSE_TIME_TRIE) {
      this._INVERSE_TIME_TRIE = newTrie([...this.INVERSE_TIME_MAPPING.keys()])
    }
    return this._INVERSE_TIME_TRIE
  }

  static formatTime(
    expression: string | exp.Expression | undefined | null,
  ): exp.Expression | string | undefined | null {
    if (typeof expression === "string") {
      const unquoted = expression.slice(1, -1)
      return exp.Literal.string(
        formatTime(unquoted, this.TIME_MAPPING, this.TIME_TRIE),
      )
    }

    if (
      expression instanceof exp.Expression &&
      expression instanceof exp.Literal &&
      expression.isString
    ) {
      return exp.Literal.string(
        formatTime(
          String(expression.args.this),
          this.TIME_MAPPING,
          this.TIME_TRIE,
        ),
      )
    }

    return expression
  }

  // Instance getters for class references (for proper inheritance)
  protected get TokenizerClass(): typeof Tokenizer {
    return (this.constructor as typeof Dialect).TokenizerClass
  }

  protected get ParserClass(): typeof Parser {
    return (this.constructor as typeof Dialect).ParserClass
  }

  protected get GeneratorClass(): typeof Generator {
    return (this.constructor as typeof Dialect).GeneratorClass
  }

  version: [number, number, number] = [
    Number.POSITIVE_INFINITY,
    Number.POSITIVE_INFINITY,
    Number.POSITIVE_INFINITY,
  ]

  constructor(protected options: DialectOptions = {}) {}

  // Instance getters for feature flags (allow dialect subclasses to override via static)
  get STRICT_STRING_CONCAT(): boolean {
    return (this.constructor as typeof Dialect).STRICT_STRING_CONCAT
  }

  get CONCAT_COALESCE(): boolean {
    return (this.constructor as typeof Dialect).CONCAT_COALESCE
  }

  get LOG_BASE_FIRST(): boolean | null {
    return (this.constructor as typeof Dialect).LOG_BASE_FIRST
  }

  get ARRAY_AGG_INCLUDES_NULLS(): boolean | null {
    return (this.constructor as typeof Dialect).ARRAY_AGG_INCLUDES_NULLS
  }

  get LEAST_GREATEST_IGNORES_NULLS(): boolean {
    return (this.constructor as typeof Dialect).LEAST_GREATEST_IGNORES_NULLS
  }

  get NULL_ORDERING():
    | "nulls_are_small"
    | "nulls_are_large"
    | "nulls_are_last" {
    return (this.constructor as typeof Dialect).NULL_ORDERING
  }

  get INDEX_OFFSET(): number {
    return (this.constructor as typeof Dialect).INDEX_OFFSET
  }

  get HEX_STRING_IS_INTEGER_TYPE(): boolean {
    return (this.constructor as typeof Dialect).HEX_STRING_IS_INTEGER_TYPE
  }

  get HEX_LOWERCASE(): boolean {
    return (this.constructor as typeof Dialect).HEX_LOWERCASE
  }

  get BYTE_STRING_IS_BYTES_TYPE(): boolean {
    return (this.constructor as typeof Dialect).BYTE_STRING_IS_BYTES_TYPE
  }

  get PRESERVE_ORIGINAL_NAMES(): boolean {
    return (this.constructor as typeof Dialect).PRESERVE_ORIGINAL_NAMES
  }

  get TYPED_DIVISION(): boolean {
    return (this.constructor as typeof Dialect).TYPED_DIVISION
  }

  get SAFE_DIVISION(): boolean {
    return (this.constructor as typeof Dialect).SAFE_DIVISION
  }

  get BIT_START(): string | null {
    return (this.constructor as typeof Dialect).BIT_START
  }

  get BIT_END(): string | null {
    return (this.constructor as typeof Dialect).BIT_END
  }

  get HEX_START(): string | null {
    return (this.constructor as typeof Dialect).HEX_START
  }

  get HEX_END(): string | null {
    return (this.constructor as typeof Dialect).HEX_END
  }

  get BYTE_START(): string | null {
    return (this.constructor as typeof Dialect).BYTE_START
  }

  get BYTE_END(): string | null {
    return (this.constructor as typeof Dialect).BYTE_END
  }

  get TIME_MAPPING(): Map<string, string> {
    return (this.constructor as typeof Dialect).TIME_MAPPING
  }

  get INVERSE_TIME_MAPPING(): Map<string, string> {
    const cls = this.constructor as typeof Dialect
    // Auto-compute inverse if empty and TIME_MAPPING has entries
    if (cls.INVERSE_TIME_MAPPING.size === 0 && cls.TIME_MAPPING.size > 0) {
      for (const [k, v] of cls.TIME_MAPPING) {
        cls.INVERSE_TIME_MAPPING.set(v, k)
      }
    }
    return cls.INVERSE_TIME_MAPPING
  }

  get name(): string {
    return (this.constructor as typeof Dialect).name
  }

  createTokenizer(): Tokenizer {
    return new this.TokenizerClass(this.options.tokenizer)
  }

  createParser(): Parser {
    const tokenizer = this.createTokenizer()
    const parser = new this.ParserClass({ ...this.options.parser, tokenizer })
    // Dialect implements DialectSettings via getters
    parser.setDialect(this as unknown as import("./parser.js").DialectSettings)
    return parser
  }

  createGenerator(options?: GenerateOptions): Generator {
    const versionOpt =
      this.version[0] !== Number.POSITIVE_INFINITY
        ? { version: this.version }
        : {}
    return new this.GeneratorClass({
      ...this.options.generator,
      ...versionOpt,
      ...options,
    })
  }

  parse(sql: string): Expression[] {
    const parser = this.createParser()
    return parser.parse(sql)
  }

  parseOne(sql: string): Expression {
    const parser = this.createParser()
    return parser.parseOne(sql)
  }

  generate(expression: Expression, options?: GenerateOptions): string {
    const generator = this.createGenerator(options)
    return generator.generate(expression)
  }

  transpile(
    sql: string,
    targetDialect: Dialect,
    options?: GenerateOptions,
  ): string[] {
    const expressions = this.parse(sql)
    return expressions.map((expr) => targetDialect.generate(expr, options))
  }

  // Static methods for dialect registry

  static register(dialectClass: typeof Dialect): void {
    const instance = new dialectClass()
    DIALECTS.set(instance.name.toLowerCase(), instance)
    // Also register class name variants
    DIALECTS.set(dialectClass.name.toLowerCase(), instance)
  }

  static get(dialect?: string | Dialect): Dialect {
    if (dialect instanceof Dialect) {
      return dialect
    }

    if (typeof dialect === "string" && dialect.length > 0) {
      // Parse comma-separated settings: "duckdb, version=1.0"
      const [dialectName, ...kvStrings] = dialect.split(",")
      const name = dialectName!.trim().toLowerCase()
      const found = DIALECTS.get(name)
      if (!found) {
        throw new Error(`Unknown dialect: ${dialectName!.trim()}`)
      }

      // Parse settings
      let version: [number, number, number] | undefined
      for (const kv of kvStrings) {
        const [key, value] = kv.split("=").map((s) => s.trim())
        if (key === "version" && value) {
          const parts = value.split(".").map(Number)
          version = [parts[0] ?? 0, parts[1] ?? 0, parts[2] ?? 0]
        }
      }

      if (version) {
        const clone = Object.create(
          Object.getPrototypeOf(found) as object,
        ) as Dialect
        Object.assign(clone, found)
        clone.version = version
        return clone
      }

      return found
    }

    // Default dialect (or empty string)
    return getDefaultDialect()
  }

  static getOrThrow(dialect: string): Dialect {
    const found = DIALECTS.get(dialect.toLowerCase())
    if (!found) {
      throw new Error(`Unknown dialect: ${dialect}`)
    }
    return found
  }

  static list(): string[] {
    return [...new Set(DIALECTS.keys())].sort()
  }
}

// Default (base) dialect singleton
let defaultDialect: Dialect | undefined

function getDefaultDialect(): Dialect {
  if (!defaultDialect) {
    defaultDialect = new Dialect()
  }
  return defaultDialect
}

// Register base dialect
Dialect.register(Dialect)

// Export for tests
export { DIALECTS }
