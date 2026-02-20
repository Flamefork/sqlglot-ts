/**
 * MySQL dialect
 */

import {
  Dialect,
  buildEscapedSequences,
  buildUnescapedSequences,
} from "../dialect.js"
import type { ExpressionClass } from "../expression-base.js"
import * as exp from "../expressions.js"
import { Generator } from "../generator.js"
import { Parser } from "../parser.js"
import { TokenType } from "../tokens.js"
import {
  datestrtodate_sql,
  eliminateQualify,
  eliminateSemiAndAntiJoins,
  preprocess,
  timestrtotime_sql,
} from "../transforms.js"

type Transform = (generator: Generator, expression: exp.Expression) => string

export class MySQLParser extends Parser {
  static override CONJUNCTION: Map<TokenType, ExpressionClass> = new Map([
    ...Parser.CONJUNCTION,
    [TokenType.XOR, exp.Xor],
  ])

  static override FUNCTIONS = new Map([
    ...Parser.FUNCTIONS,
    [
      "FORMAT",
      (args: exp.Expression[]) =>
        new exp.NumberToStr({
          this: args[0],
          format: args[1],
          culture: args[2],
        }),
    ],
  ])
}

export class MySQLGenerator extends Generator {
  static override BIT_START: string | null = "b'"
  static override BIT_END: string | null = "'"
  static override HEX_START: string | null = "x'"
  static override HEX_END: string | null = "'"
  static override STRINGS_SUPPORT_ESCAPED_SEQUENCES = true
  static override ESCAPED_SEQUENCES = buildEscapedSequences(
    buildUnescapedSequences(),
  )
  static override STRING_ESCAPES = ["'", '"', "\\"]

  static CHAR_CAST_MAPPING: Record<string, string> = {
    LONGTEXT: "CHAR",
    LONGBLOB: "CHAR",
    MEDIUMBLOB: "CHAR",
    MEDIUMTEXT: "CHAR",
    TEXT: "CHAR",
    TINYBLOB: "CHAR",
    TINYTEXT: "CHAR",
    VARCHAR: "CHAR",
  }

  static SIGNED_CAST_MAPPING: Record<string, string> = {
    BIGINT: "SIGNED",
    BOOLEAN: "SIGNED",
    INT: "SIGNED",
    SMALLINT: "SIGNED",
    TINYINT: "SIGNED",
    MEDIUMINT: "SIGNED",
  }

  static override FEATURES = {
    ...Generator.FEATURES,
    INTERVAL_ALLOWS_PLURAL_FORM: false,
    NULL_ORDERING_SUPPORTED: null as boolean | null,
    CONCAT_COALESCE: true,
    SAFE_DIVISION: true,
  }

  static override TYPE_MAPPING: Map<string, string> = new Map([
    ...Generator.TYPE_MAPPING,
    // Unsigned types
    ["UBIGINT", "BIGINT"],
    ["UINT", "INT"],
    ["UMEDIUMINT", "MEDIUMINT"],
    ["USMALLINT", "SMALLINT"],
    ["UTINYINT", "TINYINT"],
    ["UDECIMAL", "DECIMAL"],
    ["UDOUBLE", "DOUBLE"],
    // Timestamp types
    ["DATETIME2", "DATETIME"],
    ["SMALLDATETIME", "DATETIME"],
    ["TIMESTAMP", "DATETIME"],
    ["TIMESTAMPNTZ", "DATETIME"],
    ["TIMESTAMPTZ", "TIMESTAMP"],
    ["TIMESTAMPLTZ", "TIMESTAMP"],
    // Remove base mappings that MySQL supports natively
    ["MEDIUMTEXT", "MEDIUMTEXT"],
    ["LONGTEXT", "LONGTEXT"],
    ["TINYTEXT", "TINYTEXT"],
    ["BLOB", "BLOB"],
    ["MEDIUMBLOB", "MEDIUMBLOB"],
    ["LONGBLOB", "LONGBLOB"],
    ["TINYBLOB", "TINYBLOB"],
  ])

  static override TRANSFORMS: Map<ExpressionClass, Transform> = new Map<
    ExpressionClass,
    Transform
  >([
    ...Generator.TRANSFORMS,
    [exp.Select, preprocess([eliminateQualify, eliminateSemiAndAntiJoins])],
    [exp.CurrentDate, () => "CURRENT_DATE"],
    [exp.CurrentTimestamp, () => "CURRENT_TIMESTAMP"],
    [
      exp.NullSafeEQ,
      (gen: Generator, e: exp.Expression) =>
        gen.binary_sql(e as exp.Binary, "<=>"),
    ],
    [
      exp.NullSafeNEQ,
      (gen: Generator, e: exp.Expression) =>
        `NOT ${gen.binary_sql(e as exp.Binary, "<=>")}`,
    ],
    [
      exp.LogicalOr,
      (gen: Generator, e: exp.Expression) =>
        gen.funcCall("MAX", [(e as exp.Func).args.this as exp.Expression]),
    ],
    [
      exp.LogicalAnd,
      (gen: Generator, e: exp.Expression) =>
        gen.funcCall("MIN", [(e as exp.Func).args.this as exp.Expression]),
    ],
    [exp.Rand, () => "RAND()"],
    [
      exp.NumberToStr,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.NumberToStr
        const args: exp.Expression[] = [expr.args.this as exp.Expression]
        if (expr.args.format) args.push(expr.args.format as exp.Expression)
        if (expr.args.culture) args.push(expr.args.culture as exp.Expression)
        return gen.funcCall("FORMAT", args)
      },
    ],
    [
      exp.Length,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.Length
        const funcName = expr.args.binary ? "LENGTH" : "CHAR_LENGTH"
        return gen.funcCall(funcName, [expr.args.this as exp.Expression])
      },
    ],
    [
      exp.GroupConcat,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.GroupConcat
        const thisExpr = gen.sql(expr.args.this as exp.Expression)
        const sep = expr.args.separator as exp.Expression | undefined
        const sepSql = sep ? gen.sql(sep) : "','"
        return `GROUP_CONCAT(${thisExpr} SEPARATOR ${sepSql})`
      },
    ],
    [
      exp.ArrayAgg,
      (gen: Generator, e: exp.Expression) =>
        gen.funcCall("GROUP_CONCAT", [
          (e as exp.Func).args.this as exp.Expression,
        ]),
    ],
    [
      exp.TryCast,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.TryCast
        const thisExpr = gen.sql(expr.args.this as exp.Expression)
        const to = gen.sql(expr.args.to as exp.Expression)
        return `CAST(${thisExpr} AS ${to})`
      },
    ],
    [
      exp.UnixToTime,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.UnixToTime
        const scale = expr.args.scale as exp.Literal | undefined
        const scaleValue =
          scale instanceof exp.Literal ? String(scale.value) : undefined
        const timestamp = gen.sql(expr.args.this as exp.Expression)
        if (!scaleValue || scaleValue === "0") {
          return `FROM_UNIXTIME(${timestamp})`
        }
        return `FROM_UNIXTIME(${timestamp} / POWER(10, ${scaleValue}))`
      },
    ],
    [
      exp.BitwiseAndAgg,
      (gen: Generator, e: exp.Expression) =>
        gen.funcCall("BIT_AND", [(e as exp.Func).args.this as exp.Expression]),
    ],
    [
      exp.BitwiseOrAgg,
      (gen: Generator, e: exp.Expression) =>
        gen.funcCall("BIT_OR", [(e as exp.Func).args.this as exp.Expression]),
    ],
    [
      exp.BitwiseXorAgg,
      (gen: Generator, e: exp.Expression) =>
        gen.funcCall("BIT_XOR", [(e as exp.Func).args.this as exp.Expression]),
    ],
    [
      exp.CountIf,
      (gen: Generator, e: exp.Expression) => {
        const cond = gen.sql((e as exp.CountIf).args.this as exp.Expression)
        return `SUM(CASE WHEN ${cond} THEN 1 ELSE 0 END)`
      },
    ],
    [exp.TimeStrToTime, timestrtotime_sql],
    [exp.DateStrToDate, datestrtodate_sql],
  ])

  // MySQL uses backticks for identifier quoting
  protected override quoteIdentifier(name: string): string {
    return `\`${name.replace(/`/g, "``")}\``
  }

  // MySQL uses LIMIT offset, count syntax (can also use LIMIT count OFFSET offset)
  protected override limit_sql(expression: exp.Limit): string {
    const count = this.sql(expression.args.this as exp.Expression)
    const offset = expression.args.offset
    if (offset) {
      return `LIMIT ${this.sql(offset as exp.Expression)}, ${count}`
    }
    return `LIMIT ${count}`
  }

  // MySQL uses DIV for integer division
  protected override div_sql(expression: exp.Div): string {
    // Regular / for floating point division
    return this.binary_sql(expression, "/")
  }

  protected override anonymous_sql(expression: exp.Anonymous): string {
    const name = this.normalizeFunc(expression.name)

    // MySQL uses CONCAT function instead of ||
    if (name === "CONCAT") {
      const args = expression.expressions
      return `CONCAT(${this.expressions(args)})`
    }

    // IFNULL instead of COALESCE for 2 args
    if (name === "COALESCE" && expression.expressions.length === 2) {
      const args = expression.expressions
      return `IFNULL(${this.expressions(args)})`
    }

    return super.anonymous_sql(expression)
  }

  // MySQL uses 1/0 for boolean in some contexts, but also TRUE/FALSE
  protected override boolean_sql(expression: exp.Boolean): string {
    return expression.value ? "TRUE" : "FALSE"
  }

  // MySQL GROUP_CONCAT instead of STRING_AGG
  // Override if needed

  // MySQL ILIKE is not supported, use LIKE with COLLATE
  protected override ilike_sql(expression: exp.ILike): string {
    // Convert ILIKE to LIKE (MySQL LIKE is case-insensitive by default for non-binary strings)
    let sql = this.binary_sql(expression, "LIKE")
    const escapeExpr = expression.args.escape
    if (escapeExpr) {
      sql += ` ESCAPE ${this.sql(escapeExpr as exp.Expression)}`
    }
    return sql
  }

  protected override ignorenulls_sql(expression: exp.IgnoreNulls): string {
    this.unsupported("MySQL does not support IGNORE NULLS.")
    return this.sql(expression.args.this as exp.Expression)
  }

  static CAST_MAPPING: Record<string, string> = {
    ...MySQLGenerator.CHAR_CAST_MAPPING,
    ...MySQLGenerator.SIGNED_CAST_MAPPING,
    UBIGINT: "UNSIGNED",
  }

  static TIMESTAMP_FUNC_TYPES = new Set(["TIMESTAMPTZ", "TIMESTAMPLTZ"])

  protected override shouldQuote(name: string): boolean {
    if (name.toUpperCase() === "STRAIGHT_JOIN") return true
    return super.shouldQuote(name)
  }

  protected override cast_sql(expression: exp.Cast): string {
    const toDataType = expression.args.to as exp.DataType | undefined
    if (toDataType) {
      const typeName = toDataType.text("this").toUpperCase()
      if (MySQLGenerator.TIMESTAMP_FUNC_TYPES.has(typeName)) {
        return this.funcCall("TIMESTAMP", [
          expression.args.this as exp.Expression,
        ])
      }
      const mapped = MySQLGenerator.CAST_MAPPING[typeName]
      if (mapped) {
        const newDataType = new exp.DataType({ this: mapped })
        const newCast = new exp.Cast({
          this: expression.args.this,
          to: newDataType,
        })
        return super.cast_sql(newCast)
      }
    }
    return super.cast_sql(expression)
  }

  protected override show_sql(expression: exp.Show): string {
    const name = expression.name
    const full = expression.args.full ? " FULL" : ""
    const global_ = expression.args.global_ ? " GLOBAL" : ""

    let target = this.sql(expression, "target")
    if (target) {
      if (name === "COLUMNS" || name === "INDEX") {
        target = ` FROM ${target}`
      } else if (name === "GRANTS") {
        target = ` FOR ${target}`
      } else if (name === "LINKS" || name === "PARTITIONS") {
        target = ` ON ${target}`
      } else if (name === "PROJECTIONS") {
        target = ` ON TABLE ${target}`
      } else {
        target = ` ${target}`
      }
    } else {
      target = ""
    }

    const db = expression.args.db
      ? ` FROM ${this.sql(expression.args.db as exp.Expression)}`
      : ""
    const like = expression.args.like
      ? ` LIKE ${this.sql(expression.args.like as exp.Expression)}`
      : ""
    const where = expression.args.where
      ? ` ${this.sql(expression.args.where as exp.Expression)}`
      : ""

    return `SHOW${full}${global_} ${name}${target}${db}${like}${where}`
  }
}

export class MySQLDialect extends Dialect {
  static override readonly name = "mysql"
  static override CONCAT_COALESCE = true
  static override SAFE_DIVISION = true
  static override BIT_START = "b'"
  static override BIT_END = "'"
  static override HEX_START = "x'"
  static override HEX_END = "'"
  static override STRING_ESCAPES = ["'", '"', "\\"]
  static override UNESCAPED_SEQUENCES = buildUnescapedSequences()
  static override ESCAPED_SEQUENCES = buildEscapedSequences(
    MySQLDialect.UNESCAPED_SEQUENCES,
  )
  static override STRINGS_SUPPORT_ESCAPED_SEQUENCES = true
  protected static override ParserClass = MySQLParser
  protected static override GeneratorClass = MySQLGenerator

  constructor(options: any = {}) {
    super({
      ...options,
      tokenizer: {
        ...options.tokenizer,
        bitStrings: [
          ["b'", "'"],
          ["B'", "'"],
          ["0b", ""],
        ],
        hexStrings: [
          ["x'", "'"],
          ["X'", "'"],
          ["0x", ""],
        ],
      },
    })
  }
}

// Register dialect
Dialect.register(MySQLDialect)
