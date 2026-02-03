/**
 * PostgreSQL dialect
 */

import { Dialect } from "../dialect.js"
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
  regexpReplaceGlobalModifier,
  timestamptrunc_sql,
  timestrtotime_sql,
} from "../transforms.js"

type Transform = (generator: Generator, expression: exp.Expression) => string

function renameFunc(name: string): Transform {
  return (gen: Generator, e: exp.Expression) => {
    const expr = e as exp.Func
    const args: exp.Expression[] = []
    const argTypes = (expr.constructor as typeof exp.Expression).argTypes
    for (const key of Object.keys(argTypes)) {
      if (key === "order") continue
      const argValue = expr.args[key]
      if (Array.isArray(argValue)) {
        for (const value of argValue) {
          if (value instanceof exp.Expression) args.push(value)
        }
      } else if (argValue instanceof exp.Expression) {
        args.push(argValue)
      }
    }
    return gen.funcCall(name, args)
  }
}

function boolXorSql(gen: Generator, e: exp.Expression): string {
  const expr = e as exp.Xor
  const a = gen.sql(expr.left)
  const b = gen.sql(expr.right)
  return `(${a} AND (NOT ${b})) OR ((NOT ${a}) AND ${b})`
}

export class PostgresParser extends Parser {
  static override BITWISE: Map<TokenType, ExpressionClass> = new Map([
    ...Parser.BITWISE,
    [TokenType.HASH, exp.BitwiseXor],
  ])

  static override FUNCTIONS = new Map([
    ...Parser.FUNCTIONS,
    [
      "SHA256",
      (args: exp.Expression[]) =>
        new exp.SHA2({ this: args[0], length: exp.Literal.number(256) }),
    ],
    [
      "SHA384",
      (args: exp.Expression[]) =>
        new exp.SHA2({ this: args[0], length: exp.Literal.number(384) }),
    ],
    [
      "SHA512",
      (args: exp.Expression[]) =>
        new exp.SHA2({ this: args[0], length: exp.Literal.number(512) }),
    ],
    ["UNNEST", (args: exp.Expression[]) => new exp.Explode({ this: args[0] })],
  ])
}

export class PostgresGenerator extends Generator {
  static override BIT_START: string | null = "b'"
  static override BIT_END: string | null = "'"
  static override HEX_START: string | null = "x'"
  static override HEX_END: string | null = "'"
  static override BYTE_START: string | null = "e'"
  static override BYTE_END: string | null = "'"

  static override NULL_ORDERING:
    | "nulls_are_small"
    | "nulls_are_large"
    | "nulls_are_last" = "nulls_are_large"
  protected override INDEX_OFFSET = 1
  protected override ARRAY_SIZE_DIM_REQUIRED: boolean | undefined = true

  static override FEATURES = {
    ...Generator.FEATURES,
    LOCKING_READS_SUPPORTED: true,
    RENAME_TABLE_WITH_DB: false,
    CONCAT_COALESCE: true,
    TYPED_DIVISION: true,
  }

  static override TYPE_MAPPING: Map<string, string> = new Map([
    ...Generator.TYPE_MAPPING,
    ["TINYINT", "SMALLINT"],
    ["FLOAT", "REAL"],
    ["DOUBLE", "DOUBLE PRECISION"],
    ["BINARY", "BYTEA"],
    ["VARBINARY", "BYTEA"],
    ["ROWVERSION", "BYTEA"],
    ["DATETIME", "TIMESTAMP"],
    ["TIMESTAMPNTZ", "TIMESTAMP"],
    ["BLOB", "BYTEA"],
  ])

  // Postgres INVERSE_TIME_MAPPING (auto-reversed from Postgres TIME_MAPPING)
  static override INVERSE_TIME_MAPPING: Map<string, string> = new Map([
    ["%u", "D"],
    ["%d", "DD"],
    ["%j", "DDD"],
    ["%-d", "FMDD"],
    ["%-j", "FMDDD"],
    ["%-I", "FMHH12"],
    ["%-H", "FMHH24"],
    ["%-M", "FMMI"],
    ["%-m", "FMMM"],
    ["%-S", "FMSS"],
    ["%I", "HH12"],
    ["%H", "HH24"],
    ["%M", "MI"],
    ["%m", "MM"],
    ["%z", "OF"],
    ["%S", "SS"],
    ["%A", "TMDay"],
    ["%a", "TMDy"],
    ["%b", "TMMon"],
    ["%B", "TMMonth"],
    ["%Z", "TZ"],
    ["%f", "US"],
    ["%U", "WW"],
    ["%y", "YY"],
    ["%Y", "YYYY"],
  ])

  static override TRANSFORMS: Map<ExpressionClass, Transform> = new Map<
    ExpressionClass,
    Transform
  >([
    ...Generator.TRANSFORMS,
    [exp.Select, preprocess([eliminateQualify, eliminateSemiAndAntiJoins])],
    [exp.Xor, boolXorSql],
    [exp.CurrentDate, () => "CURRENT_DATE"],
    [exp.CurrentTimestamp, () => "CURRENT_TIMESTAMP"],
    [exp.CurrentUser, () => "CURRENT_USER"],
    [
      exp.UnixToTime,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.UnixToTime
        const scale = expr.args.scale as exp.Literal | undefined
        const scaleValue =
          scale instanceof exp.Literal ? String(scale.value) : undefined
        const timestamp = gen.sql(expr.args.this as exp.Expression)
        if (!scaleValue || scaleValue === "0") {
          return `TO_TIMESTAMP(${timestamp})`
        }
        return `TO_TIMESTAMP(CAST(${timestamp} AS DOUBLE PRECISION) / POWER(10, ${scaleValue}))`
      },
    ],
    [
      exp.TimeToStr,
      (gen: Generator, e: exp.Expression) => {
        const fmt = gen.formatTimeStr(e)
        const thisExpr = gen.sql(
          (e as exp.TimeToStr).args.this as exp.Expression,
        )
        return `TO_CHAR(${thisExpr}, ${fmt})`
      },
    ],
    [
      exp.StrToTime,
      (gen: Generator, e: exp.Expression) => {
        const fmt = gen.formatTimeStr(e)
        const thisExpr = gen.sql(
          (e as exp.StrToTime).args.this as exp.Expression,
        )
        return `TO_TIMESTAMP(${thisExpr}, ${fmt})`
      },
    ],
    [
      exp.StructExtract,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.StructExtract
        const thisExpr = gen.sql(expr.args.this as exp.Expression)
        const field = String(
          (expr.args.expression as exp.Expression).args.this ?? "",
        )
        return `${thisExpr}.${field}`
      },
    ],
    [exp.Variance, renameFunc("VAR_SAMP")],
    [exp.VariancePop, renameFunc("VAR_POP")],
    [
      exp.BitwiseXor,
      (gen: Generator, e: exp.Expression) =>
        gen.binary_sql(e as exp.Binary, "#"),
    ],
    [exp.BitwiseAndAgg, renameFunc("BIT_AND")],
    [exp.BitwiseOrAgg, renameFunc("BIT_OR")],
    [exp.BitwiseXorAgg, renameFunc("BIT_XOR")],
    [
      exp.ArrayConcat,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.ArrayConcat
        const args: exp.Expression[] = [expr.args.this as exp.Expression]
        const exprs = expr.expressions
        if (exprs.length > 0) args.push(...exprs)
        return gen.funcCall("ARRAY_CAT", args)
      },
    ],
    [exp.Explode, renameFunc("UNNEST")],
    [exp.ExplodingGenerateSeries, renameFunc("GENERATE_SERIES")],
    [exp.Levenshtein, renameFunc("LEVENSHTEIN")],
    [
      exp.GroupConcat,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.GroupConcat
        const args: exp.Expression[] = [expr.args.this as exp.Expression]
        const sep = expr.args.separator as exp.Expression | undefined
        if (sep) args.push(sep)
        return gen.funcCall("STRING_AGG", args)
      },
    ],
    [
      exp.DateAdd,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.DateAdd
        const thisExpr = gen.sql(expr.args.this as exp.Expression)
        const unit = expr.text("unit").toUpperCase() || "DAY"
        const amount = gen.sql(expr.args.expression as exp.Expression)
        return `${thisExpr} + INTERVAL '${amount}' ${unit}`
      },
    ],
    [
      exp.DateSub,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.DateSub
        const thisExpr = gen.sql(expr.args.this as exp.Expression)
        const unit = expr.text("unit").toUpperCase() || "DAY"
        const amount = gen.sql(expr.args.expression as exp.Expression)
        return `${thisExpr} - INTERVAL '${amount}' ${unit}`
      },
    ],
    [
      exp.DateDiff,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.DateDiff
        const unit = expr.text("unit").toUpperCase() || "DAY"
        const end = gen.sql(expr.args.this as exp.Expression)
        const start = gen.sql(expr.args.expression as exp.Expression)
        if (unit === "MONTH")
          return `(EXTRACT(YEAR FROM ${end}) * 12 + EXTRACT(MONTH FROM ${end})) - (EXTRACT(YEAR FROM ${start}) * 12 + EXTRACT(MONTH FROM ${start}))`
        if (unit === "YEAR")
          return `EXTRACT(YEAR FROM ${end}) - EXTRACT(YEAR FROM ${start})`
        if (unit === "DAY")
          return `EXTRACT(epoch FROM (${end} - ${start})) / 86400`
        if (unit === "HOUR")
          return `EXTRACT(epoch FROM (${end} - ${start})) / 3600`
        if (unit === "MINUTE")
          return `EXTRACT(epoch FROM (${end} - ${start})) / 60`
        if (unit === "SECOND") return `EXTRACT(epoch FROM (${end} - ${start}))`
        return `EXTRACT(epoch FROM (${end} - ${start}))`
      },
    ],
    [
      exp.TsOrDsAdd,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.TsOrDsAdd
        const thisExpr = gen.sql(expr.args.this as exp.Expression)
        const unit = expr.text("unit").toUpperCase() || "DAY"
        const amount = gen.sql(expr.args.expression as exp.Expression)
        return `${thisExpr} + INTERVAL '${amount}' ${unit}`
      },
    ],
    [
      exp.TsOrDsDiff,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.TsOrDsDiff
        const unit = expr.text("unit").toUpperCase() || "DAY"
        const end = gen.sql(expr.args.this as exp.Expression)
        const start = gen.sql(expr.args.expression as exp.Expression)
        if (unit === "MONTH")
          return `(EXTRACT(YEAR FROM ${end}) * 12 + EXTRACT(MONTH FROM ${end})) - (EXTRACT(YEAR FROM ${start}) * 12 + EXTRACT(MONTH FROM ${start}))`
        if (unit === "YEAR")
          return `EXTRACT(YEAR FROM ${end}) - EXTRACT(YEAR FROM ${start})`
        if (unit === "DAY")
          return `EXTRACT(epoch FROM (${end} - ${start})) / 86400`
        if (unit === "HOUR")
          return `EXTRACT(epoch FROM (${end} - ${start})) / 3600`
        if (unit === "MINUTE")
          return `EXTRACT(epoch FROM (${end} - ${start})) / 60`
        if (unit === "SECOND") return `EXTRACT(epoch FROM (${end} - ${start}))`
        return `EXTRACT(epoch FROM (${end} - ${start}))`
      },
    ],
    [exp.IntDiv, renameFunc("DIV")],
    [exp.LogicalOr, renameFunc("BOOL_OR")],
    [exp.LogicalAnd, renameFunc("BOOL_AND")],
    [exp.Rand, (_gen: Generator, _e: exp.Expression) => "RANDOM()"],
    [
      exp.TryCast,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.TryCast
        const thisExpr = gen.sql(expr.args.this as exp.Expression)
        const to = gen.sql(expr.args.to as exp.Expression)
        return `CAST(${thisExpr} AS ${to})`
      },
    ],
    [exp.Uuid, (_gen: Generator, _e: exp.Expression) => "GEN_RANDOM_UUID()"],
    [
      exp.TimeToUnix,
      (gen: Generator, e: exp.Expression) => {
        const thisExpr = gen.sql(
          (e as exp.TimeToUnix).args.this as exp.Expression,
        )
        return `DATE_PART('epoch', ${thisExpr})`
      },
    ],
    [
      exp.CountIf,
      (gen: Generator, e: exp.Expression) => {
        const cond = gen.sql((e as exp.CountIf).args.this as exp.Expression)
        return `SUM(CASE WHEN ${cond} THEN 1 ELSE 0 END)`
      },
    ],
    [
      exp.ArrayContains,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.ArrayContains
        const value = gen.sql(expr.args.expression as exp.Expression)
        const array = gen.sql(expr.args.this as exp.Expression)
        return `CASE WHEN ${value} IS NULL THEN NULL ELSE COALESCE(${value} = ANY(${array}), FALSE) END`
      },
    ],
    [exp.Unicode, renameFunc("ASCII")],
    [
      exp.SHA2,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.SHA2
        const length = String(
          (expr.args.length as exp.Expression | undefined)?.args?.this ?? "256",
        )
        return gen.funcCall(`SHA${length}`, [expr.args.this as exp.Expression])
      },
    ],
    [
      exp.RegexpReplace,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.RegexpReplace
        const modifiers = regexpReplaceGlobalModifier(expr)
        return gen.funcCall(
          "REGEXP_REPLACE",
          [
            expr.args.this as exp.Expression,
            expr.args.expression as exp.Expression,
            expr.args.replacement as exp.Expression | undefined,
            expr.args.position as exp.Expression | undefined,
            expr.args.occurrence as exp.Expression | undefined,
            modifiers,
          ].filter((x): x is exp.Expression => x != null),
        )
      },
    ],
    [exp.TimestampTrunc, timestamptrunc_sql("DATE_TRUNC", true)],
    [exp.TimeStrToTime, timestrtotime_sql],
    [exp.DateStrToDate, datestrtodate_sql],
  ])

  // PostgreSQL uses CAST() for generated output (:: is only for parsing)
  protected override cast_sql(expression: exp.Cast): string {
    const expr = this.sql(expression.args.this as exp.Expression)
    const to = this.sql(expression.args.to as exp.Expression)
    return `CAST(${expr} AS ${to})`
  }

  // PostgreSQL uses ILIKE for case-insensitive LIKE
  protected override ilike_sql(expression: exp.ILike): string {
    let sql = this.binary_sql(expression, "ILIKE")
    const escapeExpr = expression.args.escape
    if (escapeExpr) {
      sql += ` ESCAPE ${this.sql(escapeExpr as exp.Expression)}`
    }
    return sql
  }

  // PostgreSQL uses || for string concatenation
  protected override anonymous_sql(expression: exp.Anonymous): string {
    const name = expression.name.toUpperCase()

    // CONCAT -> ||
    if (name === "CONCAT") {
      const args = expression.expressions
      if (args.length >= 2) {
        return args.map((a) => this.sql(a)).join(" || ")
      }
    }

    return super.anonymous_sql(expression)
  }

  // PostgreSQL uses TRUE/FALSE (not 1/0)
  protected override boolean_sql(expression: exp.Boolean): string {
    return expression.value ? "TRUE" : "FALSE"
  }

  // Postgres uses BIGINT[] syntax for arrays (postfix brackets)
  protected override datatype_sql(expression: exp.DataType): string {
    const typeStr = expression.text("this").toUpperCase()
    if (typeStr === "ARRAY" && expression.args.nested) {
      const inner = this.expressions(expression.expressions)
      const values = expression.args.values
      if (Array.isArray(values) && values.length > 0) {
        return `${inner}[${this.expressions(values)}]`
      }
      return `${inner}[]`
    }
    return super.datatype_sql(expression)
  }

  // Quote identifiers with double quotes
  protected override quoteIdentifier(name: string): string {
    return `"${name.replace(/"/g, '""')}"`
  }

  // PostgreSQL uses ~ for regex match
  protected override regexplike_sql(expression: exp.RegexpLike): string {
    return this.binary_sql(expression, "~")
  }

  // PostgreSQL uses ~* for case-insensitive regex match
  protected override regexpilike_sql(expression: exp.RegexpILike): string {
    return this.binary_sql(expression, "~*")
  }

  // PostgreSQL uses SUBSTRING(string FROM start FOR length) syntax
  protected override substring_sql(expression: exp.Substring): string {
    const thisExpr = this.sql(expression.args.this as exp.Expression)
    const startVal = expression.args.start
    const start = startVal ? this.sql(startVal as exp.Expression) : ""
    const lengthVal = expression.args.length
    const length = lengthVal ? this.sql(lengthVal as exp.Expression) : ""

    const fromPart = start ? ` FROM ${start}` : ""
    const forPart = length ? ` FOR ${length}` : ""

    return `SUBSTRING(${thisExpr}${fromPart}${forPart})`
  }

  // PostgreSQL uses @@ for text search match
  protected override matchagainst_sql(expression: exp.MatchAgainst): string {
    const expressions = expression.args.expressions as exp.Expression[]
    const left = expressions && expressions[0] ? this.sql(expressions[0]) : ""
    const right = this.sql(expression.args.this as exp.Expression)
    return `${left} @@ ${right}`
  }

  protected override jsonextract_sql(expression: exp.JSONExtract): string {
    return this.binary_sql(expression, "->")
  }

  protected override jsonextractscalar_sql(
    expression: exp.JSONExtractScalar,
  ): string {
    return this.binary_sql(expression, "->>")
  }

  protected override ignorenulls_sql(expression: exp.IgnoreNulls): string {
    this.unsupported("PostgreSQL does not support IGNORE NULLS.")
    return this.sql(expression.args.this as exp.Expression)
  }

  protected override respectnulls_sql(expression: exp.RespectNulls): string {
    this.unsupported("PostgreSQL does not support RESPECT NULLS.")
    return this.sql(expression.args.this as exp.Expression)
  }
}

export class PostgresDialect extends Dialect {
  static override readonly name = "postgres"
  static override NULL_ORDERING:
    | "nulls_are_small"
    | "nulls_are_large"
    | "nulls_are_last" = "nulls_are_large"
  static override INDEX_OFFSET = 1
  static override TYPED_DIVISION = true
  static override CONCAT_COALESCE = true
  static override BIT_START = "b'"
  static override BIT_END = "'"
  static override HEX_START = "x'"
  static override HEX_END = "'"
  static override BYTE_START = "e'"
  static override BYTE_END = "'"
  protected static override ParserClass = PostgresParser
  protected static override GeneratorClass = PostgresGenerator

  // Postgres date format -> strftime mapping
  static override TIME_MAPPING = new Map([
    ["YYYY", "%Y"],
    ["YY", "%y"],
    ["MM", "%m"],
    ["DD", "%d"],
    ["HH24", "%H"],
    ["HH12", "%I"],
    ["HH", "%I"],
    ["MI", "%M"],
    ["SS", "%S"],
    ["MS", "%f"],
    ["US", "%f"],
    ["AM", "%p"],
    ["PM", "%p"],
    ["TZ", "%Z"],
    ["MON", "%b"],
    ["MONTH", "%B"],
    ["DY", "%a"],
    ["DAY", "%A"],
  ])

  constructor(options: any = {}) {
    super({
      ...options,
      tokenizer: {
        ...options.tokenizer,
        bitStrings: [
          ["b'", "'"],
          ["B'", "'"],
        ],
        hexStrings: [
          ["x'", "'"],
          ["X'", "'"],
        ],
      },
    })
  }
}

// Register dialect
Dialect.register(PostgresDialect)
