/**
 * BigQuery dialect
 */

import {
  Dialect,
  buildEscapedSequences,
  buildUnescapedSequences,
} from "../dialect.js"
import type { ExpressionClass } from "../expression-base.js"
import * as exp from "../expressions.js"
import { Generator } from "../generator.js"
import { FunctionBuilder, Parser } from "../parser.js"
import { formatTime } from "../time.js"
import { TokenType, Tokenizer } from "../tokens.js"
import {
  datestrtodate_sql,
  eliminateSemiAndAntiJoins,
  explodeProjectionToUnnest,
  no_ilike_sql,
  preprocess,
  regexpReplaceSql,
  removePrecisionParameterizedTypes,
  renameFunc,
  timestrtotime_sql,
  unqualifyUnnest,
} from "../transforms.js"

type Transform = (generator: Generator, expression: exp.Expression) => string

function bqJsonExtractSql(gen: Generator, e: exp.Expression): string {
  const name =
    (e._meta && (e._meta["name"] as string)) ||
    (e as exp.Func).name.toUpperCase()
  const args: exp.Expression[] = [e.args.this as exp.Expression]
  const expr = e.args.expression as exp.Expression | undefined
  if (expr) args.push(expr)
  return gen.funcCall(name, args)
}

// BigQuery TIME_MAPPING: BigQuery format codes -> strftime codes
// Applied during parsing to normalize format strings
const BQ_TIME_MAPPING = new Map([
  ["%x", "%m/%d/%y"],
  ["%D", "%m/%d/%y"],
  ["%E6S", "%S.%f"],
  ["%e", "%-d"],
  ["%F", "%Y-%m-%d"],
  ["%T", "%H:%M:%S"],
  ["%c", "%a %b %e %H:%M:%S %Y"],
])

function buildFormatTime(
  exprType: "date" | "datetime" | "timestamp" | "time",
): (args: exp.Expression[]) => exp.TimeToStr {
  return (args: exp.Expression[]) => {
    const formatArg = args[0]!
    const valueArg = args[1]!
    const zoneArg = args.length > 2 ? args[2] : undefined

    // Convert format string from BigQuery codes to strftime codes
    let format: exp.Expression = formatArg
    if (formatArg instanceof exp.Literal && formatArg.isString) {
      const converted = formatTime(String(formatArg.value), BQ_TIME_MAPPING)
      format = new exp.Literal({ this: converted, is_string: true })
    }

    // Wrap value in appropriate type converter
    let wrappedValue: exp.Expression
    switch (exprType) {
      case "date":
        wrappedValue = new exp.TsOrDsToDate({ this: valueArg })
        break
      case "datetime":
        wrappedValue = new exp.TsOrDsToDatetime({ this: valueArg })
        break
      case "timestamp":
        wrappedValue = new exp.TsOrDsToTimestamp({ this: valueArg })
        break
      case "time":
        wrappedValue = new exp.TsOrDsToTime({ this: valueArg })
        break
    }

    const result = new exp.TimeToStr({ this: wrappedValue, format })
    if (zoneArg) {
      result.set("zone", zoneArg)
    }
    return result
  }
}

export class BigQueryParser extends Parser {
  static override FUNCTION_PARSERS: Map<
    string,
    (parser: Parser) => exp.Expression
  > = new Map([
    ...Parser.FUNCTION_PARSERS,
    [
      "ARRAY",
      (p) =>
        new exp.Array({
          expressions: [(p as BigQueryParser).parseStatement()!],
        }),
    ],
  ])

  static override TYPE_NAME_MAPPING: Map<string, string> = new Map([
    ...Parser.TYPE_NAME_MAPPING,
    ["DATETIME", "TIMESTAMP"],
    ["TIMESTAMP", "TIMESTAMPTZ"],
  ])

  static override FUNCTIONS: Map<string, FunctionBuilder> = new Map([
    ...Parser.FUNCTIONS,
    [
      "DATE",
      (args: exp.Expression[]) => {
        if (args.length === 3) {
          return new exp.DateFromParts({
            year: args[0],
            month: args[1],
            day: args[2],
          })
        }
        return new exp.Date({ this: args[0], zone: args[1] })
      },
    ],
    [
      "PARSE_DATE",
      (args: exp.Expression[]) => {
        // PARSE_DATE(format, date_string) → StrToDate(this=date_string, format=format)
        return new exp.StrToDate({ this: args[1], format: args[0] })
      },
    ],
    [
      "BIT_AND",
      (args: exp.Expression[]) => new exp.BitwiseAndAgg({ this: args[0] }),
    ],
    [
      "BIT_OR",
      (args: exp.Expression[]) => new exp.BitwiseOrAgg({ this: args[0] }),
    ],
    [
      "BIT_XOR",
      (args: exp.Expression[]) => new exp.BitwiseXorAgg({ this: args[0] }),
    ],
    [
      "DATE_TRUNC",
      (args: exp.Expression[]) =>
        new exp.DateTrunc({
          this: args[0],
          unit: args[1],
          zone: args[2],
        }),
    ],
    ["FORMAT_DATE", buildFormatTime("date")],
    ["FORMAT_DATETIME", buildFormatTime("datetime")],
    ["FORMAT_TIMESTAMP", buildFormatTime("timestamp")],
    ["FORMAT_TIME", buildFormatTime("time")],
    [
      "JSON_QUERY",
      (args: exp.Expression[]) => {
        const e = new exp.JSONExtract({ this: args[0], expression: args[1] })
        e._meta = { ...e._meta, name: "JSON_QUERY" }
        return e
      },
    ],
    [
      "JSON_EXTRACT_SCALAR",
      (args: exp.Expression[]) => {
        if (args.length === 1) args.push(exp.Literal.string("$"))
        const e = new exp.JSONExtractScalar({
          this: args[0],
          expression: args[1],
          scalar_only: true,
        })
        e._meta = { ...e._meta, name: "JSON_EXTRACT_SCALAR" }
        return e
      },
    ],
    [
      "JSON_VALUE",
      (args: exp.Expression[]) => {
        if (args.length === 1) args.push(exp.Literal.string("$"))
        const e = new exp.JSONExtractScalar({
          this: args[0],
          expression: args[1],
          scalar_only: true,
        })
        e._meta = { ...e._meta, name: "JSON_VALUE" }
        return e
      },
    ],
    [
      "JSON_EXTRACT_ARRAY",
      (args: exp.Expression[]) => {
        if (args.length === 1) args.push(exp.Literal.string("$"))
        const e = new exp.JSONExtractArray({
          this: args[0],
          expression: args[1],
        })
        e._meta = { ...e._meta, name: "JSON_EXTRACT_ARRAY" }
        return e
      },
    ],
    [
      "JSON_QUERY_ARRAY",
      (args: exp.Expression[]) => {
        if (args.length === 1) args.push(exp.Literal.string("$"))
        const e = new exp.JSONExtractArray({
          this: args[0],
          expression: args[1],
        })
        e._meta = { ...e._meta, name: "JSON_QUERY_ARRAY" }
        return e
      },
    ],
    [
      "UNIX_SECONDS",
      (args: exp.Expression[]) => new exp.UnixSeconds({ this: args[0] }),
    ],
    [
      "UNIX_MILLIS",
      (args: exp.Expression[]) => new exp.UnixMillis({ this: args[0] }),
    ],
    [
      "UNIX_MICROS",
      (args: exp.Expression[]) => new exp.UnixMicros({ this: args[0] }),
    ],
    [
      "GENERATE_DATE_ARRAY",
      (args: exp.Expression[]) => {
        const step =
          args[2] ??
          new exp.Interval({
            this: new exp.Literal({ this: "1", is_string: true }),
            unit: new exp.Var({ this: "DAY" }),
          })
        return new exp.GenerateDateArray({
          start: args[0],
          end: args[1],
          step,
        })
      },
    ],
    [
      "GENERATE_TIMESTAMP_ARRAY",
      (args: exp.Expression[]) =>
        new exp.GenerateTimestampArray({
          start: args[0],
          end: args[1],
          step: args[2],
        }),
    ],
    [
      "SHA256",
      (args: exp.Expression[]) =>
        new exp.SHA2Digest({
          this: args[0],
          length: exp.Literal.number(256),
        }),
    ],
    [
      "SHA512",
      (args: exp.Expression[]) =>
        new exp.SHA2({ this: args[0], length: exp.Literal.number(512) }),
    ],
  ])

  protected override parseUnary(): exp.Expression {
    if (this.match(TokenType.AT)) {
      const nextType = this.current.tokenType as TokenType
      if (nextType === TokenType.VAR || nextType === TokenType.NUMBER) {
        const name = this.advance().text
        return new exp.Parameter({ this: name })
      }
      return new exp.Parameter({})
    }
    return super.parseUnary()
  }

  protected override parseUnnest(): exp.Unnest | undefined {
    const unnest = super.parseUnnest()
    if (!unnest) return undefined

    const alias = unnest.args.alias as exp.TableAlias | undefined
    if (alias instanceof exp.TableAlias) {
      if (alias.args.columns) {
        // Already has columns — unexpected for BigQuery
      } else if (alias.args.this) {
        alias.set("columns", [alias.args.this as exp.Expression])
        alias.set("this", undefined)
      }
    }
    return unnest
  }
}

export class BigQueryGenerator extends Generator {
  static override NORMALIZE_FUNCTIONS: boolean | "upper" | "lower" = false
  static override PRESERVE_ORIGINAL_NAMES = true
  static override HEX_START: string | null = "0x"
  static override HEX_END: string | null = ""
  static override HEX_STRING_IS_INTEGER_TYPE = true
  static override BYTE_START: string | null = "b'"
  static override BYTE_END: string | null = "'"
  static override BYTE_STRING_IS_BYTES_TYPE = true
  static override STRINGS_SUPPORT_ESCAPED_SEQUENCES = true
  static override ESCAPED_SEQUENCES: Record<string, string> =
    buildEscapedSequences(buildUnescapedSequences())
  static override STRING_ESCAPES: string[] = ["\\"]

  static override FEATURES: {
    NULL_ORDERING_SUPPORTED: boolean
    INTERVAL_ALLOWS_PLURAL_FORM: boolean
    RENAME_TABLE_WITH_DB: boolean
    UNNEST_WITH_ORDINALITY: boolean
    LOCKING_READS_SUPPORTED: boolean
    LIMIT_FETCH: "ALL" | "LIMIT" | "FETCH"
    LIMIT_IS_TOP: boolean
    EXTRACT_ALLOWS_QUOTES: boolean
    IGNORE_NULLS_IN_FUNC: boolean
    NVL2_SUPPORTED: boolean
    SUPPORTS_SINGLE_ARG_CONCAT: boolean
    LAST_DAY_SUPPORTS_DATE_PART: boolean
    COLLATE_IS_FUNC: boolean
    EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE: boolean
    WRAP_DERIVED_VALUES: boolean
    VALUES_AS_TABLE: boolean
    SINGLE_STRING_INTERVAL: boolean
    ALTER_TABLE_INCLUDE_COLUMN_KEYWORD: boolean
    ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN: boolean
    ALTER_TABLE_SUPPORTS_CASCADE: boolean
    SUPPORTS_TABLE_COPY: boolean
    SUPPORTS_TABLE_ALIAS_COLUMNS: boolean
    JOIN_HINTS: boolean
    TABLE_HINTS: boolean
    QUERY_HINTS: boolean
    IS_BOOL_ALLOWED: boolean
    ENSURE_BOOLS: boolean
    TZ_TO_WITH_TIME_ZONE: boolean
    AGGREGATE_FILTER_SUPPORTED: boolean
    SEMI_ANTI_JOIN_WITH_SIDE: boolean
    TABLESAMPLE_REQUIRES_PARENS: boolean
    CTE_RECURSIVE_KEYWORD_REQUIRED: boolean
    UNPIVOT_ALIASES_ARE_IDENTIFIERS: boolean
    SUPPORTS_SELECT_INTO: boolean
    STAR_EXCEPT: "EXCEPT" | "EXCLUDE" | null
    CONCAT_COALESCE: boolean
    SAFE_DIVISION: boolean
    TYPED_DIVISION: boolean
  } = {
    ...Generator.FEATURES,
    NULL_ORDERING_SUPPORTED: false,
    INTERVAL_ALLOWS_PLURAL_FORM: false,
    RENAME_TABLE_WITH_DB: false,
    UNNEST_WITH_ORDINALITY: false,
  }

  static override TYPE_MAPPING: Map<string, string> = new Map([
    ...Generator.TYPE_MAPPING,
    ["BIGDECIMAL", "BIGNUMERIC"],
    ["BIGINT", "INT64"],
    ["BINARY", "BYTES"],
    ["BLOB", "BYTES"],
    ["BOOLEAN", "BOOL"],
    ["CHAR", "STRING"],
    ["DECIMAL", "NUMERIC"],
    ["DOUBLE", "FLOAT64"],
    ["FLOAT", "FLOAT64"],
    ["INT", "INT64"],
    ["NCHAR", "STRING"],
    ["NVARCHAR", "STRING"],
    ["SMALLINT", "INT64"],
    ["TEXT", "STRING"],
    ["TIMESTAMP", "DATETIME"],
    ["TIMESTAMPNTZ", "DATETIME"],
    ["TIMESTAMPTZ", "TIMESTAMP"],
    ["TIMESTAMPLTZ", "TIMESTAMP"],
    ["TINYINT", "INT64"],
    ["ROWVERSION", "BYTES"],
    ["UUID", "STRING"],
    ["VARBINARY", "BYTES"],
    ["VARCHAR", "STRING"],
    ["VARIANT", "ANY TYPE"],
  ])

  // BigQuery TIME_MAPPING reversed: strftime -> BigQuery format
  // Auto-reversed from: {%x: %m/%d/%y, %D: %m/%d/%y, %E6S: %S.%f, %e: %-d, %F: %Y-%m-%d, %T: %H:%M:%S, %c: %a %b %e %H:%M:%S %Y}
  // Plus manual: {%H:%M:%S.%f: %H:%M:%E6S}
  static override INVERSE_TIME_MAPPING: Map<string, string> = new Map([
    ["%m/%d/%y", "%D"],
    ["%S.%f", "%E6S"],
    ["%-d", "%e"],
    ["%Y-%m-%d", "%F"],
    ["%H:%M:%S", "%T"],
    ["%a %b %e %H:%M:%S %Y", "%c"],
    ["%H:%M:%S.%f", "%H:%M:%E6S"],
  ])

  static override TRANSFORMS: Map<ExpressionClass, Transform> = new Map<
    ExpressionClass,
    Transform
  >([
    ...Generator.TRANSFORMS,
    [exp.Cast, preprocess([removePrecisionParameterizedTypes])],
    [
      exp.Select,
      preprocess(
        [
          explodeProjectionToUnnest(),
          unqualifyUnnest,
          eliminateSemiAndAntiJoins,
        ],
        (gen: Generator, expr: exp.Expression) => {
          // Call select_sql directly without checking TRANSFORMS again
          return (gen as any).select_sql(expr)
        },
      ),
    ],
    [
      exp.DateTrunc,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.DateTrunc
        const unit = expr.args.unit as exp.Expression
        const unitSql =
          unit instanceof exp.Literal && unit.isString
            ? unit.name
            : gen.sql(unit)
        const args: exp.Expression[] = [expr.args.this as exp.Expression]
        const zone = expr.args.zone as exp.Expression | undefined
        return `DATE_TRUNC(${gen.sql(args[0]!)}, ${unitSql}${zone ? `, ${gen.sql(zone)}` : ""})`
      },
    ],
    [
      exp.DateFromParts,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.DateFromParts
        return gen.funcCall("DATE", [
          expr.args.year as exp.Expression,
          expr.args.month as exp.Expression,
          expr.args.day as exp.Expression,
        ])
      },
    ],
    [
      exp.UnixToTime,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.UnixToTime
        const scale = expr.args.scale as exp.Literal | undefined
        const scaleValue =
          scale instanceof exp.Literal ? String(scale.value) : undefined
        const timestamp = expr.args.this as exp.Expression
        if (scaleValue === "3") {
          return gen.funcCall("TIMESTAMP_MILLIS", [timestamp])
        }
        if (scaleValue === "6") {
          return gen.funcCall("TIMESTAMP_MICROS", [timestamp])
        }
        return gen.funcCall("TIMESTAMP_SECONDS", [timestamp])
      },
    ],
    [
      exp.TimeToStr,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.TimeToStr
        const inner = expr.args.this as exp.Expression
        let funcName: string
        if (inner instanceof exp.TsOrDsToDatetime) {
          funcName = "FORMAT_DATETIME"
        } else if (inner instanceof exp.TsOrDsToTimestamp) {
          funcName = "FORMAT_TIMESTAMP"
        } else if (inner instanceof exp.TsOrDsToTime) {
          funcName = "FORMAT_TIME"
        } else {
          funcName = "FORMAT_DATE"
        }
        const fmt = gen.formatTimeStr(e)
        const TS_OR_DS_TYPES = [
          exp.TsOrDsToDate,
          exp.TsOrDsToDatetime,
          exp.TsOrDsToTimestamp,
          exp.TsOrDsToTime,
        ] as const
        const isWrapped = TS_OR_DS_TYPES.some((t) => inner instanceof t)
        const valueExpr = isWrapped
          ? gen.sql(inner.args.this as exp.Expression)
          : gen.sql(inner)
        const zone = expr.args.zone
          ? `, ${gen.sql(expr.args.zone as exp.Expression)}`
          : ""
        return `${funcName}(${fmt}, ${valueExpr}${zone})`
      },
    ],
    [
      exp.StrToTime,
      (gen: Generator, e: exp.Expression) => {
        const fmt = gen.formatTimeStr(e)
        const thisExpr = gen.sql(
          (e as exp.StrToTime).args.this as exp.Expression,
        )
        return `PARSE_TIMESTAMP(${fmt}, ${thisExpr})`
      },
    ],
    [
      exp.StrToDate,
      (gen: Generator, e: exp.Expression) => {
        const fmt = gen.formatTimeStr(e)
        const thisExpr = gen.sql(
          (e as exp.StrToDate).args.this as exp.Expression,
        )
        return `PARSE_DATE(${fmt}, ${thisExpr})`
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
    [exp.RegexpReplace, regexpReplaceSql],
    [exp.JSONExtract, bqJsonExtractSql],
    [exp.JSONExtractScalar, bqJsonExtractSql],
    [exp.JSONExtractArray, bqJsonExtractSql],
    [
      exp.UnixSeconds,
      (gen: Generator, e: exp.Expression) =>
        gen.funcCall("UNIX_SECONDS", [
          (e as exp.UnixSeconds).args.this as exp.Expression,
        ]),
    ],
    [
      exp.UnixMillis,
      (gen: Generator, e: exp.Expression) =>
        gen.funcCall("UNIX_MILLIS", [
          (e as exp.UnixMillis).args.this as exp.Expression,
        ]),
    ],
    [
      exp.UnixMicros,
      (gen: Generator, e: exp.Expression) =>
        gen.funcCall("UNIX_MICROS", [
          (e as exp.UnixMicros).args.this as exp.Expression,
        ]),
    ],
    [
      exp.SHA,
      (gen: Generator, e: exp.Expression) =>
        gen.funcCall("SHA1", [(e as exp.SHA).args.this as exp.Expression]),
    ],
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
      exp.SHA2Digest,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.SHA2Digest
        const length = String(
          (expr.args.length as exp.Expression | undefined)?.args?.this ?? "256",
        )
        return gen.funcCall(`SHA${length}`, [expr.args.this as exp.Expression])
      },
    ],
    [
      exp.If,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.If
        const args: exp.Expression[] = [expr.args.this as exp.Expression]
        if (expr.args.true) args.push(expr.args.true as exp.Expression)
        const falseVal = expr.args.false as exp.Expression | undefined
        if (falseVal) {
          args.push(falseVal)
        } else {
          args.push(new exp.Null({}))
        }
        return gen.funcCall("IF", args)
      },
    ],
    [
      exp.GenerateSeries,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.GenerateSeries
        const args: exp.Expression[] = []
        if (expr.args.start) args.push(expr.args.start as exp.Expression)
        if (expr.args.end) args.push(expr.args.end as exp.Expression)
        if (expr.args.step) args.push(expr.args.step as exp.Expression)
        return gen.funcCall("GENERATE_ARRAY", args)
      },
    ],
    [
      exp.CountIf,
      (gen: Generator, e: exp.Expression) =>
        gen.funcCall("COUNTIF", [
          (e as exp.CountIf).args.this as exp.Expression,
        ]),
    ],
    [exp.Uuid, (_gen: Generator, _e: exp.Expression) => "GENERATE_UUID()"],
    [exp.TimeStrToTime, timestrtotime_sql],
    [exp.DateStrToDate, datestrtodate_sql],
    [exp.ILike, no_ilike_sql],
    [
      exp.IntDiv,
      (gen: Generator, e: exp.Expression) =>
        gen.funcCall("DIV", [
          (e as exp.IntDiv).args.this as exp.Expression,
          (e as exp.IntDiv).args.expression as exp.Expression,
        ]),
    ],
    [exp.VariancePop, renameFunc("VAR_POP")],
    [exp.ApproxDistinct, renameFunc("APPROX_COUNT_DISTINCT")],
    [
      exp.HexString,
      (gen: Generator, e: exp.Expression) =>
        (gen as any).hexstring_sql(e as exp.HexString, "FROM_HEX"),
    ],
  ])

  protected override mod_sql(expression: exp.Mod): string {
    let thisExpr = expression.args.this as exp.Expression
    let exprExpr = expression.args.expression as exp.Expression
    if (
      thisExpr instanceof exp.Paren &&
      thisExpr.args.this instanceof exp.Expression
    ) {
      thisExpr = thisExpr.args.this as exp.Expression
    }
    if (
      exprExpr instanceof exp.Paren &&
      exprExpr.args.this instanceof exp.Expression
    ) {
      exprExpr = exprExpr.args.this as exp.Expression
    }
    return this.funcCall("MOD", [thisExpr, exprExpr])
  }

  protected override attimezone_sql(expression: exp.AtTimeZone): string {
    const thisExpr = this.sql(expression.args.this as exp.Expression)
    const zone = this.sql(expression.args.zone as exp.Expression)
    return `TIMESTAMP(DATETIME(${thisExpr}, ${zone}))`
  }

  // BigQuery uses backticks for identifier quoting
  protected override quoteIdentifier(name: string): string {
    return `\`${name.replace(/`/g, "\\`")}\``
  }

  // BigQuery uses SAFE_CAST for TRY_CAST
  protected override trycast_sql(expression: exp.TryCast): string {
    const expr = this.sql(expression.args.this as exp.Expression)
    const to = this.sql(expression.args.to as exp.Expression)
    return `SAFE_CAST(${expr} AS ${to})`
  }

  // BigQuery uses || for string concatenation (like PostgreSQL)
  protected override anonymous_sql(expression: exp.Anonymous): string {
    const name = expression.name.toUpperCase()

    // CONCAT -> use CONCAT function (BigQuery has both || and CONCAT)
    if (name === "CONCAT") {
      const args = expression.expressions
      return `CONCAT(${this.expressions(args)})`
    }

    // NVL -> IFNULL
    if (name === "NVL" && expression.expressions.length === 2) {
      const args = expression.expressions
      return `IFNULL(${this.expressions(args)})`
    }

    return super.anonymous_sql(expression)
  }

  // BigQuery STRUCT syntax: STRUCT(value AS key, ...)
  protected override struct_sql(expression: exp.Struct): string {
    const exprs = expression.expressions.map((e) => {
      if (e instanceof exp.PropertyEQ) {
        const key = e.args.this as exp.Expression
        const value = e.args.expression as exp.Expression
        const keyStr =
          key instanceof exp.Identifier
            ? String(key.args.this ?? "")
            : key instanceof exp.Literal && key.isString
              ? String(key.args.this ?? "")
              : this.sql(key)
        return `${this.sql(value)} AS ${keyStr}`
      }
      return this.sql(e)
    })
    return `STRUCT(${exprs.join(", ")})`
  }

  // BigQuery uses inline array syntax [1, 2, 3] not ARRAY[1, 2, 3]
  protected override array_sql(expression: exp.Array): string {
    const exprs = expression.expressions
    if (exprs.length === 1) {
      const inner = exprs[0]
      if (inner instanceof exp.Select || inner instanceof exp.Subquery) {
        return `ARRAY(${this.sql(inner)})`
      }
    }
    return `[${this.expressions(exprs)}]`
  }

  // BigQuery uses EXCEPT DISTINCT / INTERSECT DISTINCT by default
  protected override setOperation(
    expression: exp.SetOperation,
    op: string,
  ): string {
    const left = this.sql(expression.args.this as exp.Expression)
    const right = this.sql(expression.args.expression as exp.Expression)
    const distinct = expression.args.distinct !== false ? " DISTINCT" : " ALL"
    return `${left} ${op}${distinct} ${right}`
  }

  protected override bracket_sql(expression: exp.Bracket): string {
    const thisExpr = expression.args.this as exp.Expression | undefined
    const bracketExprs = expression.expressions

    // STRUCT field access: col['field'] → col.field
    if (bracketExprs.length === 1 && thisExpr && thisExpr.isType("STRUCT")) {
      const arg = bracketExprs[0]
      if (arg) {
        const argType = arg._type ?? arg.type
        const isText = argType
          ? exp.DataType.TEXT_TYPES.has(
              argType.text("this") as typeof exp.DataType.Type.CHAR,
            )
          : arg instanceof exp.Literal && arg.isString
        if (isText) {
          const name =
            arg instanceof exp.Literal
              ? String(arg.args.this ?? "")
              : arg.text("this")
          return `${this.sql(thisExpr)}.${name}`
        }
      }
    }

    const base = this.sql(thisExpr)
    const indices = this.bracketOffsetExpressions(expression)
    let expressionsSql = this.expressions(indices)

    const offset = expression.args.offset as number | undefined
    if (offset === 0) {
      expressionsSql = `OFFSET(${expressionsSql})`
    } else if (offset === 1) {
      expressionsSql = `ORDINAL(${expressionsSql})`
    }

    if (expression.args.safe) {
      expressionsSql = `SAFE_${expressionsSql}`
    }

    return `${base}[${expressionsSql}]`
  }

  // BigQuery UNNEST with UNNEST_COLUMN_ONLY: AS t(col) → AS col
  protected override unnest_sql(expression: exp.Unnest): string {
    const exprsArg = expression.args.expressions
    const args = Array.isArray(exprsArg)
      ? this.expressions(exprsArg)
      : exprsArg instanceof exp.Expression
        ? this.sql(exprsArg)
        : ""
    const alias = expression.args.alias as exp.Expression | undefined
    let aliasSql = ""
    if (alias instanceof exp.TableAlias) {
      const columns = alias.args.columns as exp.Expression[] | undefined
      if (columns && columns.length > 0) {
        aliasSql = ` AS ${this.sql(columns[0])}`
      } else {
        const aliasName = alias.text("this")
        if (aliasName) aliasSql = ` AS ${aliasName}`
      }
    }

    const offset = expression.args.offset
    if (offset instanceof exp.Expression) {
      aliasSql = `${aliasSql} WITH OFFSET AS ${this.sql(offset)}`
    } else if (offset) {
      aliasSql = `${aliasSql} WITH OFFSET`
    }

    return `UNNEST(${args})${aliasSql}`
  }

  protected override select_sql(expression: exp.Select): string {
    unqualifyUnnest(expression)
    return super.select_sql(expression)
  }

  // BigQuery-specific date/time functions
  // TIMESTAMP_TRUNC, DATE_TRUNC, etc.

  // BigQuery uses ILIKE for case-insensitive matching (recent addition)
  // But traditionally uses LOWER(x) LIKE LOWER(pattern)

  // BigQuery doesn't have traditional LIMIT OFFSET - uses LIMIT n OFFSET m
  protected override offset_sql(expression: exp.Offset): string {
    return `OFFSET ${this.sql(expression.args.this as exp.Expression)}`
  }
}

export class BigQueryDialect extends Dialect {
  static override readonly name = "bigquery"
  static override HEX_STRING_IS_INTEGER_TYPE = true
  static override HEX_LOWERCASE = true
  static override BYTE_STRING_IS_BYTES_TYPE = true
  static override BYTE_START: string | null = "b'"
  static override BYTE_END: string | null = "'"
  static override PRESERVE_ORIGINAL_NAMES = true
  static override STRING_ESCAPES: string[] = ["\\"]
  static override UNESCAPED_SEQUENCES: Record<string, string> =
    buildUnescapedSequences()
  static override ESCAPED_SEQUENCES: Record<string, string> =
    buildEscapedSequences(BigQueryDialect.UNESCAPED_SEQUENCES)
  static override STRINGS_SUPPORT_ESCAPED_SEQUENCES = true
  protected static override ParserClass: typeof BigQueryParser = BigQueryParser
  protected static override GeneratorClass: typeof BigQueryGenerator =
    BigQueryGenerator

  override createTokenizer(): Tokenizer {
    return new Tokenizer({
      ...this.options.tokenizer,
      keywords: new Map([
        ...(this.options.tokenizer?.keywords ?? []),
        ["DATETIME", TokenType.TIMESTAMP],
        ["TIMESTAMP", TokenType.TIMESTAMPTZ],
      ]),
      hexStrings: [
        ["0x", ""],
        ["0X", ""],
      ],
    })
  }
}

// Register dialect
Dialect.register(BigQueryDialect)
