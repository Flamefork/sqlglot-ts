/**
 * Snowflake dialect
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
import { TokenType, Tokenizer } from "../tokens.js"
import {
  dateDeltaSql,
  datestrtodate_sql,
  eliminateSemiAndAntiJoins,
  eliminateWindowClause,
  explodeProjectionToUnnest,
  preprocess,
  renameFunc,
  timestamptrunc_sql,
  timestrtotime_sql,
} from "../transforms.js"

type Transform = (generator: Generator, expression: exp.Expression) => string

export class SnowflakeTokenizer extends Tokenizer {
  constructor(options = {}) {
    super({
      ...options,
      keywords: new Map([
        ["SAMPLE", TokenType.TABLESAMPLE],
        ["MATCH_RECOGNIZE", TokenType.MATCH_RECOGNIZE],
        ...((options as { keywords?: Map<string, TokenType> }).keywords ?? []),
      ]),
    })
  }
}

export class SnowflakeParser extends Parser {
  static override FUNCTION_PARSERS: Map<
    string,
    (parser: Parser) => exp.Expression
  > = new Map([
    ...Parser.FUNCTION_PARSERS,
    ["LISTAGG", (p) => (p as SnowflakeParser).parseStringAgg()],
  ])

  protected override parseColonAsVariantExtract(
    thisExpr: exp.Expression,
  ): exp.Expression {
    const jsonPath: string[] = []

    while (this.match(TokenType.COLON)) {
      const path = this.parsePrimary()
      const name =
        path instanceof exp.Identifier
          ? String(path.args.this ?? "")
          : path instanceof exp.Column
            ? String((path.args.this as exp.Identifier)?.args.this ?? "")
            : String(path.args.this ?? "")
      jsonPath.push(name)
    }

    if (jsonPath.length > 0) {
      return new exp.JSONExtract({
        this: thisExpr,
        expression: exp.Literal.string(`$.${jsonPath.join(".")}`),
      })
    }
    return thisExpr
  }

  static override TYPE_NAME_MAPPING: Map<string, string> = new Map([
    ...Parser.TYPE_NAME_MAPPING,
    ["NUMBER", "DECIMAL"],
  ])

  static override TYPE_CONVERTERS: Map<
    string,
    (dt: exp.DataType) => exp.DataType
  > = new Map([
    ...Parser.TYPE_CONVERTERS,
    [
      "DECIMAL",
      (dt: exp.DataType) => {
        if (dt.expressions.length > 0) return dt
        return new exp.DataType({
          this: "DECIMAL",
          expressions: [exp.Literal.number(38), exp.Literal.number(0)],
        })
      },
    ],
  ])

  // Map Snowflake function names to expressions
  static override FUNCTIONS: Map<string, FunctionBuilder> = new Map([
    ...Parser.FUNCTIONS,
    // BOOLXOR → Xor
    [
      "BOOLXOR",
      (args: exp.Expression[]) =>
        new exp.Xor({ this: args[0], expression: args[1] }),
    ],
    [
      "REPLACE",
      (args: exp.Expression[]) =>
        new exp.Replace({
          this: args[0],
          expression: args[1],
          replacement: args[2] ?? exp.Literal.string(""),
        }),
    ],
    [
      "REGEXP_REPLACE",
      (args: exp.Expression[]) => {
        const r = new exp.RegexpReplace({
          this: args[0],
          expression: args[1],
          replacement: args[2],
          position: args[3],
          occurrence: args[4],
          modifiers: args[5],
        })
        if (!r.args.replacement) r.set("replacement", exp.Literal.string(""))
        return r
      },
    ],
    // APPROX_PERCENTILE → ApproxQuantile
    [
      "APPROX_PERCENTILE",
      (args: exp.Expression[]) => {
        const [thisArg, quantile] = args
        return new exp.ApproxQuantile({ this: thisArg, quantile })
      },
    ],
    [
      "TIMEADD",
      (args: exp.Expression[]) =>
        new exp.TimeAdd({ this: args[2], expression: args[1], unit: args[0] }),
    ],
    [
      "DATEADD",
      (args: exp.Expression[]) =>
        new exp.DateAdd({ this: args[2], expression: args[1], unit: args[0] }),
    ],
    [
      "DATEDIFF",
      (args: exp.Expression[]) =>
        new exp.DateDiff({ this: args[2], expression: args[1], unit: args[0] }),
    ],
    [
      "DATE_DIFF",
      (args: exp.Expression[]) =>
        new exp.DateDiff({ this: args[2], expression: args[1], unit: args[0] }),
    ],
    [
      "TIMEDIFF",
      (args: exp.Expression[]) =>
        new exp.DateDiff({ this: args[2], expression: args[1], unit: args[0] }),
    ],
    [
      "TIMESTAMPDIFF",
      (args: exp.Expression[]) =>
        new exp.DateDiff({ this: args[2], expression: args[1], unit: args[0] }),
    ],
    // MD5/SHA hash functions
    ["MD5_HEX", (args: exp.Expression[]) => new exp.MD5({ this: args[0] })],
    [
      "MD5_BINARY",
      (args: exp.Expression[]) => new exp.MD5Digest({ this: args[0] }),
    ],
    [
      "MD5_NUMBER_LOWER64",
      (args: exp.Expression[]) => new exp.MD5NumberLower64({ this: args[0] }),
    ],
    [
      "MD5_NUMBER_UPPER64",
      (args: exp.Expression[]) => new exp.MD5NumberUpper64({ this: args[0] }),
    ],
    [
      "SHA1_BINARY",
      (args: exp.Expression[]) => new exp.SHA1Digest({ this: args[0] }),
    ],
    ["SHA1_HEX", (args: exp.Expression[]) => new exp.SHA({ this: args[0] })],
    [
      "SHA2_BINARY",
      (args: exp.Expression[]) =>
        new exp.SHA2Digest({ this: args[0], length: args[1] }),
    ],
    [
      "SHA2_HEX",
      (args: exp.Expression[]) =>
        new exp.SHA2({ this: args[0], length: args[1] }),
    ],
    // Other function mappings
    [
      "HEX_DECODE_BINARY",
      (args: exp.Expression[]) => new exp.Unhex({ this: args[0] }),
    ],
    [
      "IFF",
      (args: exp.Expression[]) =>
        new exp.If({ this: args[0], true: args[1], false: args[2] }),
    ],
    ["FLATTEN", (args: exp.Expression[]) => new exp.Explode({ this: args[0] })],
    [
      "ARRAY_CONSTRUCT",
      (args: exp.Expression[]) => new exp.Array({ expressions: args }),
    ],
    [
      "ARRAY_FLATTEN",
      (args: exp.Expression[]) => new exp.Flatten({ this: args[0] }),
    ],
    [
      "OBJECT_KEYS",
      (args: exp.Expression[]) => new exp.JSONKeys({ this: args[0] }),
    ],
    [
      "OCTET_LENGTH",
      (args: exp.Expression[]) => new exp.ByteLength({ this: args[0] }),
    ],
    [
      "ARRAY_SORT",
      (args: exp.Expression[]) =>
        new exp.SortArray({ this: args[0], asc: args[1] }),
    ],
    [
      "STRTOK_TO_ARRAY",
      (args: exp.Expression[]) =>
        new exp.StringToArray({ this: args[0], expression: args[1] }),
    ],
    [
      "SQUARE",
      (args: exp.Expression[]) =>
        new exp.Pow({ this: args[0], expression: exp.Literal.number(2) }),
    ],
    [
      "STDDEV_SAMP",
      (args: exp.Expression[]) => new exp.Stddev({ this: args[0] }),
    ],
    [
      "BOOLAND_AGG",
      (args: exp.Expression[]) => new exp.LogicalAnd({ this: args[0] }),
    ],
    [
      "BOOLOR_AGG",
      (args: exp.Expression[]) => new exp.LogicalOr({ this: args[0] }),
    ],
    // Snowflake BITORAGG → BitwiseOrAgg etc (all naming variants)
    [
      "BITORAGG",
      (args: exp.Expression[]) => new exp.BitwiseOrAgg({ this: args[0] }),
    ],
    [
      "BITOR_AGG",
      (args: exp.Expression[]) => new exp.BitwiseOrAgg({ this: args[0] }),
    ],
    [
      "BIT_OR_AGG",
      (args: exp.Expression[]) => new exp.BitwiseOrAgg({ this: args[0] }),
    ],
    [
      "BIT_ORAGG",
      (args: exp.Expression[]) => new exp.BitwiseOrAgg({ this: args[0] }),
    ],
    [
      "BITANDAGG",
      (args: exp.Expression[]) => new exp.BitwiseAndAgg({ this: args[0] }),
    ],
    [
      "BITAND_AGG",
      (args: exp.Expression[]) => new exp.BitwiseAndAgg({ this: args[0] }),
    ],
    [
      "BIT_AND_AGG",
      (args: exp.Expression[]) => new exp.BitwiseAndAgg({ this: args[0] }),
    ],
    [
      "BIT_ANDAGG",
      (args: exp.Expression[]) => new exp.BitwiseAndAgg({ this: args[0] }),
    ],
    [
      "BITXORAGG",
      (args: exp.Expression[]) => new exp.BitwiseXorAgg({ this: args[0] }),
    ],
    [
      "BITXOR_AGG",
      (args: exp.Expression[]) => new exp.BitwiseXorAgg({ this: args[0] }),
    ],
    [
      "BIT_XOR_AGG",
      (args: exp.Expression[]) => new exp.BitwiseXorAgg({ this: args[0] }),
    ],
    [
      "BIT_XORAGG",
      (args: exp.Expression[]) => new exp.BitwiseXorAgg({ this: args[0] }),
    ],
    [
      "CONVERT_TIMEZONE",
      (args: exp.Expression[]) => {
        if (args.length === 2) {
          return new exp.ConvertTimezone({
            target_tz: args[0],
            timestamp: args[1],
          })
        }
        return new exp.ConvertTimezone({
          source_tz: args[0],
          target_tz: args[1],
          timestamp: args[2],
        })
      },
    ],
    [
      "ARRAY_REMOVE",
      (args: exp.Expression[]) =>
        new exp.ArrayRemove({
          this: args[0],
          expression: args[1],
          null_propagation: true,
        }),
    ],
    [
      "TO_TIME",
      (args: exp.Expression[]) => {
        if (
          args.length === 1 &&
          args[0] instanceof exp.Literal &&
          args[0].isString
        ) {
          return new exp.Cast({
            this: args[0],
            to: new exp.DataType({ this: "TIME" }),
          })
        }
        return new exp.Anonymous({ this: "TO_TIME", expressions: args })
      },
    ],
    [
      "TIME_ADD",
      (args: exp.Expression[]) =>
        new exp.TimeAdd({ this: args[2], expression: args[1], unit: args[0] }),
    ],
    [
      "GET_PATH",
      (args: exp.Expression[]) =>
        new exp.JSONExtract({ this: args[0], expression: args[1] }),
    ],
    [
      "GET",
      (args: exp.Expression[]) =>
        new exp.GetExtract({ this: args[0], expression: args[1] }),
    ],
    [
      "CORR",
      (args: exp.Expression[]) =>
        new exp.Corr({
          this: args[0],
          expression: args[1],
          null_on_zero_variance: true,
        }),
    ],
    [
      "TRY_PARSE_JSON",
      (args: exp.Expression[]) =>
        new exp.ParseJSON({ this: args[0], safe: true }),
    ],
    [
      "PARSE_JSON",
      (args: exp.Expression[]) => new exp.ParseJSON({ this: args[0] }),
    ],
    [
      "REGEXP_SUBSTR",
      (args: exp.Expression[]) =>
        new exp.RegexpExtract({
          this: args[0],
          expression: args[1],
          position: args[2],
          occurrence: args[3],
          parameters: args[4],
          group: args[5] ?? exp.Literal.number(0),
        }),
    ],
    [
      "REGEXP_EXTRACT_ALL",
      (args: exp.Expression[]) =>
        new exp.RegexpExtractAll({
          this: args[0],
          expression: args[1],
          position: args[2],
          occurrence: args[3],
          parameters: args[4],
          group: args[5] ?? exp.Literal.number(0),
        }),
    ],
    [
      "REGEXP_SUBSTR_ALL",
      (args: exp.Expression[]) =>
        new exp.RegexpExtractAll({
          this: args[0],
          expression: args[1],
          position: args[2],
          occurrence: args[3],
          parameters: args[4],
          group: args[5] ?? exp.Literal.number(0),
        }),
    ],
  ])
}

export class SnowflakeGenerator extends Generator {
  static override NULL_ORDERING:
    | "nulls_are_small"
    | "nulls_are_large"
    | "nulls_are_last" = "nulls_are_large"
  static override HEX_START: string | null = "x'"
  static override HEX_END: string | null = "'"
  static override STRINGS_SUPPORT_ESCAPED_SEQUENCES = true
  static override ESCAPED_SEQUENCES: Record<string, string> =
    buildEscapedSequences(buildUnescapedSequences())
  static override STRING_ESCAPES: string[] = ["\\", "'"]
  protected override STRUCT_DELIMITER: [string, string] = ["(", ")"]
  protected override INSERT_OVERWRITE = " OVERWRITE INTO"
  protected override ARRAY_SIZE_NAME = "ARRAY_SIZE"

  static override TYPE_MAPPING: Map<string, string> = new Map([
    ...Generator.TYPE_MAPPING,
    ["BIGDECIMAL", "DOUBLE"],
    ["NESTED", "OBJECT"],
    ["STRUCT", "OBJECT"],
    ["TEXT", "VARCHAR"],
  ])

  static override FEATURES: {
    SINGLE_STRING_INTERVAL: boolean
    AGGREGATE_FILTER_SUPPORTED: boolean
    NULL_ORDERING_SUPPORTED: boolean | null
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
    INTERVAL_ALLOWS_PLURAL_FORM: boolean
    RENAME_TABLE_WITH_DB: boolean
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
    UNNEST_WITH_ORDINALITY: boolean
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
    SINGLE_STRING_INTERVAL: true,
    AGGREGATE_FILTER_SUPPORTED: false,
  }

  static override TRANSFORMS: Map<ExpressionClass, Transform> = new Map<
    ExpressionClass,
    Transform
  >([
    ...Generator.TRANSFORMS,
    [exp.ArrayConcat, renameFunc("ARRAY_CAT")],
    [exp.Xor, renameFunc("BOOLXOR")],
    [
      exp.Select,
      preprocess([
        eliminateWindowClause,
        explodeProjectionToUnnest(),
        eliminateSemiAndAntiJoins,
      ]),
    ],
    // APPROX_QUANTILE → APPROX_PERCENTILE
    [
      exp.ApproxQuantile,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.ApproxQuantile
        const thisExpr = expr.args.this as exp.Expression
        const quantile = expr.args.quantile as exp.Expression
        return gen.funcCall("APPROX_PERCENTILE", [thisExpr, quantile])
      },
    ],
    [exp.TimeAdd, dateDeltaSql("TIMEADD")],
    [exp.DateAdd, dateDeltaSql("DATEADD")],
    [exp.DateDiff, dateDeltaSql("DATEDIFF")],
    [exp.DatetimeAdd, dateDeltaSql("TIMESTAMPADD")],
    [exp.TimestampAdd, dateDeltaSql("TIMESTAMPADD")],
    [exp.TsOrDsAdd, dateDeltaSql("DATEADD", true)],
    [exp.TsOrDsDiff, dateDeltaSql("DATEDIFF")],
    [exp.BitwiseAnd, renameFunc("BITAND")],
    [exp.BitwiseOr, renameFunc("BITOR")],
    [exp.BitwiseXor, renameFunc("BITXOR")],
    [exp.BitwiseNot, renameFunc("BITNOT")],
    [exp.BitwiseLeftShift, renameFunc("BITSHIFTLEFT")],
    [exp.BitwiseRightShift, renameFunc("BITSHIFTRIGHT")],
    [exp.BitwiseOrAgg, renameFunc("BITORAGG")],
    [exp.BitwiseAndAgg, renameFunc("BITANDAGG")],
    [exp.BitwiseXorAgg, renameFunc("BITXORAGG")],
    // AT TIME ZONE → CONVERT_TIMEZONE
    [
      exp.AtTimeZone,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.AtTimeZone
        return gen.funcCall("CONVERT_TIMEZONE", [
          expr.args.zone as exp.Expression,
          expr.args.this as exp.Expression,
        ])
      },
    ],
    [
      exp.If,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.If
        const falseVal = expr.args.false
          ? gen.sql(expr.args.false as exp.Expression)
          : "NULL"
        return `IFF(${gen.sql(expr.args.this as exp.Expression)}, ${gen.sql(expr.args.true as exp.Expression)}, ${falseVal})`
      },
    ],
    // MD5/SHA TRANSFORMS
    [exp.SHA, renameFunc("SHA1")],
    [exp.SHA1Digest, renameFunc("SHA1_BINARY")],
    [exp.MD5Digest, renameFunc("MD5_BINARY")],
    [exp.MD5NumberLower64, renameFunc("MD5_NUMBER_LOWER64")],
    [exp.MD5NumberUpper64, renameFunc("MD5_NUMBER_UPPER64")],
    [
      exp.SHA2Digest,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.SHA2Digest
        const length = expr.args.length ?? exp.Literal.number(256)
        return gen.funcCall("SHA2_BINARY", [
          expr.args.this as exp.Expression,
          length as exp.Expression,
        ])
      },
    ],
    // Other TRANSFORMS
    [exp.ApproxDistinct, renameFunc("APPROX_COUNT_DISTINCT")],
    [exp.ArgMax, renameFunc("MAX_BY")],
    [exp.ArgMin, renameFunc("MIN_BY")],
    [exp.ArrayIntersect, renameFunc("ARRAY_INTERSECTION")],
    [exp.DayOfMonth, renameFunc("DAYOFMONTH")],
    [exp.DayOfWeek, renameFunc("DAYOFWEEK")],
    [exp.DayOfYear, renameFunc("DAYOFYEAR")],
    [exp.Explode, renameFunc("FLATTEN")],
    [
      exp.GenerateSeries,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.GenerateSeries
        const start = expr.args.start as exp.Expression
        const end = expr.args.end as exp.Expression
        const step = expr.args.step as exp.Expression | undefined
        const wrappedEnd =
          end instanceof exp.Binary ? new exp.Paren({ this: end }) : end
        const endPlusOne = new exp.Add({
          this: wrappedEnd,
          expression: exp.Literal.number(1),
        })
        return gen.funcCall(
          "ARRAY_GENERATE_RANGE",
          step ? [start, endPlusOne, step] : [start, endPlusOne],
        )
      },
    ],
    [exp.JSONKeys, renameFunc("OBJECT_KEYS")],
    [exp.LogicalAnd, renameFunc("BOOLAND_AGG")],
    [exp.LogicalOr, renameFunc("BOOLOR_AGG")],
    [exp.Rand, renameFunc("RANDOM")],
    [exp.SortArray, renameFunc("ARRAY_SORT")],
    [exp.Skewness, renameFunc("SKEW")],
    [exp.StartsWith, renameFunc("STARTSWITH")],
    [exp.EndsWith, renameFunc("ENDSWITH")],
    [exp.StringToArray, renameFunc("STRTOK_TO_ARRAY")],
    [exp.Unhex, renameFunc("HEX_DECODE_BINARY")],
    [exp.Uuid, renameFunc("UUID_STRING")],
    [exp.WeekOfYear, renameFunc("WEEKISO")],
    [exp.ByteLength, renameFunc("OCTET_LENGTH")],
    [exp.Flatten, renameFunc("ARRAY_FLATTEN")],
    [exp.ToArray, renameFunc("TO_ARRAY")],
    [exp.GetExtract, renameFunc("GET")],
    [
      exp.RegexpExtract,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.RegexpExtract
        let group = expr.args.group as exp.Expression | undefined
        if (
          group &&
          group instanceof exp.Literal &&
          String(group.args.this) === "0"
        ) {
          group = undefined
        }
        const parameters =
          (expr.args.parameters as exp.Expression | undefined) ||
          (group ? exp.Literal.string("c") : undefined)
        const occurrence =
          (expr.args.occurrence as exp.Expression | undefined) ||
          (parameters ? exp.Literal.number(1) : undefined)
        const position =
          (expr.args.position as exp.Expression | undefined) ||
          (occurrence ? exp.Literal.number(1) : undefined)
        return gen.funcCall("REGEXP_SUBSTR", [
          expr.args.this as exp.Expression,
          expr.args.expression as exp.Expression,
          ...(position ? [position] : []),
          ...(occurrence ? [occurrence] : []),
          ...(parameters ? [parameters] : []),
          ...(group ? [group] : []),
        ])
      },
    ],
    [
      exp.GroupConcat,
      (gen: Generator, e: exp.Expression) => {
        const expression = e as exp.GroupConcat
        let thisExpr = expression.args.this as exp.Expression
        const separator = expression.args.separator as
          | exp.Expression
          | undefined
        const separatorSql = separator ? gen.sql(separator) : ""

        let order: exp.Order | undefined
        if (
          thisExpr instanceof exp.Order &&
          thisExpr.args.this instanceof exp.Expression
        ) {
          order = thisExpr
          thisExpr = order.args.this as exp.Expression
        }

        let argsSql = gen.sql(thisExpr)
        if (separatorSql) {
          argsSql += `, ${separatorSql}`
        }

        let result = `LISTAGG(${argsSql})`
        if (order) {
          const orderSql = gen.sql(
            new exp.Order({ expressions: order.expressions }),
          )
          result += ` WITHIN GROUP (${orderSql})`
        }
        return result
      },
    ],
    [exp.TimestampTrunc, timestamptrunc_sql()],
    [exp.TimeStrToTime, timestrtotime_sql],
    [exp.DateStrToDate, datestrtodate_sql],
  ])

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

  protected override struct_sql(expression: exp.Struct): string {
    const interleaved: exp.Expression[] = []
    for (let i = 0; i < expression.expressions.length; i++) {
      const e = expression.expressions[i]!
      if (e instanceof exp.PropertyEQ) {
        const key = e.args.this as exp.Expression
        interleaved.push(
          key instanceof exp.Identifier
            ? exp.Literal.string(String(key.args.this ?? ""))
            : key,
        )
        interleaved.push(e.args.expression as exp.Expression)
      } else {
        interleaved.push(exp.Literal.string(`_${i}`))
        interleaved.push(e)
      }
    }
    return this.funcCall("OBJECT_CONSTRUCT", interleaved)
  }

  // Snowflake uses double quotes for identifier quoting (like PostgreSQL)
  protected override quoteIdentifier(name: string): string {
    return `"${name.replace(/"/g, '""')}"`
  }

  // Snowflake uses :: for type casting (like PostgreSQL)
  protected override cast_sql(expression: exp.Cast): string {
    const expr = this.sql(expression.args.this as exp.Expression)
    const to = this.sql(expression.args.to as exp.Expression)
    return `CAST(${expr} AS ${to})`
  }

  // Snowflake uses TRY_CAST
  protected override trycast_sql(expression: exp.TryCast): string {
    const expr = this.sql(expression.args.this as exp.Expression)
    const to = this.sql(expression.args.to as exp.Expression)
    return `TRY_CAST(${expr} AS ${to})`
  }

  // Snowflake uses || for string concatenation
  protected override anonymous_sql(expression: exp.Anonymous): string {
    const name = expression.name.toUpperCase()

    // CONCAT -> ||
    if (name === "CONCAT") {
      const args = expression.expressions
      if (args.length >= 2) {
        return args.map((a) => this.sql(a)).join(" || ")
      }
    }

    // NVL is supported
    if (name === "NVL") {
      return `NVL(${this.expressions(expression.expressions)})`
    }

    // IFF instead of IF
    if (name === "IF" && expression.expressions.length === 3) {
      return `IFF(${this.expressions(expression.expressions)})`
    }

    return super.anonymous_sql(expression)
  }

  // Snowflake supports ILIKE
  protected override ilike_sql(expression: exp.ILike): string {
    let sql = this.binary_sql(expression, "ILIKE")
    const escapeExpr = expression.args.escape
    if (escapeExpr) {
      sql += ` ESCAPE ${this.sql(escapeExpr as exp.Expression)}`
    }
    return sql
  }

  protected override unnest_sql(expression: exp.Unnest): string {
    const unnestAlias = expression.args.alias as exp.TableAlias | undefined
    const offset = expression.args.offset

    const unnestAliasColumns = unnestAlias?.args.columns as
      | exp.Expression[]
      | undefined
    const value =
      (unnestAliasColumns && unnestAliasColumns[0]) || exp.toIdentifier("value")

    const columns = [
      exp.toIdentifier("seq"),
      exp.toIdentifier("key"),
      exp.toIdentifier("path"),
      offset instanceof exp.Expression
        ? offset.pop()
        : exp.toIdentifier("index"),
      value,
      exp.toIdentifier("this"),
    ]

    if (unnestAlias) {
      unnestAlias.set("columns", columns)
    } else {
      expression.set("alias", new exp.TableAlias({ this: "_u", columns }))
    }

    const exprsArg = expression.args.expressions
    let tableInput = Array.isArray(exprsArg)
      ? this.expressions(exprsArg)
      : exprsArg instanceof exp.Expression
        ? this.sql(exprsArg)
        : ""

    if (!tableInput.startsWith("INPUT =>")) {
      tableInput = `INPUT => ${tableInput}`
    }

    const parent = expression.parent
    const explode =
      parent instanceof exp.Lateral
        ? `FLATTEN(${tableInput})`
        : `TABLE(FLATTEN(${tableInput}))`

    const alias = expression.args.alias
      ? ` AS ${this.sql(expression.args.alias as exp.Expression)}`
      : ""

    const valueSql =
      parent instanceof exp.From ||
      parent instanceof exp.Join ||
      parent instanceof exp.Lateral
        ? ""
        : `${this.sql(value)} FROM `

    return `${valueSql}${explode}${alias}`
  }

  protected parsejson_sql(expression: exp.ParseJSON): string {
    const name = expression.args.safe ? "TRY_PARSE_JSON" : "PARSE_JSON"
    return this.funcCall(name, [expression.args.this as exp.Expression])
  }

  protected override jsonextract_sql(expression: exp.JSONExtract): string {
    let thisExpr: exp.Expression = expression.args.this as exp.Expression
    if (
      !(thisExpr instanceof exp.ParseJSON) &&
      !(thisExpr instanceof exp.JSONExtract)
    ) {
      thisExpr = new exp.ParseJSON({ this: thisExpr })
    }
    return this.funcCall("GET_PATH", [
      thisExpr,
      expression.args.expression as exp.Expression,
    ])
  }

  protected override jsonextractscalar_sql(
    expression: exp.JSONExtractScalar,
  ): string {
    let thisExpr: exp.Expression = expression.args.this as exp.Expression
    if (
      !(thisExpr instanceof exp.ParseJSON) &&
      !(thisExpr instanceof exp.JSONExtractScalar)
    ) {
      thisExpr = new exp.ParseJSON({ this: thisExpr })
    }
    return this.funcCall("GET_PATH", [
      thisExpr,
      expression.args.expression as exp.Expression,
    ])
  }
}

export class SnowflakeDialect extends Dialect {
  static override readonly name = "snowflake"
  static override NULL_ORDERING:
    | "nulls_are_small"
    | "nulls_are_large"
    | "nulls_are_last" = "nulls_are_large"
  static override STRING_ESCAPES: string[] = ["\\", "'"]
  static override UNESCAPED_SEQUENCES: Record<string, string> =
    buildUnescapedSequences()
  static override ESCAPED_SEQUENCES: Record<string, string> =
    buildEscapedSequences(SnowflakeDialect.UNESCAPED_SEQUENCES)
  static override STRINGS_SUPPORT_ESCAPED_SEQUENCES = true
  protected static override TokenizerClass: typeof SnowflakeTokenizer =
    SnowflakeTokenizer
  protected static override ParserClass: typeof SnowflakeParser =
    SnowflakeParser
  protected static override GeneratorClass: typeof SnowflakeGenerator =
    SnowflakeGenerator
}

// Register dialect
Dialect.register(SnowflakeDialect)
