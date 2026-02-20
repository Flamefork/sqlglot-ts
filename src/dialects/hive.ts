/**
 * Apache Hive SQL dialect
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
import { formatTime } from "../time.js"
import { TokenType } from "../tokens.js"
import {
  anyToExists,
  datestrtodate_sql,
  eliminateQualify,
  preprocess,
  regexpReplaceSql,
  renameFunc,
  sequenceSql,
  timestrtotime_sql,
  unitToStr,
  unnestToExplode,
} from "../transforms.js"

type Transform = (generator: Generator, expression: exp.Expression) => string
type FunctionBuilder = (args: exp.Expression[]) => exp.Expression

const NUMERIC_LITERALS: Map<string, string> = new Map([
  ["D", "DOUBLE"],
  ["F", "FLOAT"],
  ["L", "BIGINT"],
  ["S", "SMALLINT"],
  ["Y", "TINYINT"],
  ["BD", "DECIMAL"],
])

const HIVE_TIME_MAPPING: Map<string, string> = new Map([
  ["y", "%Y"],
  ["Y", "%Y"],
  ["YYYY", "%Y"],
  ["yyyy", "%Y"],
  ["YY", "%y"],
  ["yy", "%y"],
  ["MMMM", "%B"],
  ["MMM", "%b"],
  ["MM", "%m"],
  ["M", "%-m"],
  ["dd", "%d"],
  ["d", "%-d"],
  ["HH", "%H"],
  ["H", "%-H"],
  ["hh", "%I"],
  ["h", "%-I"],
  ["mm", "%M"],
  ["m", "%-M"],
  ["ss", "%S"],
  ["s", "%-S"],
  ["SSSSSS", "%f"],
  ["a", "%p"],
  ["DD", "%j"],
  ["D", "%-j"],
  ["E", "%a"],
  ["EE", "%a"],
  ["EEE", "%a"],
  ["EEEE", "%A"],
  ["z", "%Z"],
  ["Z", "%z"],
])

const HIVE_TIME_FORMAT = "'yyyy-MM-dd HH:mm:ss'"

function hiveFormatTime(
  expression: exp.Expression | undefined,
): exp.Expression | undefined {
  if (!expression) return undefined
  if (expression instanceof exp.Literal && expression.isString) {
    return exp.Literal.string(
      formatTime(String(expression.args.this), HIVE_TIME_MAPPING),
    )
  }
  return expression
}

function buildFormattedTime(
  ExprClass: new (args: exp.Args) => exp.Expression,
  useDefault: boolean,
): FunctionBuilder {
  return (args: exp.Expression[]) => {
    const fmt =
      args[1] ??
      (useDefault
        ? exp.Literal.string(HIVE_TIME_FORMAT.slice(1, -1))
        : undefined)
    return new ExprClass({
      this: args[0],
      format: hiveFormatTime(fmt),
    })
  }
}

export class HiveParser extends Parser {
  static override ADD_JOIN_ON_TRUE = true
  static override STRICT_CAST = false

  static override FUNCTION_PARSERS: Map<
    string,
    (parser: Parser) => exp.Expression
  > = new Map([
    ...Parser.FUNCTION_PARSERS,
    [
      "PERCENTILE",
      (p) => (p as HiveParser).parseQuantileFunction(exp.Quantile),
    ],
    [
      "PERCENTILE_APPROX",
      (p) => (p as HiveParser).parseQuantileFunction(exp.ApproxQuantile),
    ],
  ])

  protected parseQuantileFunction(
    ExprClass: typeof exp.Quantile | typeof exp.ApproxQuantile,
  ): exp.Expression {
    let firstArg: exp.Expression
    if (this.match(TokenType.DISTINCT)) {
      firstArg = new exp.Distinct({ expressions: [this.parseLambda()] })
    } else {
      this.match(TokenType.ALL)
      firstArg = this.parseLambda()
    }

    const args: exp.Args = { this: firstArg }
    if (this.match(TokenType.COMMA)) {
      args.quantile = this.parseLambda()
    }
    if (this.match(TokenType.COMMA)) {
      args.accuracy = this.parseLambda()
    }
    this.expect(TokenType.R_PAREN)
    return new ExprClass(args)
  }

  static override FUNCTIONS = new Map([
    ...Parser.FUNCTIONS,
    ["SIZE", (args: exp.Expression[]) => new exp.ArraySize({ this: args[0] })],
    [
      "DATE_ADD",
      (args: exp.Expression[]) =>
        new exp.TsOrDsAdd({
          this: args[0],
          expression: args[1],
          unit: exp.Literal.string("DAY"),
        }),
    ],
    [
      "DATE_SUB",
      (args: exp.Expression[]) => {
        const expression = args[1]
          ? new exp.Mul({
              this: args[1],
              expression: new exp.Neg({ this: exp.Literal.number(1) }),
            })
          : undefined
        return new exp.TsOrDsAdd({
          this: args[0],
          expression,
          unit: exp.Literal.string("DAY"),
        })
      },
    ],
    [
      "PERCENTILE",
      (args: exp.Expression[]) =>
        new exp.Quantile({ this: args[0], quantile: args[1] }),
    ],
    [
      "PERCENTILE_APPROX",
      (args: exp.Expression[]) =>
        new exp.ApproxQuantile({
          this: args[0],
          quantile: args[1],
          accuracy: args[2],
        }),
    ],
    [
      "APPROX_PERCENTILE",
      (args: exp.Expression[]) =>
        new exp.ApproxQuantile({
          this: args[0],
          quantile: args[1],
          accuracy: args[2],
        }),
    ],
    [
      "COLLECT_SET",
      (args: exp.Expression[]) => new exp.ArrayUniqueAgg({ this: args[0] }),
    ],
    [
      "COLLECT_LIST",
      (args: exp.Expression[]) => new exp.ArrayAgg({ this: args[0] }),
    ],
    [
      "DATE_FORMAT",
      (args: exp.Expression[]) => {
        const fmtArgs: exp.Expression[] = [
          new exp.TimeStrToTime({ this: args[0] }),
        ]
        if (args[1]) fmtArgs.push(args[1])
        return buildFormattedTime(exp.TimeToStr, false)(fmtArgs)
      },
    ],
    [
      "DATEDIFF",
      (args: exp.Expression[]) =>
        new exp.DateDiff({
          this: new exp.TsOrDsToDate({ this: args[0] }),
          expression: new exp.TsOrDsToDate({ this: args[1] }),
        }),
    ],
    [
      "DAY",
      (args: exp.Expression[]) =>
        new exp.Day({ this: new exp.TsOrDsToDate({ this: args[0] }) }),
    ],
    ["FROM_UNIXTIME", buildFormattedTime(exp.UnixToStr, true)],
    [
      "MONTH",
      (args: exp.Expression[]) =>
        new exp.Month({ this: new exp.TsOrDsToDate({ this: args[0] }) }),
    ],
    [
      "TO_DATE",
      (args: exp.Expression[]) => {
        const expr = buildFormattedTime(exp.TsOrDsToDate, false)(args)
        expr.args.safe = true
        return expr
      },
    ],
    [
      "UNIX_TIMESTAMP",
      (args: exp.Expression[]) =>
        buildFormattedTime(
          exp.StrToUnix,
          true,
        )(args.length > 0 ? args : [new exp.CurrentTimestamp({})]),
    ],
    [
      "YEAR",
      (args: exp.Expression[]) =>
        new exp.Year({ this: new exp.TsOrDsToDate({ this: args[0] }) }),
    ],
  ])

  protected override parsePrimary(): exp.Expression {
    const expr = super.parsePrimary()
    if (
      expr instanceof exp.Literal &&
      !expr.isString &&
      this.current.tokenType === TokenType.VAR
    ) {
      const type = NUMERIC_LITERALS.get(this.current.text.toUpperCase())
      if (type) {
        this.advance()
        return new exp.TryCast({
          this: expr,
          to: new exp.DataType({ this: type }),
        })
      }
    }
    return expr
  }
}

export class HiveGenerator extends Generator {
  protected override ARRAY_SIZE_NAME = "SIZE"
  protected override ALTER_SET_TYPE = ""

  static override STRINGS_SUPPORT_ESCAPED_SEQUENCES = true
  static override ESCAPED_SEQUENCES = buildEscapedSequences(
    buildUnescapedSequences(),
  )
  static override STRING_ESCAPES = ["\\"]

  static override FEATURES = {
    ...Generator.FEATURES,
    SAFE_DIVISION: true,
  }

  static override EXPRESSIONS_WITHOUT_NESTED_CTES: Set<ExpressionClass> =
    new Set([
      exp.Insert,
      exp.Select,
      exp.Subquery,
      exp.Union,
      exp.Intersect,
      exp.Except,
    ])

  static override TYPE_MAPPING: Map<string, string> = new Map([
    ...Generator.TYPE_MAPPING,
    ["BIT", "BOOLEAN"],
    ["BLOB", "BINARY"],
    ["DATETIME", "TIMESTAMP"],
    ["ROWVERSION", "BINARY"],
    ["TEXT", "STRING"],
    ["TIME", "TIMESTAMP"],
    ["TIMESTAMPNTZ", "TIMESTAMP"],
    ["TIMESTAMPTZ", "TIMESTAMP"],
    ["UTINYINT", "SMALLINT"],
    ["VARBINARY", "BINARY"],
  ])

  // Hive INVERSE_TIME_MAPPING (auto-reversed from Hive TIME_MAPPING: Java date format -> strftime)
  static override INVERSE_TIME_MAPPING: Map<string, string> = new Map([
    ["%Y", "yyyy"],
    ["%y", "yy"],
    ["%B", "MMMM"],
    ["%b", "MMM"],
    ["%m", "MM"],
    ["%-m", "M"],
    ["%d", "dd"],
    ["%-d", "d"],
    ["%H", "HH"],
    ["%-H", "H"],
    ["%I", "hh"],
    ["%-I", "h"],
    ["%M", "mm"],
    ["%-M", "m"],
    ["%S", "ss"],
    ["%-S", "s"],
    ["%f", "SSSSSS"],
    ["%p", "a"],
    ["%j", "DD"],
    ["%-j", "D"],
    ["%a", "EEE"],
    ["%A", "EEEE"],
    ["%Z", "z"],
    ["%z", "Z"],
  ])

  static override TRANSFORMS: Map<ExpressionClass, Transform> = new Map<
    ExpressionClass,
    Transform
  >([
    ...Generator.TRANSFORMS,
    [
      exp.Select,
      preprocess([
        eliminateQualify,
        (e) => unnestToExplode(e, false),
        anyToExists,
      ]),
    ],
    [exp.TimeToUnix, renameFunc("UNIX_TIMESTAMP")],
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
        return `FROM_UNIXTIME(${timestamp} / POW(10, ${scaleValue}))`
      },
    ],
    [
      exp.TimeToStr,
      (gen: Generator, e: exp.Expression) => {
        const fmt = gen.formatTimeStr(e)
        const thisExpr = gen.sql(
          (e as exp.TimeToStr).args.this as exp.Expression,
        )
        return `DATE_FORMAT(${thisExpr}, ${fmt})`
      },
    ],
    [
      exp.StrToTime,
      (gen: Generator, e: exp.Expression) => {
        const fmt = gen.formatTimeStr(e)
        const thisExpr = gen.sql(
          (e as exp.StrToTime).args.this as exp.Expression,
        )
        return `CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(${thisExpr}, ${fmt})) AS TIMESTAMP)`
      },
    ],
    [
      exp.TryCast,
      (gen: Generator, e: exp.Expression) => {
        const thisExpr = gen.sql((e as exp.TryCast).args.this as exp.Expression)
        const to = gen.sql((e as exp.TryCast).args.to as exp.Expression)
        return `CAST(${thisExpr} AS ${to})`
      },
    ],
    [exp.GenerateSeries, sequenceSql],
    [
      exp.If,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.If
        const args: exp.Expression[] = [expr.args.this as exp.Expression]
        if (expr.args.true) args.push(expr.args.true as exp.Expression)
        if (expr.args.false) args.push(expr.args.false as exp.Expression)
        return gen.funcCall("IF", args)
      },
    ],
    [
      exp.Quantile,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.Quantile
        return gen.funcCall("PERCENTILE", [
          expr.args.this as exp.Expression,
          expr.args.quantile as exp.Expression,
        ])
      },
    ],
    [
      exp.RegexpLike,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.RegexpLike
        return gen.binary_sql(expr, "RLIKE")
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
    [exp.ArrayConcat, renameFunc("CONCAT")],
    [
      exp.IntDiv,
      (gen: Generator, e: exp.Expression) =>
        gen.binary_sql(e as exp.Binary, "DIV"),
    ],
    [
      exp.ApproxQuantile,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.ApproxQuantile
        const args: exp.Expression[] = [
          expr.args.this as exp.Expression,
          expr.args.quantile as exp.Expression,
        ]
        if (expr.args.accuracy) args.push(expr.args.accuracy as exp.Expression)
        return gen.funcCall("PERCENTILE_APPROX", args)
      },
    ],
    [exp.ArrayUniqueAgg, renameFunc("COLLECT_SET")],
    [exp.Unnest, renameFunc("EXPLODE")],
    [exp.TimeStrToDate, renameFunc("TO_DATE")],
    [exp.WeekOfYear, renameFunc("WEEKOFYEAR")],
    [exp.DayOfMonth, renameFunc("DAYOFMONTH")],
    [exp.DayOfWeek, renameFunc("DAYOFWEEK")],
    [exp.Unicode, renameFunc("ASCII")],
    [
      exp.ArrayToString,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.ArrayToString
        return gen.funcCall("CONCAT_WS", [
          expr.args.expression as exp.Expression,
          expr.args.this as exp.Expression,
        ])
      },
    ],
    [exp.RegexpReplace, regexpReplaceSql],
    [
      exp.RegexpSplit,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.RegexpSplit
        const args: exp.Expression[] = [expr.args.this as exp.Expression]
        if (expr.args.expression)
          args.push(expr.args.expression as exp.Expression)
        return gen.funcCall("SPLIT", args)
      },
    ],
    [
      exp.Split,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.Split
        const thisStr = gen.sql(expr.args.this as exp.Expression)
        const delimStr = gen.sql(expr.args.expression as exp.Expression)
        return `SPLIT(${thisStr}, CONCAT('\\\\Q', ${delimStr}, '\\\\E'))`
      },
    ],
    [
      exp.TimestampTrunc,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.TimestampTrunc
        const unit = unitToStr(expr)
        return gen.funcCall("TRUNC", [
          expr.args.this as exp.Expression,
          exp.Literal.string(unit),
        ])
      },
    ],
    [exp.TimeStrToTime, timestrtotime_sql],
    [exp.DateStrToDate, datestrtodate_sql],
  ])

  protected override ALIAS_POST_TABLESAMPLE = true
  protected override TABLESAMPLE_WITH_METHOD = false

  private static PARAMETERIZABLE_TEXT_TYPES = new Set([
    "NVARCHAR",
    "VARCHAR",
    "CHAR",
    "NCHAR",
  ])

  protected override array_sql(expression: exp.Array): string {
    return `ARRAY(${this.expressions(expression.expressions)})`
  }

  protected override datatype_sql(expression: exp.DataType): string {
    const typeStr = expression.text("this")
    if (
      HiveGenerator.PARAMETERIZABLE_TEXT_TYPES.has(typeStr) &&
      expression.expressions.length === 0
    ) {
      return super.datatype_sql(new exp.DataType({ this: "TEXT" }))
    }
    return super.datatype_sql(expression)
  }

  protected arrayagg_sql(expression: exp.ArrayAgg): string {
    const thisArg = expression.args.this
    const inner =
      thisArg instanceof exp.Order
        ? (thisArg.args.this as exp.Expression)
        : (thisArg as exp.Expression)
    return this.funcCall("COLLECT_LIST", [inner])
  }

  // Hive uses backticks for identifier quoting
  protected override quoteIdentifier(name: string): string {
    return `\`${name.replace(/`/g, "``")}\``
  }
}

export class HiveDialect extends Dialect {
  static override readonly name = "hive"
  static override SAFE_DIVISION = true
  static override TIME_MAPPING = HIVE_TIME_MAPPING
  static override STRING_ESCAPES = ["\\"]
  static override UNESCAPED_SEQUENCES = buildUnescapedSequences()
  static override ESCAPED_SEQUENCES = buildEscapedSequences(
    HiveDialect.UNESCAPED_SEQUENCES,
  )
  static override STRINGS_SUPPORT_ESCAPED_SEQUENCES = true
  protected static override ParserClass = HiveParser
  protected static override GeneratorClass = HiveGenerator

  constructor(options: Record<string, unknown> = {}) {
    super({
      ...options,
      tokenizer: {
        ...(options.tokenizer as Record<string, unknown> | undefined),
        identifiers: ["`"],
        quotes: ["'", '"'],
      },
    })
  }
}

// Register dialect
Dialect.register(HiveDialect)
