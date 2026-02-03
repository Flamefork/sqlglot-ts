/**
 * Presto/Trino dialect
 */

import { Dialect } from "../dialect.js"
import type { ExpressionClass } from "../expression-base.js"
import * as exp from "../expressions.js"
import { Generator } from "../generator.js"
import { annotateTypes } from "../optimizer/annotate_types.js"
import { Parser } from "../parser.js"
import { TokenType, Tokenizer } from "../tokens.js"
import {
  addRecursiveCteColumnNames,
  datestrtodate_sql,
  eliminateQualify,
  eliminateSemiAndAntiJoins,
  eliminateWindowClause,
  explodeProjectionToUnnest,
  preprocess,
  removePrecisionParameterizedTypes,
  sequenceSql,
  timestrtotime_sql,
  tsOrDsAddCast,
  unitToStr,
} from "../transforms.js"

type Transform = (generator: Generator, expression: exp.Expression) => string

function renameFunc(name: string): Transform {
  return (gen: Generator, e: exp.Expression) => {
    const expr = e as exp.Func
    const args: exp.Expression[] = []
    const thisArg = expr.args.this
    if (thisArg instanceof exp.Expression) {
      args.push(thisArg)
    }
    args.push(...expr.expressions)
    return gen.funcCall(name, args)
  }
}

function boolXorSql(gen: Generator, e: exp.Expression): string {
  const expr = e as exp.Xor
  const a = gen.sql(expr.left)
  const b = gen.sql(expr.right)
  return `(${a} AND (NOT ${b})) OR ((NOT ${a}) AND ${b})`
}

export class PrestoParser extends Parser {
  static override FUNCTIONS = new Map([
    ...Parser.FUNCTIONS,
    [
      "TO_UNIXTIME",
      (args: exp.Expression[]) => new exp.TimeToUnix({ this: args[0] }),
    ],
    [
      "FROM_UNIXTIME",
      (args: exp.Expression[]) => new exp.UnixToTime({ this: args[0] }),
    ],
    [
      "TO_UTF8",
      (args: exp.Expression[]) =>
        new exp.Encode({ this: args[0], charset: exp.Literal.string("utf-8") }),
    ],
    [
      "FROM_UTF8",
      (args: exp.Expression[]) =>
        new exp.Decode({
          this: args[0],
          charset: exp.Literal.string("utf-8"),
          replace: args[1],
        }),
    ],
    [
      "DATE_ADD",
      (args: exp.Expression[]) =>
        new exp.DateAdd({ this: args[2], expression: args[1], unit: args[0] }),
    ],
    [
      "DATE_DIFF",
      (args: exp.Expression[]) =>
        new exp.DateDiff({ this: args[2], expression: args[1], unit: args[0] }),
    ],
    [
      "DATE_TRUNC",
      (args: exp.Expression[]) =>
        new exp.TimestampTrunc({ this: args[1], unit: args[0] }),
    ],
    ["NOW", () => new exp.CurrentTimestamp({})],
    [
      "DAY_OF_WEEK",
      (args: exp.Expression[]) => new exp.DayOfWeekIso({ this: args[0] }),
    ],
    [
      "DOW",
      (args: exp.Expression[]) => new exp.DayOfWeekIso({ this: args[0] }),
    ],
    [
      "BITWISE_AND",
      (args: exp.Expression[]) =>
        new exp.BitwiseAnd({ this: args[0], expression: args[1] }),
    ],
    [
      "BITWISE_OR",
      (args: exp.Expression[]) =>
        new exp.BitwiseOr({ this: args[0], expression: args[1] }),
    ],
    [
      "BITWISE_XOR",
      (args: exp.Expression[]) =>
        new exp.BitwiseXor({ this: args[0], expression: args[1] }),
    ],
    [
      "BITWISE_NOT",
      (args: exp.Expression[]) => new exp.BitwiseNot({ this: args[0] }),
    ],
    [
      "BITWISE_ARITHMETIC_SHIFT_LEFT",
      (args: exp.Expression[]) =>
        new exp.BitwiseLeftShift({ this: args[0], expression: args[1] }),
    ],
    [
      "BITWISE_ARITHMETIC_SHIFT_RIGHT",
      (args: exp.Expression[]) =>
        new exp.BitwiseRightShift({ this: args[0], expression: args[1] }),
    ],
    [
      "SHA256",
      (args: exp.Expression[]) =>
        new exp.SHA2({ this: args[0], length: exp.Literal.number(256) }),
    ],
    [
      "SHA512",
      (args: exp.Expression[]) =>
        new exp.SHA2({ this: args[0], length: exp.Literal.number(512) }),
    ],
    [
      "CARDINALITY",
      (args: exp.Expression[]) => new exp.ArraySize({ this: args[0] }),
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
  ])
}

export class PrestoGenerator extends Generator {
  static override NULL_ORDERING:
    | "nulls_are_small"
    | "nulls_are_large"
    | "nulls_are_last" = "nulls_are_last"
  static override HEX_START: string | null = "x'"
  static override HEX_END: string | null = "'"
  protected override INDEX_OFFSET = 1
  protected override STRUCT_DELIMITER: [string, string] = ["(", ")"]
  protected override ARRAY_SIZE_NAME = "CARDINALITY"
  protected override HEX_FUNC = "TO_HEX"
  protected override PAD_FILL_PATTERN_IS_REQUIRED = true

  static override FEATURES = {
    ...Generator.FEATURES,
    INTERVAL_ALLOWS_PLURAL_FORM: false,
    TYPED_DIVISION: true,
  }

  static override TYPE_MAPPING: Map<string, string> = new Map([
    ...Generator.TYPE_MAPPING,
    ["BINARY", "VARBINARY"],
    ["BIT", "BOOLEAN"],
    ["DATETIME", "TIMESTAMP"],
    ["DATETIME64", "TIMESTAMP"],
    ["FLOAT", "REAL"],
    ["HLLSKETCH", "HYPERLOGLOG"],
    ["INT", "INTEGER"],
    ["STRUCT", "ROW"],
    ["TEXT", "VARCHAR"],
    ["TIMESTAMPTZ", "TIMESTAMP"],
    ["TIMESTAMPNTZ", "TIMESTAMP"],
    ["TIMETZ", "TIME"],
  ])

  // MySQL/Presto INVERSE_TIME_MAPPING (auto-reversed from MySQL TIME_MAPPING)
  static override INVERSE_TIME_MAPPING: Map<string, string> = new Map([
    ["%B", "%M"],
    ["%-m", "%c"],
    ["%-d", "%e"],
    ["%I", "%h"],
    ["%M", "%i"],
    ["%S", "%s"],
    ["%W", "%u"],
    ["%-H", "%k"],
    ["%-I", "%l"],
    ["%H:%M:%S", "%T"],
    ["%A", "%W"],
  ])

  static override TRANSFORMS: Map<ExpressionClass, Transform> = new Map<
    ExpressionClass,
    Transform
  >([
    ...Generator.TRANSFORMS,
    [exp.Xor, boolXorSql],
    [exp.Cast, preprocess([removePrecisionParameterizedTypes])],
    [exp.TryCast, preprocess([removePrecisionParameterizedTypes])],
    [exp.With, preprocess([addRecursiveCteColumnNames])],
    [
      exp.Select,
      preprocess([
        eliminateWindowClause,
        eliminateQualify,
        explodeProjectionToUnnest(1),
        eliminateSemiAndAntiJoins,
      ]),
    ],
    [exp.ArrayConcat, renameFunc("CONCAT")],
    [exp.TimeToUnix, renameFunc("TO_UNIXTIME")],
    [
      exp.Encode,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.Encode
        const charset = expr.args.charset as exp.Expression | undefined
        if (
          charset &&
          !["utf-8", "utf8"].includes(charset.name.toLowerCase())
        ) {
          gen.unsupported(`Expected utf-8 character set, got ${charset.name}.`)
        }
        return gen.funcCall("TO_UTF8", [expr.args.this as exp.Expression])
      },
    ],
    [
      exp.Decode,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.Decode
        const charset = expr.args.charset as exp.Expression | undefined
        if (
          charset &&
          !["utf-8", "utf8"].includes(charset.name.toLowerCase())
        ) {
          gen.unsupported(`Expected utf-8 character set, got ${charset.name}.`)
        }
        const args: exp.Expression[] = [expr.args.this as exp.Expression]
        if (expr.args.replace) args.push(expr.args.replace as exp.Expression)
        return gen.funcCall("FROM_UTF8", args)
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
        return `FROM_UNIXTIME(CAST(${timestamp} AS DOUBLE) / POW(10, ${scaleValue}))`
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
        return `DATE_PARSE(${thisExpr}, ${fmt})`
      },
    ],
    [
      exp.DateAdd,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.DateAdd
        return gen.funcCall("DATE_ADD", [
          exp.Literal.string(unitToStr(expr)),
          expr.args.expression as exp.Expression,
          expr.args.this as exp.Expression,
        ])
      },
    ],
    [
      exp.DateSub,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.DateSub
        const amount = expr.args.expression as exp.Expression
        const negated = new exp.Neg({ this: amount })
        return gen.funcCall("DATE_ADD", [
          exp.Literal.string(unitToStr(expr)),
          negated,
          expr.args.this as exp.Expression,
        ])
      },
    ],
    [
      exp.DateDiff,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.DateDiff
        return gen.funcCall("DATE_DIFF", [
          exp.Literal.string(unitToStr(expr)),
          expr.args.expression as exp.Expression,
          expr.args.this as exp.Expression,
        ])
      },
    ],
    [
      exp.DateTrunc,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.DateTrunc
        const unit = unitToStr(expr)
        return gen.funcCall("DATE_TRUNC", [
          exp.Literal.string(unit),
          expr.args.this as exp.Expression,
        ])
      },
    ],
    [
      exp.TimestampTrunc,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.TimestampTrunc
        const unit = unitToStr(expr)
        return gen.funcCall("DATE_TRUNC", [
          exp.Literal.string(unit),
          expr.args.this as exp.Expression,
        ])
      },
    ],
    [
      exp.If,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.If
        return gen.funcCall("IF", [
          expr.args.this as exp.Expression,
          ...(expr.args.true ? [expr.args.true as exp.Expression] : []),
          ...(expr.args.false ? [expr.args.false as exp.Expression] : []),
        ])
      },
    ],
    [exp.CurrentTimestamp, () => "CURRENT_TIMESTAMP"],
    [exp.CurrentUser, () => "CURRENT_USER"],
    [exp.CurrentTime, () => "CURRENT_TIME"],
    [exp.DayOfWeekIso, renameFunc("DAY_OF_WEEK")],
    [
      exp.ArrayToString,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.ArrayToString
        const args: exp.Expression[] = [
          expr.args.this as exp.Expression,
          expr.args.expression as exp.Expression,
        ]
        return gen.funcCall("ARRAY_JOIN", args)
      },
    ],
    [exp.GenerateSeries, sequenceSql],
    [
      exp.Quantile,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.Quantile
        return gen.funcCall("APPROX_PERCENTILE", [
          expr.args.this as exp.Expression,
          expr.args.quantile as exp.Expression,
        ])
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
    [
      exp.BitwiseAnd,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.BitwiseAnd
        return gen.funcCall("BITWISE_AND", [
          expr.args.this as exp.Expression,
          expr.args.expression as exp.Expression,
        ])
      },
    ],
    [
      exp.BitwiseOr,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.BitwiseOr
        return gen.funcCall("BITWISE_OR", [
          expr.args.this as exp.Expression,
          expr.args.expression as exp.Expression,
        ])
      },
    ],
    [
      exp.BitwiseXor,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.BitwiseXor
        return gen.funcCall("BITWISE_XOR", [
          expr.args.this as exp.Expression,
          expr.args.expression as exp.Expression,
        ])
      },
    ],
    [
      exp.BitwiseNot,
      (gen: Generator, e: exp.Expression) => {
        return gen.funcCall("BITWISE_NOT", [e.args.this as exp.Expression])
      },
    ],
    [
      exp.BitwiseLeftShift,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.BitwiseLeftShift
        return gen.funcCall("BITWISE_ARITHMETIC_SHIFT_LEFT", [
          expr.args.this as exp.Expression,
          expr.args.expression as exp.Expression,
        ])
      },
    ],
    [
      exp.BitwiseRightShift,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.BitwiseRightShift
        return gen.funcCall("BITWISE_ARITHMETIC_SHIFT_RIGHT", [
          expr.args.this as exp.Expression,
          expr.args.expression as exp.Expression,
        ])
      },
    ],
    [
      exp.SortArray,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.SortArray
        const asc = expr.args.asc
        const isDesc = asc instanceof exp.Boolean && asc.args.this === false
        const thisStr = gen.sql(expr.args.this as exp.Expression)
        if (isDesc) {
          return `ARRAY_SORT(${thisStr}, (a, b) -> CASE WHEN a < b THEN 1 WHEN a > b THEN -1 ELSE 0 END)`
        }
        return `ARRAY_SORT(${thisStr})`
      },
    ],
    [exp.SHA, renameFunc("SHA1")],
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
    [exp.TimeStrToTime, timestrtotime_sql],
    [exp.TimeStrToDate, timestrtotime_sql],
    [exp.DateStrToDate, datestrtodate_sql],
    [exp.VariancePop, renameFunc("VAR_POP")],
    [
      exp.TimestampAdd,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.TimestampAdd
        return gen.funcCall("DATE_ADD", [
          exp.Literal.string(unitToStr(expr)),
          expr.args.expression as exp.Expression,
          expr.args.this as exp.Expression,
        ])
      },
    ],
    [
      exp.TsOrDsAdd,
      (gen: Generator, e: exp.Expression) => {
        const expression = tsOrDsAddCast(e as exp.TsOrDsAdd)
        return gen.funcCall("DATE_ADD", [
          exp.Literal.string(unitToStr(expression)),
          expression.args.expression as exp.Expression,
          expression.args.this as exp.Expression,
        ])
      },
    ],
    [
      exp.TsOrDsDiff,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.TsOrDsDiff
        const thisArg = exp.cast(
          expr.args.this as exp.Expression,
          exp.DataType.Type.TIMESTAMP,
        )
        const exprArg = exp.cast(
          expr.args.expression as exp.Expression,
          exp.DataType.Type.TIMESTAMP,
        )
        return gen.funcCall("DATE_DIFF", [
          exp.Literal.string(unitToStr(expr)),
          exprArg,
          thisArg,
        ])
      },
    ],
    [exp.Unhex, renameFunc("FROM_HEX")],
  ])

  protected override regexplike_sql(expression: exp.RegexpLike): string {
    return this.funcCall("REGEXP_LIKE", [
      expression.args.this as exp.Expression,
      expression.args.expression as exp.Expression,
    ])
  }

  // Presto uses double quotes for identifier quoting
  protected override quoteIdentifier(name: string): string {
    return `"${name.replace(/"/g, '""')}"`
  }

  // Presto uses CAST
  protected override cast_sql(expression: exp.Cast): string {
    const expr = this.sql(expression.args.this as exp.Expression)
    const to = this.sql(expression.args.to as exp.Expression)
    return `CAST(${expr} AS ${to})`
  }

  // Presto uses TRY_CAST
  protected override trycast_sql(expression: exp.TryCast): string {
    const expr = this.sql(expression.args.this as exp.Expression)
    const to = this.sql(expression.args.to as exp.Expression)
    return `TRY_CAST(${expr} AS ${to})`
  }

  // Presto uses || for string concatenation
  protected override dpipe_sql(expression: exp.DPipe): string {
    return this.binary_sql(expression, "||")
  }

  // Presto ARRAY[...] syntax
  protected override array_sql(expression: exp.Array): string {
    const exprs = expression.expressions
    return `ARRAY[${this.expressions(exprs)}]`
  }

  protected override struct_sql(expression: exp.Struct): string {
    if (!expression._type) {
      annotateTypes(expression)
    }

    const values: string[] = []
    const schema: string[] = []
    let unknownType = false

    for (const e of expression.expressions) {
      if (e instanceof exp.PropertyEQ) {
        if (e._type && e._type.text("this") === "UNKNOWN") {
          unknownType = true
        } else if (e._type) {
          schema.push(
            `${this.sql(e.args.this as exp.Expression)} ${this.sql(e._type)}`,
          )
        }
        values.push(this.sql(e.args.expression as exp.Expression))
      } else {
        values.push(this.sql(e))
      }
    }

    const size = expression.expressions.length
    if (!size || schema.length !== size) {
      if (unknownType) {
        this.unsupported(
          "Cannot convert untyped key-value definitions (try annotate_types).",
        )
      }
      return this.funcCall(
        "ROW",
        expression.expressions.map((e) =>
          e instanceof exp.PropertyEQ
            ? (e.args.expression as exp.Expression)
            : e,
        ),
      )
    }
    return `CAST(ROW(${values.join(", ")}) AS ROW(${schema.join(", ")}))`
  }

  protected override properties_sql(expression: exp.Properties): string {
    const propsSql = expression.expressions.map((p) => this.sql(p)).join(", ")
    return `WITH (${propsSql})`
  }

  protected override fileformatproperty_sql(
    expression: exp.FileFormatProperty,
  ): string {
    return `format=${this.sql(expression.args.this as exp.Expression)}`
  }

  protected override transaction_sql(expression: exp.Transaction): string {
    const modes = this.expressionsFromKey(expression, "modes")
    const modesSql = modes ? ` ${modes}` : ""
    return `START TRANSACTION${modesSql}`
  }
}

export class PrestoDialect extends Dialect {
  static override readonly name = "presto"
  static override NULL_ORDERING:
    | "nulls_are_small"
    | "nulls_are_large"
    | "nulls_are_last" = "nulls_are_last"
  static override INDEX_OFFSET = 1
  static override TYPED_DIVISION = true
  protected static override ParserClass = PrestoParser
  protected static override GeneratorClass = PrestoGenerator

  override createTokenizer(): Tokenizer {
    return new Tokenizer({
      ...this.options.tokenizer,
      keywords: new Map([
        ...(this.options.tokenizer?.keywords ?? []),
        ["ROW", TokenType.STRUCT],
        ["MATCH_RECOGNIZE", TokenType.MATCH_RECOGNIZE],
      ]),
    })
  }
}

// Register dialect
Dialect.register(PrestoDialect)
