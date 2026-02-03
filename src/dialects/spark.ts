/**
 * Apache Spark SQL dialect
 */

import { Dialect } from "../dialect.js"
import type { ExpressionClass } from "../expression-base.js"
import * as exp from "../expressions.js"
import type { Generator } from "../generator.js"
import {
  anyToExists,
  eliminateDistinctOn,
  eliminateQualify,
  preprocess,
  sequenceSql,
  timestamptrunc_sql,
  unitToVar,
  unnestToExplode,
} from "../transforms.js"
import { HiveGenerator, HiveParser } from "./hive.js"

type Transform = (generator: Generator, expression: exp.Expression) => string

function renameFunc(name: string): Transform {
  return (gen: Generator, e: exp.Expression) => {
    const expr = e as exp.Func
    const args: exp.Expression[] = []
    const thisArg = expr.args.this
    if (thisArg instanceof exp.Expression) args.push(thisArg)
    args.push(...expr.expressions)
    return gen.funcCall(name, args)
  }
}

export class SparkParser extends HiveParser {
  static override FUNCTIONS = new Map([
    ...HiveParser.FUNCTIONS,
    [
      "ARRAY_INSERT",
      (args: exp.Expression[]) =>
        new exp.ArrayInsert({
          this: args[0],
          position: args[1],
          expression: args[2],
          offset: 1,
        }),
    ],
    [
      "SHIFTLEFT",
      (args: exp.Expression[]) =>
        new exp.BitwiseLeftShift({ this: args[0], expression: args[1] }),
    ],
    [
      "SHIFTRIGHT",
      (args: exp.Expression[]) =>
        new exp.BitwiseRightShift({ this: args[0], expression: args[1] }),
    ],
    [
      "TRY_ADD",
      (args: exp.Expression[]) =>
        new exp.SafeAdd({ this: args[0], expression: args[1] }),
    ],
    [
      "TRY_MULTIPLY",
      (args: exp.Expression[]) =>
        new exp.SafeMultiply({ this: args[0], expression: args[1] }),
    ],
    [
      "TRY_SUBTRACT",
      (args: exp.Expression[]) =>
        new exp.SafeSubtract({ this: args[0], expression: args[1] }),
    ],
  ])
}

export class Spark2Generator extends HiveGenerator {
  static override HEX_START: string | null = "X'"
  static override HEX_END: string | null = "'"

  static override TRANSFORMS: Map<ExpressionClass, Transform> = new Map<
    ExpressionClass,
    Transform
  >([
    ...HiveGenerator.TRANSFORMS,
    [
      exp.Select,
      preprocess([
        eliminateQualify,
        eliminateDistinctOn,
        unnestToExplode,
        anyToExists,
      ]),
    ],
    [
      exp.UnixToTime,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.UnixToTime
        const scale = expr.args.scale as exp.Literal | undefined
        const scaleValue =
          scale instanceof exp.Literal ? String(scale.value) : undefined
        const timestamp = gen.sql(expr.args.this as exp.Expression)
        if (scaleValue === undefined) {
          return `CAST(FROM_UNIXTIME(${timestamp}) AS TIMESTAMP)`
        }
        if (scaleValue === "0") {
          return `TIMESTAMP_SECONDS(${timestamp})`
        }
        if (scaleValue === "3") {
          return `TIMESTAMP_MILLIS(${timestamp})`
        }
        if (scaleValue === "6") {
          return `TIMESTAMP_MICROS(${timestamp})`
        }
        return `TIMESTAMP_SECONDS(${timestamp} / POW(10, ${scaleValue}))`
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
      exp.Encode,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.Encode
        return gen.funcCall("ENCODE", [
          expr.args.this as exp.Expression,
          exp.Literal.string("utf-8"),
        ])
      },
    ],
    [
      exp.Decode,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.Decode
        return gen.funcCall("DECODE", [
          expr.args.this as exp.Expression,
          exp.Literal.string("utf-8"),
        ])
      },
    ],
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
    [exp.TimestampTrunc, timestamptrunc_sql()],
    [exp.GenerateSeries, sequenceSql],
    [exp.LogicalOr, renameFunc("BOOL_OR")],
    [exp.LogicalAnd, renameFunc("BOOL_AND")],
    [exp.VariancePop, renameFunc("VAR_POP")],
    [exp.ApproxDistinct, renameFunc("APPROX_COUNT_DISTINCT")],
    [
      exp.BitwiseLeftShift,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.BitwiseLeftShift
        return gen.funcCall("SHIFTLEFT", [
          expr.args.this as exp.Expression,
          expr.args.expression as exp.Expression,
        ])
      },
    ],
    [
      exp.BitwiseRightShift,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.BitwiseRightShift
        return gen.funcCall("SHIFTRIGHT", [
          expr.args.this as exp.Expression,
          expr.args.expression as exp.Expression,
        ])
      },
    ],
    [
      exp.RegexpReplace,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.RegexpReplace
        return gen.funcCall(
          "REGEXP_REPLACE",
          [
            expr.args.this as exp.Expression,
            expr.args.expression as exp.Expression,
            expr.args.replacement as exp.Expression,
            expr.args.position as exp.Expression | undefined,
          ].filter((x): x is exp.Expression => x != null),
        )
      },
    ],
  ])

  protected override array_sql(expression: exp.Array): string {
    return `ARRAY(${this.expressions(expression.expressions)})`
  }
}

function dateaddSql(gen: Generator, e: exp.Expression): string {
  const expression = e as exp.TsOrDsAdd | exp.TimestampAdd
  const unitArg = (expression as exp.Func).args.unit
  const unitStr =
    unitArg instanceof exp.Expression
      ? String(unitArg.args.this ?? "").toUpperCase()
      : typeof unitArg === "string"
        ? unitArg.toUpperCase()
        : ""

  if (!unitStr || (e instanceof exp.TsOrDsAdd && unitStr === "DAY")) {
    return gen.funcCall("DATE_ADD", [
      expression.args.this as exp.Expression,
      expression.args.expression as exp.Expression,
    ])
  }

  const unit = unitToVar(expression)
  const args: exp.Expression[] = []
  if (unit) args.push(unit)
  args.push(expression.args.expression as exp.Expression)
  args.push(expression.args.this as exp.Expression)
  let result = gen.funcCall("DATE_ADD", args)

  if (e instanceof exp.TsOrDsAdd) {
    const returnTypeArg = e.args.return_type
    const returnType =
      returnTypeArg instanceof exp.DataType
        ? returnTypeArg
        : exp.DataType.build("DATE")
    if (
      !returnType.isType(
        exp.DataType.Type.TIMESTAMP,
        exp.DataType.Type.DATETIME,
      )
    ) {
      result = `CAST(${result} AS ${gen.sql(returnType)})`
    }
  }

  return result
}

const SPARK_DATETIME_ADD: Set<Function> = new Set([
  exp.DatetimeAdd,
  exp.TimeAdd,
  exp.TsOrDsAdd,
  exp.TimestampAdd,
  exp.DateAdd,
])

function dateDeltaToBinaryIntervalOp(
  gen: Generator,
  e: exp.Expression,
): string {
  const op = SPARK_DATETIME_ADD.has(e.constructor) ? "+" : "-"

  let thisNode = e.args.this as exp.Expression
  if (e instanceof exp.TsOrDsAdd) {
    const returnType = e.args.return_type
    const toType =
      returnType instanceof exp.DataType ? returnType.text("this") : "DATE"
    thisNode = new exp.Cast({
      this: thisNode,
      to: new exp.DataType({ this: toType }),
    })
  }

  const unitRaw = (e as exp.Func).args.unit
  const unitStr =
    typeof unitRaw === "string"
      ? unitRaw.toUpperCase()
      : unitRaw instanceof exp.Expression
        ? String(unitRaw.args.this ?? "DAY").toUpperCase()
        : "DAY"
  const expr = e.args.expression as exp.Expression
  const interval =
    expr instanceof exp.Interval
      ? gen.sql(expr)
      : `INTERVAL ${gen.sql(expr)} ${unitStr}`
  return `${gen.sql(thisNode)} ${op} ${interval}`
}

export class SparkGenerator extends Spark2Generator {
  static override TYPE_MAPPING: Map<string, string> = new Map([
    ...Spark2Generator.TYPE_MAPPING,
    ["MONEY", "DECIMAL(15, 4)"],
    ["SMALLMONEY", "DECIMAL(6, 4)"],
    ["UUID", "STRING"],
    ["TIMESTAMPLTZ", "TIMESTAMP_LTZ"],
    ["TIMESTAMPNTZ", "TIMESTAMP_NTZ"],
  ])

  static override TRANSFORMS: Map<ExpressionClass, Transform> = new Map<
    ExpressionClass,
    Transform
  >([
    ...Spark2Generator.TRANSFORMS,
    [
      exp.TryCast,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.TryCast
        const thisExpr = gen.sql(expr.args.this as exp.Expression)
        const to = gen.sql(expr.args.to as exp.Expression)
        return expr.args.safe
          ? `TRY_CAST(${thisExpr} AS ${to})`
          : `CAST(${thisExpr} AS ${to})`
      },
    ],
    [exp.DatetimeAdd, dateDeltaToBinaryIntervalOp],
    [exp.DatetimeSub, dateDeltaToBinaryIntervalOp],
    [exp.TimeAdd, dateDeltaToBinaryIntervalOp],
    [exp.TimeSub, dateDeltaToBinaryIntervalOp],
    [exp.TimestampSub, dateDeltaToBinaryIntervalOp],
    [exp.TsOrDsAdd, dateaddSql],
    [exp.TimestampAdd, dateaddSql],
    [exp.BitwiseAndAgg, renameFunc("BIT_AND")],
    [exp.BitwiseOrAgg, renameFunc("BIT_OR")],
    [exp.BitwiseXorAgg, renameFunc("BIT_XOR")],
    [exp.EndsWith, renameFunc("ENDSWITH")],
    [exp.StartsWith, renameFunc("STARTSWITH")],
    [
      exp.SafeAdd,
      (gen: Generator, e: exp.Expression) =>
        gen.funcCall("TRY_ADD", [
          e.args.this as exp.Expression,
          e.args.expression as exp.Expression,
        ]),
    ],
    [
      exp.SafeMultiply,
      (gen: Generator, e: exp.Expression) =>
        gen.funcCall("TRY_MULTIPLY", [
          e.args.this as exp.Expression,
          e.args.expression as exp.Expression,
        ]),
    ],
    [
      exp.SafeSubtract,
      (gen: Generator, e: exp.Expression) =>
        gen.funcCall("TRY_SUBTRACT", [
          e.args.this as exp.Expression,
          e.args.expression as exp.Expression,
        ]),
    ],
  ])

  protected datediff_sql(expression: exp.DateDiff): string {
    const end = this.sql(expression.args.this as exp.Expression)
    const start = this.sql(expression.args.expression as exp.Expression)

    if (expression.args.unit) {
      const unit = unitToVar(expression)
      if (unit) {
        return `DATEDIFF(${this.sql(unit)}, ${start}, ${end})`
      }
    }

    return `DATEDIFF(${end}, ${start})`
  }
}

export class SparkDialect extends Dialect {
  static override readonly name = "spark"
  static override SAFE_DIVISION = true
  protected static override ParserClass = SparkParser
  protected static override GeneratorClass = SparkGenerator
}

// Register dialect
Dialect.register(SparkDialect)

export class Spark2Dialect extends Dialect {
  static override readonly name = "spark2"
  static override SAFE_DIVISION = true
  protected static override ParserClass = SparkParser
  protected static override GeneratorClass = Spark2Generator
}

Dialect.register(Spark2Dialect)
