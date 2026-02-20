/**
 * Dremio dialect
 */

import { Dialect } from "../dialect.js"
import type { ExpressionClass } from "../expression-base.js"
import * as exp from "../expressions.js"
import { Generator } from "../generator.js"
import { Parser } from "../parser.js"
import { formatTime } from "../time.js"
import { renameFunc } from "../transforms.js"

type Transform = (generator: Generator, expression: exp.Expression) => string

const DREMIO_TIME_MAPPING = new Map([
  ["YYYY", "%Y"],
  ["yyyy", "%Y"],
  ["YY", "%y"],
  ["yy", "%y"],
  ["MM", "%m"],
  ["mm", "%m"],
  ["MON", "%b"],
  ["mon", "%b"],
  ["MONTH", "%B"],
  ["month", "%B"],
  ["DDD", "%j"],
  ["ddd", "%j"],
  ["DD", "%d"],
  ["dd", "%d"],
  ["DY", "%a"],
  ["dy", "%a"],
  ["DAY", "%A"],
  ["day", "%A"],
  ["HH24", "%H"],
  ["hh24", "%H"],
  ["HH12", "%I"],
  ["hh12", "%I"],
  ["HH", "%I"],
  ["hh", "%I"],
  ["MI", "%M"],
  ["mi", "%M"],
  ["SS", "%S"],
  ["ss", "%S"],
  ["FFF", "%f"],
  ["fff", "%f"],
  ["AMPM", "%p"],
  ["ampm", "%p"],
  ["WW", "%W"],
  ["ww", "%W"],
  ["D", "%w"],
  ["d", "%w"],
  ["CC", "%C"],
  ["cc", "%C"],
  ["TZD", "%Z"],
  ["tzd", "%Z"],
  ["TZO", "%z"],
  ["tzo", "%z"],
])

const DREMIO_INVERSE_TIME_MAPPING = new Map([
  ["%Y", "yyyy"],
  ["%y", "yy"],
  ["%m", "mm"],
  ["%b", "mon"],
  ["%B", "month"],
  ["%j", "ddd"],
  ["%d", "dd"],
  ["%a", "dy"],
  ["%A", "day"],
  ["%H", "hh24"],
  ["%I", "hh12"],
  ["%M", "mi"],
  ["%S", "ss"],
  ["%f", "fff"],
  ["%p", "ampm"],
  ["%W", "ww"],
  ["%w", "d"],
  ["%C", "cc"],
  ["%Z", "tzd"],
  ["%z", "tzo"],
])

export class DremioParser extends Parser {
  static override FUNCTIONS = new Map([
    ...Parser.FUNCTIONS,
    [
      "ARRAY_GENERATE_RANGE",
      (args: exp.Expression[]) =>
        new exp.GenerateSeries({
          start: args[0],
          end: args[1],
          step: args[2],
        }),
    ],
    [
      "TO_CHAR",
      (args: exp.Expression[]) => {
        const fmt = args[1]
        if (
          fmt instanceof exp.Literal &&
          fmt.args.is_string &&
          typeof fmt.name === "string" &&
          fmt.name.includes("#")
        ) {
          const toChar = new exp.ToChar({
            this: args[0],
            format: fmt,
            is_numeric: true,
          })
          return toChar
        }
        if (fmt instanceof exp.Literal && fmt.args.is_string) {
          const converted = formatTime(fmt.name, DREMIO_TIME_MAPPING)
          return new exp.TimeToStr({
            this: args[0],
            format: exp.Literal.string(converted),
          })
        }
        return new exp.TimeToStr({ this: args[0], format: fmt })
      },
    ],
    [
      "DATE_FORMAT",
      (args: exp.Expression[]) => {
        const fmt = args[1]
        if (fmt instanceof exp.Literal && fmt.args.is_string) {
          const converted = formatTime(fmt.name, DREMIO_TIME_MAPPING)
          return new exp.TimeToStr({
            this: args[0],
            format: exp.Literal.string(converted),
          })
        }
        return new exp.TimeToStr({ this: args[0], format: fmt })
      },
    ],
  ])
}

export class DremioGenerator extends Generator {
  static override FEATURES = {
    ...Generator.FEATURES,
    TYPED_DIVISION: true,
  }

  static override TYPE_MAPPING: Map<string, string> = new Map([
    ...Generator.TYPE_MAPPING,
    ["SMALLINT", "INT"],
    ["TINYINT", "INT"],
    ["BINARY", "VARBINARY"],
    ["TEXT", "VARCHAR"],
    ["NCHAR", "VARCHAR"],
    ["CHAR", "VARCHAR"],
    ["TIMESTAMPNTZ", "TIMESTAMP"],
    ["DATETIME", "TIMESTAMP"],
    ["ARRAY", "LIST"],
    ["BIT", "BOOLEAN"],
  ])

  static override TRANSFORMS: Map<ExpressionClass, Transform> = new Map<
    ExpressionClass,
    Transform
  >([
    ...Generator.TRANSFORMS,
    [exp.GenerateSeries, renameFunc("ARRAY_GENERATE_RANGE")],
    [exp.ToChar, renameFunc("TO_CHAR")],
    [
      exp.TimeToStr,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.TimeToStr
        const thisExpr = gen.sql(expr.args.this as exp.Expression)
        const fmtExpr = expr.args.format as exp.Expression | undefined
        if (fmtExpr instanceof exp.Literal && fmtExpr.args.is_string) {
          const converted = formatTime(
            fmtExpr.name,
            DREMIO_INVERSE_TIME_MAPPING,
          )
          return `TO_CHAR(${thisExpr}, '${converted}')`
        }
        const format = gen.sql(fmtExpr as exp.Expression)
        return `TO_CHAR(${thisExpr}, ${format})`
      },
    ],
  ])
}

export class DremioDialect extends Dialect {
  static override readonly name = "dremio"
  static override TYPED_DIVISION = true
  static override NULL_ORDERING:
    | "nulls_are_small"
    | "nulls_are_large"
    | "nulls_are_last" = "nulls_are_last"
  static override INDEX_OFFSET = 1
  protected static override ParserClass = DremioParser
  protected static override GeneratorClass = DremioGenerator
}

// Register dialect
Dialect.register(DremioDialect)
