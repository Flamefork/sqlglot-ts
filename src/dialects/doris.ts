import { Dialect } from "../dialect.js"
import type { ExpressionClass } from "../expression-base.js"
import * as exp from "../expressions.js"
import { Generator } from "../generator.js"
import { Parser } from "../parser.js"
import {
  eliminateSemiAndAntiJoins,
  preprocess,
  unitToStr,
} from "../transforms.js"

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

export class DorisParser extends Parser {}

export class DorisGenerator extends Generator {
  static override FEATURES = {
    ...Generator.FEATURES,
    SAFE_DIVISION: true,
  }

  static override TYPE_MAPPING: Map<string, string> = new Map([
    ...Generator.TYPE_MAPPING,
    ["TEXT", "STRING"],
    ["TIMESTAMP", "DATETIME"],
    ["TIMESTAMPTZ", "DATETIME"],
  ])

  static override TRANSFORMS: Map<ExpressionClass, Transform> = new Map<
    ExpressionClass,
    Transform
  >([
    ...Generator.TRANSFORMS,
    [exp.Select, preprocess([eliminateSemiAndAntiJoins])],
    [
      exp.ApproxDistinct,
      (gen: Generator, e: exp.Expression) =>
        gen.funcCall("APPROX_COUNT_DISTINCT", [
          (e as exp.ApproxDistinct).args.this as exp.Expression,
        ]),
    ],
    [
      exp.ArrayToString,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.ArrayToString
        return gen.funcCall("ARRAY_JOIN", [
          expr.args.this as exp.Expression,
          expr.args.expression as exp.Expression,
        ])
      },
    ],
    [exp.CurrentDate, () => "CURRENT_DATE()"],
    [exp.CurrentTimestamp, (_gen: Generator, _e: exp.Expression) => "NOW()"],
    [
      exp.DateTrunc,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.DateTrunc
        return gen.funcCall("DATE_TRUNC", [
          expr.args.this as exp.Expression,
          exp.Literal.string(unitToStr(expr)),
        ])
      },
    ],
    [
      exp.RegexpLike,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.RegexpLike
        return gen.funcCall("REGEXP", [
          expr.args.this as exp.Expression,
          expr.args.expression as exp.Expression,
        ])
      },
    ],
    [exp.Split, renameFunc("SPLIT_BY_STRING")],
    [exp.RegexpSplit, renameFunc("SPLIT_BY_STRING")],
    [exp.TimeToUnix, renameFunc("UNIX_TIMESTAMP")],
    [
      exp.TimestampTrunc,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.TimestampTrunc
        return gen.funcCall("DATE_TRUNC", [
          expr.args.this as exp.Expression,
          exp.Literal.string(unitToStr(expr)),
        ])
      },
    ],
    [
      exp.TsOrDsToDate,
      (gen: Generator, e: exp.Expression) =>
        gen.funcCall("TO_DATE", [
          (e as exp.TsOrDsToDate).args.this as exp.Expression,
        ]),
    ],
    [exp.UnixToTime, renameFunc("FROM_UNIXTIME")],
  ])
}

export class DorisDialect extends Dialect {
  static override readonly name = "doris"
  static override SAFE_DIVISION = true
  protected static override ParserClass = DorisParser
  protected static override GeneratorClass = DorisGenerator
}

Dialect.register(DorisDialect)
