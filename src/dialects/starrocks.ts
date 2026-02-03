/**
 * StarRocks dialect (extends MySQL)
 */

import { Dialect } from "../dialect.js"
import type { ExpressionClass } from "../expression-base.js"
import * as exp from "../expressions.js"
import type { Generator } from "../generator.js"
import { Parser } from "../parser.js"
import { unitToStr } from "../transforms.js"
import { MySQLGenerator } from "./mysql.js"

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

export class StarRocksParser extends Parser {}

export class StarRocksGenerator extends MySQLGenerator {
  protected override INDEX_OFFSET = 1
  static override TYPE_MAPPING: Map<string, string> = new Map([
    ...MySQLGenerator.TYPE_MAPPING,
    ["INT128", "LARGEINT"],
    ["TEXT", "STRING"],
    ["TIMESTAMP", "DATETIME"],
    ["TIMESTAMPTZ", "DATETIME"],
  ])

  static override TRANSFORMS: Map<ExpressionClass, Transform> = new Map<
    ExpressionClass,
    Transform
  >([
    ...MySQLGenerator.TRANSFORMS,
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
    [exp.RegexpLike, renameFunc("REGEXP")],
    [exp.TimeStrToDate, renameFunc("TO_DATE")],
    [exp.UnixToTime, renameFunc("FROM_UNIXTIME")],
    [
      exp.TimestampTrunc,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.TimestampTrunc
        return gen.funcCall("DATE_TRUNC", [
          exp.Literal.string(unitToStr(expr)),
          expr.args.this as exp.Expression,
        ])
      },
    ],
  ])
}

export class StarRocksDialect extends Dialect {
  static override readonly name = "starrocks"
  static override INDEX_OFFSET = 1
  static override SAFE_DIVISION = true
  protected static override ParserClass = StarRocksParser
  protected static override GeneratorClass = StarRocksGenerator
}

Dialect.register(StarRocksDialect)
