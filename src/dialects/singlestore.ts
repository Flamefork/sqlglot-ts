/**
 * SingleStore dialect (extends MySQL)
 */

import { Dialect } from "../dialect.js"
import type { ExpressionClass } from "../expression-base.js"
import * as exp from "../expressions.js"
import type { Generator } from "../generator.js"
import { Parser } from "../parser.js"
import { renameFunc } from "../transforms.js"
import { MySQLGenerator } from "./mysql.js"

type Transform = (generator: Generator, expression: exp.Expression) => string

export class SingleStoreParser extends Parser {
  static override FUNCTIONS = new Map([
    ...Parser.FUNCTIONS,
    [
      "UNIX_TIMESTAMP",
      (args: exp.Expression[]) => {
        if (args.length === 0) {
          return new exp.UnixSeconds({})
        }
        return new exp.StrToUnix({ this: args[0], format: args[1] })
      },
    ],
  ])
}

export class SingleStoreGenerator extends MySQLGenerator {
  static override TRANSFORMS: Map<ExpressionClass, Transform> = new Map<
    ExpressionClass,
    Transform
  >([
    ...MySQLGenerator.TRANSFORMS,
    [exp.VariancePop, renameFunc("VAR_POP")],
    [exp.Variance, renameFunc("VAR_SAMP")],
    [exp.ApproxDistinct, renameFunc("APPROX_COUNT_DISTINCT")],
    [exp.StrToUnix, renameFunc("UNIX_TIMESTAMP")],
    [exp.TimeToUnix, renameFunc("UNIX_TIMESTAMP")],
    [exp.TimeStrToUnix, renameFunc("UNIX_TIMESTAMP")],
    [exp.UnixSeconds, renameFunc("UNIX_TIMESTAMP")],
    [
      exp.UnixToStr,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.UnixToStr
        const thisExpr = gen.sql(expr.args.this as exp.Expression)
        const format = gen.formatTimeStr(expr)
        if (format) {
          return `FROM_UNIXTIME(${thisExpr}, ${format})`
        }
        return `FROM_UNIXTIME(${thisExpr})`
      },
    ],
  ])
}

export class SingleStoreDialect extends Dialect {
  static override readonly name = "singlestore"
  static override CONCAT_COALESCE = true
  protected static override ParserClass = SingleStoreParser
  protected static override GeneratorClass = SingleStoreGenerator
}

// Register dialect
Dialect.register(SingleStoreDialect)
