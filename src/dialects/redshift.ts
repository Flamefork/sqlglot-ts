/**
 * Amazon Redshift dialect (extends Postgres)
 */

import { Dialect } from "../dialect.js"
import type { ExpressionClass } from "../expression-base.js"
import * as exp from "../expressions.js"
import type { Generator } from "../generator.js"
import { Parser } from "../parser.js"
import {
  dateDeltaSql,
  eliminateSemiAndAntiJoins,
  eliminateWindowClause,
  preprocess,
  unqualifyUnnest,
} from "../transforms.js"
import { PostgresGenerator } from "./postgres.js"

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

export class RedshiftParser extends Parser {
  static override FUNCTIONS = new Map([
    ...Parser.FUNCTIONS,
    ["GETDATE", () => new exp.CurrentTimestamp({})],
    [
      "LISTAGG",
      (args: exp.Expression[]) =>
        new exp.GroupConcat({
          this: args[0],
          separator: args[1],
        }),
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
        }),
    ],
  ])
}

export class RedshiftGenerator extends PostgresGenerator {
  // Override Postgres BIT_START - Redshift doesn't support bit string literals
  static override BIT_START: string | null = null
  static override BIT_END: string | null = null
  static override HEX_START: string | null = null
  static override HEX_END: string | null = null

  static override TRANSFORMS: Map<ExpressionClass, Transform> = new Map<
    ExpressionClass,
    Transform
  >([
    ...PostgresGenerator.TRANSFORMS,
    [
      exp.Select,
      preprocess([
        eliminateWindowClause,
        unqualifyUnnest,
        eliminateSemiAndAntiJoins,
      ]),
    ],
    [
      exp.CurrentTimestamp,
      (_gen: Generator, e: exp.Expression) =>
        (e as exp.CurrentTimestamp).args.sysdate ? "SYSDATE" : "GETDATE()",
    ],
    [exp.CurrentDate, () => "GETDATE()"],
    [
      exp.GroupConcat,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.GroupConcat
        const args: exp.Expression[] = [expr.args.this as exp.Expression]
        if (expr.args.separator)
          args.push(expr.args.separator as exp.Expression)
        return gen.funcCall("LISTAGG", args)
      },
    ],
    [exp.RegexpExtract, renameFunc("REGEXP_SUBSTR")],
    [
      exp.Explode,
      (gen: Generator) => {
        gen.unsupported("Unsupported EXPLODE() function")
        return ""
      },
    ],
    [exp.DateAdd, dateDeltaSql("DATEADD")],
    [exp.DateDiff, dateDeltaSql("DATEDIFF")],
    [exp.TsOrDsAdd, dateDeltaSql("DATEADD")],
    [exp.TsOrDsDiff, dateDeltaSql("DATEDIFF")],
  ])

  protected override INDEX_OFFSET = 0
  static override TYPE_MAPPING: Map<string, string> = new Map([
    ...PostgresGenerator.TYPE_MAPPING,
    ["TIMESTAMPTZ", "TIMESTAMP"],
  ])
}

export class RedshiftDialect extends Dialect {
  static override readonly name = "redshift"
  static override INDEX_OFFSET = 0
  static override HEX_LOWERCASE = true
  protected static override ParserClass = RedshiftParser
  protected static override GeneratorClass = RedshiftGenerator
}

Dialect.register(RedshiftDialect)
