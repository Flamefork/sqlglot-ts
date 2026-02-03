import { Dialect } from "../dialect.js"
import type { ExpressionClass } from "../expression-base.js"
import * as exp from "../expressions.js"
import { Generator } from "../generator.js"
import { Parser } from "../parser.js"
import { eliminateSemiAndAntiJoins, preprocess } from "../transforms.js"

type Transform = (generator: Generator, expression: exp.Expression) => string

export class TeradataParser extends Parser {}

export class TeradataGenerator extends Generator {
  static override HEX_START: string | null = "X'"
  static override HEX_END: string | null = "'"

  static override FEATURES = {
    ...Generator.FEATURES,
    TYPED_DIVISION: true,
  }

  static override TYPE_MAPPING: Map<string, string> = new Map([
    ...Generator.TYPE_MAPPING,
    ["DOUBLE", "DOUBLE PRECISION"],
    ["TIMESTAMPTZ", "TIMESTAMP"],
  ])

  static override TRANSFORMS: Map<ExpressionClass, Transform> = new Map<
    ExpressionClass,
    Transform
  >([
    ...Generator.TRANSFORMS,
    [exp.Select, preprocess([eliminateSemiAndAntiJoins])],
  ])

  protected override mod_sql(expression: exp.Mod): string {
    return this.binary_sql(expression, "MOD")
  }
}

export class TeradataDialect extends Dialect {
  static override readonly name = "teradata"
  static override TYPED_DIVISION = true
  protected static override ParserClass = TeradataParser
  protected static override GeneratorClass = TeradataGenerator
}

Dialect.register(TeradataDialect)
