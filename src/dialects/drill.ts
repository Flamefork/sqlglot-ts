import {
  Dialect,
  buildEscapedSequences,
  buildUnescapedSequences,
} from "../dialect.js"
import type { ExpressionClass } from "../expression-base.js"
import * as exp from "../expressions.js"
import { Generator } from "../generator.js"
import { Parser } from "../parser.js"
import { eliminateSemiAndAntiJoins, preprocess } from "../transforms.js"

type Transform = (generator: Generator, expression: exp.Expression) => string

export class DrillParser extends Parser {}

export class DrillGenerator extends Generator {
  static override FEATURES = {
    ...Generator.FEATURES,
    TYPED_DIVISION: true,
  }
  static override STRINGS_SUPPORT_ESCAPED_SEQUENCES = true
  static override ESCAPED_SEQUENCES = buildEscapedSequences(
    buildUnescapedSequences(),
  )
  static override STRING_ESCAPES = ["\\"]

  static override TRANSFORMS: Map<ExpressionClass, Transform> = new Map<
    ExpressionClass,
    Transform
  >([
    ...Generator.TRANSFORMS,
    [exp.Select, preprocess([eliminateSemiAndAntiJoins])],
  ])
}

export class DrillDialect extends Dialect {
  static override readonly name = "drill"
  static override TYPED_DIVISION = true
  static override CONCAT_COALESCE = true
  static override STRING_ESCAPES = ["\\"]
  static override UNESCAPED_SEQUENCES = buildUnescapedSequences()
  static override ESCAPED_SEQUENCES = buildEscapedSequences(
    DrillDialect.UNESCAPED_SEQUENCES,
  )
  static override STRINGS_SUPPORT_ESCAPED_SEQUENCES = true
  protected static override ParserClass = DrillParser
  protected static override GeneratorClass = DrillGenerator
}

Dialect.register(DrillDialect)
