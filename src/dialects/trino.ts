/**
 * Trino dialect (extends Presto)
 */

import { Dialect } from "../dialect.js"
import type { ExpressionClass } from "../expression-base.js"
import * as exp from "../expressions.js"
import type { Generator } from "../generator.js"
import {
  eliminateDistinctOn,
  eliminateQualify,
  eliminateSemiAndAntiJoins,
  explodeProjectionToUnnest,
  preprocess,
} from "../transforms.js"
import { PrestoGenerator, PrestoParser } from "./presto.js"

type Transform = (generator: Generator, expression: exp.Expression) => string

export class TrinoParser extends PrestoParser {}

export class TrinoGenerator extends PrestoGenerator {
  static override TRANSFORMS: Map<ExpressionClass, Transform> = new Map<
    ExpressionClass,
    Transform
  >([
    ...PrestoGenerator.TRANSFORMS,
    [
      exp.Select,
      preprocess([
        eliminateQualify,
        eliminateDistinctOn,
        explodeProjectionToUnnest(1),
        eliminateSemiAndAntiJoins,
      ]),
    ],
  ])
}

export class TrinoDialect extends Dialect {
  static override readonly name = "trino"
  static override NULL_ORDERING:
    | "nulls_are_small"
    | "nulls_are_large"
    | "nulls_are_last" = "nulls_are_last"
  static override INDEX_OFFSET = 1
  protected static override ParserClass = TrinoParser
  protected static override GeneratorClass = TrinoGenerator
}

Dialect.register(TrinoDialect)
