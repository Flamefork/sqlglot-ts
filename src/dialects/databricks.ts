/**
 * Databricks SQL dialect (extends Spark)
 */

import { Dialect } from "../dialect.js"
import type { ExpressionClass } from "../expression-base.js"
import * as exp from "../expressions.js"
import type { Generator } from "../generator.js"
import { dateDeltaSql } from "../transforms.js"
import { SparkGenerator, SparkParser } from "./spark.js"

type Transform = (generator: Generator, expression: exp.Expression) => string

export class DatabricksGenerator extends SparkGenerator {
  static override FEATURES = {
    ...SparkGenerator.FEATURES,
    SAFE_DIVISION: false,
  }

  static override TRANSFORMS: Map<ExpressionClass, Transform> = new Map<
    ExpressionClass,
    Transform
  >([
    ...SparkGenerator.TRANSFORMS,
    [exp.DateAdd, dateDeltaSql("DATEADD")],
    [exp.DateDiff, dateDeltaSql("DATEDIFF")],
  ])
}

export class DatabricksDialect extends Dialect {
  static override readonly name = "databricks"
  protected static override ParserClass = SparkParser
  protected static override GeneratorClass = DatabricksGenerator
}

Dialect.register(DatabricksDialect)
