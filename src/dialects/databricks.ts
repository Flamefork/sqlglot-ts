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
  static override FEATURES: {
    SAFE_DIVISION: boolean
    NULL_ORDERING_SUPPORTED: boolean | null
    LOCKING_READS_SUPPORTED: boolean
    LIMIT_FETCH: "ALL" | "LIMIT" | "FETCH"
    LIMIT_IS_TOP: boolean
    EXTRACT_ALLOWS_QUOTES: boolean
    IGNORE_NULLS_IN_FUNC: boolean
    NVL2_SUPPORTED: boolean
    SUPPORTS_SINGLE_ARG_CONCAT: boolean
    LAST_DAY_SUPPORTS_DATE_PART: boolean
    COLLATE_IS_FUNC: boolean
    EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE: boolean
    WRAP_DERIVED_VALUES: boolean
    VALUES_AS_TABLE: boolean
    SINGLE_STRING_INTERVAL: boolean
    INTERVAL_ALLOWS_PLURAL_FORM: boolean
    RENAME_TABLE_WITH_DB: boolean
    ALTER_TABLE_INCLUDE_COLUMN_KEYWORD: boolean
    ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN: boolean
    ALTER_TABLE_SUPPORTS_CASCADE: boolean
    SUPPORTS_TABLE_COPY: boolean
    SUPPORTS_TABLE_ALIAS_COLUMNS: boolean
    JOIN_HINTS: boolean
    TABLE_HINTS: boolean
    QUERY_HINTS: boolean
    IS_BOOL_ALLOWED: boolean
    ENSURE_BOOLS: boolean
    TZ_TO_WITH_TIME_ZONE: boolean
    UNNEST_WITH_ORDINALITY: boolean
    AGGREGATE_FILTER_SUPPORTED: boolean
    SEMI_ANTI_JOIN_WITH_SIDE: boolean
    TABLESAMPLE_REQUIRES_PARENS: boolean
    CTE_RECURSIVE_KEYWORD_REQUIRED: boolean
    UNPIVOT_ALIASES_ARE_IDENTIFIERS: boolean
    SUPPORTS_SELECT_INTO: boolean
    STAR_EXCEPT: "EXCEPT" | "EXCLUDE" | null
    CONCAT_COALESCE: boolean
    TYPED_DIVISION: boolean
  } = {
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
  protected static override ParserClass: typeof SparkParser = SparkParser
  protected static override GeneratorClass: typeof DatabricksGenerator =
    DatabricksGenerator
}

Dialect.register(DatabricksDialect)
