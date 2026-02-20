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
  static override FEATURES: {
    TYPED_DIVISION: boolean
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
    SAFE_DIVISION: boolean
  } = {
    ...Generator.FEATURES,
    TYPED_DIVISION: true,
  }
  static override STRINGS_SUPPORT_ESCAPED_SEQUENCES = true
  static override ESCAPED_SEQUENCES: Record<string, string> =
    buildEscapedSequences(buildUnescapedSequences())
  static override STRING_ESCAPES: string[] = ["\\"]

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
  static override STRING_ESCAPES: string[] = ["\\"]
  static override UNESCAPED_SEQUENCES: Record<string, string> =
    buildUnescapedSequences()
  static override ESCAPED_SEQUENCES: Record<string, string> =
    buildEscapedSequences(DrillDialect.UNESCAPED_SEQUENCES)
  static override STRINGS_SUPPORT_ESCAPED_SEQUENCES = true
  protected static override ParserClass: typeof DrillParser = DrillParser
  protected static override GeneratorClass: typeof DrillGenerator =
    DrillGenerator
}

Dialect.register(DrillDialect)
