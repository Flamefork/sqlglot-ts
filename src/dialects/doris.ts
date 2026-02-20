import { Dialect } from "../dialect.js"
import type { ExpressionClass } from "../expression-base.js"
import * as exp from "../expressions.js"
import { Generator } from "../generator.js"
import { Parser } from "../parser.js"
import {
  eliminateSemiAndAntiJoins,
  preprocess,
  renameFunc,
  unitToStr,
} from "../transforms.js"

type Transform = (generator: Generator, expression: exp.Expression) => string

export class DorisParser extends Parser {}

export class DorisGenerator extends Generator {
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
  protected static override ParserClass: typeof DorisParser = DorisParser
  protected static override GeneratorClass: typeof DorisGenerator =
    DorisGenerator
}

Dialect.register(DorisDialect)
