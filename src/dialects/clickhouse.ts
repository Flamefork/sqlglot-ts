/**
 * ClickHouse dialect
 */

import {
  Dialect,
  buildEscapedSequences,
  buildUnescapedSequences,
} from "../dialect.js"
import type { ExpressionClass } from "../expression-base.js"
import * as exp from "../expressions.js"
import { Generator } from "../generator.js"
import { FunctionBuilder, Parser } from "../parser.js"

type Transform = (generator: Generator, expression: exp.Expression) => string

function buildTimestampTrunc(unit: string) {
  return (args: exp.Expression[]) =>
    new exp.TimestampTrunc({
      this: args[0],
      unit: new exp.Var({ this: unit }),
      zone: args[1],
    })
}

function buildSplit(expClass: typeof exp.Split | typeof exp.RegexpSplit) {
  return (args: exp.Expression[]) =>
    new expClass({
      this: args[1],
      expression: args[0],
      limit: args[2],
    })
}

const TIMESTAMP_TRUNC_UNITS = [
  "MICROSECOND",
  "MILLISECOND",
  "SECOND",
  "MINUTE",
  "HOUR",
  "DAY",
  "MONTH",
  "QUARTER",
  "YEAR",
]

export class ClickHouseParser extends Parser {
  static override FUNCTIONS: Map<string, FunctionBuilder> = new Map([
    ...Parser.FUNCTIONS,
    [
      "SHA256",
      (args: exp.Expression[]) =>
        new exp.SHA2({ this: args[0], length: exp.Literal.number(256) }),
    ],
    [
      "SHA512",
      (args: exp.Expression[]) =>
        new exp.SHA2({ this: args[0], length: exp.Literal.number(512) }),
    ],
    ["SPLITBYSTRING", buildSplit(exp.Split)],
    ["SPLITBYREGEXP", buildSplit(exp.RegexpSplit)],
    ["TOMONDAY", buildTimestampTrunc("WEEK")],
    ...TIMESTAMP_TRUNC_UNITS.map(
      (unit) =>
        [`TOSTARTOF${unit}`, buildTimestampTrunc(unit)] as [
          string,
          (args: exp.Expression[]) => exp.Expression,
        ],
    ),
  ])
}

export class ClickHouseGenerator extends Generator {
  static override BIT_START: string | null = "0b"
  static override BIT_END: string | null = ""
  static override HEX_START: string | null = "0x"
  static override HEX_END: string | null = ""
  static override HEX_STRING_IS_INTEGER_TYPE = true
  static override STRINGS_SUPPORT_ESCAPED_SEQUENCES = true
  static override ESCAPED_SEQUENCES: Record<string, string> =
    buildEscapedSequences(buildUnescapedSequences({ "\\0": "\0" }))
  static override STRING_ESCAPES: string[] = ["'", "\\"]

  static override NULL_ORDERING:
    | "nulls_are_small"
    | "nulls_are_large"
    | "nulls_are_last" = "nulls_are_last"
  static override NORMALIZE_FUNCTIONS: boolean | "upper" | "lower" = false
  static override PRESERVE_ORIGINAL_NAMES = true

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
  protected override INDEX_OFFSET = 1
  protected override ARRAY_SIZE_NAME = "LENGTH"
  static override TYPE_MAPPING: Map<string, string> = new Map([
    ...Generator.TYPE_MAPPING,
    // String types
    ["BLOB", "String"],
    ["CHAR", "String"],
    ["LONGBLOB", "String"],
    ["LONGTEXT", "String"],
    ["MEDIUMBLOB", "String"],
    ["MEDIUMTEXT", "String"],
    ["TINYBLOB", "String"],
    ["TINYTEXT", "String"],
    ["TEXT", "String"],
    ["VARBINARY", "String"],
    ["VARCHAR", "String"],
    // Numeric types
    ["BOOLEAN", "Bool"],
    ["BIGINT", "Int64"],
    ["INT", "Int32"],
    ["MEDIUMINT", "Int32"],
    ["SMALLINT", "Int16"],
    ["TINYINT", "Int8"],
    ["UBIGINT", "UInt64"],
    ["UINT", "UInt32"],
    ["USMALLINT", "UInt16"],
    ["UTINYINT", "UInt8"],
    ["DOUBLE", "Float64"],
    ["FLOAT", "Float32"],
    ["DECIMAL", "Decimal"],
    ["DECIMAL32", "Decimal32"],
    ["DECIMAL64", "Decimal64"],
    ["DECIMAL128", "Decimal128"],
    ["DECIMAL256", "Decimal256"],
    ["INT128", "Int128"],
    ["INT256", "Int256"],
    ["UINT128", "UInt128"],
    ["UINT256", "UInt256"],
    // Date/time types
    ["DATE32", "Date32"],
    ["DATETIME", "DateTime"],
    ["DATETIME2", "DateTime"],
    ["SMALLDATETIME", "DateTime"],
    ["DATETIME64", "DateTime64"],
    ["TIMESTAMP", "DateTime"],
    ["TIMESTAMPNTZ", "DateTime"],
    ["TIMESTAMPTZ", "DateTime"],
    // Complex types
    ["ARRAY", "Array"],
    ["MAP", "Map"],
    ["STRUCT", "Tuple"],
    ["NESTED", "Nested"],
    // Enum types
    ["ENUM", "Enum"],
    ["ENUM8", "Enum8"],
    ["ENUM16", "Enum16"],
    // Network types
    ["IPV4", "IPv4"],
    ["IPV6", "IPv6"],
    // Geo types
    ["POINT", "Point"],
    ["RING", "Ring"],
    ["LINESTRING", "LineString"],
    ["MULTILINESTRING", "MultiLineString"],
    ["POLYGON", "Polygon"],
    ["MULTIPOLYGON", "MultiPolygon"],
    // Other types
    ["AGGREGATEFUNCTION", "AggregateFunction"],
    ["SIMPLEAGGREGATEFUNCTION", "SimpleAggregateFunction"],
    ["FIXEDSTRING", "FixedString"],
    ["LOWCARDINALITY", "LowCardinality"],
    ["NOTHING", "Nothing"],
    ["DYNAMIC", "Dynamic"],
    ["STRING", "String"],
    ["BOOL", "Bool"],
    ["FLOAT32", "Float32"],
    ["FLOAT64", "Float64"],
    ["INT8", "Int8"],
    ["INT16", "Int16"],
    ["INT32", "Int32"],
    ["INT64", "Int64"],
    ["INT128", "Int128"],
    ["INT256", "Int256"],
    ["UINT8", "UInt8"],
    ["UINT16", "UInt16"],
    ["UINT32", "UInt32"],
    ["UINT64", "UInt64"],
    ["UINT128", "UInt128"],
    ["UINT256", "UInt256"],
  ])

  static override TRANSFORMS: Map<ExpressionClass, Transform> = new Map<
    ExpressionClass,
    Transform
  >([
    ...Generator.TRANSFORMS,
    [exp.CurrentDate, () => "CURRENT_DATE()"],
    [
      exp.CountIf,
      (gen: Generator, e: exp.Expression) =>
        gen.funcCall("countIf", [
          (e as exp.CountIf).args.this as exp.Expression,
        ]),
    ],
    [
      exp.Explode,
      (gen: Generator, e: exp.Expression) =>
        gen.funcCall("arrayJoin", [
          (e as exp.Explode).args.this as exp.Expression,
        ]),
    ],
    [
      exp.Length,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.Length
        const funcName = expr.args.binary ? "LENGTH" : "CHAR_LENGTH"
        return gen.funcCall(funcName, [expr.args.this as exp.Expression])
      },
    ],
    [exp.Rand, () => "randCanonical()"],
    [
      exp.Nullif,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.Func
        return gen.funcCall("nullIf", [
          expr.args.this as exp.Expression,
          expr.args.expression as exp.Expression,
        ])
      },
    ],
    [
      exp.RegexpLike,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.RegexpLike
        return gen.funcCall("match", [
          expr.args.this as exp.Expression,
          expr.args.expression as exp.Expression,
        ])
      },
    ],
    [
      exp.TimeStrToTime,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.TimeStrToTime
        const ts = expr.args.this as exp.Expression
        const zone = expr.args.zone as exp.Expression | undefined
        const expressions: exp.Expression[] = [
          new exp.DataTypeParam({
            this: new exp.Literal({ this: "6", is_string: false }),
          }),
        ]
        if (zone) {
          expressions.push(new exp.DataTypeParam({ this: zone }))
        }
        const datatype = new exp.DataType({
          this: "DATETIME64",
          expressions,
          nullable: false,
        })
        return gen.sql(new exp.Cast({ this: ts, to: datatype }))
      },
    ],
    [
      exp.UnixToTime,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.UnixToTime
        const scale = expr.args.scale as exp.Literal | undefined
        const scaleValue =
          scale instanceof exp.Literal ? String(scale.value) : undefined
        const timestamp = gen.sql(expr.args.this as exp.Expression)
        if (!scaleValue || scaleValue === "0") {
          return `fromUnixTimestamp(CAST(${timestamp} AS Int64))`
        }
        if (scaleValue === "3") {
          return `fromUnixTimestamp64Milli(CAST(${timestamp} AS Nullable(Int64)))`
        }
        if (scaleValue === "6") {
          return `fromUnixTimestamp64Micro(CAST(${timestamp} AS Int64))`
        }
        if (scaleValue === "9") {
          return `fromUnixTimestamp64Nano(CAST(${timestamp} AS Int64))`
        }
        return `fromUnixTimestamp(CAST(${timestamp} / POW(10, ${scaleValue}) AS Int64))`
      },
    ],
    [
      exp.SHA,
      (gen: Generator, e: exp.Expression) =>
        gen.funcCall("SHA1", [(e as exp.SHA).args.this as exp.Expression]),
    ],
    [
      exp.SHA2,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.SHA2
        const length = String(
          (expr.args.length as exp.Expression | undefined)?.args?.this ?? "256",
        )
        return gen.funcCall(`SHA${length}`, [expr.args.this as exp.Expression])
      },
    ],
    [
      exp.Split,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.Split
        return gen.funcCall("splitByString", [
          expr.args.expression as exp.Expression,
          expr.args.this as exp.Expression,
          ...(expr.args.limit ? [expr.args.limit as exp.Expression] : []),
        ])
      },
    ],
    [
      exp.RegexpSplit,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.RegexpSplit
        return gen.funcCall("splitByRegexp", [
          expr.args.expression as exp.Expression,
          expr.args.this as exp.Expression,
          ...(expr.args.limit ? [expr.args.limit as exp.Expression] : []),
        ])
      },
    ],
    [
      exp.TimestampTrunc,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.TimestampTrunc
        const unit = gen.sql(
          new exp.Var({ this: (expr.text("unit") || "DAY").toUpperCase() }),
        )
        const args: exp.Expression[] = [
          new exp.Literal({ this: unit, is_string: true }),
          expr.args.this as exp.Expression,
        ]
        if (expr.args.zone) args.push(expr.args.zone as exp.Expression)
        return gen.funcCall("dateTrunc", args)
      },
    ],
    [
      exp.Variance,
      (gen: Generator, e: exp.Expression) =>
        gen.funcCall("varSamp", [
          (e as exp.Variance).args.this as exp.Expression,
        ]),
    ],
    [
      exp.Stddev,
      (gen: Generator, e: exp.Expression) =>
        gen.funcCall("stddevSamp", [
          (e as exp.Stddev).args.this as exp.Expression,
        ]),
    ],
    [
      exp.ApproxDistinct,
      (gen: Generator, e: exp.Expression) =>
        gen.funcCall("uniq", [
          (e as exp.ApproxDistinct).args.this as exp.Expression,
        ]),
    ],
    [
      exp.DateTrunc,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.DateTrunc
        const unit = gen.sql(expr.args.unit as exp.Expression)
        const thisExpr = gen.sql(expr.args.this as exp.Expression)
        const zone = expr.args.zone as exp.Expression | undefined
        const zoneStr = zone ? `, ${gen.sql(zone)}` : ""
        return `dateTrunc(${unit}, ${thisExpr}${zoneStr})`
      },
    ],
  ])

  // Types that cannot be wrapped in Nullable()
  private static NON_NULLABLE_TYPES = new Set([
    "ARRAY",
    "Array",
    "MAP",
    "Map",
    "STRUCT",
    "Tuple",
    "NESTED",
    "Nested",
  ])

  // ClickHouse uses backticks or double quotes for identifier quoting
  protected override quoteIdentifier(name: string): string {
    return `\`${name.replace(/`/g, "``")}\``
  }

  protected override datatype_sql(expression: exp.DataType): string {
    const typeStr = expression.text("this").toUpperCase()
    const dtype = super.datatype_sql(expression)
    const nullable = expression.args.nullable

    // nullable=false → never wrap (explicitly non-nullable, e.g. from ClickHouse parser)
    if (nullable === false) {
      return dtype
    }

    // nullable=true → always wrap (unless composite)
    if (nullable === true) {
      if (ClickHouseGenerator.NON_NULLABLE_TYPES.has(typeStr)) {
        return dtype
      }
      return `Nullable(${dtype})`
    }

    // nullable=undefined/null → wrap only if mapped from a truly foreign type
    const mapped = ClickHouseGenerator.TYPE_MAPPING.get(typeStr)
    if (
      !mapped ||
      ClickHouseGenerator.NON_NULLABLE_TYPES.has(typeStr) ||
      mapped.toUpperCase() === typeStr
    ) {
      return dtype
    }

    return `Nullable(${dtype})`
  }
}

export class ClickHouseDialect extends Dialect {
  static override readonly name = "clickhouse"
  static override NULL_ORDERING:
    | "nulls_are_small"
    | "nulls_are_large"
    | "nulls_are_last" = "nulls_are_last"
  static override INDEX_OFFSET = 1
  static override SAFE_DIVISION = true
  static override PRESERVE_ORIGINAL_NAMES = true
  static override HEX_STRING_IS_INTEGER_TYPE = true
  static override BIT_START = "0b"
  static override BIT_END = ""
  static override HEX_START = "0x"
  static override HEX_END = ""
  static override STRING_ESCAPES: string[] = ["'", "\\"]
  static override UNESCAPED_SEQUENCES: Record<string, string> =
    buildUnescapedSequences({ "\\0": "\0" })
  static override ESCAPED_SEQUENCES: Record<string, string> =
    buildEscapedSequences(ClickHouseDialect.UNESCAPED_SEQUENCES)
  static override STRINGS_SUPPORT_ESCAPED_SEQUENCES = true
  protected static override ParserClass: typeof ClickHouseParser =
    ClickHouseParser
  protected static override GeneratorClass: typeof ClickHouseGenerator =
    ClickHouseGenerator
}

// Register dialect
Dialect.register(ClickHouseDialect)
