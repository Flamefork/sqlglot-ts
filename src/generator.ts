/**
 * SQL generator - converts AST back to SQL strings
 */

import type { ArgValue, ExpressionClass } from "./expression-base.js"
import * as exp from "./expressions.js"
import { indexOffsetLogs } from "./expressions.js"
import { formatTime } from "./time.js"
import { ensureBools, moveCTEsToTopLevel } from "./transforms.js"

const PROPERTY_TO_NAME: Map<ExpressionClass, string> = new Map([
  [exp.ExecuteAsProperty, "EXECUTE AS"],
  [exp.LanguageProperty, "LANGUAGE"],
  [exp.LocationProperty, "LOCATION"],
  [exp.ReturnsProperty, "RETURNS"],
])

const TIME_PART_SINGULARS = new Map([
  ["MICROSECONDS", "MICROSECOND"],
  ["SECONDS", "SECOND"],
  ["MINUTES", "MINUTE"],
  ["HOURS", "HOUR"],
  ["DAYS", "DAY"],
  ["WEEKS", "WEEK"],
  ["MONTHS", "MONTH"],
  ["QUARTERS", "QUARTER"],
  ["YEARS", "YEAR"],
])

export interface GeneratorFeatures {
  // Core behavior
  NULL_ORDERING_SUPPORTED: boolean | null // true/false/null for partial
  LOCKING_READS_SUPPORTED: boolean
  LIMIT_FETCH: "ALL" | "LIMIT" | "FETCH"
  LIMIT_IS_TOP: boolean
  EXTRACT_ALLOWS_QUOTES: boolean

  // Function behavior
  IGNORE_NULLS_IN_FUNC: boolean
  NVL2_SUPPORTED: boolean
  SUPPORTS_SINGLE_ARG_CONCAT: boolean
  LAST_DAY_SUPPORTS_DATE_PART: boolean
  COLLATE_IS_FUNC: boolean

  // Set operations
  EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE: boolean

  // Values and derived tables
  WRAP_DERIVED_VALUES: boolean
  VALUES_AS_TABLE: boolean

  // Interval handling
  SINGLE_STRING_INTERVAL: boolean
  INTERVAL_ALLOWS_PLURAL_FORM: boolean

  // Table operations
  RENAME_TABLE_WITH_DB: boolean
  ALTER_TABLE_INCLUDE_COLUMN_KEYWORD: boolean
  ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN: boolean
  ALTER_TABLE_SUPPORTS_CASCADE: boolean
  SUPPORTS_TABLE_COPY: boolean
  SUPPORTS_TABLE_ALIAS_COLUMNS: boolean

  // Query hints
  JOIN_HINTS: boolean
  TABLE_HINTS: boolean
  QUERY_HINTS: boolean

  // Boolean handling
  IS_BOOL_ALLOWED: boolean
  ENSURE_BOOLS: boolean

  // Timezone
  TZ_TO_WITH_TIME_ZONE: boolean

  // UNNEST
  UNNEST_WITH_ORDINALITY: boolean

  // Aggregate functions
  AGGREGATE_FILTER_SUPPORTED: boolean

  // Joins
  SEMI_ANTI_JOIN_WITH_SIDE: boolean

  // Table sampling
  TABLESAMPLE_REQUIRES_PARENS: boolean

  // CTE
  CTE_RECURSIVE_KEYWORD_REQUIRED: boolean

  // UNPIVOT
  UNPIVOT_ALIASES_ARE_IDENTIFIERS: boolean

  // SELECT INTO
  SUPPORTS_SELECT_INTO: boolean

  // Star exclusion syntax
  STAR_EXCEPT: "EXCEPT" | "EXCLUDE" | null

  // Concat behavior
  CONCAT_COALESCE: boolean

  // Division behavior
  SAFE_DIVISION: boolean
  TYPED_DIVISION: boolean
}

export const DEFAULT_FEATURES: GeneratorFeatures = {
  // Core behavior
  NULL_ORDERING_SUPPORTED: true,
  LOCKING_READS_SUPPORTED: false,
  LIMIT_FETCH: "ALL",
  LIMIT_IS_TOP: false,
  EXTRACT_ALLOWS_QUOTES: true,

  // Function behavior
  IGNORE_NULLS_IN_FUNC: false,
  NVL2_SUPPORTED: true,
  SUPPORTS_SINGLE_ARG_CONCAT: true,
  LAST_DAY_SUPPORTS_DATE_PART: true,
  COLLATE_IS_FUNC: false,

  // Set operations
  EXCEPT_INTERSECT_SUPPORT_ALL_CLAUSE: true,

  // Values and derived tables
  WRAP_DERIVED_VALUES: true,
  VALUES_AS_TABLE: true,

  // Interval handling
  SINGLE_STRING_INTERVAL: false,
  INTERVAL_ALLOWS_PLURAL_FORM: true,

  // Table operations
  RENAME_TABLE_WITH_DB: true,
  ALTER_TABLE_INCLUDE_COLUMN_KEYWORD: true,
  ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN: true,
  ALTER_TABLE_SUPPORTS_CASCADE: false,
  SUPPORTS_TABLE_COPY: true,
  SUPPORTS_TABLE_ALIAS_COLUMNS: true,

  // Query hints
  JOIN_HINTS: true,
  TABLE_HINTS: true,
  QUERY_HINTS: true,

  // Boolean handling
  IS_BOOL_ALLOWED: true,
  ENSURE_BOOLS: false,

  // Timezone
  TZ_TO_WITH_TIME_ZONE: false,

  // UNNEST
  UNNEST_WITH_ORDINALITY: true,

  // Aggregate functions
  AGGREGATE_FILTER_SUPPORTED: true,

  // Joins
  SEMI_ANTI_JOIN_WITH_SIDE: true,

  // Table sampling
  TABLESAMPLE_REQUIRES_PARENS: true,

  // CTE
  CTE_RECURSIVE_KEYWORD_REQUIRED: true,

  // UNPIVOT
  UNPIVOT_ALIASES_ARE_IDENTIFIERS: true,

  // SELECT INTO
  SUPPORTS_SELECT_INTO: false,

  // Star exclusion syntax
  STAR_EXCEPT: "EXCEPT",

  // Concat behavior
  CONCAT_COALESCE: false,

  // Division behavior
  SAFE_DIVISION: false,
  TYPED_DIVISION: false,
}

export interface GenerateOptions {
  pretty?: boolean
  indent?: number
  pad?: number
  unsupportedLevel?: "IGNORE" | "WARN" | "RAISE"
  version?: [number, number, number]
}

type Transform = (generator: Generator, expression: exp.Expression) => string

export class Generator {
  // Static TYPE_MAPPING that can be overridden by dialects
  // Maps data type names to dialect-specific equivalents
  static TYPE_MAPPING: Map<string, string> = new Map([
    ["DATETIME2", "TIMESTAMP"],
    ["NCHAR", "CHAR"],
    ["NVARCHAR", "VARCHAR"],
    ["MEDIUMTEXT", "TEXT"],
    ["LONGTEXT", "TEXT"],
    ["TINYTEXT", "TEXT"],
    ["MEDIUMBLOB", "BLOB"],
    ["LONGBLOB", "BLOB"],
    ["TINYBLOB", "BLOB"],
    ["INET", "INET"],
    ["ROWVERSION", "VARBINARY"],
    ["SMALLDATETIME", "TIMESTAMP"],
  ])

  // Inverse time mapping (strftime -> dialect format) for format string conversion
  static INVERSE_TIME_MAPPING: Map<string, string> = new Map()

  // Static TRANSFORMS that can be overridden by dialects
  // Uses Expression class (constructor function) as key for type-safe dispatch
  static TRANSFORMS: Map<ExpressionClass, Transform> = new Map<
    ExpressionClass,
    Transform
  >([
    // Binary operators
    [exp.Adjacent, (g, e) => g.binary_sql(e as exp.Binary, "-|-")],
    [exp.ArrayContainsAll, (g, e) => g.binary_sql(e as exp.Binary, "@>")],
    [exp.ArrayOverlaps, (g, e) => g.binary_sql(e as exp.Binary, "&&")],
    [exp.ExtendsLeft, (g, e) => g.binary_sql(e as exp.Binary, "&<")],
    [exp.ExtendsRight, (g, e) => g.binary_sql(e as exp.Binary, "&>")],
    [exp.Operator, (g, e) => g.binary_sql(e as exp.Binary, "")],

    // JSONB operators (Postgres)
    [exp.JSONBContains, (g, e) => g.binary_sql(e as exp.Binary, "@>")],
    [
      exp.JSONBContainsAnyTopKeys,
      (g, e) => g.binary_sql(e as exp.Binary, "?|"),
    ],
    [
      exp.JSONBContainsAllTopKeys,
      (g, e) => g.binary_sql(e as exp.Binary, "?&"),
    ],
    [exp.JSONBDeleteAtPath, (g, e) => g.binary_sql(e as exp.Binary, "#-")],
    [exp.JSONBExtract, (g, e) => g.binary_sql(e as exp.Binary, "#>")],
    [exp.JSONBExtractScalar, (g, e) => g.binary_sql(e as exp.Binary, "#>>")],

    // JSON path parts
    [exp.JSONPathFilter, (_g, e) => `?${(e as exp.JSONPathFilter).args.this}`],
    [exp.JSONPathKey, (g, e) => g.jsonpathkey_sql(e as exp.JSONPathKey)],
    [
      exp.JSONPathRecursive,
      (_g, e) => `..${(e as exp.JSONPathRecursive).args.this ?? ""}`,
    ],
    [exp.JSONPathRoot, () => "$"],
    [exp.JSONPathScript, (_g, e) => `(${(e as exp.JSONPathScript).args.this}`],
    [
      exp.JSONPathSelector,
      (g, e) =>
        `[${g.json_path_part((e as exp.JSONPathSelector).args.this as string | number | exp.JSONPathPart)}]`,
    ],
    [
      exp.JSONPathSlice,
      (g, e) => {
        const args = (e as exp.JSONPathSlice).args
        const parts = [args.start, args.end, args.step]
        return parts
          .filter((p) => p !== undefined && p !== null)
          .map((p) =>
            p === false || p === true
              ? ""
              : g.json_path_part(p as string | number | exp.JSONPathPart),
          )
          .join(":")
      },
    ],
    [
      exp.JSONPathSubscript,
      (g, e) => g.jsonpathsubscript_sql(e as exp.JSONPathSubscript),
    ],
    [
      exp.JSONPathUnion,
      (g, e) =>
        `[${(e as exp.JSONPathUnion).expressions
          .map((p) => g.json_path_part(p as string | number | exp.JSONPathPart))
          .join(",")}]`,
    ],
    [exp.JSONPathWildcard, () => "*"],

    // Simple constants
    [
      exp.CurrentDate,
      (g, e) => {
        const z = g.sql(e.args.this)
        return z ? `CURRENT_DATE(${z})` : "CURRENT_DATE"
      },
    ],
    [
      exp.CurrentTime,
      (g, e) => {
        const z = g.sql(e.args.this)
        return z ? `CURRENT_TIME(${z})` : "CURRENT_TIME"
      },
    ],
    [exp.CurrentCatalog, () => "CURRENT_CATALOG"],
    [exp.SessionUser, () => "SESSION_USER"],
    [exp.PositionalColumn, (g, e) => `#${g.sql(e.this)}`],

    // Keyword-only properties
    [exp.CopyGrantsProperty, () => "COPY GRANTS"],
    [exp.DynamicProperty, () => "DYNAMIC"],
    [exp.EmptyProperty, () => "EMPTY"],
    [exp.ExternalProperty, () => "EXTERNAL"],
    [exp.ForceProperty, () => "FORCE"],
    [exp.GlobalProperty, () => "GLOBAL"],
    [exp.HeapProperty, () => "HEAP"],
    [exp.IcebergProperty, () => "ICEBERG"],
    [exp.MaterializedProperty, () => "MATERIALIZED"],
    [exp.NoPrimaryIndexProperty, () => "NO PRIMARY INDEX"],
    [exp.NotForReplicationColumnConstraint, () => "NOT FOR REPLICATION"],
    [exp.SecureProperty, () => "SECURE"],
    [exp.StreamingTableProperty, () => "STREAMING"],
    [exp.StrictProperty, () => "STRICT"],
    [exp.TemporaryProperty, () => "TEMPORARY"],
    [exp.TransientProperty, () => "TRANSIENT"],
    [exp.UnloggedProperty, () => "UNLOGGED"],
    [exp.UppercaseColumnConstraint, () => "UPPERCASE"],
    [exp.VolatileProperty, () => "VOLATILE"],
    [exp.ZeroFillColumnConstraint, () => "ZEROFILL"],

    // Prefix + SQL(this) properties
    [exp.AutoRefreshProperty, (g, e) => `AUTO REFRESH ${g.sql(e.args.this)}`],
    [exp.BackupProperty, (g, e) => `BACKUP ${g.sql(e.args.this)}`],
    [
      exp.CharacterSetColumnConstraint,
      (g, e) => `CHARACTER SET ${g.sql(e.args.this)}`,
    ],
    [exp.CollateColumnConstraint, (g, e) => `COLLATE ${g.sql(e.args.this)}`],
    [exp.CommentColumnConstraint, (g, e) => `COMMENT ${g.sql(e.args.this)}`],
    [exp.ConnectByRoot, (g, e) => `CONNECT_BY_ROOT ${g.sql(e.args.this)}`],
    [exp.DateFormatColumnConstraint, (g, e) => `FORMAT ${g.sql(e.args.this)}`],
    [exp.DefaultColumnConstraint, (g, e) => `DEFAULT ${g.sql(e.args.this)}`],
    [exp.EncodeColumnConstraint, (g, e) => `ENCODE ${g.sql(e.args.this)}`],
    [
      exp.InlineLengthColumnConstraint,
      (g, e) => `INLINE LENGTH ${g.sql(e.args.this)}`,
    ],
    [exp.InputModelProperty, (g, e) => `INPUT${g.sql(e.args.this)}`],
    [exp.OnProperty, (g, e) => `ON ${g.sql(e.args.this)}`],
    [exp.OnUpdateColumnConstraint, (g, e) => `ON UPDATE ${g.sql(e.args.this)}`],
    [exp.OutputModelProperty, (g, e) => `OUTPUT${g.sql(e.args.this)}`],
    [exp.PathColumnConstraint, (g, e) => `PATH ${g.sql(e.args.this)}`],
    [
      exp.ProjectionPolicyColumnConstraint,
      (g, e) => `PROJECTION POLICY ${g.sql(e.args.this)}`,
    ],
    [
      exp.RemoteWithConnectionModelProperty,
      (g, e) => `REMOTE WITH CONNECTION ${g.sql(e.args.this)}`,
    ],
    [exp.SampleProperty, (g, e) => `SAMPLE BY ${g.sql(e.args.this)}`],
    [exp.SecurityProperty, (g, e) => `SECURITY ${g.sql(e.args.this)}`],
    [exp.SharingProperty, (g, e) => `SHARING=${g.sql(e.args.this)}`],
    [exp.SqlSecurityProperty, (g, e) => `SQL SECURITY ${g.sql(e.args.this)}`],
    [exp.Stream, (g, e) => `STREAM ${g.sql(e.args.this)}`],
    [exp.TitleColumnConstraint, (g, e) => `TITLE ${g.sql(e.args.this)}`],
    [exp.ToMap, (g, e) => `MAP ${g.sql(e.args.this)}`],
    [exp.ToTableProperty, (g, e) => `TO ${g.sql(e.this)}`],
    [
      exp.UsingTemplateProperty,
      (g, e) => `USING TEMPLATE ${g.sql(e.args.this)}`,
    ],
    [exp.UsingData, (g, e) => `USING DATA ${g.sql(e.args.this)}`],
    [exp.Variadic, (g, e) => `VARIADIC ${g.sql(e.args.this)}`],
    [exp.ViewAttributeProperty, (g, e) => `WITH ${g.sql(e.args.this)}`],
    [
      exp.WithJournalTableProperty,
      (g, e) => `WITH JOURNAL TABLE=${g.sql(e.args.this)}`,
    ],
    [
      exp.WithSchemaBindingProperty,
      (g, e) => `WITH SCHEMA ${g.sql(e.args.this)}`,
    ],

    // Conditional properties
    [
      exp.CaseSpecificColumnConstraint,
      (_g, e) => `${e.args.not_ ? "NOT " : ""}CASESPECIFIC`,
    ],
    [
      exp.CharacterSetProperty,
      (g, e) =>
        `${e.args.default ? "DEFAULT " : ""}CHARACTER SET=${g.sql(e.args.this)}`,
    ],
    [
      exp.EphemeralColumnConstraint,
      (g, e) => {
        const thisSql = g.sql(e.args.this)
        return `EPHEMERAL${thisSql ? ` ${thisSql}` : ""}`
      },
    ],
    [exp.LogProperty, (_g, e) => `${e.args.no ? "NO " : ""}LOG`],
    [
      exp.OnCommitProperty,
      (_g, e) => `ON COMMIT ${e.args.delete ? "DELETE" : "PRESERVE"} ROWS`,
    ],
    [exp.SetProperty, (_g, e) => `${e.args.multi ? "MULTI" : ""}SET`],

    // Expression list properties
    [
      exp.AllowedValuesProperty,
      (g, e) => `ALLOWED_VALUES ${g.expressions(e.expressions)}`,
    ],
    [
      exp.ClusteredColumnConstraint,
      (g, e) => `CLUSTERED (${g.expressions(e.expressions)})`,
    ],
    [
      exp.CredentialsProperty,
      (g, e) => `CREDENTIALS=(${g.expressions(e.expressions, " ")})`,
    ],
    [
      exp.EnviromentProperty,
      (g, e) => `ENVIRONMENT (${g.expressions(e.expressions)})`,
    ],
    [
      exp.InheritsProperty,
      (g, e) => `INHERITS (${g.expressions(e.expressions)})`,
    ],
    [
      exp.NonClusteredColumnConstraint,
      (g, e) => `NONCLUSTERED (${g.expressions(e.expressions)})`,
    ],
    [exp.Tags, (g, e) => `TAG (${g.expressions(e.expressions)})`],
    [
      exp.TransformModelProperty,
      (g, e) => g.funcCall("TRANSFORM", e.expressions),
    ],
    [
      exp.WithProcedureOptions,
      (g, e) => `WITH ${g.expressions(e.expressions)}`,
    ],

    // Name-based properties
    [exp.SqlReadWriteProperty, (_g, e) => e.name],
    [exp.StabilityProperty, (_g, e) => e.name],

    // Naked properties (PROPERTY_NAME + sql(this))
    [exp.ExecuteAsProperty, (g, e) => g.nakedPropertySql(e)],
    [exp.LanguageProperty, (g, e) => g.nakedPropertySql(e)],
    [exp.LocationProperty, (g, e) => g.nakedPropertySql(e)],
    [
      exp.ReturnsProperty,
      (g, e) =>
        e.args.null ? "RETURNS NULL ON NULL INPUT" : g.nakedPropertySql(e),
    ],

    // Misc expressions
    [exp.AnalyzeColumns, (g, e) => g.sql(e.args.this)],
    [exp.AnalyzeWith, (g, e) => `WITH ${g.expressions(e.expressions, " ")}`],
    [
      exp.ExcludeColumnConstraint,
      (g, e) => `EXCLUDE ${g.sql(e.args.this).trimStart()}`,
    ],
    [exp.NetFunc, (g, e) => `NET.${g.sql(e.args.this)}`],
    [exp.PivotAny, (g, e) => `ANY${g.sql(e.args.this)}`],
    [exp.SafeFunc, (g, e) => `SAFE.${g.sql(e.args.this)}`],
    [exp.SetConfigProperty, (g, e) => g.sql(e.args.this)],
    [
      exp.SettingsProperty,
      (g, e) => `SETTINGS ${g.expressions(e.expressions)}`,
    ],
    [exp.SwapTable, (g, e) => `SWAP WITH ${g.sql(e.args.this)}`],
    [exp.TableColumn, (g, e) => g.sql(e.this)],
    [
      exp.WithOperator,
      (g, e) => `${g.sql(e.args.this)} WITH ${g.sql(e.args.op)}`,
    ],

    // Interval
    [exp.IntervalSpan, (g, e) => `${g.sql(e.this)} TO ${g.sql(e.expression)}`],

    // Functions / casts
    [exp.Ceil, (g, e) => g.ceil_floor_sql(e as exp.Ceil)],
    [exp.Floor, (g, e) => g.ceil_floor_sql(e as exp.Floor)],
    [
      exp.ConvertToCharset,
      (g, e) => {
        const args: exp.Expression[] = [
          e.args.this as exp.Expression,
          e.args.dest as exp.Expression,
        ]
        const source = e.args.source as exp.Expression | undefined
        if (source) args.push(source)
        return g.funcCall("CONVERT", args)
      },
    ],
    [exp.Int64, (g, e) => g.sql(exp.cast(e.this as exp.Expression, "BIGINT"))],
    [
      exp.PartitionedByBucket,
      (g, e) =>
        g.funcCall("BUCKET", [
          e.args.this as exp.Expression,
          e.args.expression as exp.Expression,
        ]),
    ],
    [
      exp.PartitionByTruncate,
      (g, e) =>
        g.funcCall("TRUNCATE", [
          e.args.this as exp.Expression,
          e.args.expression as exp.Expression,
        ]),
    ],
    [
      exp.VarMap,
      (g, e) =>
        g.funcCall("MAP", [
          e.args.keys as exp.Expression,
          e.args.values as exp.Expression,
        ]),
    ],

    // Set operations are handled by auto-discovered methods: union_sql, except_sql, intersect_sql

    // UTC functions → CurrentDate/Time/Timestamp with "UTC" literal
    [
      exp.UtcDate,
      (g, _e) =>
        g.sql(new exp.CurrentDate({ this: exp.Literal.string("UTC") })),
    ],
    [
      exp.UtcTime,
      (g, _e) =>
        g.sql(new exp.CurrentTime({ this: exp.Literal.string("UTC") })),
    ],
    [
      exp.UtcTimestamp,
      (g, _e) =>
        g.sql(new exp.CurrentTimestamp({ this: exp.Literal.string("UTC") })),
    ],
  ])

  // Static FEATURES that can be overridden by dialects
  static FEATURES: GeneratorFeatures = { ...DEFAULT_FEATURES }

  // Expressions that need to have all CTEs under them bubbled up to them
  static EXPRESSIONS_WITHOUT_NESTED_CTES: Set<ExpressionClass> = new Set()

  static RESERVED_KEYWORDS: Set<string> = new Set()
  static NULL_ORDERING:
    | "nulls_are_small"
    | "nulls_are_large"
    | "nulls_are_last" = "nulls_are_small"
  static NORMALIZE_FUNCTIONS: boolean | "upper" | "lower" = "upper"
  static PRESERVE_ORIGINAL_NAMES = false

  // String literal delimiters (can be overridden by dialect generators)
  static BIT_START: string | null = null
  static BIT_END: string | null = null
  static HEX_START: string | null = null
  static HEX_END: string | null = null
  static HEX_STRING_IS_INTEGER_TYPE = false
  static BYTE_START: string | null = null
  static BYTE_END: string | null = null
  static BYTE_STRING_IS_BYTES_TYPE = false
  static UNICODE_START: string | null = null
  static UNICODE_END: string | null = null

  // String escape configuration (matches Python Dialect properties)
  static STRINGS_SUPPORT_ESCAPED_SEQUENCES = false
  static ESCAPED_SEQUENCES: Record<string, string> = {}
  static STRING_ESCAPES: string[] = ["'"]

  protected PARSE_JSON_NAME: string | null = "PARSE_JSON"
  protected SUPPORTS_UESCAPE = true

  protected INDEX_OFFSET = 0
  protected STRUCT_DELIMITER: [string, string] = ["<", ">"]
  protected SUPPORTS_LIKE_QUANTIFIERS = true

  protected options: GenerateOptions
  protected _indent: number
  protected pad: number
  protected pretty: boolean
  protected leadingComma: boolean
  protected maxTextWidth: number
  protected version: [number, number, number]
  private _nameCounter = 0
  private _escapedQuoteEnd = "''"
  protected _quoteJsonPathKeyUsingBrackets = true

  // Instance transforms (merged with static)
  protected transforms: Map<ExpressionClass, Transform>

  // Instance features (merged from static for dialect inheritance)
  protected features: GeneratorFeatures

  constructor(options: GenerateOptions = {}) {
    this.options = options
    this.pretty = options.pretty ?? false
    this._indent = options.indent ?? 2
    this.pad = options.pad ?? 0
    this.leadingComma = false
    this.maxTextWidth = 80
    this.version = options.version ?? [
      Number.POSITIVE_INFINITY,
      Number.POSITIVE_INFINITY,
      Number.POSITIVE_INFINITY,
    ]

    // Merge static TRANSFORMS with instance
    this.transforms = new Map([
      ...(this.constructor as typeof Generator).TRANSFORMS,
    ])

    // Merge features from class (for dialect inheritance)
    this.features = { ...(this.constructor as typeof Generator).FEATURES }

    const ctor = this.constructor as typeof Generator
    this._escapedQuoteEnd = ctor.STRING_ESCAPES[0] + "'"
  }

  generate(expression: exp.Expression, copy = true): string {
    if (copy) {
      expression = expression.copy()
    }
    expression = this.preprocess(expression)
    return this.sql(expression)
  }

  preprocess(expression: exp.Expression): exp.Expression {
    expression = this.moveCTEsToTopLevelImpl(expression)

    if (this.features.ENSURE_BOOLS) {
      expression = ensureBools(expression)
    }

    return expression
  }

  protected moveCTEsToTopLevelImpl<E extends exp.Expression>(expression: E): E {
    const expressionType = expression.constructor as ExpressionClass
    const generatorClass = this.constructor as typeof Generator

    if (
      !expression.parent &&
      generatorClass.EXPRESSIONS_WITHOUT_NESTED_CTES.has(expressionType) &&
      expression.findAll(exp.With).some((node) => node.parent !== expression)
    ) {
      expression = moveCTEsToTopLevel(expression)
    }

    return expression
  }

  unsupported(message: string): void {
    if (this.options.unsupportedLevel === "RAISE") {
      const err = new Error(message)
      err.name = "UnsupportedError"
      throw err
    }
    if (this.options.unsupportedLevel !== "IGNORE") {
      indexOffsetLogs.push(`WARNING:sqlglot:${message}`)
    }
  }

  protected maybeComment(sql: string, expression: exp.Expression): string {
    const comments = expression.comments
    if (!comments || comments.length === 0) return sql

    const commentsSql = comments
      .filter((c) => c)
      .map((c) => `/*${c}*/`)
      .join(" ")

    if (!commentsSql) return sql

    return `${sql} ${commentsSql}`
  }

  sql(expression: ArgValue): string {
    if (expression == null) return ""
    if (typeof expression === "string") return expression
    if (typeof expression === "number" || typeof expression === "boolean")
      return String(expression)
    if (Array.isArray(expression)) return ""

    // 1. Check TRANSFORMS by constructor (class as key) - like Python
    const transform = this.transforms.get(
      expression.constructor as ExpressionClass,
    )

    let result: string
    if (transform) {
      result = transform(this, expression)
    } else {
      // 2. Method dispatch by key (existing - keep for compatibility)
      const exprKey = expression.key
      const methodName = `${exprKey}_sql`
      const method = (this as Record<string, unknown>)[methodName]
      if (typeof method === "function") {
        result = (method as (e: exp.Expression) => string).call(
          this,
          expression,
        )
      } else if (expression instanceof exp.Func) {
        // 3. Fallback for Func
        result = this.function_fallback_sql(expression)
      } else if (
        expression instanceof exp.Binary &&
        "sqlNames" in
          (expression.constructor as unknown as Record<string, unknown>)
      ) {
        // 3b. Fallback for Binary that acts like Func (e.g., ArrayContains extends Binary+Func in Python)
        result = this.function_fallback_sql(expression as exp.Func)
      } else if (expression instanceof exp.Property) {
        // 4. Fallback for Property
        result = this.property_sql(expression)
      } else {
        // 5. Generic fallback (Python raises ValueError here)
        if (typeof process !== "undefined") {
          console.error(
            `[sqlglot-ts] No handler for expression type: ${expression.key}`,
          )
        }
        result = this.expression_sql(expression)
      }
    }

    return this.maybeComment(result, expression)
  }

  // ==================== Core Expression Types ====================

  protected identifier_sql(expression: exp.Identifier): string {
    const name = expression.name
    if (expression.quoted) {
      return this.quoteIdentifier(name)
    }
    // Quote if needed (reserved words, special chars)
    if (this.shouldQuote(name)) {
      return this.quoteIdentifier(name)
    }
    return name
  }

  protected literal_sql(expression: exp.Literal): string {
    if (expression.isString) {
      const text = expression.text("this")
      return `'${this.escape_str(text)}'`
    }
    return expression.text("this")
  }

  // Escape strings: e'...' (Postgres/DuckDB), b'...' (BigQuery)
  // Re-encodes special characters as escape sequences for output
  protected bytestring_sql(expression: exp.ByteString): string {
    const ctor = this.constructor as typeof Generator
    const inner = expression.args.this as exp.Expression
    const value =
      inner instanceof exp.Literal ? inner.text("this") : this.sql(inner)
    // Re-encode escape sequences for output (reverse of tokenizer decoding)
    const escaped = value
      .replace(/\\/g, "\\\\") // backslash must be first
      .replace(/'/g, "''") // single quote doubled
      .replace(/\n/g, "\\n")
      .replace(/\t/g, "\\t")
      .replace(/\r/g, "\\r")

    const byteStart = ctor.BYTE_START
    const byteEnd = ctor.BYTE_END
    if (byteStart) {
      const delimited = `${byteStart}${escaped}${byteEnd || ""}`
      const isBytes = !!expression.args.is_bytes

      // Source dialect treats byte strings as BYTES type, target doesn't → cast to BINARY
      if (isBytes && !ctor.BYTE_STRING_IS_BYTES_TYPE) {
        const typeSql = this.datatype_sql(new exp.DataType({ this: "BINARY" }))
        return `CAST(${delimited} AS ${typeSql})`
      }
      // Source dialect treats byte strings as TEXT, target treats as BYTES → cast to VARCHAR
      if (!isBytes && ctor.BYTE_STRING_IS_BYTES_TYPE) {
        const typeSql = this.datatype_sql(new exp.DataType({ this: "VARCHAR" }))
        return `CAST(${delimited} AS ${typeSql})`
      }
      return delimited
    }
    return `e'${escaped}'`
  }

  // National strings: N'...' (Unicode strings)
  protected national_sql(expression: exp.National): string {
    const inner = expression.args.this as exp.Expression
    const value =
      inner instanceof exp.Literal ? inner.text("this") : this.sql(inner)
    return `N'${value.replace(/'/g, "''")}'`
  }

  // Bit strings: B'101010' or 0b101010
  protected bitstring_sql(expression: exp.BitString): string {
    const inner = expression.args.this as exp.Expression
    const value =
      inner instanceof exp.Literal ? inner.text("this") : this.sql(inner)

    // If dialect has BIT_START, use that format, otherwise convert to decimal
    const bitStart = (this.constructor as typeof Generator).BIT_START
    const bitEnd = (this.constructor as typeof Generator).BIT_END
    if (bitStart) {
      return `${bitStart}${value}${bitEnd || ""}`
    }
    return String(Number.parseInt(value, 2))
  }

  // Hex strings: X'DEADBEEF' or 0xDEADBEEF
  hexstring_sql(
    expression: exp.HexString,
    binaryFunctionRepr?: string,
  ): string {
    const inner = expression.args.this as exp.Expression
    const hexDigits =
      inner instanceof exp.Literal ? inner.text("this") : this.sql(inner)
    const isIntegerType = expression.args.is_integer
    const ctor = this.constructor as typeof Generator

    if (
      (isIntegerType && !ctor.HEX_STRING_IS_INTEGER_TYPE) ||
      (!ctor.HEX_START && !binaryFunctionRepr)
    ) {
      return `${Number.parseInt(hexDigits, 16)}`
    }

    if (!isIntegerType) {
      if (binaryFunctionRepr) {
        return this.funcCall(binaryFunctionRepr, [
          exp.Literal.string(hexDigits),
        ])
      }
      if (ctor.HEX_STRING_IS_INTEGER_TYPE) {
        this.unsupported(
          "Unsupported transpilation from BINARY/BLOB hex string",
        )
      }
    }

    return `${ctor.HEX_START}${hexDigits}${ctor.HEX_END}`
  }

  protected var_sql(expression: exp.Var): string {
    return expression.name
  }

  protected null_sql(_expression: exp.Null): string {
    return "NULL"
  }

  protected boolean_sql(expression: exp.Boolean): string {
    return expression.value ? "TRUE" : "FALSE"
  }

  protected booland_sql(expression: exp.Booland): string {
    return `((${this.sql(expression.args.this)}) AND (${this.sql(expression.args.expression)}))`
  }

  protected boolor_sql(expression: exp.Boolor): string {
    return `((${this.sql(expression.args.this)}) OR (${this.sql(expression.args.expression)}))`
  }

  // ==================== Column and Table ====================

  protected column_sql(expression: exp.Column): string {
    const parts: string[] = []

    if (expression.catalog) {
      parts.push(this.sql(expression.args.catalog))
    }
    if (expression.db) {
      parts.push(this.sql(expression.args.db))
    }
    if (expression.table) {
      parts.push(this.sql(expression.args.table))
    }

    const col = expression.args.this
    if (col instanceof exp.Star) {
      parts.push("*")
    } else {
      parts.push(this.sql(col))
    }

    return parts.join(".")
  }

  protected pseudocolumn_sql(expression: exp.Pseudocolumn): string {
    return this.column_sql(expression)
  }

  protected table_sql(expression: exp.Table, sep = " AS "): string {
    const parts: string[] = []

    if (expression.catalog) {
      parts.push(this.sql(expression.args.catalog))
    }
    if (expression.db) {
      parts.push(this.sql(expression.args.db))
    }
    parts.push(this.sql(expression.args.this))

    let result = parts.join(".")

    // Handle table with column list: TBL(col1, col2)
    const tableExprs = expression.expressions
    if (tableExprs.length > 0) {
      result += `(${this.expressions(tableExprs)})`
    }

    // Generate table-level TABLESAMPLE (pre-alias if ALIAS_POST_TABLESAMPLE)
    const sample = expression.args.sample
    const sampleSql =
      sample instanceof exp.TableSample ? this.tablesample_sql(sample) : ""
    if (this.ALIAS_POST_TABLESAMPLE && sampleSql) {
      result += sampleSql
    }

    const when = expression.args.when
    if (when instanceof exp.Expression) {
      result += ` ${this.sql(when)}`
    }

    // Generate alias (e.g., Table.alias = TableAlias)
    const alias = expression.args.alias
    if (alias instanceof exp.Expression) {
      result += `${sep}${this.sql(alias)}`
    }

    if (!this.ALIAS_POST_TABLESAMPLE && sampleSql) {
      result += sampleSql
    }

    // Generate PIVOT/UNPIVOT clauses
    const pivots = expression.args.pivots
    if (Array.isArray(pivots)) {
      for (const pivot of pivots) {
        result += ` ${this.sql(pivot)}`
      }
    }

    return result
  }

  protected alias_sql(expression: exp.Alias): string {
    const inner = expression.args.this as exp.Expression
    let sampleSql = ""
    if (
      !this.ALIAS_POST_TABLESAMPLE &&
      inner instanceof exp.Subquery &&
      inner.args.sample instanceof exp.TableSample
    ) {
      sampleSql = this.tablesample_sql(inner.args.sample)
      inner.set("sample", undefined)
    }
    const expr = this.sql(inner)
    const alias = this.sql(expression.args.alias)
    const aliasSql = alias ? ` AS ${alias}` : ""
    return `${expr}${aliasSql}${sampleSql}`
  }

  protected star_sql(expression: exp.Star): string {
    const except_ = expression.args.except_ as exp.Expression[] | undefined
    const replace = expression.args.replace as exp.Expression[] | undefined
    let result = "*"
    if (except_?.length) {
      const keyword = this.STAR_EXCEPT
      result += ` ${keyword} (${this.expressions(except_)})`
    }
    if (replace?.length) {
      result += ` REPLACE (${this.expressions(replace)})`
    }
    return result
  }

  protected STAR_EXCEPT = "EXCEPT"

  // ==================== SELECT Statement ====================

  protected AFTER_HAVING_MODIFIER_TRANSFORMS: Record<
    string,
    (gen: Generator, e: exp.Expression) => string
  > = {
    cluster: (gen, e) => {
      const cluster = e.args.cluster
      return cluster ? ` ${gen.sql(cluster)}` : ""
    },
    distribute: (gen, e) => {
      const distribute = e.args.distribute
      return distribute ? ` ${gen.sql(distribute)}` : ""
    },
    sort: (gen, e) => {
      const sort = e.args.sort
      return sort ? ` ${gen.sql(sort)}` : ""
    },
    windows: (gen, e) => {
      const windows = e.args.windows
      if (Array.isArray(windows) && windows.length > 0) {
        return ` WINDOW ${windows.map((w: exp.Expression) => gen.sql(w)).join(", ")}`
      }
      return ""
    },
    qualify: (gen, e) => {
      const qualify = e.args.qualify
      return qualify ? ` ${gen.sql(qualify)}` : ""
    },
  }

  protected queryModifiers(
    expression: exp.Expression,
    ...sqls: string[]
  ): string {
    const parts: string[] = []
    for (const s of sqls) {
      if (s) parts.push(s)
    }

    // JOINs
    const joins = expression.args.joins
    if (Array.isArray(joins)) {
      for (const join of joins) {
        const joinSql = this.sql(join)
        if (joinSql.startsWith(",")) {
          parts.push(parts.length > 0 ? parts.pop() + joinSql : joinSql)
        } else {
          parts.push(joinSql)
        }
      }
    }

    // LATERALs
    const laterals = expression.args.laterals
    if (Array.isArray(laterals)) {
      for (const lateral of laterals) {
        parts.push(this.sql(lateral))
      }
    }

    // MATCH_RECOGNIZE
    const matchExpr = expression.args.match
    if (matchExpr) {
      const matchSql = this.sql(matchExpr)
      if (matchSql) parts.push(matchSql)
    }

    // PREWHERE (ClickHouse)
    const prewhere = expression.args.prewhere
    if (prewhere) {
      const prewhereSql = this.sql(prewhere)
      if (prewhereSql) parts.push(prewhereSql)
    }

    // WHERE
    const where = expression.args.where
    if (where) parts.push(this.sql(where))

    // GROUP BY
    const group = expression.args.group
    if (group) parts.push(this.sql(group))

    // HAVING
    const having = expression.args.having
    if (having) parts.push(this.sql(having))

    // AFTER_HAVING modifiers (WINDOW defs, QUALIFY, etc.)
    for (const gen of Object.values(this.AFTER_HAVING_MODIFIER_TRANSFORMS)) {
      const s = gen(this, expression)
      if (s) parts.push(s.trimStart())
    }

    // ORDER BY
    const order = expression.args.order
    if (order) parts.push(this.sql(order))

    // LIMIT / FETCH conversion
    let limit = expression.args.limit as exp.Expression | undefined
    const isFetch = limit instanceof exp.Fetch
    if (this.features.LIMIT_FETCH === "LIMIT" && isFetch) {
      limit = new exp.Limit({
        this: (limit as exp.Fetch).args.count as exp.Expression,
      })
    } else if (
      this.features.LIMIT_FETCH === "FETCH" &&
      limit instanceof exp.Limit
    ) {
      limit = new exp.Fetch({
        direction: "FIRST",
        count: limit.args.this as exp.Expression,
      })
    }

    // OFFSET and LIMIT/FETCH (order depends on whether it's FETCH)
    const offsetLimitParts = this.offsetLimitModifiers(
      expression,
      isFetch,
      limit,
    )
    for (const part of offsetLimitParts) {
      if (part) parts.push(part)
    }

    // LOCKS (FOR UPDATE / FOR SHARE)
    const locks = expression.args.locks
    if (Array.isArray(locks)) {
      for (const lock of locks) {
        const lockSql = this.sql(lock)
        if (lockSql) parts.push(lockSql)
      }
    }

    // SAMPLE (after locks, matching Python's after_limit_modifiers)
    const sampleSql = this.sql(expression.args.sample)
    if (sampleSql) parts.push(sampleSql.trimStart())

    // OPTIONS (TSQL OPTION clause)
    const optionsMod = this.optionsModifier(expression)
    if (optionsMod) parts.push(optionsMod.trimStart())

    // FOR XML (TSQL FOR XML clause)
    const forMod = this.forModifiers(expression)
    if (forMod) parts.push(forMod.trimStart())

    return parts.join(this.pretty ? "\n" : " ")
  }

  select_sql(expression: exp.Select): string {
    // INTO handling
    const into = expression.args.into
    if (!this.features.SUPPORTS_SELECT_INTO && into) {
      expression.args.into = undefined
    }

    // WITH clause
    const withSql =
      expression.args.with_ instanceof exp.With
        ? this.sql(expression.args.with_) + " "
        : ""

    // Hint
    const hint = this.sql(expression.args.hint)

    // SELECT [hint] [AS STRUCT/VALUE] [DISTINCT [ON (...)]] expressions
    const kind = expression.args.kind
      ? ` AS ${expression.args.kind as string}`
      : ""
    const distinct = expression.args.distinct
      ? ` ${this.sql(expression.args.distinct)}`
      : ""
    const exprs = expression.expressions
    const rawExprsSql = exprs.length > 0 ? this.expressions(exprs) : "*"
    const exprsSql = this.pretty ? `\n  ${rawExprsSql}` : ` ${rawExprsSql}`

    // INTO
    const intoSql = expression.args.into
      ? ` ${this.sql(expression.args.into)}`
      : ""

    // FROM
    const from = expression.args.from_
    const fromSql = from ? ` ${this.sql(from)}` : ""

    return (
      withSql +
      this.queryModifiers(
        expression,
        `SELECT${hint}${kind}${distinct}${exprsSql}${intoSql}`,
        fromSql.trimStart(),
      )
    )
  }

  protected from_sql(expression: exp.From): string {
    return `FROM ${this.sql(expression.args.this)}`
  }

  protected where_sql(expression: exp.Where): string {
    return `WHERE ${this.sql(expression.args.this)}`
  }

  protected group_sql(expression: exp.Group): string {
    const parts: string[] = []
    const withParts: string[] = []
    const exprs = expression.expressions
    if (exprs.length > 0) {
      parts.push(this.expressions(exprs))
    }
    const groupingSets = expression.args.grouping_sets
    if (Array.isArray(groupingSets)) {
      parts.push(...groupingSets.map((gs: exp.Expression) => this.sql(gs)))
    }
    const cube = expression.args.cube
    if (Array.isArray(cube)) {
      for (const c of cube) {
        const sql = this.sql(c)
        if (sql.startsWith("WITH ")) {
          withParts.push(sql)
        } else {
          parts.push(sql)
        }
      }
    }
    const rollup = expression.args.rollup
    if (Array.isArray(rollup)) {
      for (const r of rollup) {
        const sql = this.sql(r)
        if (sql.startsWith("WITH ")) {
          withParts.push(sql)
        } else {
          parts.push(sql)
        }
      }
    }
    let result = `GROUP BY ${parts.join(", ")}`
    if (withParts.length > 0) {
      result += ` ${withParts.join(" ")}`
    }
    return result
  }

  protected cube_sql(expression: exp.Cube): string {
    const exprs = this.expressions(expression.expressions)
    return exprs ? `CUBE (${exprs})` : "WITH CUBE"
  }

  protected rollup_sql(expression: exp.Rollup): string {
    const exprs = this.expressions(expression.expressions)
    return exprs ? `ROLLUP (${exprs})` : "WITH ROLLUP"
  }

  protected groupingsets_sql(expression: exp.GroupingSets): string {
    return `GROUPING SETS (${this.expressions(expression.expressions)})`
  }

  protected having_sql(expression: exp.Having): string {
    return `HAVING ${this.sql(expression.args.this)}`
  }

  protected qualify_sql(expression: exp.Qualify): string {
    return `QUALIFY ${this.sql(expression.args.this)}`
  }

  protected into_sql(expression: exp.Into): string {
    const temporary = expression.args.temporary ? " TEMPORARY" : ""
    const unlogged = expression.args.unlogged ? " UNLOGGED" : ""
    return `INTO${temporary || unlogged} ${this.sql(expression.args.this)}`
  }

  protected prior_sql(expression: exp.Prior): string {
    return `PRIOR ${this.sql(expression.args.this)}`
  }

  protected prewhere_sql(_expression: exp.PreWhere): string {
    return ""
  }

  protected hint_sql(expression: exp.Hint): string {
    if (!this.features.QUERY_HINTS) {
      this.unsupported("Hints are not supported")
      return ""
    }
    return ` /*+ ${this.expressions(expression.expressions).trim()} */`
  }

  protected lock_sql(expression: exp.Lock): string {
    if (!this.features.LOCKING_READS_SUPPORTED) {
      return ""
    }
    const update = expression.args.update
    const key = expression.args.key
    const lockType = update
      ? key
        ? "FOR NO KEY UPDATE"
        : "FOR UPDATE"
      : key
        ? "FOR KEY SHARE"
        : "FOR SHARE"
    const exprs = this.expressions(expression.expressions)
    const exprsSql = exprs ? ` OF ${exprs}` : ""
    const wait = expression.args.wait
    let waitSql = ""
    if (wait === true) waitSql = " NOWAIT"
    else if (
      wait !== undefined &&
      wait !== false &&
      wait instanceof exp.Expression
    )
      waitSql = ` WAIT ${this.sql(wait)}`
    else if (wait === false) waitSql = " SKIP LOCKED"
    return `${lockType}${exprsSql}${waitSql}`
  }

  protected lateral_op(expression: exp.Lateral): string {
    const crossApply = expression.args.cross_apply
    if (crossApply === true) return "INNER JOIN LATERAL"
    if (crossApply === false) return "LEFT JOIN LATERAL"
    return "LATERAL"
  }

  protected lateral_sql(expression: exp.Lateral): string {
    const thisExpr = this.sql(expression.args.this)
    if (expression.args.view) {
      const alias = expression.args.alias as exp.TableAlias
      const columns = alias
        ? this.expressions((alias.args.columns as exp.Expression[]) || [])
        : ""
      const table = alias?.args.this ? ` ${this.sql(alias.args.this)}` : ""
      const columnsSql = columns ? ` AS ${columns}` : ""
      const outer = expression.args.outer ? " OUTER" : ""
      return `LATERAL VIEW${outer} ${thisExpr}${table}${columnsSql}`
    }
    const alias = this.sql(expression.args.alias)
    const aliasSql = alias ? ` AS ${alias}` : ""
    const ordinality = expression.args.ordinality
    if (ordinality) {
      return `${this.lateral_op(expression)} ${thisExpr} WITH ORDINALITY${aliasSql}`
    }
    return `${this.lateral_op(expression)} ${thisExpr}${aliasSql}`
  }

  protected order_sql(expression: exp.Order): string {
    const thisExpr = this.sql(expression.args.this)
    const thisPrefix = thisExpr ? `${thisExpr} ` : ""
    const siblings = expression.args.siblings ? "SIBLINGS " : ""
    const exprs = expression.expressions
    return `${thisPrefix}ORDER ${siblings}BY ${this.expressions(exprs)}`
  }

  protected ordered_sql(expression: exp.Ordered): string {
    const desc = expression.args.desc
    const asc = !desc

    const nullsFirst = expression.args.nulls_first
    const nullsLast = nullsFirst === undefined ? undefined : !nullsFirst
    const nullOrdering = (this.constructor as typeof Generator).NULL_ORDERING
    const nullsAreLarge = nullOrdering === "nulls_are_large"
    const nullsAreSmall = nullOrdering === "nulls_are_small"
    const nullsAreLast = nullOrdering === "nulls_are_last"

    let thisSql = this.sql(expression.args.this)

    const sortOrder = desc ? " DESC" : desc === false ? " ASC" : ""
    let nullsSortChange = ""
    if (
      nullsFirst &&
      ((asc && nullsAreLarge) || (desc && nullsAreSmall) || nullsAreLast)
    ) {
      nullsSortChange = " NULLS FIRST"
    } else if (
      nullsLast &&
      ((asc && nullsAreSmall) || (desc && nullsAreLarge)) &&
      !nullsAreLast
    ) {
      nullsSortChange = " NULLS LAST"
    }

    // If the NULLS FIRST/LAST clause is unsupported, simulate it
    if (nullsSortChange && !this.features.NULL_ORDERING_SUPPORTED) {
      const window = expression.findAncestor<exp.Expression>(
        exp.Window,
        exp.Select,
      )
      if (window instanceof exp.Window && window.args.spec) {
        this.unsupported(
          `'${nullsSortChange.trim()}' translation not supported in window functions`,
        )
        nullsSortChange = ""
      } else if (
        this.features.NULL_ORDERING_SUPPORTED === false &&
        ((asc && nullsSortChange === " NULLS LAST") ||
          (desc && nullsSortChange === " NULLS FIRST"))
      ) {
        const ancestor = expression.findAncestor<exp.Expression>(
          exp.AggFunc,
          exp.Window,
          exp.Select,
        )
        let effectiveAncestor = ancestor
        if (effectiveAncestor instanceof exp.Window) {
          effectiveAncestor = effectiveAncestor.args.this as
            | exp.Expression
            | undefined
        }
        if (effectiveAncestor instanceof exp.AggFunc) {
          this.unsupported(
            `'${nullsSortChange.trim()}' translation not supported for aggregate functions with${sortOrder} sort order`,
          )
          nullsSortChange = ""
        }
      } else if (this.features.NULL_ORDERING_SUPPORTED === null) {
        const thisExpr = expression.args.this as exp.Expression
        if (
          thisExpr instanceof exp.Literal &&
          thisExpr.args.is_string === false
        ) {
          this.unsupported(
            `'${nullsSortChange.trim()}' translation not supported with positional ordering`,
          )
        } else if (!(thisExpr instanceof exp.Rand)) {
          const nullSortOrder =
            nullsSortChange === " NULLS FIRST" ? " DESC" : ""
          thisSql = `CASE WHEN ${thisSql} IS NULL THEN 1 ELSE 0 END${nullSortOrder}, ${thisSql}`
        }
        nullsSortChange = ""
      }
    }

    return `${thisSql}${sortOrder}${nullsSortChange}`
  }

  protected limit_sql(expression: exp.Limit): string {
    const limitOptions = expression.args.limit_options
      ? this.sql(expression.args.limit_options)
      : ""
    return `LIMIT ${this.sql(expression.args.this)}${limitOptions}`
  }

  protected limitoptions_sql(expression: exp.LimitOptions): string {
    const percent = expression.args.percent ? " PERCENT" : ""
    const rows = expression.args.rows ? " ROWS" : ""
    let withTies = expression.args.with_ties ? " WITH TIES" : ""
    if (!withTies && rows) withTies = " ONLY"
    return `${percent}${rows}${withTies}`
  }

  protected offset_sql(expression: exp.Offset): string {
    return `OFFSET ${this.sql(expression.args.this)}`
  }

  protected fetch_sql(expression: exp.Fetch): string {
    const direction = expression.args.direction
    const dirStr = direction ? ` ${direction}` : ""
    const count = this.sql(expression.args.count)
    const countStr = count ? ` ${count}` : ""
    const limitOptions = this.sql(expression.args.limit_options)
    const limitStr = limitOptions || " ROWS ONLY"
    return `FETCH${dirStr}${countStr}${limitStr}`
  }

  protected offsetLimitModifiers(
    expression: exp.Expression,
    isFetch: boolean,
    limit: exp.Expression | undefined,
  ): string[] {
    const offsetSql = this.sql(expression.args.offset)
    const limitSql = limit ? this.sql(limit) : ""
    return isFetch ? [offsetSql, limitSql] : [limitSql, offsetSql]
  }

  protected join_sql(expression: exp.Join): string {
    const method = expression.args.method
    const side = expression.args.side
    const kind = expression.args.kind

    // Comma join (implicit cross join): no method, side, kind, on, or using
    if (
      !method &&
      !side &&
      !kind &&
      !expression.args.on &&
      !expression.args.using
    ) {
      return `, ${this.sql(expression.args.this)}`
    }

    const parts: string[] = []

    // Order: method -> side -> kind -> JOIN
    if (method) parts.push(String(method))
    const isSemiAnti = kind === "SEMI" || kind === "ANTI"
    if (side && !(isSemiAnti && !this.features.SEMI_ANTI_JOIN_WITH_SIDE))
      parts.push(String(side))
    if (kind) parts.push(String(kind))
    parts.push("JOIN")

    parts.push(this.sql(expression.args.this))

    const on = expression.args.on
    if (on) {
      parts.push("ON")
      parts.push(this.sql(on))
    }

    const using = expression.args.using
    if (using && Array.isArray(using)) {
      parts.push("USING")
      parts.push(`(${this.expressions(using)})`)
    }

    return parts.join(" ")
  }

  // ==================== Set Operations ====================

  protected union_sql(expression: exp.Union): string {
    return this.setOperation(expression, "UNION")
  }

  protected except_sql(expression: exp.Except): string {
    return this.setOperation(expression, "EXCEPT")
  }

  protected intersect_sql(expression: exp.Intersect): string {
    return this.setOperation(expression, "INTERSECT")
  }

  protected setOperation(expression: exp.SetOperation, _op?: string): string {
    const sqls: string[] = []
    const stack: (string | exp.Expression)[] = [expression]

    while (stack.length > 0) {
      const node = stack.pop()!
      if (node instanceof exp.SetOperation) {
        stack.push(node.args.expression as exp.Expression)
        const distinct = node.args.distinct
        const distinctOrAll = distinct === false ? " ALL" : ""
        const byName = node.args.by_name ? " BY NAME" : ""
        const nodeOp = node.key.toUpperCase()
        stack.push(`${nodeOp}${distinctOrAll}${byName}`)
        stack.push(node.args.this as exp.Expression)
      } else if (typeof node === "string") {
        sqls.push(node)
      } else {
        sqls.push(this.sql(node))
      }
    }

    let sql = sqls.join(" ")
    sql = this.queryModifiers(expression, sql)
    return this.prependCtes(expression, sql)
  }

  protected ceil_floor_sql(expression: exp.Ceil | exp.Floor): string {
    const func = expression.key === "ceil" ? "CEIL" : "FLOOR"
    return `${func}(${this.sql(expression.this)})`
  }

  protected subquery_sql(expression: exp.Subquery, sep = " AS "): string {
    const inner = this.sql(expression.args.this)
    let sql = `(${inner})`
    const alias = expression.args.alias
    const aliasSql = alias ? `${sep}${this.sql(alias)}` : ""
    const sample = expression.args.sample
    const sampleSql =
      sample instanceof exp.TableSample ? this.tablesample_sql(sample) : ""
    if (this.ALIAS_POST_TABLESAMPLE && sampleSql) {
      sql += `${sampleSql}${aliasSql}`
      expression.set("sample", undefined)
    } else {
      sql += aliasSql
    }
    sql = this.queryModifiers(expression, sql)
    sql = this.prependCtes(expression, sql)
    const pivots = expression.args.pivots as exp.Expression[] | undefined
    if (pivots) {
      for (const p of pivots) {
        sql += ` ${this.sql(p)}`
      }
    }
    return sql
  }

  // Table sample keyword - override in dialects
  protected TABLESAMPLE_KEYWORDS = "TABLESAMPLE"
  protected TABLESAMPLE_SEED_KEYWORD = "SEED"
  protected TABLESAMPLE_WITH_METHOD = true
  protected TABLESAMPLE_REQUIRES_PARENS = true
  protected TABLESAMPLE_SIZE_IS_ROWS = true
  protected ALIAS_POST_TABLESAMPLE = false

  protected tablesample_sql(
    expression: exp.TableSample,
    tablesampleKeyword?: string,
  ): string {
    const method = this.sql(
      expression.args.method as exp.Expression | undefined,
    )
    const methodStr = method && this.TABLESAMPLE_WITH_METHOD ? `${method} ` : ""
    const seed = this.sql(expression.args.seed as exp.Expression | undefined)
    const seedStr = seed ? ` ${this.TABLESAMPLE_SEED_KEYWORD} (${seed})` : ""

    const size = this.sql(expression.args.size as exp.Expression | undefined)
    const sizeStr =
      size && this.TABLESAMPLE_SIZE_IS_ROWS ? `${size} ROWS` : size || ""

    const percent = this.sql(
      expression.args.percent as exp.Expression | undefined,
    )
    const percentStr = percent ? `${percent} PERCENT` : ""

    const expr = `${percentStr}${sizeStr}`
    const exprStr = this.TABLESAMPLE_REQUIRES_PARENS ? `(${expr})` : expr

    return ` ${tablesampleKeyword || this.TABLESAMPLE_KEYWORDS} ${methodStr}${exprStr}${seedStr}`
  }

  protected pivot_sql(expression: exp.Pivot): string {
    const expressionsSql = this.expressions(expression.expressions)
    const direction = expression.args.unpivot ? "UNPIVOT" : "PIVOT"

    const group = this.sql(expression.args.group as exp.Expression | undefined)

    // Simplified pivot (DuckDB): has `this` set (the table)
    if (expression.args.this) {
      const thisExpr = this.sql(expression.args.this)
      if (!expressionsSql) {
        return `UNPIVOT ${thisExpr}`
      }
      const on = ` ON ${expressionsSql}`
      const using = expression.args.using as exp.Expression[] | undefined
      const usingSql = using
        ? ` USING ${using.map((u) => this.sql(u)).join(", ")}`
        : ""
      return `${direction} ${thisExpr}${on}${usingSql}${group ? ` ${group}` : ""}`
    }

    // Standard pivot: PIVOT(aggregation FOR field IN (values))
    const alias = this.sql(expression.args.alias as exp.Expression | undefined)
    const aliasSql = alias ? ` AS ${alias}` : ""

    const fields = expression.args.fields as exp.Expression[] | undefined
    const fieldsSql = fields ? fields.map((f) => this.sql(f)).join(" ") : ""

    return `${direction}(${expressionsSql} FOR ${fieldsSql}${group ? ` ${group}` : ""})${aliasSql}`
  }

  protected pivotalias_sql(expression: exp.PivotAlias): string {
    return this.alias_sql(expression)
  }

  protected aliases_sql(expression: exp.Aliases): string {
    return `${this.sql(expression.args.this)} AS (${this.expressions(expression.expressions)})`
  }

  // ==================== Binary Operations ====================

  binary_sql(expression: exp.Binary, op: string): string {
    const left = this.sql(expression.left)
    const right = this.sql(expression.right)
    return `${left} ${op} ${right}`
  }

  protected and_sql(expression: exp.And): string {
    return this.binary_sql(expression, "AND")
  }

  protected or_sql(expression: exp.Or): string {
    return this.binary_sql(expression, "OR")
  }

  protected xor_sql(expression: exp.Xor): string {
    return this.binary_sql(expression, "XOR")
  }

  protected add_sql(expression: exp.Add): string {
    return this.binary_sql(expression, "+")
  }

  protected sub_sql(expression: exp.Sub): string {
    return this.binary_sql(expression, "-")
  }

  protected mul_sql(expression: exp.Mul): string {
    return this.binary_sql(expression, "*")
  }

  protected div_sql(expression: exp.Div): string {
    const l = expression.args.this as exp.Expression
    const r = expression.args.expression as exp.Expression

    if (!this.features.SAFE_DIVISION && expression.args.safe) {
      const nullif = new exp.Nullif({
        this: r.copy(),
        expression: exp.Literal.number(0),
      })
      r.replace(nullif)
    }

    if (this.features.TYPED_DIVISION && !expression.args.typed) {
      l.replace(
        new exp.Cast({
          this: l.copy(),
          to: new exp.DataType({ this: "DOUBLE" }),
        }),
      )
    }

    return this.binary_sql(expression, "/")
  }

  protected mod_sql(expression: exp.Mod): string {
    return this.binary_sql(expression, "%")
  }

  protected eq_sql(expression: exp.EQ): string {
    return this.binary_sql(expression, "=")
  }

  protected neq_sql(expression: exp.NEQ): string {
    return this.binary_sql(expression, "<>")
  }

  protected lt_sql(expression: exp.LT): string {
    return this.binary_sql(expression, "<")
  }

  protected lte_sql(expression: exp.LTE): string {
    return this.binary_sql(expression, "<=")
  }

  protected gt_sql(expression: exp.GT): string {
    return this.binary_sql(expression, ">")
  }

  protected gte_sql(expression: exp.GTE): string {
    return this.binary_sql(expression, ">=")
  }

  protected is_sql(expression: exp.Is): string {
    return this.binary_sql(expression, "IS")
  }

  protected glob_sql(expression: exp.Glob): string {
    return this.binary_sql(expression, "GLOB")
  }

  protected like_sql(expression: exp.Like): string {
    return this._like_sql(expression, "LIKE", exp.Like)
  }

  protected ilike_sql(expression: exp.ILike): string {
    return this._like_sql(expression, "ILIKE", exp.ILike)
  }

  private _like_sql(
    expression: exp.Like | exp.ILike,
    op: string,
    expClass: typeof exp.Like | typeof exp.ILike,
  ): string {
    const rhs = expression.args.expression as exp.Expression | undefined

    if (
      (rhs instanceof exp.Any || rhs instanceof exp.All) &&
      !this.SUPPORTS_LIKE_QUANTIFIERS
    ) {
      const inner = rhs.args.this as exp.Expression
      const exprs =
        inner instanceof exp.Tuple
          ? (inner.expressions as exp.Expression[])
          : [inner]

      let likeExpr: exp.Expression = new expClass({
        this: expression.args.this,
        expression: exprs[0],
      })
      for (let i = 1; i < exprs.length; i++) {
        const next = new expClass({
          this: expression.args.this,
          expression: exprs[i],
        })
        likeExpr =
          rhs instanceof exp.Any
            ? new exp.Or({ this: likeExpr, expression: next })
            : new exp.And({ this: likeExpr, expression: next })
      }

      const parent = expression.parent
      if (
        parent &&
        !(parent instanceof (rhs instanceof exp.Any ? exp.Or : exp.And)) &&
        parent instanceof exp.Condition
      ) {
        likeExpr = new exp.Paren({ this: likeExpr })
      }

      return this.sql(likeExpr)
    }

    let sql = this.binary_sql(expression, op)
    const escapeExpr = expression.args.escape
    if (escapeExpr) {
      sql += ` ESCAPE ${this.sql(escapeExpr)}`
    }
    return sql
  }

  protected similarto_sql(expression: exp.SimilarTo): string {
    return this.binary_sql(expression, "SIMILAR TO")
  }

  protected escape_sql(expression: exp.Escape): string {
    return this.binary_sql(expression, "ESCAPE")
  }

  protected overlaps_sql(expression: exp.Overlaps): string {
    return this.binary_sql(expression, "OVERLAPS")
  }

  protected distance_sql(expression: exp.Distance): string {
    return this.binary_sql(expression, "<->")
  }

  protected dot_sql(expression: exp.Dot): string {
    const left = this.sql(expression.left)
    const right = this.sql(expression.right)
    return `${left}.${right}`
  }

  protected concat_sql(expression: exp.Concat): string {
    if (this.features.CONCAT_COALESCE && !expression.args.coalesce) {
      return this.concatToDpipe(expression)
    }

    const args = this.convertConcatArgs(expression)
    if (!this.features.SUPPORTS_SINGLE_ARG_CONCAT && args.length === 1) {
      return this.sql(args[0])
    }
    return this.funcCall("CONCAT", args)
  }

  protected concatws_sql(expression: exp.ConcatWs): string {
    const sep = expression.expressions[0]
    const args = this.convertConcatArgs(expression)
    return this.funcCall("CONCAT_WS", sep ? [sep, ...args] : args)
  }

  protected convertConcatArgs(
    expression: exp.Concat | exp.ConcatWs,
  ): exp.Expression[] {
    let args = expression.expressions.slice()
    if (expression instanceof exp.ConcatWs && args.length > 0) {
      args = args.slice(1)
    }

    if (!this.features.CONCAT_COALESCE && expression.args.coalesce) {
      const isArrayLike = (e: exp.Expression): boolean => {
        if (e instanceof exp.Array) return true
        if (e instanceof exp.Coalesce) {
          const inner = e.args.this
          return inner instanceof exp.Expression && isArrayLike(inner)
        }
        return false
      }
      args = args.map((e) => {
        if (e instanceof exp.Literal && e.isString) return e
        if (isArrayLike(e)) return e
        return new exp.Coalesce({
          this: e,
          expressions: [new exp.Literal({ this: "", is_string: true })],
        })
      })
    }

    return args
  }

  protected concatToDpipe(expression: exp.Concat): string {
    const nodes = expression.expressions
    if (nodes.length === 0) return "''"
    if (nodes.length === 1) return this.sql(nodes[0])
    const safe = expression.args.safe
    let result = nodes[0] as exp.Expression
    for (let i = 1; i < nodes.length; i++) {
      result = new exp.DPipe({ this: result, expression: nodes[i], safe })
    }
    return this.sql(result)
  }

  protected dpipe_sql(expression: exp.DPipe): string {
    return this.binary_sql(expression, "||")
  }

  protected bitwiseand_sql(expression: exp.BitwiseAnd): string {
    return this.binary_sql(expression, "&")
  }

  protected bitwiseor_sql(expression: exp.BitwiseOr): string {
    return this.binary_sql(expression, "|")
  }

  protected bitwisexor_sql(expression: exp.BitwiseXor): string {
    return this.binary_sql(expression, "^")
  }

  protected bitwiseleftshift_sql(expression: exp.BitwiseLeftShift): string {
    return this.binary_sql(expression, "<<")
  }

  protected bitwiserightshift_sql(expression: exp.BitwiseRightShift): string {
    return this.binary_sql(expression, ">>")
  }

  protected bitwisenot_sql(expression: exp.BitwiseNot): string {
    return `~${this.sql(expression.args.this)}`
  }

  // Regex LIKE operators
  protected regexplike_sql(expression: exp.RegexpLike): string {
    const flag = this.sql(expression.args.flag)
    const flagStr = flag ? `, ${flag}` : ""
    return `REGEXP_LIKE(${this.sql(expression.args.this)}, ${this.sql(expression.args.expression)}${flagStr})`
  }

  protected regexpilike_sql(expression: exp.RegexpILike): string {
    const flag = this.sql(expression.args.flag)
    const flagStr = flag ? `, ${flag}` : ""
    return `REGEXP_ILIKE(${this.sql(expression.args.this)}, ${this.sql(expression.args.expression)}${flagStr})`
  }

  // MatchAgainst (text search - MySQL MATCH...AGAINST or Postgres @@)
  protected matchagainst_sql(expression: exp.MatchAgainst): string {
    const expressions = expression.args.expressions as exp.Expression[]
    const exprs = expressions
      ? expressions.map((e) => this.sql(e)).join(", ")
      : ""
    const thisVal = this.sql(expression.args.this)
    return `MATCH_AGAINST(${thisVal}, ${exprs})`
  }

  // Default SUBSTRING output (comma-separated args)
  protected substring_sql(expression: exp.Substring): string {
    const args: string[] = [this.sql(expression.args.this)]
    if (expression.args.start) {
      args.push(this.sql(expression.args.start))
    }
    if (expression.args.length) {
      args.push(this.sql(expression.args.length))
    }
    return `SUBSTRING(${args.join(", ")})`
  }

  // ==================== Unary Operations ====================

  protected not_sql(expression: exp.Not): string {
    return `NOT ${this.sql(expression.args.this)}`
  }

  protected neg_sql(expression: exp.Neg): string {
    return `-${this.sql(expression.args.this)}`
  }

  protected paren_sql(expression: exp.Paren): string {
    return `(${this.sql(expression.args.this)})`
  }

  protected all_sql(expression: exp.All): string {
    const thisExpr = expression.args.this as exp.Expression | undefined
    const thisSql = this.sql(thisExpr)
    if (thisExpr instanceof exp.Tuple || thisExpr instanceof exp.Paren) {
      return `ALL ${thisSql}`
    }
    return `ALL (${thisSql})`
  }

  protected any_sql(expression: exp.Any): string {
    const thisExpr = expression.args.this as exp.Expression | undefined
    const thisSql = this.sql(thisExpr)
    if (thisExpr instanceof exp.Paren) {
      return `ANY${thisSql}`
    }
    return `ANY ${thisSql}`
  }

  protected exists_sql(expression: exp.Exists): string {
    return `EXISTS(${this.sql(expression.args.this)})`
  }

  // ==================== Special Predicates ====================

  protected between_sql(expression: exp.Between): string {
    const val = this.sql(expression.args.this)
    const low = this.sql(expression.args.low)
    const high = this.sql(expression.args.high)
    return `${val} BETWEEN ${low} AND ${high}`
  }

  protected in_sql(expression: exp.In): string {
    const val = this.sql(expression.args.this)

    const query = expression.args.query
    if (query) {
      return `${val} IN ${this.sql(query)}`
    }

    const unnest = expression.args.unnest
    if (unnest instanceof exp.Expression) {
      return `${val} IN ${this.inUnnestOp(unnest)}`
    }

    const field = expression.args.field
    if (field instanceof exp.Expression) {
      return `${val} IN ${this.sql(field)}`
    }

    const exprs = expression.expressions
    return `${val} IN (${this.expressions(exprs)})`
  }

  protected inUnnestOp(unnest: exp.Expression): string {
    return `(SELECT ${this.sql(unnest)})`
  }

  // ==================== Functions ====================

  protected func_sql(expression: exp.Func): string {
    const name = this.normalizeFunc(expression.name)
    const args = expression.expressions
    return this.funcCall(name, args)
  }

  protected anonymous_sql(expression: exp.Anonymous): string {
    const name = this.normalizeFunc(expression.name)
    const args = expression.expressions
    return this.funcCall(name, args)
  }

  protected abs_sql(expression: exp.Abs): string {
    return `ABS(${this.sql(expression.args.this)})`
  }

  protected space_sql(expression: exp.Space): string {
    return this.sql(
      new exp.Repeat({
        this: exp.Literal.string(" "),
        times: expression.args.this as exp.Expression,
      }),
    )
  }

  protected groupconcat_sql(expression: exp.GroupConcat): string {
    let thisExpr = expression.args.this as exp.Expression
    const separator = expression.args.separator as exp.Expression | undefined
    const separatorSql = separator ? this.sql(separator) : "','"

    let order: exp.Order | undefined
    if (
      thisExpr instanceof exp.Order &&
      thisExpr.args.this instanceof exp.Expression
    ) {
      order = thisExpr
      thisExpr = order.args.this as exp.Expression
    }

    let argsSql = this.sql(thisExpr)
    if (separatorSql) {
      argsSql += `, ${separatorSql}`
    }
    if (order) {
      const orderSql = this.sql(
        new exp.Order({ expressions: order.expressions }),
      )
      argsSql += ` ${orderSql}`
    }

    return `GROUP_CONCAT(${argsSql})`
  }

  protected array_sql(expression: exp.Array): string {
    const exprs = expression.expressions

    // For ARRAY(subquery) - expressions contains a single Select/Subquery
    if (exprs.length === 1) {
      const inner = exprs[0]
      if (inner instanceof exp.Select) {
        return `ARRAY(${this.sql(inner)})`
      }
      if (inner instanceof exp.Subquery) {
        return `ARRAY(${this.sql(inner)})`
      }
      // List comprehensions: [expr FOR x IN iter] - no ARRAY prefix
      if (inner instanceof exp.Comprehension) {
        return `[${this.sql(inner)}]`
      }
    }

    // Legacy: check args.this for older AST format
    const subquery = expression.args.this
    if (subquery instanceof exp.Subquery) {
      const inner = subquery.args.this
      return `ARRAY(${this.sql(inner)})`
    }

    // For ARRAY[...], use brackets (Postgres style)
    return `ARRAY[${this.expressions(exprs)}]`
  }

  protected arraysize_sql(expression: exp.ArraySize): string {
    let dim = expression.args.expression as exp.Expression | undefined

    // None (base): strip dim if literal 1, otherwise unsupported
    if (dim && this.ARRAY_SIZE_DIM_REQUIRED === undefined) {
      if (
        dim instanceof exp.Literal &&
        !dim.isString &&
        String(dim.args.this) === "1"
      ) {
        dim = undefined
      } else {
        this.unsupported("Cannot transpile dimension argument for ARRAY_LENGTH")
        dim = undefined
      }
    }

    // True (Postgres): add dim=1 if missing
    if (this.ARRAY_SIZE_DIM_REQUIRED && !dim) {
      dim = new exp.Literal({ this: "1", is_string: false })
    }

    // False (DuckDB): pass dim through as-is (no modification)

    return dim
      ? this.funcCall(this.ARRAY_SIZE_NAME, [
          expression.args.this as exp.Expression,
          dim,
        ])
      : this.funcCall(this.ARRAY_SIZE_NAME, [
          expression.args.this as exp.Expression,
        ])
  }

  protected unnest_sql(expression: exp.Unnest): string {
    const exprsArg = expression.args.expressions
    const args = Array.isArray(exprsArg)
      ? this.expressions(exprsArg)
      : exprsArg instanceof exp.Expression
        ? this.sql(exprsArg)
        : ""

    const alias = expression.args.alias as exp.Expression | undefined
    const offset = expression.args.offset

    if (this.features.UNNEST_WITH_ORDINALITY) {
      if (alias && offset instanceof exp.Expression) {
        ;(alias as exp.TableAlias).append("columns", offset)
      }
    }

    let aliasSql: string
    if (alias && this.UNNEST_COLUMN_ONLY) {
      const columns = (alias as exp.TableAlias).args.columns as
        | exp.Expression[]
        | undefined
      aliasSql = columns && columns[0] ? this.sql(columns[0]) : ""
    } else {
      aliasSql = alias ? this.sql(alias) : ""
    }
    aliasSql = aliasSql ? ` AS ${aliasSql}` : ""

    let suffix: string
    if (this.features.UNNEST_WITH_ORDINALITY) {
      suffix = offset ? ` WITH ORDINALITY${aliasSql}` : aliasSql
    } else {
      if (offset instanceof exp.Expression) {
        suffix = `${aliasSql} WITH OFFSET AS ${this.sql(offset)}`
      } else if (offset) {
        suffix = `${aliasSql} WITH OFFSET`
      } else {
        suffix = aliasSql
      }
    }

    return `UNNEST(${args})${suffix}`
  }

  protected struct_sql(expression: exp.Struct): string {
    const args = expression.expressions.map((e) => {
      if (e instanceof exp.PropertyEQ) {
        const key = e.args.this as exp.Expression
        const value = e.args.expression as exp.Expression
        const alias =
          key instanceof exp.Identifier
            ? key
            : key instanceof exp.Literal && key.isString
              ? new exp.Identifier({ this: String(key.args.this ?? "") })
              : key
        return this.sql(new exp.Alias({ this: value, alias }))
      }
      return this.sql(e)
    })
    return `STRUCT(${args.join(", ")})`
  }

  // Slice expression for array slicing: arr[0:5] or arr[::2]
  protected slice_sql(expression: exp.Slice): string {
    const begin = this.sql(expression.args.this as exp.Expression | undefined)
    const end = this.sql(
      expression.args.expression as exp.Expression | undefined,
    )
    const step = this.sql(expression.args.step as exp.Expression | undefined)

    if (step) {
      return `${begin}:${end}:${step}`
    }
    return `${begin}:${end}`
  }

  // PropertyEQ for named parameters: name := value
  protected propertyeq_sql(expression: exp.PropertyEQ): string {
    const left = this.sql(expression.args.this)
    const right = this.sql(expression.args.expression)
    return `${left} := ${right}`
  }

  protected summarize_sql(expression: exp.Summarize): string {
    const table = expression.args.table ? " TABLE" : ""
    return `SUMMARIZE${table} ${this.sql(expression.args.this)}`
  }

  protected count_sql(expression: exp.Count): string {
    const distinct = expression.args.distinct ? "DISTINCT " : ""

    const val = expression.args.this
    if (val) {
      return `COUNT(${distinct}${this.sql(val)})`
    }

    const exprs = expression.expressions
    if (exprs.length > 0) {
      return `COUNT(${distinct}${this.expressions(exprs)})`
    }

    return "COUNT(*)"
  }

  protected sum_sql(expression: exp.Sum): string {
    return this.aggFunc("SUM", expression)
  }

  protected avg_sql(expression: exp.Avg): string {
    return this.aggFunc("AVG", expression)
  }

  protected min_sql(expression: exp.Min): string {
    return this.aggFunc("MIN", expression)
  }

  protected max_sql(expression: exp.Max): string {
    return this.aggFunc("MAX", expression)
  }

  protected aggFunc(name: string, expression: exp.AggFunc): string {
    const distinct = expression.args.distinct ? "DISTINCT " : ""
    const val = this.sql(expression.args.this)
    return `${name}(${distinct}${val})`
  }

  // ==================== CASE Expression ====================

  protected case_sql(expression: exp.Case): string {
    const parts: string[] = ["CASE"]

    const subject = expression.args.this
    if (subject) {
      parts.push(this.sql(subject))
    }

    const ifs = expression.args.ifs
    if (Array.isArray(ifs)) {
      for (const if_ of ifs) {
        const ifExpr = if_ as exp.If
        const cond = this.sql(ifExpr.args.this)
        const then = this.sql(ifExpr.args.true)
        parts.push(`WHEN ${cond} THEN ${then}`)
      }
    }

    const default_ = expression.args.default
    if (default_) {
      parts.push("ELSE")
      parts.push(this.sql(default_))
    }

    parts.push("END")
    return parts.join(" ")
  }

  protected if_sql(expression: exp.If): string {
    return this.case_sql(
      new exp.Case({
        ifs: [expression],
        default: expression.args.false as exp.Expression | undefined,
      }),
    )
  }

  // ==================== Cast ====================

  protected cast_sql(expression: exp.Cast): string {
    const expr = this.sql(expression.args.this)
    const to = this.sql(expression.args.to)
    return `CAST(${expr} AS ${to})`
  }

  protected trycast_sql(expression: exp.TryCast): string {
    const expr = this.sql(expression.args.this)
    const to = this.sql(expression.args.to)
    return `TRY_CAST(${expr} AS ${to})`
  }

  protected tsordstodate_sql(expression: exp.TsOrDsToDate): string {
    const thisExpr = expression.args.this as exp.Expression
    const fmt = expression.args.format as exp.Expression | undefined
    const safe = expression.args.safe

    const STANDARD_FORMATS = new Set(["%Y-%m-%d", "%Y-%m-%d %H:%M:%S"])
    const fmtStr =
      fmt instanceof exp.Literal ? String(fmt.args.this) : undefined
    if (fmt && (!fmtStr || !STANDARD_FORMATS.has(fmtStr))) {
      return this.sql(
        new exp.Cast({
          this: new exp.StrToTime({ this: thisExpr, format: fmt, safe }),
          to: exp.DataType.build("DATE"),
        }),
      )
    }

    if (thisExpr instanceof exp.TsOrDsToDate) {
      return this.sql(thisExpr)
    }

    if (safe) {
      return this.trycast_sql(
        new exp.TryCast({ this: thisExpr, to: exp.DataType.build("DATE") }),
      )
    }

    return this.cast_sql(
      new exp.Cast({
        this: thisExpr,
        to: exp.DataType.build("DATE"),
      }),
    )
  }

  protected tsordstodatetime_sql(expression: exp.TsOrDsToDatetime): string {
    const inner = expression.args.this as exp.Expression
    if (inner instanceof exp.TsOrDsToDatetime || inner instanceof exp.Cast)
      return this.sql(inner)
    return this.cast_sql(
      new exp.Cast({
        this: inner,
        to: exp.DataType.build("DATETIME"),
      }),
    )
  }

  protected tsordstotimestamp_sql(expression: exp.TsOrDsToTimestamp): string {
    const inner = expression.args.this as exp.Expression
    if (inner instanceof exp.TsOrDsToTimestamp || inner instanceof exp.Cast)
      return this.sql(inner)
    return this.cast_sql(
      new exp.Cast({
        this: inner,
        to: exp.DataType.build("TIMESTAMP"),
      }),
    )
  }

  protected tsordstotime_sql(expression: exp.TsOrDsToTime): string {
    const inner = expression.args.this as exp.Expression
    if (inner instanceof exp.TsOrDsToTime || inner instanceof exp.Cast)
      return this.sql(inner)
    return this.cast_sql(
      new exp.Cast({
        this: inner,
        to: exp.DataType.build("TIME"),
      }),
    )
  }

  protected datatype_sql(expression: exp.DataType): string {
    const typeStr = expression.text("this")
    const mappedType =
      (this.constructor as typeof Generator).TYPE_MAPPING.get(typeStr) ||
      typeStr
    const exprs = expression.expressions
    const nested = expression.args.nested
    const values = expression.args.values

    let interior = ""
    if (exprs.length > 0) {
      // For STRUCT/ROW types, expressions are ColumnDef - output as "name TYPE"
      if (
        typeStr === "STRUCT" ||
        typeStr === "OBJECT" ||
        typeStr === "ROW" ||
        typeStr === "UNION"
      ) {
        interior = exprs
          .map((e) => {
            if (e instanceof exp.ColumnDef) {
              const name = this.sql(e.args.this)
              const kind = this.sql(e.args.kind)
              return `${name} ${kind}`
            }
            return this.sql(e)
          })
          .join(", ")
      } else {
        interior = this.expressions(exprs)
      }
    }

    let result = mappedType
    if (interior) {
      if (nested) {
        result = `${result}${this.STRUCT_DELIMITER[0]}${interior}${this.STRUCT_DELIMITER[1]}`
        if (Array.isArray(values) && values.length > 0) {
          result = `${result}[${this.expressions(values)}]`
        }
      } else {
        // For types with size/precision: VARCHAR(100), STRUCT(col TYPE)
        result = `${result}(${interior})`
      }
    }

    return result
  }

  protected pseudotype_sql(expression: exp.PseudoType): string {
    return expression.name
  }

  protected datatypesize_sql(expression: exp.DataTypeSize): string {
    const size = expression.text("this")
    const max = expression.args.max
    if (max) {
      return "MAX"
    }
    return size
  }

  protected datatypeparam_sql(expression: exp.DataTypeParam): string {
    return this.sql(expression.args.this)
  }

  // ==================== JSON Operators ====================

  protected jsonextract_sql(expression: exp.JSONExtract): string {
    return this.funcCall("JSON_EXTRACT", [
      expression.args.this as exp.Expression,
      expression.args.expression as exp.Expression,
    ])
  }

  protected jsonextractscalar_sql(expression: exp.JSONExtractScalar): string {
    return this.funcCall("JSON_EXTRACT_SCALAR", [
      expression.args.this as exp.Expression,
      expression.args.expression as exp.Expression,
    ])
  }

  // ==================== Window Functions ====================

  protected window_sql(expression: exp.Window): string {
    const func = this.sql(expression.args.this)
    const alias = expression.args.alias ? this.sql(expression.args.alias) : ""

    const parts: string[] = []

    const partitionBy = expression.args.partition_by
    if (Array.isArray(partitionBy) && partitionBy.length > 0) {
      parts.push(`PARTITION BY ${this.expressions(partitionBy)}`)
    }

    const order = expression.args.order
    if (order) {
      parts.push(this.sql(order))
    }

    const spec = expression.args.spec
    if (spec) {
      parts.push(this.sql(spec))
    }

    const windowSpec = `(${parts.join(" ")})`

    // Named window reference: func OVER name (no spec)
    if (alias && parts.length === 0) {
      return `${func} OVER ${alias}`
    }

    // Named window definition: name AS (spec) — used in WINDOW clause
    if (expression.args.this instanceof exp.Identifier) {
      return `${func} AS ${windowSpec}`
    }

    return `${func} OVER ${windowSpec}`
  }

  protected windowspec_sql(expression: exp.WindowSpec): string {
    const kind = expression.args.kind ?? "ROWS"
    const start = expression.args.start
    const end = expression.args.end

    const formatBound = (bound: exp.ArgValue, side: exp.ArgValue): string => {
      if (bound === "CURRENT ROW") return "CURRENT ROW"
      if (bound === "UNBOUNDED") return `UNBOUNDED ${side}`
      if (bound instanceof exp.Expression) {
        return `${this.sql(bound)} ${side}`
      }
      return String(bound)
    }

    if (end) {
      const startStr = formatBound(start, expression.args.start_side)
      const endStr = formatBound(end, expression.args.end_side)
      return `${kind} BETWEEN ${startStr} AND ${endStr}`
    }

    const startStr = formatBound(start, expression.args.start_side)
    return `${kind} ${startStr}`
  }

  protected ignorenulls_sql(expression: exp.IgnoreNulls): string {
    const inner = this.sql(expression.args.this)
    if (this.features.IGNORE_NULLS_IN_FUNC) {
      return this.embedIgnoreNulls(expression, "IGNORE NULLS")
    }
    return `${inner} IGNORE NULLS`
  }

  protected respectnulls_sql(expression: exp.RespectNulls): string {
    const inner = this.sql(expression.args.this)
    if (this.features.IGNORE_NULLS_IN_FUNC) {
      return this.embedIgnoreNulls(expression, "RESPECT NULLS")
    }
    return `${inner} RESPECT NULLS`
  }

  protected embedIgnoreNulls(
    expression: exp.IgnoreNulls | exp.RespectNulls,
    text: string,
  ): string {
    const inner = expression.args.this as exp.Expression
    const innerSql = this.sql(inner)
    if (inner instanceof exp.AggFunc || inner instanceof exp.Anonymous) {
      const parenIdx = innerSql.lastIndexOf(")")
      if (parenIdx > 0) {
        return `${innerSql.slice(0, parenIdx)} ${text})`
      }
    }
    return `${innerSql} ${text}`
  }

  protected percentilecont_sql(expression: exp.PercentileCont): string {
    return this._percentile_sql("PERCENTILE_CONT", expression)
  }

  protected percentiledisc_sql(expression: exp.PercentileDisc): string {
    return this._percentile_sql("PERCENTILE_DISC", expression)
  }

  private _percentile_sql(
    name: string,
    expression: exp.PercentileCont | exp.PercentileDisc,
  ): string {
    const thisExpr = expression.args.this as exp.Expression
    const quantile = expression.args.expression as exp.Expression | undefined
    if (quantile) {
      const order = new exp.Order({
        expressions: [new exp.Ordered({ this: thisExpr })],
      })
      const func =
        name === "PERCENTILE_CONT"
          ? new exp.PercentileCont({ this: quantile })
          : new exp.PercentileDisc({ this: quantile })
      const withinGroup = new exp.WithinGroup({
        this: func,
        expression: order,
      })
      return this.withingroup_sql(withinGroup)
    }
    return this.funcCall(name, [thisExpr])
  }

  protected withingroup_sql(expression: exp.WithinGroup): string {
    const func = this.sql(expression.args.this)
    const order = expression.args.expression
    if (order) {
      return `${func} WITHIN GROUP (${this.sql(order)})`
    }
    return `${func} WITHIN GROUP ()`
  }

  protected filter_sql(expression: exp.Filter): string {
    if (this.features.AGGREGATE_FILTER_SUPPORTED) {
      const func = this.sql(expression.args.this)
      const whereExpr = expression.args.expression as exp.Expression
      const where = this.sql(whereExpr).trim()
      return `${func} FILTER(${where})`
    }

    const agg = expression.args.this as exp.Expression
    const aggArg = agg.args.this as exp.Expression
    const whereExpr = expression.args.expression as exp.Where
    const cond = whereExpr.args.this as exp.Expression
    agg.args.this = new exp.If({ this: cond.copy(), true: aggArg.copy() })
    return this.sql(agg)
  }

  protected lambda_sql(expression: exp.Lambda): string {
    const params = expression.expressions
    const body = this.sql(expression.args.this)
    const useColon = expression.args.colon

    if (useColon) {
      // DuckDB syntax: LAMBDA x : body or LAMBDA x, y : body
      return `LAMBDA ${this.expressions(params)} : ${body}`
    }

    // Arrow syntax: x -> body or (x, y) -> body
    if (params.length === 1) {
      return `${this.sql(params[0])} -> ${body}`
    }
    return `(${this.expressions(params)}) -> ${body}`
  }

  // List comprehension: [x FOR x IN l IF condition]
  protected comprehension_sql(expression: exp.Comprehension): string {
    const thisExpr = this.sql(expression.args.this)
    const expr = this.sql(expression.args.expression)
    const positionExpr = expression.args.position as exp.Expression | undefined
    const position = positionExpr ? `, ${this.sql(positionExpr)}` : ""
    const iterator = this.sql(expression.args.iterator)
    const conditionExpr = expression.args.condition as
      | exp.Expression
      | undefined
    const condition = conditionExpr ? ` IF ${this.sql(conditionExpr)}` : ""
    return `${thisExpr} FOR ${expr}${position} IN ${iterator}${condition}`
  }

  protected properties_sql(expression: exp.Properties): string {
    const rootProps: exp.Expression[] = []
    const withProps: exp.Expression[] = []
    for (const p of expression.expressions) {
      if (this.isRootProperty(p)) {
        rootProps.push(p)
      } else {
        withProps.push(p)
      }
    }
    const rootSql = rootProps.map((p) => this.sql(p)).join(" ")
    let withSql = ""
    if (withProps.length > 0) {
      const inner = withProps.map((p) => this.sql(p)).join(", ")
      withSql = `${this.WITH_PROPERTIES_PREFIX} (${inner})`
    }
    if (rootSql && withSql) return `${rootSql} ${withSql}`
    return rootSql || withSql
  }

  protected isRootProperty(_p: exp.Expression): boolean {
    return false
  }

  protected partitionedbyproperty_sql(
    expression: exp.PartitionedByProperty,
  ): string {
    return `PARTITIONED BY ${this.sql(expression.args.this)}`
  }

  protected fileformatproperty_sql(expression: exp.FileFormatProperty): string {
    return `USING ${this.sql(expression.args.this)}`
  }

  protected schemacommentproperty_sql(
    expression: exp.SchemaCommentProperty,
  ): string {
    return `COMMENT ${this.sql(expression.args.this)}`
  }

  protected sortkeyproperty_sql(expression: exp.SortKeyProperty): string {
    return `SORT BY ${this.sql(expression.args.this)}`
  }

  protected clusteredbyproperty_sql(
    expression: exp.ClusteredByProperty,
  ): string {
    return `CLUSTER BY (${this.expressions(expression.expressions)})`
  }

  // Sequence properties: START WITH, INCREMENT BY, etc.
  protected sequenceproperties_sql(expression: exp.SequenceProperties): string {
    const startExpr = expression.args.start as exp.Expression | undefined
    const start = startExpr ? `START WITH ${this.sql(startExpr)}` : ""
    const incrementExpr = expression.args.increment as
      | exp.Expression
      | undefined
    const increment = incrementExpr
      ? ` INCREMENT BY ${this.sql(incrementExpr)}`
      : ""
    const minvalueExpr = expression.args.minvalue as exp.Expression | undefined
    const minvalue = minvalueExpr ? ` MINVALUE ${this.sql(minvalueExpr)}` : ""
    const maxvalueExpr = expression.args.maxvalue as exp.Expression | undefined
    const maxvalue = maxvalueExpr ? ` MAXVALUE ${this.sql(maxvalueExpr)}` : ""
    const ownedExpr = expression.args.owned as exp.Expression | undefined
    const owned = ownedExpr ? ` OWNED BY ${this.sql(ownedExpr)}` : ""

    const options = expression.args.options as exp.Expression[] | undefined
    const optionsStr = options
      ? ` ${options.map((o) => this.sql(o)).join(" ")}`
      : ""

    return `${start}${increment}${minvalue}${maxvalue}${optionsStr}${owned}`
  }

  // ==================== Miscellaneous ====================

  protected bracketOffsetExpressions(
    expression: exp.Bracket,
  ): exp.Expression[] {
    const offset = this.INDEX_OFFSET - ((expression.args.offset as number) ?? 0)
    return exp.applyIndexOffset(expression.expressions, offset)
  }

  protected bracket_sql(expression: exp.Bracket): string {
    const base = this.sql(expression.args.this)
    const indices = this.bracketOffsetExpressions(expression)
    return `${base}[${this.expressions(indices)}]`
  }

  protected set_sql(expression: exp.Set): string {
    const exprs = expression.expressions
    const body = exprs.length > 0 ? ` ${this.expressions(exprs)}` : ""
    return `SET${body}`
  }

  protected setitem_sql(expression: exp.SetItem): string {
    const kind = expression.text("kind")
    const kindStr =
      !this.SET_ASSIGNMENT_REQUIRES_VARIABLE_KEYWORD && kind === "VARIABLE"
        ? ""
        : kind
          ? `${kind} `
          : ""
    const thisStr = this.sql(expression.args.this)
    return `${kindStr}${thisStr}`
  }

  protected SET_ASSIGNMENT_REQUIRES_VARIABLE_KEYWORD = false
  protected PARAMETER_TOKEN = "@"

  protected historicaldata_sql(expression: exp.HistoricalData): string {
    const thisStr = this.sql(expression.args.this)
    const kind = this.sql(expression.args.kind)
    const expr = this.sql(expression.args.expression)
    return `${thisStr} (${kind} => ${expr})`
  }

  protected string_sql(expression: exp.String): string {
    return this.sql(
      new exp.Cast({
        this: expression.args.this as exp.Expression,
        to: new exp.DataType({ this: "VARCHAR" }),
      }),
    )
  }

  protected pow_sql(expression: exp.Pow): string {
    return this.function_fallback_sql(expression)
  }

  protected columns_sql(expression: exp.Columns): string {
    const func = this.function_fallback_sql(expression)
    return expression.args.unpack ? `*${func}` : func
  }

  protected command_sql(expression: exp.Command): string {
    let result = expression.text("this")
    const embedded = expression.args.expressions as exp.Expression[] | undefined
    if (embedded) {
      result = result.replace(/\0(\d+)/g, (_match, idx) => {
        const expr = embedded[Number(idx)]
        return expr ? `(${this.sql(expr.args.this)})` : ""
      })
    }
    const withExpr = expression.args.with_
    if (withExpr instanceof exp.With) {
      result = `${this.sql(withExpr)} ${result}`
    }
    return result
  }

  protected copyparameter_sql(expression: exp.CopyParameter): string {
    const option = this.sql(expression.args.this)
    const value = this.sql(expression.args.expression)
    if (!value) return option
    return `${option} ${value}`
  }

  protected schema_sql(expression: exp.Schema): string {
    const thisExpr = this.sql(expression.args.this)
    const exprs = expression.expressions
    const columnsSql = exprs.length > 0 ? `(${this.expressions(exprs)})` : ""
    if (thisExpr && columnsSql) return `${thisExpr} ${columnsSql}`
    return thisExpr || columnsSql
  }

  protected copy_sql(expression: exp.Copy): string {
    const thisExpr = this.sql(expression.args.this)
    const thisStr = this.COPY_HAS_INTO_KEYWORD
      ? ` INTO ${thisExpr}`
      : ` ${thisExpr}`

    const files = expression.args.files as exp.Expression[] | undefined
    const filesStr = files ? files.map((f) => this.sql(f)).join(", ") : ""
    const kind = filesStr ? (expression.args.kind ? " FROM " : " TO ") : ""

    const params = expression.args.params as exp.CopyParameter[] | undefined
    const paramsStr = params
      ? ` WITH (${params.map((p) => this.sql(p)).join(", ")})`
      : ""

    return `COPY${thisStr}${kind}${filesStr}${paramsStr}`
  }

  protected COPY_HAS_INTO_KEYWORD = true
  protected WITH_PROPERTIES_PREFIX = "WITH"

  protected create_sql(expression: exp.Create): string {
    const kind = expression.args.kind as string
    const replace = expression.args.replace ? " OR REPLACE" : ""
    const exists = expression.args.exists ? " IF NOT EXISTS" : ""
    const thisExpr = this.sql(expression.args.this)

    let body = this.sql(
      expression.args.expression as exp.Expression | undefined,
    )
    if (body) {
      body = ` AS ${body}`
    }

    const props = expression.args.properties as exp.Expression | undefined
    const propsStr = props ? ` ${this.sql(props)}` : ""

    return `CREATE${replace} ${kind}${exists} ${thisExpr}${propsStr}${body}`
  }

  protected userdefinedfunction_sql(
    expression: exp.UserDefinedFunction,
  ): string {
    const name = this.sql(expression.args.this)
    const exprs = expression.expressions
    if (expression.args.wrapped && exprs.length > 0) {
      return `${name}(${this.expressions(exprs)})`
    }
    if (expression.args.wrapped) {
      return `${name}()`
    }
    return name
  }

  protected columndef_sql(expression: exp.ColumnDef): string {
    const column = this.sql(expression.args.this)
    const exists = expression.args.exists ? "IF NOT EXISTS " : ""
    let kind = this.sql(expression.args.kind)
    kind = kind ? ` ${kind}` : ""

    const constraints = this.expressions(expression, {
      key: "constraints",
      sep: " ",
      flat: true,
    })
    const constraintsSql = constraints ? ` ${constraints}` : ""

    const position = this.sql(expression.args.position)
    const positionSql = position ? ` ${position}` : ""

    if (
      expression.find(exp.ComputedColumnConstraint) &&
      !this.COMPUTED_COLUMN_WITH_TYPE
    ) {
      kind = ""
    }

    return `${exists}${column}${kind}${constraintsSql}${positionSql}`
  }

  protected notnullcolumnconstraint_sql(
    _expression: exp.NotNullColumnConstraint,
  ): string {
    return "NOT NULL"
  }

  protected primarykeycolumnconstraint_sql(
    _expression: exp.PrimaryKeyColumnConstraint,
  ): string {
    return "PRIMARY KEY"
  }

  protected uniquecolumnconstraint_sql(
    _expression: exp.UniqueColumnConstraint,
  ): string {
    return "UNIQUE"
  }

  protected placeholder_sql(_expression: exp.Placeholder): string {
    return "?"
  }

  protected parameter_sql(expression: exp.Parameter): string {
    const name = expression.text("this")
    return `${this.PARAMETER_TOKEN}${name}`
  }

  protected interval_sql(expression: exp.Interval): string {
    const unitText = expression.text("unit").toUpperCase()
    let unit = unitText ? ` ${unitText}` : ""

    if (!this.features.INTERVAL_ALLOWS_PLURAL_FORM && unit) {
      const trimmed = unit.trim()
      const singular = TIME_PART_SINGULARS.get(trimmed)
      if (singular) unit = ` ${singular}`
    }

    if (this.features.SINGLE_STRING_INTERVAL) {
      const thisExpr = expression.args.this as exp.Expression | undefined
      const name = thisExpr instanceof exp.Literal ? String(thisExpr.value) : ""
      if (name) {
        if (
          expression.args.unit &&
          expression.args.this instanceof exp.IntervalSpan
        ) {
          return `INTERVAL '${name}'${unit}`
        }
        return `INTERVAL '${name}${unit}'`
      }
      return `INTERVAL${unit}`
    }

    const thisExpr = expression.args.this as exp.Expression | undefined
    if (thisExpr) {
      const unwrapped = this.isUnwrappedIntervalValue(thisExpr)
      const val = this.sql(thisExpr)
      return unwrapped ? `INTERVAL ${val}${unit}` : `INTERVAL (${val})${unit}`
    }
    return `INTERVAL${unit}`
  }

  protected isUnwrappedIntervalValue(expr: exp.Expression): boolean {
    return (
      expr instanceof exp.Column ||
      expr instanceof exp.Literal ||
      expr instanceof exp.Neg ||
      expr instanceof exp.Paren
    )
  }

  protected attimezone_sql(expression: exp.AtTimeZone): string {
    const this_ = this.sql(expression.args.this)
    const zone = this.sql(expression.args.zone)
    return `${this_} AT TIME ZONE ${zone}`
  }

  protected converttimezone_sql(expression: exp.ConvertTimezone): string {
    const targetTz = expression.args.target_tz as exp.Expression
    const timestamp = expression.args.timestamp as exp.Expression
    return this.attimezone_sql(
      new exp.AtTimeZone({ this: timestamp, zone: targetTz }),
    )
  }

  protected extract_sql(expression: exp.Extract): string {
    const unit = expression.text("this")
    const expr = this.sql(expression.args.expression)
    return `EXTRACT(${unit} FROM ${expr})`
  }

  protected distinct_sql(expression: exp.Distinct): string {
    const exprs = expression.expressions
    const exprsSql = exprs.length > 0 ? ` ${this.expressions(exprs)}` : ""
    const on = this.sql(expression.args.on)
    const onSql = on ? ` ON ${on}` : ""
    return `DISTINCT${exprsSql}${onSql}`
  }

  protected tablealias_sql(expression: exp.TableAlias): string {
    let alias = this.sql(expression.args.this)
    const columns = expression.args.columns as exp.Expression[] | undefined
    if (columns && columns.length > 0) {
      if (!alias) {
        alias = `_t${this._nameCounter++}`
      }
      return `${alias}(${this.expressions(columns)})`
    }
    return alias
  }

  protected cte_sql(expression: exp.CTE): string {
    const alias = this.sql(expression.args.alias)
    const query = this.sql(expression.args.this)

    const keyExprs = expression.args.key_expressions as
      | exp.Expression[]
      | undefined
    const keyExprStr =
      keyExprs && keyExprs.length > 0
        ? ` USING KEY (${this.expressions(keyExprs)})`
        : ""

    const mat = expression.args.materialized
    const matStr =
      mat === false ? "NOT MATERIALIZED " : mat === true ? "MATERIALIZED " : ""

    return `${alias}${keyExprStr} AS ${matStr}(${query})`
  }

  protected tuple_sql(expression: exp.Tuple): string {
    return `(${this.expressions(expression.expressions)})`
  }

  protected queryoption_sql(_expression: exp.QueryOption): string {
    this.unsupported("Unsupported query option.")
    return ""
  }

  protected xmlkeyvalueoption_sql(expression: exp.XMLKeyValueOption): string {
    const this_ = this.sql(expression.args.this)
    const expr = this.sql(expression.args.expression)
    return `${this_}${expr ? `(${expr})` : ""}`
  }

  protected optionsModifier(expression: exp.Expression): string {
    const options = expression.args.options as exp.Expression[] | undefined
    if (!options || options.length === 0) return ""
    return ` ${this.expressions(options)}`
  }

  protected forModifiers(expression: exp.Expression): string {
    const forExprs = expression.args.for_ as exp.Expression[] | undefined
    if (!forExprs || forExprs.length === 0) return ""
    const forModifiers = this.expressions(expression, { key: "for_" })
    if (!forModifiers) return ""
    if (this.pretty) {
      return `\nFOR XML\n${this.indentSql(forModifiers, 1, 0)}`
    }
    return ` FOR XML ${forModifiers}`
  }

  protected values_sql(expression: exp.Values): string {
    const args = this.expressions(expression.expressions)
    const alias = this.sql(expression.args.alias)
    let values = `VALUES ${args}`
    if (
      this.features.WRAP_DERIVED_VALUES &&
      (alias ||
        expression.parent instanceof exp.From ||
        expression.parent instanceof exp.Table)
    ) {
      values = `(${values})`
    }
    values = this.queryModifiers(expression, values)
    return alias ? `${values} AS ${alias}` : values
  }

  protected with_sql(expression: exp.With): string {
    const recursive = expression.args.recursive ? "RECURSIVE " : ""
    const exprs = expression.expressions
    return `WITH ${recursive}${this.expressions(exprs)}`
  }

  // ==================== Config Flags ====================

  protected ARRAY_SIZE_NAME = "ARRAY_LENGTH"
  protected ARRAY_SIZE_DIM_REQUIRED: boolean | undefined = undefined
  protected UNNEST_COLUMN_ONLY = false

  protected ALTER_SET_TYPE = "SET DATA TYPE"
  protected ALTER_TABLE_INCLUDE_COLUMN_KEYWORD = true
  protected ALTER_SET_WRAPPED = false
  protected INDEX_ON = "ON"
  protected COMPUTED_COLUMN_WITH_TYPE = true

  protected HEX_FUNC = "HEX"
  protected HEX_LOWERCASE = false
  protected PAD_FILL_PATTERN_IS_REQUIRED = false
  protected TRY_SUPPORTED = true
  protected ON_CONDITION_EMPTY_BEFORE_ERROR = true
  protected LOG_BASE_FIRST: boolean | null = true
  protected QUOTE_JSON_PATH = true
  protected JSON_KEY_VALUE_PAIR_SEP = ":"
  protected JSON_PATH_SINGLE_QUOTE_ESCAPE = false
  protected JSON_PATH_BRACKETED_KEY_SUPPORTED = true

  protected alter_sql(expression: exp.Alter): string {
    const actions = expression.args.actions as exp.Expression[] | undefined
    if (!actions?.length) return ""

    let actionsSql: string
    if (
      !this.features.ALTER_TABLE_ADD_REQUIRED_FOR_EACH_COLUMN &&
      actions[0] instanceof exp.ColumnDef
    ) {
      actionsSql = `ADD ${this.expressions(expression, { key: "actions", flat: true })}`
    } else {
      const actionParts: string[] = []
      for (const action of actions) {
        if (action instanceof exp.ColumnDef || action instanceof exp.Schema) {
          actionParts.push(this.addColumnSql(action))
        } else if (action instanceof exp.Query) {
          actionParts.push(`AS ${this.sql(action)}`)
        } else {
          actionParts.push(this.sql(action))
        }
      }
      actionsSql = actionParts.join(", ")
    }

    const exists = expression.args.exists ? " IF EXISTS" : ""
    const onCluster = this.sql(expression.args.cluster)
    const onClusterSql = onCluster ? ` ${onCluster}` : ""
    const only = expression.args.only ? " ONLY" : ""
    const options = this.expressions(expression, { key: "options" })
    const optionsSql = options ? `, ${options}` : ""
    const kind = this.sql(expression.args.kind)
    const notValid = expression.args.not_valid ? " NOT VALID" : ""
    const check = expression.args.check ? " WITH CHECK" : ""
    const cascade =
      expression.args.cascade && this.features.ALTER_TABLE_SUPPORTS_CASCADE
        ? " CASCADE"
        : ""
    const thisSql = this.sql(expression.args.this)
    const thisClause = thisSql ? ` ${thisSql}` : ""

    return `ALTER ${kind}${exists}${only}${thisClause}${onClusterSql}${check} ${actionsSql}${notValid}${optionsSql}${cascade}`
  }

  protected addColumnSql(expression: exp.Expression): string {
    const sql = this.sql(expression)
    let columnText: string
    if (expression instanceof exp.Schema) {
      columnText = " COLUMNS"
    } else if (
      expression instanceof exp.ColumnDef &&
      this.ALTER_TABLE_INCLUDE_COLUMN_KEYWORD
    ) {
      columnText = " COLUMN"
    } else {
      columnText = ""
    }
    return `ADD${columnText} ${sql}`
  }

  protected alterrename_sql(expression: exp.AlterRename): string {
    let expr = expression
    if (!this.features.RENAME_TABLE_WITH_DB) {
      const table = expr.args.this
      if (table instanceof exp.Table) {
        const nameOnly = new exp.Table({ this: table.args.this })
        expr = new exp.AlterRename({ this: nameOnly })
      }
    }
    const thisSql = this.sql(expr.args.this)
    return `RENAME TO ${thisSql}`
  }

  // ==================== DML ====================

  protected RETURNING_END = true
  protected DUPLICATE_KEY_UPDATE_WITH_SET = false
  protected INSERT_OVERWRITE = " OVERWRITE TABLE"

  protected prependCtes(expression: exp.Expression, sql: string): string {
    const with_ = this.sql(expression.args.with_)
    if (with_) {
      return `${with_} ${sql}`
    }
    return sql
  }

  protected returning_sql(expression: exp.Returning): string {
    return `RETURNING ${this.expressionsFromKey(expression, "expressions")}`
  }

  protected expressionsFromKey(
    expression: exp.Expression,
    key: string,
  ): string {
    const items = expression.args[key]
    if (Array.isArray(items)) {
      const exprs = items.filter(
        (x): x is exp.Expression => x instanceof exp.Expression,
      )
      return this.expressions(exprs)
    }
    return ""
  }

  protected delete_sql(expression: exp.Delete): string {
    const this_ = this.sql(expression.args.this)
    const thisSql = this_ ? ` FROM ${this_}` : ""
    const using = this.expressionsFromKey(expression, "using")
    const usingSql = using ? ` USING ${using}` : ""
    const where = this.sql(expression.args.where)
    const whereSql = where ? ` ${where}` : ""
    const returning = this.sql(expression.args.returning)
    const returningSql = returning ? ` ${returning}` : ""
    const order = this.sql(expression.args.order)
    const orderSql = order ? ` ${order}` : ""
    const limit = this.sql(expression.args.limit)
    const limitSql = limit ? ` ${limit}` : ""
    const tables = this.expressionsFromKey(expression, "tables")
    const tablesSql = tables ? ` ${tables}` : ""

    let expressionSql: string
    if (this.RETURNING_END) {
      expressionSql = `${thisSql}${usingSql}${whereSql}${returningSql}${orderSql}${limitSql}`
    } else {
      expressionSql = `${returningSql}${thisSql}${usingSql}${whereSql}${orderSql}${limitSql}`
    }
    return this.prependCtes(expression, `DELETE${tablesSql}${expressionSql}`)
  }

  protected drop_sql(expression: exp.Drop): string {
    const this_ = this.sql(expression.args.this)
    const expressionsSql = this.expressionsFromKey(expression, "expressions")
    const exprsSql = expressionsSql ? ` (${expressionsSql})` : ""
    const kind = String(expression.args.kind ?? "")
    const exists = expression.args.exists ? " IF EXISTS " : " "
    const concurrently = expression.args.concurrently ? " CONCURRENTLY" : ""
    const temporary = expression.args.temporary ? " TEMPORARY" : ""
    const materialized = expression.args.materialized ? " MATERIALIZED" : ""
    const cascade = expression.args.cascade ? " CASCADE" : ""
    const constraints = expression.args.constraints ? " CONSTRAINTS" : ""
    const purge = expression.args.purge ? " PURGE" : ""
    return `DROP${temporary}${materialized} ${kind}${concurrently}${exists}${this_}${exprsSql}${cascade}${constraints}${purge}`
  }

  protected insert_sql(expression: exp.Insert): string {
    const overwrite = expression.args.overwrite
    const this_ = overwrite ? this.INSERT_OVERWRITE : " INTO"
    const alternative = expression.args.alternative
    const alternativeSql = alternative ? ` OR ${alternative}` : ""
    const ignore = expression.args.ignore ? " IGNORE" : ""
    const isFunction = expression.args.is_function
    const funcSql = isFunction ? " FUNCTION" : ""
    const thisSql = `${this_}${funcSql} ${this.sql(expression.args.this)}`

    const exists = expression.args.exists ? " IF EXISTS" : ""
    const byName = expression.args.by_name ? " BY NAME" : ""
    const defaultValues = expression.args.default ? " DEFAULT VALUES" : ""

    const expressionBody = this.sql(expression.args.expression)
    const expressionSql = expressionBody ? ` ${expressionBody}` : ""
    const onConflict = this.sql(expression.args.conflict)
    const onConflictSql = onConflict ? ` ${onConflict}` : ""
    const returning = this.sql(expression.args.returning)
    const returningSql = returning ? ` ${returning}` : ""

    let bodySql: string
    if (this.RETURNING_END) {
      bodySql = `${expressionSql}${onConflictSql}${defaultValues}${returningSql}`
    } else {
      bodySql = `${returningSql}${expressionSql}${onConflictSql}`
    }

    const partition = this.sql(expression.args.partition)
    const partitionSql = partition ? ` ${partition}` : ""

    return this.prependCtes(
      expression,
      `INSERT${alternativeSql}${ignore}${thisSql}${byName}${exists}${partitionSql}${bodySql}`,
    )
  }

  protected onconflict_sql(expression: exp.OnConflict): string {
    const conflict = expression.args.duplicate
      ? "ON DUPLICATE KEY"
      : "ON CONFLICT"

    const constraint = this.sql(expression.args.constraint)
    const constraintSql = constraint ? ` ON CONSTRAINT ${constraint}` : ""

    let conflictKeys = this.expressionsFromKey(expression, "conflict_keys")
    if (conflictKeys) {
      conflictKeys = `(${conflictKeys})`
    }

    const indexPredicate = this.sql(expression.args.index_predicate)
    const conflictKeysSql = `${conflictKeys}${indexPredicate} `

    const action = expression.args.action
      ? ` ${String(expression.args.action)}`
      : ""

    const expressionsSql = this.expressionsFromKey(expression, "expressions")
    const exprSql = expressionsSql
      ? ` ${this.DUPLICATE_KEY_UPDATE_WITH_SET ? "SET " : ""}${expressionsSql}`
      : ""

    const where = this.sql(expression.args.where)
    const whereSql = where ? ` ${where}` : ""

    return `${conflict}${constraintSql} ${conflictKeysSql}${action}${exprSql}${whereSql}`
  }

  protected update_sql(expression: exp.Update): string {
    const this_ = this.sql(expression.args.this)
    const setSql = this.expressionsFromKey(expression, "expressions")
    const from = this.sql(expression.args.from_)
    const fromSql = from ? ` ${from}` : ""
    const where = this.sql(expression.args.where)
    const whereSql = where ? ` ${where}` : ""
    const returning = this.sql(expression.args.returning)
    const returningSql = returning ? ` ${returning}` : ""
    const order = this.sql(expression.args.order)
    const orderSql = order ? ` ${order}` : ""
    const limit = this.sql(expression.args.limit)
    const limitSql = limit ? ` ${limit}` : ""

    let expressionSql: string
    if (this.RETURNING_END) {
      expressionSql = `${fromSql}${whereSql}${returningSql}`
    } else {
      expressionSql = `${returningSql}${fromSql}${whereSql}`
    }
    const optionsSql = this.expressionsFromKey(expression, "options")
    const options = optionsSql ? ` OPTION(${optionsSql})` : ""
    return this.prependCtes(
      expression,
      `UPDATE ${this_} SET ${setSql}${expressionSql}${orderSql}${limitSql}${options}`,
    )
  }

  // ==================== Transaction / Grant / Misc Statements ====================

  protected use_sql(expression: exp.Use): string {
    const kind = this.sql(expression.args.kind)
    const kindSql = kind ? ` ${kind}` : ""
    const this_ =
      this.sql(expression.args.this) ||
      this.expressionsFromKey(expression, "expressions")
    const thisSql = this_ ? ` ${this_}` : ""
    return `USE${kindSql}${thisSql}`
  }

  protected comment_sql(expression: exp.Comment): string {
    const this_ = this.sql(expression.args.this)
    const kind = String(expression.args.kind ?? "")
    const materialized = expression.args.materialized ? " MATERIALIZED" : ""
    const exists = expression.args.exists ? " IF EXISTS " : " "
    const expressionSql = this.sql(expression.args.expression)
    return `COMMENT${exists}ON${materialized} ${kind} ${this_} IS ${expressionSql}`
  }

  protected truncatetable_sql(expression: exp.TruncateTable): string {
    const target = expression.args.is_database ? "DATABASE" : "TABLE"
    const tables = ` ${this.expressions(expression.expressions)}`
    const exists = expression.args.exists ? " IF EXISTS" : ""
    const identity = this.sql(expression.args.identity)
    const identitySql = identity ? ` ${identity} IDENTITY` : ""
    const option = this.sql(expression.args.option)
    const optionSql = option ? ` ${option}` : ""
    return `TRUNCATE ${target}${exists}${tables}${identitySql}${optionSql}`
  }

  protected show_sql(expression: exp.Show): string {
    const name = expression.name
    const full = expression.args.full ? " FULL" : ""
    const terse = expression.args.terse ? " TERSE" : ""
    const target = this.sql(expression.args.target)
    const targetSql = target ? ` ${target}` : ""
    const like = expression.args.like
      ? ` LIKE ${this.sql(expression.args.like)}`
      : ""
    const db = expression.args.db ? ` IN ${this.sql(expression.args.db)}` : ""
    const where = expression.args.where
      ? ` ${this.sql(expression.args.where)}`
      : ""
    return `SHOW${full}${terse} ${name}${targetSql}${like}${db}${where}`
  }

  protected analyze_sql(expression: exp.Analyze): string {
    const options = this.expressions(expression, {
      key: "options",
      sep: " ",
    })
    const optionsSql = options ? ` ${options}` : ""
    const kind = this.sql(expression.args.kind)
    const kindSql = kind ? ` ${kind}` : ""
    const thisSql = this.sql(expression.args.this)
    const thisClause = thisSql ? ` ${thisSql}` : ""
    const mode = this.sql(expression.args.mode)
    const modeSql = mode ? ` ${mode}` : ""
    const properties = this.sql(expression.args.properties)
    const propertiesSql = properties ? ` ${properties}` : ""
    const partition = this.sql(expression.args.partition)
    const partitionSql = partition ? ` ${partition}` : ""
    const innerExpression = this.sql(expression.args.expression)
    const innerExpressionSql = innerExpression ? ` ${innerExpression}` : ""
    return `ANALYZE${optionsSql}${kindSql}${thisClause}${partitionSql}${modeSql}${innerExpressionSql}${propertiesSql}`
  }

  protected altercolumn_sql(expression: exp.AlterColumn): string {
    const thisSql = this.sql(expression.args.this)
    const dtype = this.sql(expression.args.dtype)
    if (dtype) {
      const collate = expression.args.collate
        ? ` COLLATE ${this.sql(expression.args.collate)}`
        : ""
      const using = expression.args.using
        ? ` USING ${this.sql(expression.args.using)}`
        : ""
      const alterSetType = this.ALTER_SET_TYPE ? `${this.ALTER_SET_TYPE} ` : ""
      return `ALTER COLUMN ${thisSql} ${alterSetType}${dtype}${collate}${using}`
    }
    const defaultVal = this.sql(expression.args.default)
    if (defaultVal) {
      return `ALTER COLUMN ${thisSql} SET DEFAULT ${defaultVal}`
    }
    const comment = this.sql(expression.args.comment)
    if (comment) {
      return `ALTER COLUMN ${thisSql} COMMENT ${comment}`
    }
    const allowNull = expression.args.allow_null
    const drop = expression.args.drop
    if (allowNull !== undefined) {
      const keyword = drop ? "DROP" : "SET"
      return `ALTER COLUMN ${thisSql} ${keyword} NOT NULL`
    }
    if (drop) {
      return `ALTER COLUMN ${thisSql} DROP DEFAULT`
    }
    return `ALTER COLUMN ${thisSql}`
  }

  protected alterset_sql(expression: exp.AlterSet): string {
    let exprs = this.expressions(expression, { flat: true })
    if (this.ALTER_SET_WRAPPED) {
      exprs = `(${exprs})`
    }
    return `SET ${exprs}`
  }

  protected transaction_sql(expression: exp.Transaction): string {
    const modes = this.expressionsFromKey(expression, "modes")
    const modesSql = modes ? ` ${modes}` : ""
    return `BEGIN${modesSql}`
  }

  protected commit_sql(expression: exp.Commit): string {
    const chain = expression.args.chain
    let chainSql = ""
    if (chain === true) chainSql = " AND CHAIN"
    else if (chain === false) chainSql = " AND NO CHAIN"
    return `COMMIT${chainSql}`
  }

  protected rollback_sql(expression: exp.Rollback): string {
    const savepoint = expression.args.savepoint
    const savepointSql = savepoint ? ` TO ${this.sql(savepoint)}` : ""
    return `ROLLBACK${savepointSql}`
  }

  protected grant_sql(expression: exp.Grant): string {
    const privileges = this.expressionsFromKey(expression, "privileges")
    const kind = expression.args.kind ? ` ${String(expression.args.kind)}` : ""
    const securable = this.sql(expression.args.securable)
    const principals = this.expressionsFromKey(expression, "principals")
    const grantOption = expression.args.grant_option ? " WITH GRANT OPTION" : ""
    return `GRANT ${privileges} ON${kind} ${securable} TO ${principals}${grantOption}`
  }

  protected merge_sql(expression: exp.Merge): string {
    const this_ = this.sql(expression.args.this)
    const using = `USING ${this.sql(expression.args.using)}`
    const on = this.sql(expression.args.on)
    let onSql = on ? `ON ${on}` : ""
    if (!onSql) {
      const usingCond = this.expressionsFromKey(expression, "using_cond")
      if (usingCond) {
        onSql = `USING (${usingCond})`
      }
    }
    const whens = this.sql(expression.args.whens)
    const returning = this.sql(expression.args.returning)
    const returningSql = returning ? ` ${returning}` : ""
    return this.prependCtes(
      expression,
      `MERGE INTO ${this_} ${using} ${onSql} ${whens}${returningSql}`,
    )
  }

  protected whens_sql(expression: exp.Whens): string {
    return expression.expressions.map((e) => this.sql(e)).join(" ")
  }

  protected MATCHED_BY_SOURCE = true

  protected when_sql(expression: exp.When): string {
    const matched = expression.args.matched ? "MATCHED" : "NOT MATCHED"
    const source =
      this.MATCHED_BY_SOURCE && expression.args.source ? " BY SOURCE" : ""
    const condition = this.sql(expression.args.condition)
    const conditionSql = condition ? ` AND ${condition}` : ""

    const thenExpr = expression.args.then as exp.Expression | undefined
    let then: string
    if (thenExpr instanceof exp.Insert) {
      const this_ = this.sql(thenExpr.args.this)
      const thisSql = this_ ? `INSERT ${this_}` : "INSERT"
      const valExpr = this.sql(thenExpr.args.expression)
      then = valExpr ? `${thisSql} VALUES ${valExpr}` : thisSql
    } else if (thenExpr instanceof exp.Update) {
      const expressionsSql = this.expressionsFromKey(thenExpr, "expressions")
      then = expressionsSql ? `UPDATE SET ${expressionsSql}` : "UPDATE"
    } else {
      then = thenExpr ? this.sql(thenExpr) : "DELETE"
    }
    return `WHEN ${matched}${source}${conditionSql} THEN ${then}`
  }

  protected semicolon_sql(_expression: exp.Semicolon): string {
    return ""
  }

  // ==================== Ported Methods ====================

  protected hex_sql(expression: exp.Hex): string {
    let text = this.funcCall(this.HEX_FUNC, [
      expression.args.this as exp.Expression,
    ])
    if (this.HEX_LOWERCASE) {
      text = `LOWER(${text})`
    }
    return text
  }

  protected lowerhex_sql(expression: exp.LowerHex): string {
    let text = this.funcCall(this.HEX_FUNC, [
      expression.args.this as exp.Expression,
    ])
    if (!this.HEX_LOWERCASE) {
      text = `LOWER(${text})`
    }
    return text
  }

  protected trim_sql(expression: exp.Trim): string {
    const trimType = expression.text("position")
    let funcName: string
    if (trimType === "LEADING") {
      funcName = "LTRIM"
    } else if (trimType === "TRAILING") {
      funcName = "RTRIM"
    } else {
      funcName = "TRIM"
    }
    const args: exp.Expression[] = [expression.args.this as exp.Expression]
    if (expression.expression) {
      args.push(expression.expression as exp.Expression)
    }
    return this.funcCall(funcName, args)
  }

  protected convert_sql(expression: exp.Convert): string {
    const to = expression.args.this as exp.DataType | undefined
    const value = expression.expression as exp.Expression | undefined
    const safe = expression.args.safe

    if (!to || !value) return ""

    const castCtor = exp.TryCast
    const transformed = new castCtor({ this: value, to, safe })
    return this.sql(transformed)
  }

  protected matchrecognizemeasure_sql(
    expression: exp.MatchRecognizeMeasure,
  ): string {
    const windowFrame = this.sql(expression.args.window_frame)
    const windowFrameSql = windowFrame ? `${windowFrame} ` : ""
    const thisSql = this.sql(expression.args.this)
    return `${windowFrameSql}${thisSql}`
  }

  protected matchrecognize_sql(expression: exp.MatchRecognize): string {
    const partition = this.partition_by_sql(expression)

    const orderExpr = expression.args.order as exp.Order | undefined
    let order = ""
    if (orderExpr) {
      const orderExprs = this.expressions(orderExpr.expressions)
      if (this.pretty) {
        order = `ORDER BY\n${this.indentSql(orderExprs, 1, 0)}`
      } else {
        order = `ORDER BY ${orderExprs}`
      }
    }

    const measuresExprs = expression.args.measures as
      | exp.Expression[]
      | undefined
    const measuresContent = measuresExprs?.length
      ? this.expressions(measuresExprs)
      : ""
    const measures = measuresContent
      ? this.pretty
        ? `MEASURES\n${this.indentSql(measuresContent, 1, 0)}`
        : `MEASURES ${measuresContent}`
      : ""

    const rowsSql = this.sql(expression.args.rows)
    const afterSql = this.sql(expression.args.after)

    const patternSql = this.sql(expression.args.pattern)
    const pattern = patternSql ? `PATTERN (${patternSql})` : ""

    const defines = expression.args.define as exp.Expression[] | undefined
    const definitionSqls = (defines ?? []).map(
      (d: exp.Expression) =>
        `${this.sql(d.args.alias)} AS ${this.sql(d.args.this)}`,
    )
    const definitions = definitionSqls.length
      ? this.expressions(undefined, { sqls: definitionSqls })
      : ""
    const define = definitions
      ? this.pretty
        ? `DEFINE\n${this.indentSql(definitions, 1, 0)}`
        : `DEFINE ${definitions}`
      : ""

    const bodyParts = [
      partition,
      order,
      measures,
      rowsSql,
      afterSql,
      pattern,
      define,
    ].filter(Boolean)

    const alias = this.sql(expression.args.alias)
    const aliasSql = alias ? ` ${alias}` : ""

    if (this.pretty) {
      const body = bodyParts.join("\n")
      const indentedBody = this.indentSql(body, 1, 0)
      return `MATCH_RECOGNIZE (\n${indentedBody}\n)${aliasSql}`
    }
    return `MATCH_RECOGNIZE (${bodyParts.join(" ")})${aliasSql}`
  }

  protected partition_by_sql(
    expression: exp.Expression,
    flat?: boolean,
  ): string {
    const partitionByExprs = expression.args.partition_by as
      | exp.Expression[]
      | undefined
    if (!partitionByExprs?.length) return ""
    const partition = this.expressions(partitionByExprs, {
      flat: flat ?? true,
    })
    return `PARTITION BY ${partition}`
  }

  protected log_sql(expression: exp.Log): string {
    let thisExpr = expression.args.this as exp.Expression
    let exprArg = expression.expression as exp.Expression | undefined

    if (this.LOG_BASE_FIRST === false) {
      const temp = exprArg
      exprArg = thisExpr
      thisExpr = temp as exp.Expression
    } else if (this.LOG_BASE_FIRST === null && exprArg) {
      const baseName = thisExpr.name
      if (baseName === "2" || baseName === "10") {
        return this.funcCall(`LOG${baseName}`, [exprArg])
      }
      this.unsupported(`Unsupported logarithm with base ${this.sql(thisExpr)}`)
    }

    const args: exp.Expression[] = [thisExpr]
    if (exprArg) args.push(exprArg)
    return this.funcCall("LOG", args)
  }

  protected pad_sql(expression: exp.Pad): string {
    const prefix = expression.args.is_left ? "L" : "R"

    const args: exp.Expression[] = [
      expression.args.this as exp.Expression,
      expression.expression as exp.Expression,
    ]
    const fillPatternSql = this.sql(expression.args.fill_pattern)
    if (fillPatternSql) {
      args.push(expression.args.fill_pattern as exp.Expression)
    } else if (this.PAD_FILL_PATTERN_IS_REQUIRED) {
      args.push(exp.Literal.string(" "))
    }
    return this.funcCall(`${prefix}PAD`, args)
  }

  protected initcap_sql(expression: exp.Initcap): string {
    const args: exp.Expression[] = [expression.args.this as exp.Expression]
    const delimiters = expression.expression as exp.Expression | undefined
    if (delimiters) {
      args.push(delimiters)
    }
    return this.funcCall("INITCAP", args)
  }

  protected tochar_sql(expression: exp.ToChar): string {
    return this.sql(exp.cast(expression.args.this as exp.Expression, "TEXT"))
  }

  protected safedivide_sql(expression: exp.SafeDivide): string {
    const n = expression.args.this as exp.Expression
    const d = expression.expression as exp.Expression
    const nWrapped = n instanceof exp.Binary ? new exp.Paren({ this: n }) : n
    const dWrapped = d instanceof exp.Binary ? new exp.Paren({ this: d }) : d
    const divExpr = new exp.Div({ this: nWrapped, expression: dWrapped })
    const condition = new exp.NEQ({
      this: dWrapped.copy(),
      expression: exp.Literal.number(0),
    })
    const ifExpr = new exp.If({
      this: condition,
      true: divExpr,
      false: new exp.Null({}),
    })
    return this.sql(ifExpr)
  }

  protected try_sql(expression: exp.Try): string {
    if (!this.TRY_SUPPORTED) {
      this.unsupported("Unsupported TRY function")
      return this.sql(expression.args.this)
    }
    return this.funcCall("TRY", [expression.args.this as exp.Expression])
  }

  protected oncondition_sql(expression: exp.OnCondition): string {
    const empty = expression.args.empty
    let emptySql: string
    if (empty instanceof exp.Expression) {
      emptySql = `DEFAULT ${this.sql(empty)} ON EMPTY`
    } else {
      emptySql = typeof empty === "string" ? empty : ""
    }

    const error = expression.args.error
    let errorSql: string
    if (error instanceof exp.Expression) {
      errorSql = `DEFAULT ${this.sql(error)} ON ERROR`
    } else {
      errorSql = typeof error === "string" ? error : ""
    }

    if (errorSql && emptySql) {
      errorSql = this.ON_CONDITION_EMPTY_BEFORE_ERROR
        ? `${emptySql} ${errorSql}`
        : `${errorSql} ${emptySql}`
      emptySql = ""
    }

    const nullVal = expression.args.null
    const nullSql =
      nullVal instanceof exp.Expression
        ? this.sql(nullVal)
        : typeof nullVal === "string"
          ? nullVal
          : ""
    return `${emptySql}${errorSql}${nullSql}`
  }

  protected overlay_sql(expression: exp.Overlay): string {
    const thisSql = this.sql(expression.args.this)
    const exprSql = this.sql(expression.args.expression)
    const fromSql = this.sql(expression.args.from_)
    const forSql = this.sql(expression.args.for_)
    const forPart = forSql ? ` FOR ${forSql}` : ""
    return `OVERLAY(${thisSql} PLACING ${exprSql} FROM ${fromSql}${forPart})`
  }

  protected intdiv_sql(expression: exp.IntDiv): string {
    return this.sql(
      new exp.Cast({
        this: new exp.Div({
          this: expression.args.this as exp.Expression,
          expression: expression.expression as exp.Expression,
        }),
        to: new exp.DataType({ this: "INT" }),
      }),
    )
  }

  protected nullsafeeq_sql(expression: exp.NullSafeEQ): string {
    return this.binary_sql(expression, "IS NOT DISTINCT FROM")
  }

  protected nullsafeneq_sql(expression: exp.NullSafeNEQ): string {
    return this.binary_sql(expression, "IS DISTINCT FROM")
  }

  protected inputoutputformat_sql(expression: exp.InputOutputFormat): string {
    const inputFormat = this.sql(expression.args.input_format)
    const inputSql = inputFormat ? `INPUTFORMAT ${inputFormat}` : ""
    const outputFormat = this.sql(expression.args.output_format)
    const outputSql = outputFormat ? `OUTPUTFORMAT ${outputFormat}` : ""
    return [inputSql, outputSql].filter(Boolean).join(this.sep())
  }

  // ==================== DDL / Constraints ====================

  protected constraint_sql(expression: exp.Constraint): string {
    const thisSql = this.sql(expression.args.this)
    const exprsSql = this.expressions(expression, { flat: true })
    return `CONSTRAINT ${thisSql} ${exprsSql}`
  }

  protected columnconstraint_sql(expression: exp.ColumnConstraint): string {
    const thisSql = this.sql(expression.args.this)
    const kindSql = this.sql(expression.args.kind).trim()
    return thisSql ? `CONSTRAINT ${thisSql} ${kindSql}` : kindSql
  }

  protected autoincrementcolumnconstraint_sql(
    _expression: exp.AutoIncrementColumnConstraint,
  ): string {
    return "AUTO_INCREMENT"
  }

  protected check_sql(expression: exp.Check): string {
    const thisSql = this.sql(expression.args.this)
    return `CHECK (${thisSql})`
  }

  protected foreignkey_sql(expression: exp.ForeignKey): string {
    let exprsSql = this.expressions(expression, { flat: true })
    exprsSql = exprsSql ? ` (${exprsSql})` : ""
    const reference = this.sql(expression.args.reference)
    const referenceSql = reference ? ` ${reference}` : ""
    const deleteSql = this.sql(expression.args.delete)
    const deleteClause = deleteSql ? ` ON DELETE ${deleteSql}` : ""
    const updateSql = this.sql(expression.args.update)
    const updateClause = updateSql ? ` ON UPDATE ${updateSql}` : ""
    const options = this.expressions(expression, {
      key: "options",
      flat: true,
      sep: " ",
    })
    const optionsSql = options ? ` ${options}` : ""
    return `FOREIGN KEY${exprsSql}${referenceSql}${deleteClause}${updateClause}${optionsSql}`
  }

  protected primarykey_sql(expression: exp.PrimaryKey): string {
    const thisSql = this.sql(expression.args.this)
    const thisClause = thisSql ? ` ${thisSql}` : ""
    const exprsSql = this.expressions(expression, { flat: true })
    const include = this.sql(expression.args.include)
    const options = this.expressions(expression, {
      key: "options",
      flat: true,
      sep: " ",
    })
    const optionsSql = options ? ` ${options}` : ""
    return `PRIMARY KEY${thisClause} (${exprsSql})${include}${optionsSql}`
  }

  protected addconstraint_sql(expression: exp.AddConstraint): string {
    return `ADD ${this.expressions(expression, { indent: false })}`
  }

  protected renamecolumn_sql(expression: exp.RenameColumn): string {
    const exists = expression.args.exists ? " IF EXISTS" : ""
    const oldColumn = this.sql(expression.args.this)
    const newColumn = this.sql(expression.args.to)
    return `RENAME COLUMN${exists} ${oldColumn} TO ${newColumn}`
  }

  protected columnposition_sql(expression: exp.ColumnPosition): string {
    const thisSql = this.sql(expression.args.this)
    const thisClause = thisSql ? ` ${thisSql}` : ""
    const position = this.sql(expression.args.position)
    return `${position}${thisClause}`
  }

  protected columnprefix_sql(expression: exp.ColumnPrefix): string {
    return `${this.sql(expression.args.this)}(${this.sql(expression.args.expression)})`
  }

  protected partition_sql(expression: exp.Partition): string {
    const keyword = expression.args.subpartition ? "SUBPARTITION" : "PARTITION"
    return `${keyword}(${this.expressions(expression, { flat: true })})`
  }

  protected index_sql(expression: exp.Index): string {
    const unique = expression.args.unique ? "UNIQUE " : ""
    const primary = expression.args.primary ? "PRIMARY " : ""
    const amp = expression.args.amp ? "AMP " : ""
    let name = this.sql(expression.args.this)
    name = name ? `${name} ` : ""
    const table = this.sql(expression.args.table)
    const tableSql = table ? `${this.INDEX_ON} ${table}` : ""
    const index = !tableSql ? "INDEX " : ""
    const params = this.sql(expression.args.params)
    return `${unique}${primary}${amp}${index}${name}${tableSql}${params}`
  }

  protected indexconstraintoption_sql(
    expression: exp.IndexConstraintOption,
  ): string {
    const keyBlockSize = this.sql(expression.args.key_block_size)
    if (keyBlockSize) return `KEY_BLOCK_SIZE = ${keyBlockSize}`

    const using = this.sql(expression.args.using)
    if (using) return `USING ${using}`

    const parser = this.sql(expression.args.parser)
    if (parser) return `WITH PARSER ${parser}`

    const comment = this.sql(expression.args.comment)
    if (comment) return `COMMENT ${comment}`

    const visible = expression.args.visible
    if (visible !== undefined && visible !== null) {
      return visible ? "VISIBLE" : "INVISIBLE"
    }

    const engineAttr = this.sql(expression.args.engine_attr)
    if (engineAttr) return `ENGINE_ATTRIBUTE = ${engineAttr}`

    const secondaryEngineAttr = this.sql(expression.args.secondary_engine_attr)
    if (secondaryEngineAttr)
      return `SECONDARY_ENGINE_ATTRIBUTE = ${secondaryEngineAttr}`

    return ""
  }

  protected clone_sql(expression: exp.Clone): string {
    const thisSql = this.sql(expression.args.this)
    const shallow = expression.args.shallow ? "SHALLOW " : ""
    const keyword =
      expression.args.copy && this.features.SUPPORTS_TABLE_COPY
        ? "COPY"
        : "CLONE"
    return `${shallow}${keyword} ${thisSql}`
  }

  protected characterset_sql(expression: exp.CharacterSet): string {
    if (expression.parent instanceof exp.Cast) {
      return `CHAR CHARACTER SET ${this.sql(expression.args.this)}`
    }
    const defaultStr = expression.args.default ? "DEFAULT " : ""
    return `${defaultStr}CHARACTER SET=${this.sql(expression.args.this)}`
  }

  protected collate_sql(expression: exp.Collate): string {
    if (this.features.COLLATE_IS_FUNC) {
      return this.function_fallback_sql(expression)
    }
    return this.binary_sql(expression, "COLLATE")
  }

  protected attach_sql(expression: exp.Attach): string {
    const thisSql = this.sql(expression.args.this)
    const existsSql = expression.args.exists ? " IF NOT EXISTS" : ""
    const exprsSql = this.expressions(expression)
    const exprsClause = exprsSql ? ` (${exprsSql})` : ""
    return `ATTACH${existsSql} ${thisSql}${exprsClause}`
  }

  protected detach_sql(expression: exp.Detach): string {
    const thisSql = this.sql(expression.args.this)
    const existsSql = expression.args.exists ? " DATABASE IF EXISTS" : ""
    return `DETACH${existsSql} ${thisSql}`
  }

  protected attachoption_sql(expression: exp.AttachOption): string {
    const thisSql = this.sql(expression.args.this)
    const value = this.sql(expression.args.expression)
    const valueSql = value ? ` ${value}` : ""
    return `${thisSql}${valueSql}`
  }

  protected droppartition_sql(expression: exp.DropPartition): string {
    const exprsSql = this.expressions(expression)
    const exists = expression.args.exists ? " IF EXISTS " : " "
    return `DROP${exists}${exprsSql}`
  }

  protected addpartition_sql(expression: exp.AddPartition): string {
    const exists = expression.args.exists ? "IF NOT EXISTS " : ""
    const location = this.sql(expression.args.location)
    const locationSql = location ? ` ${location}` : ""
    return `ADD ${exists}${this.sql(expression.args.this)}${locationSql}`
  }

  protected alterindex_sql(expression: exp.AlterIndex): string {
    const thisSql = this.sql(expression.args.this)
    const visibleSql = expression.args.visible ? "VISIBLE" : "INVISIBLE"
    return `ALTER INDEX ${thisSql} ${visibleSql}`
  }

  protected alterdiststyle_sql(expression: exp.AlterDistStyle): string {
    const thisSql = this.sql(expression.args.this)
    if (!(expression.args.this instanceof exp.Var)) {
      return `ALTER DISTSTYLE KEY DISTKEY ${thisSql}`
    }
    return `ALTER DISTSTYLE ${thisSql}`
  }

  protected altersortkey_sql(expression: exp.AlterSortKey): string {
    const compound = expression.args.compound ? " COMPOUND" : ""
    const thisSql = this.sql(expression.args.this)
    let exprsSql = this.expressions(expression, { flat: true })
    exprsSql = exprsSql ? `(${exprsSql})` : ""
    return `ALTER${compound} SORTKEY ${thisSql || exprsSql}`
  }

  protected describe_sql(expression: exp.Describe): string {
    const style = expression.args.style
    const styleSql = style ? ` ${style}` : ""
    const partition = this.sql(expression.args.partition)
    const partitionSql = partition ? ` ${partition}` : ""
    const format = this.sql(expression.args.format)
    const formatSql = format ? ` ${format}` : ""
    const asJson = expression.args.as_json ? " AS JSON" : ""
    return `DESCRIBE${styleSql}${formatSql} ${this.sql(expression.args.this)}${partitionSql}${asJson}`
  }

  protected cluster_sql(expression: exp.Cluster): string {
    return this.op_expressions("CLUSTER BY", expression)
  }

  protected sort_sql(expression: exp.Sort): string {
    return this.op_expressions("SORT BY", expression)
  }

  protected distribute_sql(expression: exp.Distribute): string {
    return this.op_expressions("DISTRIBUTE BY", expression)
  }

  protected pragma_sql(expression: exp.Pragma): string {
    return `PRAGMA ${this.sql(expression.args.this)}`
  }

  protected version_sql(expression: exp.Version): string {
    const thisVal = `FOR ${expression.name}`
    const kind = expression.text("kind")
    const expr = this.sql(expression.args.expression)
    return `${thisVal} ${kind} ${expr}`
  }

  protected altersession_sql(expression: exp.AlterSession): string {
    const itemsSql = this.expressions(expression, { flat: true })
    const keyword = expression.args.unset ? "UNSET" : "SET"
    return `${keyword} ${itemsSql}`
  }

  protected declare_sql(expression: exp.Declare): string {
    return `DECLARE ${this.expressions(expression, { flat: true })}`
  }

  protected declareitem_sql(expression: exp.DeclareItem): string {
    const variable = this.sql(expression.args.this)
    const defaultVal = this.sql(expression.args.default)
    const defaultSql = defaultVal ? ` = ${defaultVal}` : ""
    const kind = this.sql(expression.args.kind)
    const kindSql =
      expression.args.kind instanceof exp.Schema ? `TABLE ${kind}` : kind
    return `${variable} AS ${kindSql}${defaultSql}`
  }

  protected currentdate_sql(expression: exp.CurrentDate): string {
    const zone = this.sql(expression.args.this)
    return zone ? `CURRENT_DATE(${zone})` : "CURRENT_DATE"
  }

  protected anonymousaggfunc_sql(expression: exp.AnonymousAggFunc): string {
    return this.funcCall(
      this.normalizeFunc(expression.name),
      expression.expressions,
    )
  }

  protected combinedaggfunc_sql(expression: exp.CombinedAggFunc): string {
    return this.anonymousaggfunc_sql(expression)
  }

  protected anyvalue_sql(expression: exp.AnyValue): string {
    let thisSql = this.sql(expression.args.this)
    const having = this.sql(expression.args.having)
    if (having) {
      thisSql = `${thisSql} HAVING ${expression.args.max ? "MAX" : "MIN"} ${having}`
    }
    return `ANY_VALUE(${thisSql})`
  }

  protected apply_sql(expression: exp.Apply): string {
    const thisSql = this.sql(expression.args.this)
    const expr = this.sql(expression.args.expression)
    return `${thisSql} APPLY(${expr})`
  }

  protected atindex_sql(expression: exp.AtIndex): string {
    const thisSql = this.sql(expression.args.this)
    const index = this.sql(expression.args.expression)
    return `${thisSql} AT ${index}`
  }

  protected chr_sql(expression: exp.Chr): string {
    const thisSql = this.expressions(expression.expressions)
    const charset = this.sql(expression.args.charset)
    const using = charset ? ` USING ${charset}` : ""
    return `CHR(${thisSql}${using})`
  }

  protected jsonkeyvalue_sql(expression: exp.JSONKeyValue): string {
    return `${this.sql(expression.args.this)}${this.JSON_KEY_VALUE_PAIR_SEP} ${this.sql(expression.args.expression)}`
  }

  protected jsonpath_sql(expression: exp.JSONPath): string {
    let path = this.expressions(expression, { sep: "", flat: true }).replace(
      /^\./,
      "",
    )

    if (expression.args.escape) {
      path = this.escape_str(path)
    }

    if (this.QUOTE_JSON_PATH) {
      path = `'${path}'`
    }

    return path
  }

  json_path_part(expression: string | number | exp.JSONPathPart): string {
    if (expression instanceof exp.JSONPathPart) {
      const transform = this.transforms.get(
        expression.constructor as ExpressionClass,
      )
      if (typeof transform !== "function") {
        this.unsupported(
          `Unsupported JSONPathPart type ${expression.constructor.name}`,
        )
        return ""
      }
      return transform(this, expression)
    }

    if (typeof expression === "number") {
      return String(expression)
    }

    if (
      this._quoteJsonPathKeyUsingBrackets &&
      this.JSON_PATH_SINGLE_QUOTE_ESCAPE
    ) {
      const escaped = expression.replace(/'/g, "\\'")
      return `\\'${escaped}\\'`
    }

    const escaped = expression.replace(/"/g, '\\"')
    return `"${escaped}"`
  }

  jsonpathkey_sql(expression: exp.JSONPathKey): string {
    const thisVal = expression.args.this
    if (thisVal instanceof exp.JSONPathWildcard) {
      const part = this.json_path_part(thisVal)
      return part ? `.${part}` : ""
    }

    if (typeof thisVal === "string" && /^[_a-zA-Z]\w*$/.test(thisVal)) {
      return `.${thisVal}`
    }

    const part = this.json_path_part(
      thisVal as string | number | exp.JSONPathPart,
    )
    return this._quoteJsonPathKeyUsingBrackets &&
      this.JSON_PATH_BRACKETED_KEY_SUPPORTED
      ? `[${part}]`
      : `.${part}`
  }

  jsonpathsubscript_sql(expression: exp.JSONPathSubscript): string {
    const part = this.json_path_part(
      expression.args.this as string | number | exp.JSONPathPart,
    )
    return part ? `[${part}]` : ""
  }

  protected formatjson_sql(expression: exp.FormatJson): string {
    return `${this.sql(expression.args.this)} FORMAT JSON`
  }

  protected jsonobject_sql(
    expression: exp.JSONObject | exp.JSONObjectAgg,
  ): string {
    const nullHandling = expression.args.null_handling
    const nullHandlingSql = nullHandling ? ` ${nullHandling}` : ""

    const uniqueKeys = expression.args.unique_keys
    let uniqueKeysSql = ""
    if (uniqueKeys !== undefined && uniqueKeys !== null) {
      uniqueKeysSql = uniqueKeys ? " WITH UNIQUE KEYS" : " WITHOUT UNIQUE KEYS"
    }

    const returnType = this.sql(expression.args.return_type)
    const returnTypeSql = returnType ? ` RETURNING ${returnType}` : ""
    const encoding = this.sql(expression.args.encoding)
    const encodingSql = encoding ? ` ENCODING ${encoding}` : ""

    const name =
      expression instanceof exp.JSONObject ? "JSON_OBJECT" : "JSON_OBJECTAGG"
    const argsSql = this.expressions(expression.expressions)
    return `${this.normalizeFunc(name)}(${argsSql}${nullHandlingSql}${uniqueKeysSql}${returnTypeSql}${encodingSql})`
  }

  protected jsonobjectagg_sql(expression: exp.JSONObjectAgg): string {
    return this.jsonobject_sql(expression)
  }

  protected jsonarray_sql(expression: exp.JSONArray): string {
    const nullHandling = expression.args.null_handling
    const nullHandlingSql = nullHandling ? ` ${nullHandling}` : ""
    const returnType = this.sql(expression.args.return_type)
    const returnTypeSql = returnType ? ` RETURNING ${returnType}` : ""
    const strict = expression.args.strict ? " STRICT" : ""
    const argsSql = this.expressions(expression.expressions)
    return `${this.normalizeFunc("JSON_ARRAY")}(${argsSql}${nullHandlingSql}${returnTypeSql}${strict})`
  }

  protected jsonarrayagg_sql(expression: exp.JSONArrayAgg): string {
    const thisSql = this.sql(expression.args.this)
    const order = this.sql(expression.args.order)
    const nullHandling = expression.args.null_handling
    const nullHandlingSql = nullHandling ? ` ${nullHandling}` : ""
    const returnType = this.sql(expression.args.return_type)
    const returnTypeSql = returnType ? ` RETURNING ${returnType}` : ""
    const strict = expression.args.strict ? " STRICT" : ""
    return `${this.normalizeFunc("JSON_ARRAYAGG")}(${thisSql}${order ? ` ${order}` : ""}${nullHandlingSql}${returnTypeSql}${strict})`
  }

  protected jsonvalue_sql(expression: exp.JSONValue): string {
    const path = this.sql(expression.args.path)
    const returning = this.sql(expression.args.returning)
    const returningSql = returning ? ` RETURNING ${returning}` : ""

    const onCondition = this.sql(expression.args.on_condition)
    const onConditionSql = onCondition ? ` ${onCondition}` : ""

    return `${this.normalizeFunc("JSON_VALUE")}(${this.sql(expression.args.this)}, ${path}${returningSql}${onConditionSql})`
  }

  protected jsonexists_sql(expression: exp.JSONExists): string {
    const thisSql = this.sql(expression.args.this)
    const path = this.sql(expression.args.path)

    const passing = this.expressions(expression, { key: "passing" })
    const passingSql = passing ? ` PASSING ${passing}` : ""

    const onCondition = this.sql(expression.args.on_condition)
    const onConditionSql = onCondition ? ` ${onCondition}` : ""

    const pathSql = `${path}${passingSql}${onConditionSql}`

    return `${this.normalizeFunc("JSON_EXISTS")}(${thisSql}, ${pathSql})`
  }

  protected revoke_sql(expression: exp.Revoke): string {
    const privileges = this.expressionsFromKey(expression, "privileges")
    const kind = expression.args.kind ? ` ${String(expression.args.kind)}` : ""
    const securable = this.sql(expression.args.securable)
    const principals = this.expressionsFromKey(expression, "principals")
    const grantOption = expression.args.grant_option ? "GRANT OPTION FOR " : ""
    const cascade = this.sql(expression.args.cascade)
    const cascadeSql = cascade ? ` ${cascade}` : ""
    return `REVOKE ${grantOption}${privileges} ON${kind} ${securable} FROM ${principals}${cascadeSql}`
  }

  protected grantprivilege_sql(expression: exp.GrantPrivilege): string {
    const thisSql = this.sql(expression.args.this)
    const columns = this.expressions(expression, { flat: true })
    const columnsSql = columns ? `(${columns})` : ""
    return `${thisSql}${columnsSql}`
  }

  protected grantprincipal_sql(expression: exp.GrantPrincipal): string {
    const thisSql = this.sql(expression.args.this)
    const kind = this.sql(expression.args.kind)
    const kindSql = kind ? `${kind} ` : ""
    return `${kindSql}${thisSql}`
  }

  protected cache_sql(expression: exp.Cache): string {
    const lazy = expression.args.lazy ? " LAZY" : ""
    const table = this.sql(expression.args.this)
    const options = expression.args.options as exp.Expression[] | undefined
    const optionsSql =
      options && options.length >= 2
        ? ` OPTIONS(${this.sql(options[0])} = ${this.sql(options[1])})`
        : ""
    const exprSql = this.sql(expression.args.expression)
    const sql = exprSql ? ` AS${this.sep()}${exprSql}` : ""
    const result = `CACHE${lazy} TABLE ${table}${optionsSql}${sql}`
    return this.prependCtes(expression, result)
  }

  protected uncache_sql(expression: exp.Uncache): string {
    const table = this.sql(expression.args.this)
    const existsSql = expression.args.exists ? " IF EXISTS" : ""
    return `UNCACHE TABLE${existsSql} ${table}`
  }

  protected dateadd_sql(expression: exp.DateAdd): string {
    const unit = expression.args.unit
    let unitExpr: exp.Expression
    if (!unit) {
      unitExpr = exp.Literal.string("DAY")
    } else if (unit instanceof exp.Expression) {
      unitExpr = exp.Literal.string(unit.name)
    } else {
      unitExpr = exp.Literal.string(String(unit))
    }
    return this.funcCall("DATE_ADD", [
      expression.args.this as exp.Expression,
      expression.expression as exp.Expression,
      unitExpr,
    ])
  }

  protected datefromunixdate_sql(expression: exp.DateFromUnixDate): string {
    return this.sql(
      new exp.DateAdd({
        this: exp.cast(
          exp.Literal.string("1970-01-01"),
          exp.DataType.Type.DATE,
        ),
        expression: expression.args.this as exp.Expression,
        unit: new exp.Var({ this: "DAY" }),
      }),
    )
  }

  protected tonumber_sql(expression: exp.ToNumber): string {
    const fmt = expression.args.format as exp.Expression | undefined
    if (!fmt) {
      this.unsupported("Conversion format is required for TO_NUMBER")
      return this.sql(
        exp.cast(
          expression.args.this as exp.Expression,
          exp.DataType.Type.DOUBLE,
        ),
      )
    }
    return this.funcCall("TO_NUMBER", [
      expression.args.this as exp.Expression,
      fmt,
    ])
  }

  protected arrayany_sql(expression: exp.ArrayAny): string {
    const filtered = new exp.ArrayFilter({
      this: expression.args.this as exp.Expression,
      expression: expression.expression as exp.Expression,
    })
    const filteredNotEmpty = new exp.NEQ({
      this: new exp.ArraySize({ this: filtered }),
      expression: new exp.Literal({ this: "0", is_string: false }),
    })
    const originalIsEmpty = new exp.EQ({
      this: new exp.ArraySize({ this: expression.args.this as exp.Expression }),
      expression: new exp.Literal({ this: "0", is_string: false }),
    })
    return this.sql(
      new exp.Paren({
        this: new exp.Or({
          this: originalIsEmpty,
          expression: filteredNotEmpty,
        }),
      }),
    )
  }

  // ==================== Fallback ====================

  protected expression_sql(expression: exp.Expression): string {
    // Generic fallback - should rarely be used
    const key = expression.key.toUpperCase()
    const args: string[] = []

    for (const [_k, v] of Object.entries(expression.args)) {
      if (v instanceof exp.Expression) {
        args.push(this.sql(v))
      } else if (Array.isArray(v)) {
        const items = v.filter(
          (x): x is exp.Expression => x instanceof exp.Expression,
        )
        if (items.length > 0) {
          args.push(this.expressions(items))
        }
      } else if (v !== undefined && v !== null && v !== false) {
        args.push(String(v))
      }
    }

    if (args.length > 0) {
      return `${key}(${args.join(", ")})`
    }
    return key
  }

  // ==================== Helper Methods ====================

  expressions(
    expressionOrArray?: exp.Expression | exp.Expression[],
    optionsOrSep?:
      | string
      | {
          key?: string
          sqls?: (string | exp.Expression)[]
          flat?: boolean
          indent?: boolean
          skipFirst?: boolean
          skipLast?: boolean
          sep?: string
          prefix?: string
          dynamic?: boolean
          newLine?: boolean
        },
  ): string {
    // Backward-compatible: expressions(Expression[], sep?)
    if (Array.isArray(expressionOrArray)) {
      const sep = typeof optionsOrSep === "string" ? optionsOrSep : ", "
      return expressionOrArray
        .map((e) => {
          const sql = this.sql(e)
          if (e instanceof exp.Order && !e.args.this) return ` ${sql}`
          return sql
        })
        .join(sep)
    }

    // Python-style: expressions(expression?, options?)
    const opts =
      typeof optionsOrSep === "object" && optionsOrSep !== null
        ? optionsOrSep
        : {}
    const {
      key,
      sqls,
      flat = false,
      indent = true,
      skipFirst = false,
      skipLast = false,
      sep = ", ",
      prefix = "",
      dynamic = false,
      newLine = false,
    } = opts

    const items:
      | (string | exp.Expression)[]
      | exp.Expression[]
      | undefined
      | null = expressionOrArray
      ? (expressionOrArray.args[key || "expressions"] as
          | exp.Expression[]
          | undefined)
      : sqls

    if (!items || (Array.isArray(items) && items.length === 0)) return ""

    if (flat) {
      return items
        .map((e) => this.sql(e))
        .filter((s) => s)
        .join(sep)
    }

    const numSqls = items.length
    const resultSqls: string[] = []

    for (let i = 0; i < numSqls; i++) {
      const e = items[i]!
      const sql = this.sql(e)
      if (!sql) continue

      const comments =
        e instanceof exp.Expression ? this.maybeComment("", e) : ""

      if (this.pretty) {
        if (this.leadingComma) {
          resultSqls.push(`${i > 0 ? sep : ""}${prefix}${sql}${comments}`)
        } else {
          const sepPart =
            i + 1 < numSqls ? (comments ? sep.trimEnd() : sep) : ""
          resultSqls.push(`${prefix}${sql}${sepPart}${comments}`)
        }
      } else {
        resultSqls.push(
          `${prefix}${sql}${comments}${i + 1 < numSqls ? sep : ""}`,
        )
      }
    }

    let resultSql: string
    if (this.pretty && (!dynamic || this.tooWide(resultSqls))) {
      if (newLine) {
        resultSqls.unshift("")
        resultSqls.push("")
      }
      resultSql = resultSqls.map((s) => s.trimEnd()).join("\n")
    } else {
      resultSql = resultSqls.join("")
    }

    return indent
      ? this.indentSql(resultSql, 0, undefined, skipFirst, skipLast)
      : resultSql
  }

  protected op_expressions(op: string, expression: exp.Expression): string {
    const expressionsSql = this.expressions(expression.expressions)
    return expressionsSql ? `${op} ${expressionsSql}` : op
  }

  normalizeFunc(name: string): string {
    const nf = (this.constructor as typeof Generator).NORMALIZE_FUNCTIONS
    if (nf === "upper" || nf === true) return name.toUpperCase()
    if (nf === "lower") return name.toLowerCase()
    return name
  }

  funcCall(name: string, args: ArgValue[]): string {
    const argSqls: string[] = []
    for (const a of args) {
      let sql = this.sql(a)
      if (a instanceof exp.Order && !a.args.this) {
        sql = ` ${sql}`
      }
      argSqls.push(sql)
    }
    return `${name}(${argSqls.join(", ")})`
  }

  formatTimeStr(expression: exp.Expression): string {
    const formatExpr = expression.args.format
    if (formatExpr instanceof exp.Expression) {
      const formatSql = this.sql(formatExpr)
      const mapping = (this.constructor as typeof Generator)
        .INVERSE_TIME_MAPPING
      if (mapping.size === 0) {
        return formatSql
      }
      if (formatExpr instanceof exp.Literal && formatExpr.isString) {
        // String literal: strip quotes, convert, re-quote
        const unquoted = formatSql.replace(/^'|'$/g, "")
        const converted = formatTime(unquoted, mapping)
        return `'${converted}'`
      }
      // Non-literal expression: apply time mapping to entire SQL string
      return formatTime(formatSql, mapping)
    }
    return ""
  }

  protected function_fallback_sql(expression: exp.Func): string {
    const args: exp.Expression[] = []
    const argTypes = (expression.constructor as typeof exp.Expression).argTypes
    for (const key of Object.keys(argTypes)) {
      if (key === "order") continue
      const argValue = expression.args[key]
      if (Array.isArray(argValue)) {
        for (const value of argValue) {
          if (value instanceof exp.Expression) {
            args.push(value)
          }
        }
      } else if (argValue instanceof exp.Expression) {
        args.push(argValue)
      }
    }
    let name: string
    if (
      (this.constructor as typeof Generator).PRESERVE_ORIGINAL_NAMES &&
      expression._meta
    ) {
      name =
        (expression._meta["name"] as string) ||
        this.normalizeFunc(expression.name)
    } else {
      name = this.normalizeFunc(expression.name)
    }
    const order = expression.args.order as exp.Expression | undefined
    if (order) args.push(order)
    return this.funcCall(name, args)
  }

  protected property_sql(expression: exp.Property): string {
    const thisVal = expression.args.this as exp.Expression | undefined
    const name = thisVal ? this.sql(thisVal) : expression.key.toUpperCase()
    const value = expression.args.value as exp.Expression | undefined
    if (value) {
      return `${name}=${this.sql(value)}`
    }
    return name
  }

  nakedPropertySql(expression: exp.Expression): string {
    const propertyName = PROPERTY_TO_NAME.get(
      expression.constructor as ExpressionClass,
    )
    if (!propertyName) {
      this.unsupported(`Unsupported property ${expression.constructor.name}`)
      return ""
    }
    return `${propertyName} ${this.sql(expression.args.this)}`
  }

  protected sep(sep = " "): string {
    return this.pretty ? `${sep.trim()}\n` : sep
  }

  protected seg(sql: string, sep = " "): string {
    return `${this.sep(sep)}${sql}`
  }

  protected wrap(body: string): string {
    if (!body) return "()"
    const indented = this.indentSql(body, 1, 0)
    return `(${this.sep("")}${indented}${this.seg(")", "")}`
  }

  protected indentSql(
    sql: string,
    level = 0,
    pad?: number,
    skipFirst = false,
    skipLast = false,
  ): string {
    if (!this.pretty || !sql) return sql
    const padSize = pad ?? this.pad
    const lines = sql.split("\n")
    return lines
      .map((line, i) => {
        if ((skipFirst && i === 0) || (skipLast && i === lines.length - 1))
          return line
        return " ".repeat(level * this._indent + padSize) + line
      })
      .join("\n")
  }

  protected tooWide(args: Iterable<string>): boolean {
    let total = 0
    for (const arg of args) total += arg.length
    return total > this.maxTextWidth
  }

  protected quoteIdentifier(name: string): string {
    return `"${name.replace(/"/g, '""')}"`
  }

  protected escape_str(text: string, escapeBackslash = true): string {
    const ctor = this.constructor as typeof Generator

    if (ctor.STRINGS_SUPPORT_ESCAPED_SEQUENCES) {
      text = [...text]
        .map((ch) => {
          if (!escapeBackslash && ch === "\\") return ch
          return ctor.ESCAPED_SEQUENCES[ch] ?? ch
        })
        .join("")
    }

    return text.replaceAll("'", this._escapedQuoteEnd)
  }

  protected quoteString(value: string): string {
    return `'${this.escape_str(value)}'`
  }

  protected shouldQuote(name: string): boolean {
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
      return true
    }
    const ctor = this.constructor as typeof Generator
    return ctor.RESERVED_KEYWORDS.has(name.toUpperCase())
  }

  protected introducer_sql(expression: exp.Introducer): string {
    return `${this.sql(expression.args.this)} ${this.sql(expression.args.expression)}`
  }

  protected nextvaluefor_sql(expression: exp.NextValueFor): string {
    const order = expression.args.order as exp.Order | undefined
    const orderSql = order ? ` OVER (${this.order_sql(order)})` : ""
    return `NEXT VALUE FOR ${this.sql(expression.args.this)}${orderSql}`
  }

  protected scoperesolution_sql(expression: exp.ScopeResolution): string {
    const thisSql = this.sql(expression.args.this)
    const expr = expression.args.expression as exp.Expression
    let exprSql: string
    if (expr instanceof exp.Func) {
      const funcName = this.sql(expr.args.this)
      const funcArgs = expr.expressions
        .map((a: exp.Expression) => this.sql(a))
        .join(", ")
      exprSql = `${funcName}(${funcArgs})`
    } else {
      exprSql = this.sql(expression.args.expression)
    }
    return thisSql ? `${thisSql}::${exprSql}` : `::${exprSql}`
  }

  protected parsejson_sql(expression: exp.ParseJSON): string {
    if (this.PARSE_JSON_NAME === null) {
      return this.sql(expression.this)
    }
    const args: exp.Expression[] = [expression.this as exp.Expression]
    const expr = expression.args.expression as exp.Expression | undefined
    if (expr) args.push(expr)
    return this.funcCall(this.PARSE_JSON_NAME, args)
  }

  protected rand_sql(expression: exp.Rand): string {
    const lower = this.sql(expression.args.lower)
    const upper = this.sql(expression.args.upper)
    if (lower && upper) {
      const seedArgs = expression.args.this
        ? [expression.this as exp.Expression]
        : []
      return `(${upper} - ${lower}) * ${this.funcCall("RAND", seedArgs)} + ${lower}`
    }
    const args = expression.args.this ? [expression.this as exp.Expression] : []
    return this.funcCall("RAND", args)
  }

  protected lastday_sql(expression: exp.LastDay): string {
    if (this.features.LAST_DAY_SUPPORTS_DATE_PART) {
      return this.function_fallback_sql(expression as exp.Func)
    }
    const unit = expression.text("unit")
    if (unit && unit !== "MONTH") {
      this.unsupported("Date parts are not supported in LAST_DAY.")
    }
    return this.funcCall("LAST_DAY", [expression.this as exp.Expression])
  }

  protected unicodestring_sql(expression: exp.UnicodeString): string {
    const ctor = this.constructor as typeof Generator
    let thisStr = this.sql(expression.args.this)
    const escape = expression.args.escape as exp.Expression | undefined

    let escapeSubstitute: string
    let leftQuote: string
    let rightQuote: string
    if (ctor.UNICODE_START) {
      escapeSubstitute = "\\\\\\$1"
      leftQuote = ctor.UNICODE_START
      rightQuote = ctor.UNICODE_END || ""
    } else {
      escapeSubstitute = "\\\\u$1"
      leftQuote = "'"
      rightQuote = "'"
    }

    let escapeSql = ""
    let escapePattern: RegExp
    if (escape) {
      const escapeName = escape.text("this") || escape.text("name")
      escapePattern = new RegExp(
        `${escapeName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(\\d+)`,
        "g",
      )
      escapeSql = this.SUPPORTS_UESCAPE ? ` UESCAPE ${this.sql(escape)}` : ""
    } else {
      escapePattern = /\\(\d+)/g
    }

    if (!ctor.UNICODE_START || (escape && !this.SUPPORTS_UESCAPE)) {
      thisStr = thisStr.replace(escapePattern, escapeSubstitute)
    }

    return `${leftQuote}${thisStr}${rightQuote}${escapeSql}`
  }
}
