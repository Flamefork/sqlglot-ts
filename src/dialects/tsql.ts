/**
 * T-SQL (Microsoft SQL Server) dialect
 */

import { Dialect } from "../dialect.js"
import type { ExpressionClass } from "../expression-base.js"
import * as exp from "../expressions.js"
import { Generator } from "../generator.js"
import { Parser } from "../parser.js"
import { TokenType } from "../tokens.js"
import {
  dateDeltaSql,
  eliminateQualify,
  eliminateSemiAndAntiJoins,
  preprocess,
} from "../transforms.js"

type Transform = (generator: Generator, expression: exp.Expression) => string

// Expression types where TRUE/FALSE should be rendered as 1/0 instead of (1=1)/(1=0)
const BIT_TYPES = new Set<ExpressionClass>([
  exp.EQ,
  exp.NEQ,
  exp.Is,
  exp.In,
  exp.Select,
  exp.Alias,
])

function unitToVar(expression: exp.Expression): string {
  const unit = (expression as exp.Func).args.unit
  if (typeof unit === "string") return unit
  if (unit instanceof exp.Expression)
    return String(unit.args.this ?? "day").toUpperCase()
  return "day"
}

// TSQL date part abbreviations to canonical form
// Matches Python: sqlglot/dialects/tsql.py DATE_PART_MAPPING
const DATE_PART_MAPPING: Record<string, string> = {
  QQ: "QUARTER",
  Q: "QUARTER",
  YY: "YEAR",
  YYYY: "YEAR",
  M: "MONTH",
  MM: "MONTH",
  Y: "DAYOFYEAR",
  DY: "DAYOFYEAR",
  DD: "DAY",
  D: "DAY",
  WW: "WEEK",
  WK: "WEEK",
  HH: "HOUR",
  MI: "MINUTE",
  N: "MINUTE",
  SS: "SECOND",
  S: "SECOND",
  MS: "MILLISECOND",
  MCS: "MICROSECOND",
  NS: "NANOSECOND",
  DW: "WEEKDAY",
  TZOFFSET: "TIMEZONE_MINUTE", // Maps to internal canonical
  TZ: "TIMEZONE_MINUTE",
  // ISO_WEEK variants map to internal WEEKISO canonical
  ISO_WEEK: "WEEKISO",
  ISOWK: "WEEKISO",
  ISOWW: "WEEKISO",
}

// DEFAULT_START_DATE: 1900-01-01, TSQL epoch for integer date conversion
const DEFAULT_START_DATE = new Date(1900, 0, 1)

function addDays(date: Date, days: number): string {
  const result = new Date(date)
  result.setDate(result.getDate() + days)
  const y = result.getFullYear()
  const m = String(result.getMonth() + 1).padStart(2, "0")
  const d = String(result.getDate()).padStart(2, "0")
  return `${y}-${m}-${d}`
}

// Date delta interval mapping (same as DATE_PART_MAPPING for the common units)
// Matches Python: sqlglot/dialects/tsql.py DATE_DELTA_INTERVAL
const DATE_DELTA_INTERVAL: Record<string, string> = {
  year: "year",
  yyyy: "year",
  yy: "year",
  quarter: "quarter",
  qq: "quarter",
  q: "quarter",
  month: "month",
  mm: "month",
  m: "month",
  week: "week",
  ww: "week",
  wk: "week",
  day: "day",
  dd: "day",
  d: "day",
}

// Reverse mapping for generation (internal canonical → TSQL output)
const DATE_PART_UNMAPPING: Record<string, string> = {
  WEEKISO: "ISO_WEEK",
  DAYOFWEEK: "WEEKDAY",
  TIMEZONE_MINUTE: "TZOFFSET",
}

const XML_OPTIONS: Record<string, Array<string | string[]>> = {
  AUTO: [],
  EXPLICIT: [],
  TYPE: [],
  ELEMENTS: ["XSINIL", "ABSENT"],
  BINARY: ["BASE64"],
}

const OPTIONS: Record<string, Array<string | string[]>> = {
  DISABLE_OPTIMIZED_PLAN_FORCING: [],
  FAST: [],
  IGNORE_NONCLUSTERED_COLUMNSTORE_INDEX: [],
  LABEL: [],
  MAXDOP: [],
  MAXRECURSION: [],
  MAX_GRANT_PERCENT: [],
  MIN_GRANT_PERCENT: [],
  NO_PERFORMANCE_SPOOL: [],
  QUERYTRACEON: [],
  RECOMPILE: [],
  CONCAT: ["UNION"],
  DISABLE: ["EXTERNALPUSHDOWN", "SCALEOUTEXECUTION"],
  EXPAND: ["VIEWS"],
  FORCE: ["EXTERNALPUSHDOWN", "ORDER", "SCALEOUTEXECUTION"],
  HASH: ["GROUP", "JOIN", "UNION"],
  KEEP: ["PLAN"],
  KEEPFIXED: ["PLAN"],
  LOOP: ["JOIN"],
  MERGE: ["JOIN", "UNION"],
  OPTIMIZE: [["FOR", "UNKNOWN"]],
  ORDER: ["GROUP"],
  PARAMETERIZATION: ["FORCED", "SIMPLE"],
  ROBUST: ["PLAN"],
  USE: ["PLAN"],
}

const OPTIONS_THAT_REQUIRE_EQUAL = new Set([
  "MAX_GRANT_PERCENT",
  "MIN_GRANT_PERCENT",
  "LABEL",
])

export class TSQLParser extends Parser {
  static override SET_REQUIRES_ASSIGNMENT_DELIMITER = false

  static override TYPE_NAME_MAPPING: Map<string, string> = new Map([
    ...Parser.TYPE_NAME_MAPPING,
    ["TINYINT", "UTINYINT"],
  ])

  static override FUNCTIONS = new Map([
    ...Parser.FUNCTIONS,
    ["GETDATE", () => new exp.CurrentTimestamp({})],
    ["SYSDATETIME", () => new exp.CurrentTimestamp({})],
  ])

  static override FUNCTION_PARSERS: Map<
    string,
    (parser: Parser) => exp.Expression
  > = new Map([
    ...Parser.FUNCTION_PARSERS,
    ["DATEPART", (p) => (p as TSQLParser).parseDatepart()],
    ["DATEADD", (p) => (p as TSQLParser).parseDateadd()],
    ["DATEDIFF", (p) => (p as TSQLParser).parseDatediff()],
    ["DATEDIFF_BIG", (p) => (p as TSQLParser).parseDatediffBig()],
    ["DATENAME", (p) => (p as TSQLParser).parseDatename()],
    ["DATETRUNC", (p) => (p as TSQLParser).parseDatetrunc()],
    ["EOMONTH", (p) => (p as TSQLParser).parseEomonth()],
  ])

  static override QUERY_MODIFIER_PARSERS: typeof Parser.QUERY_MODIFIER_PARSERS =
    {
      ...Parser.QUERY_MODIFIER_PARSERS,
      [TokenType.OPTION]: (p) => ["options", (p as TSQLParser).parseOptions()],
      [TokenType.FOR]: (p) => ["for_", (p as TSQLParser).parseFor()],
    }

  private tryParseIdent(): exp.Identifier | undefined {
    if (this.isEnd() || this.current.tokenType === TokenType.SEMICOLON)
      return undefined
    if (this.match(TokenType.AT)) {
      const name = this.advance()
      return new exp.Identifier({ this: `@${name.text}` })
    }
    try {
      return this.parseIdentifier()
    } catch {
      return undefined
    }
  }

  private parseDatepart(): exp.Extract {
    // Parse the date part (e.g., 'dd', 'mm', 'yy')
    // If it's a string literal, parse as bitwise to get the literal
    const partExpr = this.parseBitwise()

    let partName: string

    if (partExpr instanceof exp.Literal && partExpr.args.is_string) {
      // String literal like "dd" - extract the value
      partName = String(partExpr.args.this)
    } else if (partExpr instanceof exp.Column && !partExpr.args.table) {
      // Bare identifier like dd - use the name preserving case
      const ident = partExpr.args.this
      partName = ident instanceof exp.Identifier ? ident.name : String(ident)
    } else if (partExpr instanceof exp.Var) {
      partName = partExpr.name
    } else {
      // Fallback - shouldn't happen but handle it
      partName = "DAY"
    }

    // Map TSQL abbreviations to canonical UPPERCASE form
    // Preserve case for full names (month stays month, MONTH stays MONTH)
    const mapped = DATE_PART_MAPPING[partName.toUpperCase()]
    if (mapped) {
      partName = mapped // Abbreviations get uppercase canonical form
    }
    // else: keep original case for full names

    // Consume comma
    this.expect(TokenType.COMMA)

    // Parse the date expression
    const expression = this.parseBitwise()

    // Close paren
    this.expect(TokenType.R_PAREN)

    // Return Extract with part name (uppercase if abbreviation, original case if full name)
    return new exp.Extract({
      this: new exp.Var({ this: partName }),
      expression,
    })
  }

  private parseDateadd(): exp.DateAdd {
    // DATEADD(datepart, number, date)
    const unitExpr = this.parseBitwise()
    let unit = this.extractDatePart(unitExpr)

    // Map using DATE_DELTA_INTERVAL
    const mapped = DATE_DELTA_INTERVAL[unit.toLowerCase()]
    if (mapped) {
      unit = mapped
    }

    this.expect(TokenType.COMMA)
    const number = this.parseBitwise()
    this.expect(TokenType.COMMA)
    const date = this.parseBitwise()
    this.expect(TokenType.R_PAREN)

    return new exp.DateAdd({
      this: date,
      expression: number,
      unit: new exp.Var({ this: unit }),
    })
  }

  private parseDatediff(): exp.DateDiff {
    return this.parseDatediffInternal(false)
  }

  private parseDatediffBig(): exp.DateDiff {
    return this.parseDatediffInternal(true)
  }

  private parseDatediffInternal(bigInt: boolean): exp.DateDiff {
    // DATEDIFF(datepart, startdate, enddate)
    const unitExpr = this.parseBitwise()
    let unit = this.extractDatePart(unitExpr)

    // Map using DATE_DELTA_INTERVAL
    const mapped = DATE_DELTA_INTERVAL[unit.toLowerCase()]
    if (mapped) {
      unit = mapped
    }

    this.expect(TokenType.COMMA)
    let startDate: exp.Expression = this.parseBitwise()
    this.expect(TokenType.COMMA)
    const endDate = this.parseBitwise()
    this.expect(TokenType.R_PAREN)

    // Convert integer start dates to actual date strings (TSQL epoch: 1900-01-01)
    if (startDate instanceof exp.Literal && !startDate.isString) {
      const numVal = Number(startDate.args.this)
      if (Number.isInteger(numVal)) {
        startDate = exp.Literal.string(addDays(DEFAULT_START_DATE, numVal))
      } else {
        // Float values can't be converted - return as-is
        return new exp.DateDiff({
          this: endDate,
          expression: startDate,
          unit: new exp.Var({ this: unit }),
          big_int: bigInt,
        })
      }
    }

    // TSQL: DATEDIFF(unit, start, end)
    // Standard DateDiff: DateDiff(this=end, expression=start, unit=unit)
    return new exp.DateDiff({
      this: new exp.TimeStrToTime({ this: endDate }),
      expression: new exp.TimeStrToTime({ this: startDate }),
      unit: new exp.Var({ this: unit }),
      big_int: bigInt,
    })
  }

  private parseDatename(): exp.TimeToStr {
    // DATENAME(datepart, date)
    const partExpr = this.parseBitwise()
    const partName = this.extractDatePart(partExpr)

    this.expect(TokenType.COMMA)
    const date = this.parseBitwise()
    this.expect(TokenType.R_PAREN)

    // Return TimeToStr with the date part as format
    // Python uses _build_formatted_time with full_format_mapping=True
    return new exp.TimeToStr({
      this: date,
      format: new exp.Var({ this: partName }),
    })
  }

  private parseEomonth(): exp.LastDay {
    // EOMONTH(start_date [, month_to_add])
    const startDate = this.parseBitwise()

    // Wrap in TsOrDsToDate to ensure it's treated as a date
    const dateWrapped = new exp.TsOrDsToDate({ this: startDate })

    let result: exp.Expression = dateWrapped

    if (this.match(TokenType.COMMA)) {
      const monthsToAdd = this.parseBitwise()
      // Build DateAdd(this=dateWrapped, expression=monthsToAdd, unit=month)
      result = new exp.DateAdd({
        this: dateWrapped,
        expression: monthsToAdd,
        unit: new exp.Var({ this: "month" }),
      })
    }

    this.expect(TokenType.R_PAREN)

    return new exp.LastDay({ this: result })
  }

  private parseDatetrunc(): exp.TimestampTrunc {
    // DATETRUNC(datepart, date)
    const unit = this.parseBitwise()
    this.expect(TokenType.COMMA)
    let thisExpr: exp.Expression = this.parseBitwise()
    this.expect(TokenType.R_PAREN)

    if (thisExpr instanceof exp.Literal && thisExpr.isString) {
      thisExpr = new exp.Cast({
        this: thisExpr,
        to: new exp.DataType({ this: "DATETIME2" }),
      })
    }

    return new exp.TimestampTrunc({ this: thisExpr, unit })
  }

  private extractDatePart(expr: exp.Expression): string {
    if (expr instanceof exp.Literal && expr.args.is_string) {
      return String(expr.args.this)
    }
    if (expr instanceof exp.Column && !expr.args.table) {
      const ident = expr.args.this
      return ident instanceof exp.Identifier ? ident.name : String(ident)
    }
    if (expr instanceof exp.Var) {
      return expr.name
    }
    return "day"
  }

  protected override parseCommitOrRollback(): exp.Commit | exp.Rollback {
    const isRollback = this.prev.tokenType === TokenType.ROLLBACK

    this.matchText("TRAN", "TRANSACTION")
    const this_ = this.tryParseIdent()

    if (isRollback) {
      return new exp.Rollback({ this: this_ })
    }

    let durability: boolean | undefined
    if (this.matchText("WITH")) {
      this.match(TokenType.L_PAREN)
      if (this.matchText("DELAYED_DURABILITY")) {
        this.match(TokenType.EQ)
        if (this.matchText("ON")) {
          durability = true
        } else {
          this.matchText("OFF")
          durability = false
        }
      }
      this.match(TokenType.R_PAREN)
    }

    return new exp.Commit({ this: this_, durability })
  }

  protected override parseTransaction(): exp.Transaction {
    this.matchText("TRAN", "TRANSACTION")
    const this_ = this.tryParseIdent()

    let mark: exp.Expression | undefined
    if (this.matchText("WITH") && this.matchText("MARK")) {
      mark = this.parsePrimary()
    }

    return new exp.Transaction({ this: this_, mark })
  }

  protected override parseUpdate(): exp.Update {
    const update = super.parseUpdate()
    const options = this.parseOptionsCoreIfPresent()
    if (options) update.set("options", options)
    return update
  }

  private parseOptions(): exp.Expression[] | undefined {
    // OPTION token already consumed by QUERY_MODIFIER_PARSERS loop
    return this.parseOptionsCore()
  }

  private parseOptionsCoreIfPresent(): exp.Expression[] | undefined {
    if (!this.match(TokenType.OPTION)) return undefined
    return this.parseOptionsCore()
  }

  private parseOptionsCore(): exp.Expression[] {
    const parseOption = (): exp.Expression | undefined => {
      const option = this.parseVarFromOptions(OPTIONS)
      if (!option) return undefined
      this.match(TokenType.EQ)
      let expression: exp.Expression | undefined
      if (
        this.current.tokenType !== TokenType.R_PAREN &&
        this.current.tokenType !== TokenType.COMMA
      ) {
        expression = this.parsePrimary()
      }
      return new exp.QueryOption({
        this: option,
        expression,
      })
    }

    this.expect(TokenType.L_PAREN)
    const result = this.parseCSV(parseOption).filter(
      (e): e is exp.Expression => e !== undefined,
    )
    this.expect(TokenType.R_PAREN)
    return result
  }

  private parseXmlKeyValueOption(): exp.XMLKeyValueOption {
    const this_ = this.parsePrimary()
    let expression: exp.Expression | undefined
    if (this.current.tokenType === TokenType.L_PAREN) {
      this.expect(TokenType.L_PAREN)
      expression = this.parsePrimary()
      this.expect(TokenType.R_PAREN)
    }
    return new exp.XMLKeyValueOption({ this: this_, expression })
  }

  private parseFor(): exp.Expression[] | undefined {
    // FOR token already consumed by QUERY_MODIFIER_PARSERS loop
    if (!this.matchText("XML")) return undefined

    const parseForXml = (): exp.Expression => {
      return new exp.QueryOption({
        this:
          this.parseVarFromOptions(XML_OPTIONS, false) ||
          this.parseXmlKeyValueOption(),
      })
    }

    return this.parseCSV(parseForXml).filter(
      (e): e is exp.Expression => e !== undefined,
    )
  }
}

export class TSQLGenerator extends Generator {
  static override HEX_START: string | null = "0x"
  static override HEX_END: string | null = ""
  protected override ALTER_SET_TYPE = ""

  static override FEATURES = {
    ...Generator.FEATURES,
    CONCAT_COALESCE: true,
    ENSURE_BOOLS: true,
    TYPED_DIVISION: true,
  }

  static override EXPRESSIONS_WITHOUT_NESTED_CTES: Set<ExpressionClass> =
    new Set([
      exp.Create,
      exp.Delete,
      exp.Insert,
      exp.Intersect,
      exp.Except,
      exp.Merge,
      exp.Select,
      exp.Subquery,
      exp.Union,
      exp.Update,
    ])

  static override TRANSFORMS: Map<ExpressionClass, Transform> = new Map<
    ExpressionClass,
    Transform
  >([
    ...Generator.TRANSFORMS,
    [exp.Select, preprocess([eliminateQualify, eliminateSemiAndAntiJoins])],
    [exp.CurrentDate, () => "GETDATE()"],
    [exp.CurrentTimestamp, () => "GETDATE()"],
    [
      exp.TimestampTrunc,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.TimestampTrunc
        return gen.funcCall("DATETRUNC", [
          expr.args.unit as exp.Expression,
          expr.args.this as exp.Expression,
        ])
      },
    ],
    [
      exp.ArrayToString,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.ArrayToString
        const args: exp.Expression[] = [
          expr.args.this as exp.Expression,
          expr.args.expression as exp.Expression,
        ]
        return gen.funcCall("STRING_AGG", args)
      },
    ],
    [exp.DateAdd, dateDeltaSql("DATEADD")],
    [exp.TsOrDsAdd, dateDeltaSql("DATEADD", true)],
    [
      exp.LastDay,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.LastDay
        return gen.funcCall("EOMONTH", [expr.args.this as exp.Expression])
      },
    ],
    [
      exp.TimeStrToTime,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.TimeStrToTime
        const zone = expr.args.zone
        const dataType = zone ? "DATETIMEOFFSET" : "DATETIME2"
        const sql = `CAST(${gen.sql(expr.args.this as exp.Expression)} AS ${dataType})`
        if (zone) {
          return gen.sql(
            new exp.AtTimeZone({ this: sql, zone: exp.Literal.string("UTC") }),
          )
        }
        return sql
      },
    ],
    [
      exp.DateDiff,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.DateDiff
        const funcName = expr.args.big_int ? "DATEDIFF_BIG" : "DATEDIFF"
        const unit = unitToVar(expr)
        // TSQL: DATEDIFF(unit, startdate, enddate)
        // DateDiff AST: this=enddate, expression=startdate
        return gen.funcCall(funcName, [
          new exp.Var({ this: unit }),
          expr.args.expression as exp.Expression,
          expr.args.this as exp.Expression,
        ])
      },
    ],
    [exp.TsOrDsDiff, dateDeltaSql("DATEDIFF")],
    [
      exp.TimeToStr,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.TimeToStr
        const format = expr.args.format
        // DATENAME uses format as date part (when it's a Var)
        if (format instanceof exp.Var) {
          return gen.funcCall("DATENAME", [
            format,
            expr.args.this as exp.Expression,
          ])
        }
        // FORMAT(this, format[, culture])
        const thisExpr = gen.sql(expr.args.this as exp.Expression)
        const fmtSql = gen.formatTimeStr(expr)
        return `FORMAT(${thisExpr}, ${fmtSql})`
      },
    ],
  ])

  static override INVERSE_TIME_MAPPING: Map<string, string> = new Map([
    ["%-H", "H"],
    ["%-I", "h"],
    ["%-M", "m"],
    ["%-S", "s"],
    ["%-d", "d"],
    ["%-m", "M"],
    ["%A", "dddd"],
    ["%B", "MMMM"],
    ["%H", "HH"],
    ["%I", "hh"],
    ["%M", "mm"],
    ["%S", "ss"],
    ["%V", "iso_week"],
    ["%W", "wk"],
    ["%Y", "yyyy"],
    ["%b", "MMM"],
    ["%d", "dd"],
    ["%f", "ffffff"],
    ["%h", "hour"],
    ["%j", "dayofyear"],
    ["%m", "MM"],
    ["%w", "dw"],
    ["%y", "yy"],
  ])

  static override TYPE_MAPPING: Map<string, string> = (() => {
    const mapping = new Map([
      ...Generator.TYPE_MAPPING,
      ["BOOLEAN", "BIT"],
      ["DATETIME2", "DATETIME2"],
      ["DECIMAL", "NUMERIC"],
      ["DOUBLE", "FLOAT"],
      ["INT", "INTEGER"],
      ["ROWVERSION", "ROWVERSION"],
      ["TEXT", "VARCHAR(MAX)"],
      ["TIMESTAMP", "DATETIME2"],
      ["TIMESTAMPNTZ", "DATETIME2"],
      ["TIMESTAMPTZ", "DATETIMEOFFSET"],
      ["SMALLDATETIME", "SMALLDATETIME"],
      ["UTINYINT", "TINYINT"],
      ["VARIANT", "SQL_VARIANT"],
      ["UUID", "UNIQUEIDENTIFIER"],
    ])
    // Remove NCHAR and NVARCHAR - TSQL keeps them as native types
    mapping.delete("NCHAR")
    mapping.delete("NVARCHAR")
    return mapping
  })()

  protected override quoteIdentifier(name: string): string {
    return `[${name.replace(/\]/g, "]]")}]`
  }

  protected override shouldQuote(name: string): boolean {
    if (name.startsWith("@")) return false
    return super.shouldQuote(name)
  }

  protected override transaction_sql(expression: exp.Transaction): string {
    const this_ = this.sql(expression, "this")
    const thisSql = this_ ? ` ${this_}` : ""
    const mark = this.sql(expression, "mark")
    const markSql = mark ? ` WITH MARK ${mark}` : ""
    return `BEGIN TRANSACTION${thisSql}${markSql}`
  }

  protected override commit_sql(expression: exp.Commit): string {
    const this_ = this.sql(expression, "this")
    const thisSql = this_ ? ` ${this_}` : ""
    const durability = expression.args.durability
    const durabilitySql =
      durability !== undefined
        ? ` WITH (DELAYED_DURABILITY = ${durability ? "ON" : "OFF"})`
        : ""
    return `COMMIT TRANSACTION${thisSql}${durabilitySql}`
  }

  protected override rollback_sql(expression: exp.Rollback): string {
    const this_ = this.sql(expression, "this")
    const thisSql = this_ ? ` ${this_}` : ""
    return `ROLLBACK TRANSACTION${thisSql}`
  }

  protected override queryoption_sql(expression: exp.QueryOption): string {
    const option = this.sql(expression, "this")
    const value = this.sql(expression, "expression")
    if (value) {
      const eqSign = OPTIONS_THAT_REQUIRE_EQUAL.has(option) ? "= " : ""
      return `${option} ${eqSign}${value}`
    }
    return option
  }

  protected override optionsModifier(expression: exp.Expression): string {
    const options = expression.args.options as exp.Expression[] | undefined
    if (!options || options.length === 0) return ""
    return ` OPTION(${this.expressions(options)})`
  }

  protected override boolean_sql(expression: exp.Boolean): string {
    // In certain contexts (BIT_TYPES), TRUE/FALSE render as 1/0
    // Otherwise, render as (1 = 1) or (1 = 0)
    const parent = expression.parent
    const parentType = parent?.constructor as ExpressionClass | undefined

    if (parentType && BIT_TYPES.has(parentType)) {
      return expression.args.this ? "1" : "0"
    }

    // Also check if we're in a VALUES clause
    const ancestor = expression.findAncestor(exp.Values, exp.Select)
    if (ancestor instanceof exp.Values) {
      return expression.args.this ? "1" : "0"
    }

    return expression.args.this ? "(1 = 1)" : "(1 = 0)"
  }

  protected override is_sql(expression: exp.Is): string {
    // IS TRUE/IS FALSE convert to = 1/= 0 in TSQL
    if (expression.args.expression instanceof exp.Boolean) {
      return this.binary_sql(expression, "=")
    }
    return this.binary_sql(expression, "IS")
  }

  protected override alter_sql(expression: exp.Alter): string {
    const actions = expression.args.actions as exp.Expression[] | undefined
    const action = actions?.[0]
    if (action instanceof exp.AlterRename) {
      const table = action.args.this
      const tableName = table instanceof exp.Table ? table.name : ""
      return `EXEC sp_rename '${this.sql(expression, "this")}', '${tableName}'`
    }
    return super.alter_sql(expression)
  }

  protected override setitem_sql(expression: exp.SetItem): string {
    const thisExpr = expression.args.this
    if (
      thisExpr instanceof exp.EQ &&
      !(thisExpr.args.this instanceof exp.Parameter)
    ) {
      return `${this.sql(thisExpr.args.this as exp.Expression)} ${this.sql(thisExpr.args.expression as exp.Expression)}`
    }
    return super.setitem_sql(expression)
  }

  protected override extract_sql(expression: exp.Extract): string {
    const part = expression.args.this
    let partName = ""

    if (part instanceof exp.Var) {
      partName = part.name
    } else if (part instanceof exp.Expression) {
      partName = this.sql(part)
    }

    // Apply unmapping for reverse conversions (WEEKISO -> ISO_WEEK, etc.)
    const unmapped = DATE_PART_UNMAPPING[partName.toUpperCase()]
    if (unmapped) {
      partName = unmapped
    }

    const dateExpr = expression.args.expression
    if (!(dateExpr instanceof exp.Expression)) {
      throw new Error("Extract expression requires date expression")
    }

    return this.funcCall("DATEPART", [
      new exp.Var({ this: partName }),
      dateExpr,
    ])
  }
}

export class TSQLDialect extends Dialect {
  static override readonly name = "tsql"
  static override TYPED_DIVISION = true
  static override CONCAT_COALESCE = true
  protected static override ParserClass = TSQLParser
  protected static override GeneratorClass = TSQLGenerator
}

// Register dialect
Dialect.register(TSQLDialect)
