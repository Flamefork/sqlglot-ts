/**
 * DuckDB dialect
 */

import { Dialect } from "../dialect.js"
import type { ExpressionClass } from "../expression-base.js"
import * as exp from "../expressions.js"
import { Generator } from "../generator.js"
import { type FunctionBuilder, Parser } from "../parser.js"
import { TokenType, Tokenizer } from "../tokens.js"
import {
  preprocess,
  regexpReplaceGlobalModifier,
  renameFunc,
  unqualifyColumns,
} from "../transforms.js"

type Transform = (generator: Generator, expression: exp.Expression) => string

const TIMEZONE_PATTERN = /:\d{2}.*?[+-]\d{2}(?::\d{2})?/

function implicitDatetimeCast(
  arg: exp.Expression,
  type = "DATE",
): exp.Expression {
  if (arg instanceof exp.Literal && arg.isString) {
    const ts = String(arg.value)
    let castType = type
    if (type === "DATE" && ts.includes(":")) {
      castType = TIMEZONE_PATTERN.test(ts) ? "TIMESTAMPTZ" : "TIMESTAMP"
    }
    return new exp.Cast({ this: arg, to: new exp.DataType({ this: castType }) })
  }
  return arg
}

function tryExtractInt(expr: exp.Expression): number | undefined {
  if (expr instanceof exp.Literal && !expr.isString) {
    const n = Number(expr.value)
    if (Number.isInteger(n)) return n
  }
  if (expr instanceof exp.Neg) {
    const inner = expr.args.this
    if (inner instanceof exp.Literal && !inner.isString) {
      const n = Number(inner.value)
      if (Number.isInteger(n)) return -n
    }
  }
  return undefined
}

const FLOAT_TYPES = new Set(["FLOAT", "REAL", "DOUBLE"])
const REAL_TYPES = new Set([...FLOAT_TYPES, "DECIMAL"])

function bitwiseAggSql(funcName: string): Transform {
  return (gen: Generator, e: exp.Expression) => {
    let arg = (e as exp.Func).args.this as exp.Expression
    if (arg instanceof exp.Cast) {
      const typeStr =
        arg.args.to instanceof exp.Expression
          ? arg.args.to.text("this").toUpperCase()
          : ""
      if (REAL_TYPES.has(typeStr)) {
        if (FLOAT_TYPES.has(typeStr)) {
          arg = new exp.Anonymous({ this: "ROUND", expressions: [arg] })
        }
        arg = new exp.Cast({ this: arg, to: new exp.DataType({ this: "INT" }) })
      }
    }
    return gen.funcCall(funcName, [arg])
  }
}

function arrayCompactSql(gen: Generator, e: exp.Expression): string {
  const expression = e as exp.ArrayCompact
  const lambdaId = exp.toIdentifier("_u")
  const cond = new exp.Not({
    this: new exp.Is({ this: lambdaId, expression: new exp.Null({}) }),
  })
  return gen.sql(
    new exp.ArrayFilter({
      this: expression.args.this,
      expression: new exp.Lambda({ this: cond, expressions: [lambdaId] }),
    }),
  )
}

// renameFunc is imported from transforms.ts

function boolXorSql(gen: Generator, e: exp.Expression): string {
  const expr = e as exp.Xor
  const a = gen.sql(expr.left)
  const b = gen.sql(expr.right)
  return `(${a} AND (NOT ${b})) OR ((NOT ${a}) AND ${b})`
}

// SEQ function helper: transpile Snowflake SEQ1/SEQ2/SEQ4/SEQ8 to DuckDB
const SEQ_BASE = "(ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1)"

function seqSql(byteWidth: number): Transform {
  return (_gen: Generator, e: exp.Expression) => {
    const bits = byteWidth * 8
    const maxVal = (BigInt(2) ** BigInt(bits)).toString()
    const thisArg = e.args.this
    const signedVal =
      thisArg instanceof exp.Literal ? String(thisArg.value) : undefined
    const isSigned = signedVal === "1"

    if (isSigned) {
      const half = (BigInt(2) ** BigInt(bits - 1)).toString()
      return `(CASE WHEN ${SEQ_BASE} % ${maxVal} >= ${half} THEN ${SEQ_BASE} % ${maxVal} - ${maxVal} ELSE ${SEQ_BASE} % ${maxVal} END)`
    }
    return `${SEQ_BASE} % ${maxVal}`
  }
}

// CEIL/FLOOR with scale parameter: FUNC(x, n) -> ROUND(FUNC(x * 10^n) / 10^n, n)
function ceilFloorSql(gen: Generator, e: exp.Expression): string {
  const expr = e as exp.Ceil | exp.Floor
  const decimals = expr.args.decimals as exp.Expression | undefined
  const toArg = expr.args.to

  if (decimals && !toArg) {
    const thisExpr = expr.args.this as exp.Expression
    const wrappedThis =
      thisExpr instanceof exp.Binary
        ? new exp.Paren({ this: thisExpr })
        : thisExpr

    let nInt: exp.Expression = decimals
    if (!decimals.is_int && !(decimals instanceof exp.DataType)) {
      nInt = new exp.Cast({
        this: decimals,
        to: new exp.DataType({ this: "INT" }),
      })
    }

    const pow = gen.funcCall("POWER", [exp.Literal.number("10"), nInt])
    const funcName = e instanceof exp.Ceil ? "CEIL" : "FLOOR"
    const scaled = `${funcName}(${gen.sql(wrappedThis)} * ${pow})`
    const divided = `${scaled} / ${pow}`
    const roundedDecimals =
      nInt === decimals ? gen.sql(decimals) : gen.sql(nInt)
    return `ROUND(${divided}, ${roundedDecimals})`
  }

  const func = expr.key === "ceil" ? "CEIL" : "FLOOR"
  return `${func}(${gen.sql(expr.args.this as exp.Expression)})`
}

// Days of week to ISO 8601 day-of-week numbers
const WEEK_START_DAY_TO_DOW: Record<string, number> = {
  MONDAY: 1,
  TUESDAY: 2,
  WEDNESDAY: 3,
  THURSDAY: 4,
  FRIDAY: 5,
  SATURDAY: 6,
  SUNDAY: 7,
}

// NEXT_DAY/PREVIOUS_DAY: transpile to date arithmetic with ISODOW
function dayNavigationSql(gen: Generator, e: exp.Expression): string {
  const expression = e as exp.NextDay | exp.PreviousDay
  const dateExpr = expression.args.this as exp.Expression
  const dayNameExpr = expression.args.expression as exp.Expression
  const isodowCall = new exp.Anonymous({
    this: "ISODOW",
    expressions: [dateExpr],
  })

  let targetDow: exp.Expression
  if (dayNameExpr instanceof exp.Literal && dayNameExpr.isString) {
    const dayNameStr = String(dayNameExpr.value).toUpperCase()
    const matchingDay = Object.keys(WEEK_START_DAY_TO_DOW).find((day) =>
      day.startsWith(dayNameStr),
    )
    if (matchingDay) {
      targetDow = exp.Literal.number(WEEK_START_DAY_TO_DOW[matchingDay]!)
    } else {
      const name =
        expression instanceof exp.NextDay ? "NEXT_DAY" : "PREVIOUS_DAY"
      return gen.funcCall(name, [dateExpr, dayNameExpr])
    }
  } else {
    const upperDayName = new exp.Upper({ this: dayNameExpr })
    targetDow = new exp.Case({
      ifs: Object.entries(WEEK_START_DAY_TO_DOW).map(
        ([day, dowNum]) =>
          new exp.If({
            this: new exp.Anonymous({
              this: "STARTS_WITH",
              expressions: [
                upperDayName.copy(),
                exp.Literal.string(day.substring(0, 2)),
              ],
            }),
            true: exp.Literal.number(dowNum),
          }),
      ),
    })
  }

  const isNext = e instanceof exp.NextDay
  // Build: paren(a - b + 6) % 7 + 1
  // Python's operator overloads wrap left operand in Paren, so we replicate that
  const sub = isNext
    ? new exp.Sub({ this: targetDow, expression: isodowCall })
    : new exp.Sub({ this: isodowCall, expression: targetDow })
  const plus6 = new exp.Add({
    this: new exp.Paren({ this: sub }),
    expression: exp.Literal.number(6),
  })
  const mod7 = new exp.Mod({
    this: new exp.Paren({ this: plus6 }),
    expression: exp.Literal.number(7),
  })
  const plus1 = new exp.Add({
    this: new exp.Paren({ this: mod7 }),
    expression: exp.Literal.number(1),
  })

  const op = isNext ? "+" : "-"
  const interval = new exp.Interval({
    this: plus1,
    unit: new exp.Var({ this: "DAY" }),
  })
  return `CAST(${gen.sql(dateExpr)} ${op} ${gen.sql(interval)} AS DATE)`
}

// CORR: null_on_zero_variance -> CASE WHEN ISNAN(CORR(a,b)) THEN NULL ELSE CORR(a,b) END
function corrSql(gen: Generator, e: exp.Expression): string {
  const expr = e as exp.Corr
  if (!expr.args.null_on_zero_variance) {
    return gen.funcCall("CORR", [
      expr.args.this as exp.Expression,
      expr.args.expression as exp.Expression,
    ])
  }
  const corrCall = gen.funcCall("CORR", [
    expr.args.this as exp.Expression,
    expr.args.expression as exp.Expression,
  ])
  return `CASE WHEN ISNAN(${corrCall}) THEN NULL ELSE ${corrCall} END`
}

// GroupConcat (LISTAGG) with ORDER BY inside function args (not WITHIN GROUP)
function groupconcatSql(gen: Generator, e: exp.Expression): string {
  const expression = e as exp.GroupConcat
  let thisExpr = expression.args.this as exp.Expression
  const separator = expression.args.separator as exp.Expression | undefined
  const separatorSql = separator ? gen.sql(separator) : "''"

  // Extract order from within the expression
  let order: exp.Order | undefined
  if (
    thisExpr instanceof exp.Order &&
    thisExpr.args.this instanceof exp.Expression
  ) {
    order = thisExpr
    thisExpr = order.args.this as exp.Expression
  }

  let argsSql = gen.sql(thisExpr)
  if (separatorSql) {
    argsSql += `, ${separatorSql}`
  }
  if (order) {
    const orderSql = gen.sql(new exp.Order({ expressions: order.expressions }))
    argsSql += ` ${orderSql}`
  }

  return `LISTAGG(${argsSql})`
}

// SPACE -> REPEAT(' ', CAST(n AS BIGINT))
function spaceSql(gen: Generator, e: exp.Expression): string {
  const expr = e as exp.Space
  const n = gen.sql(
    new exp.Cast({
      this: expr.args.this as exp.Expression,
      to: new exp.DataType({ this: "BIGINT" }),
    }),
  )
  return `REPEAT(' ', ${n})`
}

// GetExtract: array -> bracket[idx+1], map -> bracket[key], else -> JSON arrow
function getExtractSql(gen: Generator, e: exp.Expression): string {
  const expression = e as exp.GetExtract
  const thisExpr = expression.args.this as exp.Expression
  const indexExpr = expression.args.expression as exp.Expression

  // Heuristic: if this is a Cast to ARRAY or MAP, or an Array literal, use bracket access
  if (thisExpr instanceof exp.Cast) {
    const toType = thisExpr.args.to as exp.DataType | undefined
    if (toType) {
      const typeName = String(toType.args.this ?? "").toUpperCase()
      if (typeName === "MAP") {
        return `${gen.sql(thisExpr)}[${gen.sql(indexExpr)}]`
      }
      if (typeName === "ARRAY") {
        // Array is 0-based in Snowflake, 1-based in DuckDB
        if (indexExpr instanceof exp.Literal && indexExpr.is_int) {
          return `${gen.sql(thisExpr)}[${Number(indexExpr.value) + 1}]`
        }
        return `${gen.sql(thisExpr)}[${gen.sql(indexExpr)} + 1]`
      }
    }
  }
  if (thisExpr instanceof exp.Array) {
    // Array literal - 0-based to 1-based
    if (indexExpr instanceof exp.Literal && indexExpr.is_int) {
      return `${gen.sql(thisExpr)}[${Number(indexExpr.value) + 1}]`
    }
    return `${gen.sql(thisExpr)}[${gen.sql(indexExpr)} + 1]`
  }

  // Default: JSON arrow with json path
  if (indexExpr instanceof exp.Literal && indexExpr.isString) {
    return `${gen.sql(thisExpr)} -> '$.${indexExpr.value}'`
  }
  return `${gen.sql(thisExpr)} -> '$[${gen.sql(indexExpr)}]'`
}

// ARRAYS_ZIP: Snowflake pads to longest, DuckDB truncates to shortest
// Transpile to CASE WHEN null_check ... LIST_TRANSFORM(RANGE(...), __i -> struct)
function arraysZipSql(gen: Generator, e: exp.Expression): string {
  const expression = e as exp.ArraysZip
  const args = expression.expressions

  if (!args.length) {
    return "[MAP([], [])]"
  }

  // Build parts
  const nullChecks = args.map((arg) => `${gen.sql(arg)} IS NULL`).join(" OR ")
  const emptyChecks = args
    .map((arg) => `LENGTH(${gen.sql(arg)}) = 0`)
    .join(" AND ")

  const emptyStructFields = args.map((_, i) => `'$${i + 1}': NULL`).join(", ")

  const lengths = args.map((arg) => `LENGTH(${gen.sql(arg)})`)
  const nullLengthChecks = lengths.map((l) => `${l} IS NULL`).join(" OR ")
  const maxLen =
    lengths.length === 1
      ? lengths[0]
      : `CASE WHEN ${nullLengthChecks} THEN NULL ELSE GREATEST(${lengths.join(", ")}) END`

  const transformFields = args
    .map((arg, i) => `'$${i + 1}': COALESCE(${gen.sql(arg)}, [])[__i + 1]`)
    .join(", ")

  return (
    `CASE WHEN ${nullChecks} THEN NULL` +
    ` WHEN ${emptyChecks} THEN [{${emptyStructFields}}]` +
    ` ELSE LIST_TRANSFORM(RANGE(0, ${maxLen}), __i -> {${transformFields}}) END`
  )
}

const DATETIME_ADD_TYPES: Set<ExpressionClass> = new Set([
  exp.DateAdd,
  exp.TimeAdd,
  exp.DatetimeAdd,
  exp.TsOrDsAdd,
  exp.TimestampAdd,
])

function extractUnit(e: exp.Expression): string {
  const unit = (e as exp.Func).args.unit
  if (typeof unit === "string") return unit.toUpperCase()
  if (unit instanceof exp.Literal) return String(unit.value).toUpperCase()
  if (unit instanceof exp.Expression) {
    const inner = unit.args.this
    if (typeof inner === "string") return inner.toUpperCase()
    if (inner instanceof exp.Expression)
      return String(inner.args.this ?? "").toUpperCase()
  }
  return "DAY"
}

function dateDeltaToBinaryIntervalOp(
  gen: Generator,
  e: exp.Expression,
): string {
  const op = DATETIME_ADD_TYPES.has(e.constructor as ExpressionClass)
    ? "+"
    : "-"

  let thisExpr = e.args.this as exp.Expression

  if (e instanceof exp.TsOrDsAdd) {
    const returnType = e.args.return_type
    const toType =
      returnType instanceof exp.DataType ? returnType.text("this") : "DATE"
    thisExpr = new exp.Cast({
      this: thisExpr,
      to: new exp.DataType({ this: toType }),
    })
  } else if (thisExpr instanceof exp.Literal && thisExpr.isString) {
    const toType =
      e instanceof exp.DatetimeAdd || e instanceof exp.DatetimeSub
        ? "DATETIME"
        : "DATE"
    thisExpr = new exp.Cast({
      this: thisExpr,
      to: new exp.DataType({ this: toType }),
    })
  }

  const unit = extractUnit(e)
  const expr = e.args.expression as exp.Expression
  const interval =
    expr instanceof exp.Interval ? expr : new exp.Interval({ this: expr, unit })

  return `${gen.sql(thisExpr)} ${op} ${gen.sql(interval)}`
}

export class DuckDBParser extends Parser {
  static override STATEMENT_PARSERS: Map<
    TokenType,
    (parser: Parser) => exp.Expression | undefined
  > = new Map([
    ...Parser.STATEMENT_PARSERS,
    [TokenType.ATTACH, (p) => (p as DuckDBParser).parseAttachDetach(true)],
    [TokenType.DETACH, (p) => (p as DuckDBParser).parseAttachDetach(false)],
    [TokenType.INSTALL, (p) => (p as DuckDBParser).parseInstall()],
    [TokenType.FORCE, (p) => (p as DuckDBParser).parseForce()],
    [TokenType.SHOW, (p) => (p as DuckDBParser).parseShow()],
    [TokenType.PIVOT, (p) => (p as DuckDBParser).parseSimplifiedPivot(false)],
    [TokenType.UNPIVOT, (p) => (p as DuckDBParser).parseSimplifiedPivot(true)],
    [TokenType.SUMMARIZE, (p) => (p as DuckDBParser).parseSummarize()],
  ])

  static override FUNCTION_PARSERS: Map<
    string,
    (parser: Parser) => exp.Expression
  > = new Map([
    ...Parser.FUNCTION_PARSERS,
    ["GROUP_CONCAT", (p) => (p as DuckDBParser).parseStringAgg()],
    ["LISTAGG", (p) => (p as DuckDBParser).parseStringAgg()],
    ["STRINGAGG", (p) => (p as DuckDBParser).parseStringAgg()],
  ])

  static override SET_PARSERS = new Map([
    ...Parser.SET_PARSERS,
    ["VARIABLE", (p: Parser) => p.parseSetItemAssignment("VARIABLE")],
  ])

  protected override parseStatement(): exp.Expression | undefined {
    if (
      this.current.tokenType === TokenType.VAR &&
      this.current.text.toUpperCase() === "RESET"
    ) {
      this.advance()
      return this.parseAsCommand("RESET")
    }
    return super.parseStatement()
  }

  static override BITWISE: Map<TokenType, ExpressionClass> = new Map([
    ...(() => {
      const m = new Map(Parser.BITWISE)
      m.delete(TokenType.CARET)
      return m
    })(),
  ])

  static override EXPONENT: Map<TokenType, ExpressionClass> = new Map([
    [TokenType.CARET, exp.Pow],
    [TokenType.DSTAR, exp.Pow],
  ])

  static override RANGE_PARSERS: Map<
    TokenType,
    (parser: Parser, left: exp.Expression) => exp.Expression
  > = new Map([
    ...Parser.RANGE_PARSERS,
    [
      TokenType.RLIKE,
      (p, left) => p.parseLikePattern(left, exp.RegexpFullMatch),
    ],
    [
      TokenType.TILDE,
      (p, left) => p.parseLikePattern(left, exp.RegexpFullMatch),
    ],
  ])

  // DuckDB function name mappings
  static override FUNCTIONS: Map<string, FunctionBuilder> = new Map([
    ...Parser.FUNCTIONS,
    [
      "ANY_VALUE",
      (args: exp.Expression[]) =>
        new exp.IgnoreNulls({ this: new exp.AnyValue({ this: args[0] }) }),
    ],
    [
      "BIT_OR",
      (args: exp.Expression[]) => new exp.BitwiseOrAgg({ this: args[0] }),
    ],
    [
      "BIT_AND",
      (args: exp.Expression[]) => new exp.BitwiseAndAgg({ this: args[0] }),
    ],
    [
      "BIT_XOR",
      (args: exp.Expression[]) => new exp.BitwiseXorAgg({ this: args[0] }),
    ],
    [
      "XOR",
      (args: exp.Expression[]) =>
        new exp.BitwiseXor({ this: args[0], expression: args[1] }),
    ],
    [
      "STRFTIME",
      (args: exp.Expression[]) =>
        new exp.TimeToStr({ this: args[0], format: args[1] }),
    ],
    [
      "STRPTIME",
      (args: exp.Expression[]) =>
        new exp.StrToTime({ this: args[0], format: args[1] }),
    ],
    [
      "ENCODE",
      (args: exp.Expression[]) =>
        new exp.Encode({ this: args[0], charset: exp.Literal.string("utf-8") }),
    ],
    [
      "DECODE",
      (args: exp.Expression[]) =>
        new exp.Decode({ this: args[0], charset: exp.Literal.string("utf-8") }),
    ],
    [
      "LIST_VALUE",
      (args: exp.Expression[]) => new exp.Array({ expressions: args }),
    ],
    [
      "UNNEST",
      (args: exp.Expression[]) =>
        new exp.Explode({ this: args[0], expressions: args.slice(1) }),
    ],
    [
      "RANGE",
      (args: exp.Expression[]) => {
        if (args.length === 1) {
          args.unshift(exp.Literal.number("0"))
        }
        return new exp.GenerateSeries({
          start: args[0],
          end: args[1],
          step: args[2],
          is_end_exclusive: true,
        })
      },
    ],
    [
      "GENERATE_SERIES",
      (args: exp.Expression[]) => {
        if (args.length === 1) {
          return new exp.GenerateSeries({
            start: exp.Literal.number(0),
            end: args[0],
          })
        }
        return new exp.GenerateSeries({
          start: args[0],
          end: args[1],
          step: args[2],
        })
      },
    ],
    [
      "SHA256",
      (args: exp.Expression[]) =>
        new exp.SHA2({ this: args[0], length: exp.Literal.number(256) }),
    ],
    ["NOW", () => new exp.CurrentTimestamp({})],
    [
      "GET_BIT",
      (args: exp.Expression[]) =>
        new exp.Getbit({ this: args[0], expression: args[1] }),
    ],
    ["JSON", (args: exp.Expression[]) => new exp.ParseJSON({ this: args[0] })],
    [
      "JSON_EXTRACT",
      (args: exp.Expression[]) =>
        new exp.JSONExtract({ this: args[0], expression: args[1] }),
    ],
    [
      "JSON_EXTRACT_PATH",
      (args: exp.Expression[]) =>
        new exp.JSONExtract({ this: args[0], expression: args[1] }),
    ],
    [
      "JSON_EXTRACT_PATH_TEXT",
      (args: exp.Expression[]) =>
        new exp.JSONExtractScalar({ this: args[0], expression: args[1] }),
    ],
    [
      "JSON_EXTRACT_STRING",
      (args: exp.Expression[]) =>
        new exp.JSONExtractScalar({ this: args[0], expression: args[1] }),
    ],
    [
      "DATE_SUB",
      (args: exp.Expression[]) =>
        new exp.Anonymous({ this: "DATE_SUB", expressions: args }),
    ],
    [
      "DATE_DIFF",
      (args: exp.Expression[]) =>
        new exp.DateDiff({ this: args[2], expression: args[1], unit: args[0] }),
    ],
    [
      "DATEDIFF",
      (args: exp.Expression[]) =>
        new exp.DateDiff({ this: args[2], expression: args[1], unit: args[0] }),
    ],
    [
      "EDITDIST3",
      (args: exp.Expression[]) =>
        new exp.Levenshtein({ this: args[0], expression: args[1] }),
    ],
    [
      "STRING_AGG",
      (args: exp.Expression[]) =>
        new exp.GroupConcat({ this: args[0], separator: args[1] }),
    ],
    [
      "STRING_TO_ARRAY",
      (args: exp.Expression[]) =>
        new exp.Split({ this: args[0], expression: args[1] }),
    ],
    [
      "STRING_SPLIT",
      (args: exp.Expression[]) =>
        new exp.Split({ this: args[0], expression: args[1] }),
    ],
    [
      "STR_SPLIT",
      (args: exp.Expression[]) =>
        new exp.Split({ this: args[0], expression: args[1] }),
    ],
    [
      "STRING_SPLIT_REGEX",
      (args: exp.Expression[]) =>
        new exp.RegexpSplit({ this: args[0], expression: args[1] }),
    ],
    [
      "STR_SPLIT_REGEX",
      (args: exp.Expression[]) =>
        new exp.RegexpSplit({ this: args[0], expression: args[1] }),
    ],
    [
      "LIST_CONTAINS",
      (args: exp.Expression[]) =>
        new exp.ArrayContains({ this: args[0], expression: args[1] }),
    ],
    [
      "LIST_HAS_ANY",
      (args: exp.Expression[]) =>
        new exp.ArrayOverlaps({ this: args[0], expression: args[1] }),
    ],
    [
      "LIST_SORT",
      (args: exp.Expression[]) => new exp.SortArray({ this: args[0] }),
    ],
    [
      "LIST_REVERSE_SORT",
      (args: exp.Expression[]) =>
        new exp.SortArray({
          this: args[0],
          asc: new exp.Boolean({ this: false }),
        }),
    ],
    [
      "LIST_FILTER",
      (args: exp.Expression[]) =>
        new exp.ArrayFilter({ this: args[0], expression: args[1] }),
    ],
    [
      "LIST_TRANSFORM",
      (args: exp.Expression[]) =>
        new exp.Transform({ this: args[0], expression: args[1] }),
    ],
    [
      "LISTAGG",
      (args: exp.Expression[]) =>
        new exp.GroupConcat({ this: args[0], separator: args[1] }),
    ],
    [
      "GROUP_CONCAT",
      (args: exp.Expression[]) =>
        new exp.GroupConcat({ this: args[0], separator: args[1] }),
    ],
    [
      "QUANTILE",
      (args: exp.Expression[]) =>
        new exp.Quantile({ this: args[0], quantile: args[1] }),
    ],
    [
      "REGEXP_EXTRACT",
      (args: exp.Expression[]) =>
        new exp.RegexpExtract({
          this: args[0],
          expression: args[1],
          group: args[2],
          parameters: args[3],
        }),
    ],
    [
      "REGEXP_REPLACE",
      (args: exp.Expression[]) =>
        new exp.RegexpReplace({
          this: args[0],
          expression: args[1],
          replacement: args[2],
          modifiers: args[3],
          single_replace: true,
        }),
    ],
    [
      "QUANTILE_CONT",
      (args: exp.Expression[]) =>
        new exp.PercentileCont({ this: args[0], expression: args[1] }),
    ],
    [
      "QUANTILE_DISC",
      (args: exp.Expression[]) =>
        new exp.PercentileDisc({ this: args[0], expression: args[1] }),
    ],
    [
      "REGEXP_FULL_MATCH",
      (args: exp.Expression[]) =>
        new exp.RegexpFullMatch({
          this: args[0],
          expression: args[1],
          options: args[2],
        }),
    ],
    [
      "REGEXP_MATCHES",
      (args: exp.Expression[]) =>
        new exp.RegexpLike({
          this: args[0],
          expression: args[1],
          flag: args[2],
        }),
    ],
    [
      "LIST_CONCAT",
      (args: exp.Expression[]) =>
        new exp.ArrayConcat({ this: args[0], expressions: args.slice(1) }),
    ],
    [
      "ARRAY_REVERSE_SORT",
      (args: exp.Expression[]) =>
        new exp.SortArray({
          this: args[0],
          asc: new exp.Boolean({ this: false }),
        }),
    ],
    [
      "ARRAY_SORT",
      (args: exp.Expression[]) => new exp.SortArray({ this: args[0] }),
    ],
    [
      "STRUCT_PACK",
      (args: exp.Expression[]) =>
        new exp.Struct({
          expressions: args.map((a) => {
            if (
              a instanceof exp.PropertyEQ &&
              a.args.this instanceof exp.Column
            ) {
              return new exp.PropertyEQ({
                this: (a.args.this as exp.Column).args.this as exp.Expression,
                expression: a.args.expression as exp.Expression,
              })
            }
            return a
          }),
        }),
    ],
  ])

  // DuckDB type name remapping (mirrors Python DuckDB tokenizer keyword mappings)
  static override TYPE_NAME_MAPPING: Map<string, string> = new Map([
    ...Parser.TYPE_NAME_MAPPING,
    ["TIMESTAMP", "TIMESTAMPNTZ"],
    ["TIMESTAMP_US", "TIMESTAMP"],
    ["NUMERIC", "DECIMAL"],
    ["VARCHAR", "TEXT"],
    ["NVARCHAR", "TEXT"],
    ["CHAR", "TEXT"],
    ["BPCHAR", "TEXT"],
    ["STRING", "TEXT"],
    ["NCHAR", "TEXT"],
    ["BITSTRING", "BIT"],
    ["ROW", "STRUCT"],
  ])

  static override TYPE_CONVERTERS: Map<
    string,
    (dt: exp.DataType) => exp.DataType
  > = new Map([
    ...Parser.TYPE_CONVERTERS,
    [
      "DECIMAL",
      (dt: exp.DataType) => {
        if (dt.expressions.length > 0) return dt
        return new exp.DataType({
          this: "DECIMAL",
          expressions: [exp.Literal.number(18), exp.Literal.number(3)],
        })
      },
    ],
    ["TEXT", () => new exp.DataType({ this: "TEXT" })],
  ])

  protected override parseExpression(): exp.Expression {
    if (
      this.isIdentifierToken() &&
      this.peek(1).tokenType === TokenType.COLON
    ) {
      const alias = this.parseIdentifier()
      this.match(TokenType.COLON)
      const comments: string[] = []
      // Comments from `:` token (e.g., "foo" /* bla */: 1)
      if (this.prevComments) {
        comments.push(...this.prevComments)
        this.prevComments = undefined
      }
      const thisExpr = super.parseExpression()
      // Comments attached to parsed expression
      if (thisExpr.comments) {
        comments.push(...thisExpr.comments)
        thisExpr.comments = undefined
      }
      // Comments from last consumed token (e.g., "foo": 1 /* bla */ or "foo": /* bla */ 1)
      // Use indirect access to avoid TS control flow narrowing after earlier undefined assignment
      const pc = this.prevComments as string[] | undefined
      if (pc) {
        comments.push(...pc)
        this.prevComments = undefined
      }
      const result = this.expression(exp.Alias, { this: thisExpr, alias })
      if (comments.length > 0) {
        this.addComments(result, comments)
      }
      return result
    }
    return super.parseExpression()
  }

  protected override parseTableAtom(): exp.Expression {
    if (
      this.isIdentifierToken() &&
      this.peek(1).tokenType === TokenType.COLON
    ) {
      const aliasId = this.parseIdentifier()
      this.match(TokenType.COLON)
      const comments: string[] = []
      if (this.prevComments) {
        comments.push(...this.prevComments)
        this.prevComments = undefined
      }
      const table = super.parseTableAtom()
      if (table.comments) {
        comments.push(...table.comments)
        table.comments = undefined
      }
      const pc = this.prevComments as string[] | undefined
      if (pc) {
        comments.push(...pc)
        this.prevComments = undefined
      }
      const tableAlias = new exp.TableAlias({ this: aliasId })
      if (comments.length > 0) {
        tableAlias.comments = comments
      }
      // Set alias on underlying Table/Subquery (unwrap Alias if present)
      const target =
        table instanceof exp.Alias ? (table.args.this as exp.Expression) : table
      target.args.alias = tableAlias
      tableAlias.parent = target
      return target
    }
    return super.parseTableAtom()
  }

  protected override parseTableSample(
    asModifier: boolean,
  ): exp.TableSample | undefined {
    const sample = super.parseTableSample(asModifier)
    if (sample && !sample.args.method) {
      if (sample.args.size) {
        sample.args.method = new exp.Var({ this: "RESERVOIR" })
      } else {
        sample.args.method = new exp.Var({ this: "SYSTEM" })
      }
    }
    return sample
  }

  protected parseSummarize(): exp.Summarize {
    const table = this.matchText("TABLE")
    let thisExpr: exp.Expression
    if (this.match(TokenType.SELECT)) {
      thisExpr = this.parseSelect()
    } else if (this.match(TokenType.STRING)) {
      thisExpr = exp.Literal.string(this.prev.text)
    } else {
      thisExpr = this.parseTableName()
    }
    return new exp.Summarize({ this: thisExpr, table: table || undefined })
  }

  protected override parsePrimary(): exp.Expression {
    if (this.match(TokenType.HASH)) {
      if (this.current.tokenType === TokenType.NUMBER) {
        const num = this.advance().text
        return new exp.PositionalColumn({ this: exp.Literal.number(num) })
      }
    }
    return super.parsePrimary()
  }

  protected parseInstall(force = false): exp.Install {
    const name = this.parseIdentifier()
    const fromExpr = this.match(TokenType.FROM)
      ? this.parseVarOrString()
      : undefined
    return this.expression(exp.Install, { this: name, from_: fromExpr, force })
  }

  protected parseVarOrString(): exp.Expression {
    // Try to parse a string first
    if (this.currentTokenType === TokenType.STRING) {
      this.advance()
      return exp.Literal.string(this.prev.text)
    }
    // Otherwise parse as identifier
    return this.parseIdentifier()
  }

  protected parseForce(): exp.Expression {
    if (!this.match(TokenType.INSTALL)) {
      return this.parseAsCommand("FORCE")
    }
    return this.parseInstall(true)
  }

  protected parseAttachDetach(isAttach: boolean): exp.Attach | exp.Detach {
    this.match(TokenType.DATABASE)

    let exists: boolean | undefined
    if (this.matchText("IF")) {
      if (isAttach) {
        this.matchText("NOT")
      }
      this.matchText("EXISTS")
      exists = true
    }

    const thisExpr = this.parseVarOrString()

    let aliased: exp.Expression = thisExpr
    if (this.match(TokenType.AS)) {
      const alias = this.parseIdentifier()
      aliased = this.expression(exp.Alias, { this: thisExpr, alias })
    }

    if (isAttach) {
      let expressions: exp.AttachOption[] | undefined
      if (this.currentTokenType === TokenType.L_PAREN) {
        this.advance()
        expressions = this.parseCSV(() => this.parseAttachOption())
        this.expect(TokenType.R_PAREN)
      }
      return this.expression(exp.Attach, {
        this: aliased,
        exists,
        expressions,
      })
    }

    return this.expression(exp.Detach, { this: aliased, exists })
  }

  private parseAttachOption(): exp.AttachOption {
    const name = this.parseAnyToken()
    let value: exp.Expression | undefined
    if (
      this.currentTokenType !== TokenType.COMMA &&
      this.currentTokenType !== TokenType.R_PAREN
    ) {
      value = this.parseAnyToken()
    }
    return this.expression(exp.AttachOption, { this: name, expression: value })
  }

  private parseAnyToken(): exp.Expression {
    if (this.currentTokenType === TokenType.STRING) {
      this.advance()
      return exp.Literal.string(this.prev.text)
    }
    const token = this.advance()
    return new exp.Var({ this: token.text })
  }

  parseSimplifiedPivot(isUnpivot: boolean): exp.Pivot {
    const parseOnExpr = (): exp.Expression => {
      const thisExpr = this.parseBitwise()
      if (this.match(TokenType.IN)) {
        return this.parseInList(thisExpr)
      }
      if (this.currentTokenType === TokenType.AS) {
        return this.maybeParseAlias(thisExpr)
      }
      return thisExpr
    }

    const thisTable = this.parseTableExpression()
    const expressions = this.match(TokenType.ON)
      ? this.parseCSV(parseOnExpr)
      : undefined
    const into = this.parseUnpivotColumns()
    const using = this.matchText("USING")
      ? this.parseCSV(() => this.maybeParseAlias(this.parseBitwise()))
      : undefined
    const group = this.match(TokenType.GROUP_BY)
      ? this.expression(exp.Group, {
          expressions: this.parseCSV(() => this.parseExpression()),
        })
      : undefined

    return this.expression(exp.Pivot, {
      this: thisTable,
      expressions,
      using,
      group,
      unpivot: isUnpivot || undefined,
      into,
    })
  }

  private parseUnpivotColumns(): exp.UnpivotColumns | undefined {
    if (!this.matchText("INTO")) return undefined

    const name = this.matchText("NAME") ? this.parseBitwise() : undefined
    const exprs = this.matchText("VALUE")
      ? this.parseCSV(() => this.parseBitwise())
      : undefined

    return this.expression(exp.UnpivotColumns, {
      this: name,
      expressions: exprs,
    })
  }

  protected override parseCreateKind(): string | undefined {
    if (this.matchText("MACRO")) return "MACRO"
    return super.parseCreateKind()
  }

  protected parseShow(): exp.Show {
    if (this.matchTextSeq("ALL", "TABLES")) {
      return this.expression(exp.Show, {
        this: new exp.Literal({ this: "ALL TABLES", isString: false }),
      })
    }
    if (this.matchText("TABLES")) {
      return this.expression(exp.Show, {
        this: new exp.Literal({ this: "TABLES", isString: false }),
      })
    }
    const name = this.parseIdentifier()
    return this.expression(exp.Show, { this: name })
  }
}

export class DuckDBGenerator extends Generator {
  static override NULL_ORDERING:
    | "nulls_are_small"
    | "nulls_are_large"
    | "nulls_are_last" = "nulls_are_last"
  static override BYTE_START: string | null = "e'"
  static override BYTE_END: string | null = "'"
  static override RESERVED_KEYWORDS: Set<string> = new Set([
    "ALL",
    "AND",
    "ANY",
    "ARRAY",
    "AS",
    "ASC",
    "ASYMMETRIC",
    "BOTH",
    "CASE",
    "CAST",
    "CHECK",
    "COLLATE",
    "COLUMN",
    "CONSTRAINT",
    "CREATE",
    "CURRENT_CATALOG",
    "CURRENT_DATE",
    "CURRENT_ROLE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_USER",
    "DEFAULT",
    "DEFERRABLE",
    "DESC",
    "DISTINCT",
    "DO",
    "ELSE",
    "END",
    "EXCEPT",
    "FALSE",
    "FETCH",
    "FOR",
    "FOREIGN",
    "FROM",
    "GRANT",
    "GROUP",
    "HAVING",
    "IN",
    "INITIALLY",
    "INTERSECT",
    "INTO",
    "LATERAL",
    "LEADING",
    "LIMIT",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "NOT",
    "NULL",
    "OFFSET",
    "ON",
    "ONLY",
    "OR",
    "ORDER",
    "PLACING",
    "PRIMARY",
    "REFERENCES",
    "RETURNING",
    "SELECT",
    "SESSION_USER",
    "SOME",
    "SYMMETRIC",
    "TABLE",
    "THEN",
    "TO",
    "TRAILING",
    "TRUE",
    "UNION",
    "UNIQUE",
    "USER",
    "USING",
    "VARIADIC",
    "WHEN",
    "WHERE",
    "WINDOW",
    "WITH",
  ])

  protected override INDEX_OFFSET = 1
  protected override STRUCT_DELIMITER: [string, string] = ["(", ")"]
  protected override SET_ASSIGNMENT_REQUIRES_VARIABLE_KEYWORD = true
  protected override STAR_EXCEPT = "EXCLUDE"
  protected override PARAMETER_TOKEN = "$"
  protected override COPY_HAS_INTO_KEYWORD = false
  protected override SUPPORTS_LIKE_QUANTIFIERS = false
  protected override ARRAY_SIZE_DIM_REQUIRED: boolean | undefined = false

  // Window functions that support IGNORE NULLS / RESPECT NULLS
  static IGNORE_RESPECT_NULLS_WINDOW_FUNCTIONS: ExpressionClass[] = [
    exp.FirstValue,
    exp.LastValue,
    exp.NthValue,
    exp.Lead,
    exp.Lag,
  ]

  // DuckDB data type mappings
  static override TYPE_MAPPING: Map<string, string> = new Map([
    ...Generator.TYPE_MAPPING,
    ["BINARY", "BLOB"],
    ["BPCHAR", "TEXT"],
    ["BYTEA", "BLOB"],
    ["CHAR", "TEXT"],
    ["DATETIME", "TIMESTAMP"],
    ["DECFLOAT", "DECIMAL(38, 5)"],
    ["FLOAT", "REAL"],
    ["FLOAT4", "REAL"],
    ["HUGEINT", "INT128"],
    ["INT1", "TINYINT"],
    ["INT16", "SMALLINT"],
    ["INT2", "SMALLINT"],
    ["INT32", "INT"],
    ["INT4", "INT"],
    ["INT64", "BIGINT"],
    ["INT8", "BIGINT"],
    ["INTEGER", "INT"],
    ["JSONB", "JSON"],
    ["LOGICAL", "BOOLEAN"],
    ["NCHAR", "TEXT"],
    ["NUMERIC", "DECIMAL"],
    ["NVARCHAR", "TEXT"],
    ["ROWVERSION", "BLOB"],
    ["SIGNED", "INT"],
    ["STRING", "TEXT"],
    ["TIMESTAMPLTZ", "TIMESTAMPTZ"],
    ["TIMESTAMPNTZ", "TIMESTAMP"],
    ["TIMESTAMP_S", "TIMESTAMP_S"],
    ["TIMESTAMP_MS", "TIMESTAMP_MS"],
    ["TIMESTAMP_NS", "TIMESTAMP_NS"],
    ["UHUGEINT", "UINT128"],
    ["UINT", "UINTEGER"],
    ["VARBINARY", "BLOB"],
    ["VARCHAR", "TEXT"],
  ])

  static override INVERSE_TIME_MAPPING: Map<string, string> = new Map([
    ["%e", "%-d"],
    ["%:z", "%z"],
    ["%-z", "%z"],
  ])

  // Override features for DuckDB
  static override FEATURES = {
    ...Generator.FEATURES,
    IGNORE_NULLS_IN_FUNC: true,
    JOIN_HINTS: false,
    TABLE_HINTS: false,
    QUERY_HINTS: false,
    LIMIT_FETCH: "LIMIT" as const,
    RENAME_TABLE_WITH_DB: false,
    NVL2_SUPPORTED: false,
    SEMI_ANTI_JOIN_WITH_SIDE: false,
    LAST_DAY_SUPPORTS_DATE_PART: false,
    STAR_EXCEPT: "EXCLUDE" as const,
    CONCAT_COALESCE: true,
    SAFE_DIVISION: true,
  }

  static override TRANSFORMS: Map<ExpressionClass, Transform> = new Map<
    ExpressionClass,
    Transform
  >([
    ...Generator.TRANSFORMS,

    // Override base TRANSFORMS for CurrentDate to support timezone arg
    [
      exp.CurrentDate,
      (gen: Generator, e: exp.Expression) => {
        return (gen as DuckDBGenerator).currentdate_sql(e as exp.CurrentDate)
      },
    ],

    // XOR → boolean expression
    [exp.Xor, boolXorSql],

    // DuckDB struct → {'key': value, ...} dict literal
    [
      exp.Struct,
      (gen: Generator, e: exp.Expression) => {
        const expression = e as exp.Struct
        const args: string[] = []
        for (let i = 0; i < expression.expressions.length; i++) {
          const expr = expression.expressions[i]!
          const isPropertyEQ = expr instanceof exp.PropertyEQ
          const thisNode = expr.args.this as exp.Expression
          const value = isPropertyEQ
            ? (expr.args.expression as exp.Expression)
            : expr

          let key: string
          if (thisNode instanceof exp.Identifier) {
            key = gen.sql(exp.Literal.string(String(thisNode.args.this ?? "")))
          } else if (isPropertyEQ) {
            key = gen.sql(thisNode)
          } else {
            key = gen.sql(exp.Literal.string(`_${i}`))
          }
          args.push(`${key}: ${gen.sql(value)}`)
        }
        return `{${args.join(", ")}}`
      },
    ],

    // Simple function renames
    [exp.ApproxDistinct, renameFunc("APPROX_COUNT_DISTINCT")],
    [exp.ArrayAppend, renameFunc("LIST_APPEND")],
    [exp.ArrayCompact, arrayCompactSql],
    [
      exp.ArrayConstructCompact,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.ArrayConstructCompact
        return gen.sql(
          new exp.ArrayCompact({
            this: new exp.Array({
              expressions: expr.args.expressions as exp.Expression[],
            }),
          }),
        )
      },
    ],
    [exp.ArrayConcat, renameFunc("LIST_CONCAT")],
    [
      exp.SortArray,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.SortArray
        const asc = expr.args.asc
        const isDesc = asc instanceof exp.Boolean && asc.args.this === false
        const name = isDesc ? "ARRAY_REVERSE_SORT" : "ARRAY_SORT"
        return gen.funcCall(name, [expr.args.this as exp.Expression])
      },
    ],
    [
      exp.ArrayFilter,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.ArrayFilter
        const args: exp.Expression[] = [expr.args.this as exp.Expression]
        if (expr.args.expression instanceof exp.Expression)
          args.push(expr.args.expression)
        return gen.funcCall("LIST_FILTER", args)
      },
    ],
    [exp.ArraySum, renameFunc("LIST_SUM")],
    [exp.CosineDistance, renameFunc("LIST_COSINE_DISTANCE")],
    [exp.DayOfMonth, renameFunc("DAYOFMONTH")],
    [exp.DayOfWeek, renameFunc("DAYOFWEEK")],
    [exp.DayOfWeekIso, renameFunc("ISODOW")],
    [exp.DayOfYear, renameFunc("DAYOFYEAR")],
    [exp.EuclideanDistance, renameFunc("LIST_DISTANCE")],
    [exp.Explode, renameFunc("UNNEST")],
    [exp.IsInf, renameFunc("ISINF")],
    [exp.IsNan, renameFunc("ISNAN")],
    [exp.JSONBExists, renameFunc("JSON_EXISTS")],
    [
      exp.PercentileCont,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.PercentileCont
        const args = [expr.args.this as exp.Expression]
        if (expr.args.expression)
          args.push(expr.args.expression as exp.Expression)
        return gen.funcCall("QUANTILE_CONT", args)
      },
    ],
    [
      exp.PercentileDisc,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.PercentileDisc
        const args = [expr.args.this as exp.Expression]
        if (expr.args.expression)
          args.push(expr.args.expression as exp.Expression)
        return gen.funcCall("QUANTILE_DISC", args)
      },
    ],
    [exp.GroupConcat, groupconcatSql],
    [exp.Pivot, preprocess([unqualifyColumns])],
    [exp.Rand, renameFunc("RANDOM")],
    [
      exp.RegexpFullMatch,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.RegexpFullMatch
        const args: exp.Expression[] = [expr.args.this as exp.Expression]
        if (expr.args.expression instanceof exp.Expression)
          args.push(expr.args.expression)
        if (expr.args.options instanceof exp.Expression)
          args.push(expr.args.options)
        return gen.funcCall("REGEXP_FULL_MATCH", args)
      },
    ],
    [exp.RegexpSplit, renameFunc("STR_SPLIT_REGEX")],
    [exp.SHA, renameFunc("SHA1")],
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
    [exp.Split, renameFunc("STR_SPLIT")],
    [
      exp.NumberToStr,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.NumberToStr
        if (expr.args.culture) {
          gen.unsupported("NumberToStr does not support the culture argument")
        }
        const fmt = expr.args.format as exp.Expression | undefined
        if (fmt && (fmt as exp.Literal).is_int) {
          return gen.funcCall("FORMAT", [
            exp.Literal.string(`{:,.${fmt.name}f}`),
            expr.args.this as exp.Expression,
          ])
        }
        gen.unsupported("Only integer formats are supported by NumberToStr")
        return gen.funcCall("NUMBER_TO_STR", [
          expr.args.this as exp.Expression,
          ...(fmt ? [fmt] : []),
        ])
      },
    ],
    [exp.TimeToUnix, renameFunc("EPOCH")],
    [exp.Transform, renameFunc("LIST_TRANSFORM")],
    [exp.VariancePop, renameFunc("VAR_POP")],
    [exp.WeekOfYear, renameFunc("WEEKOFYEAR")],
    [exp.JSONObjectAgg, renameFunc("JSON_GROUP_OBJECT")],
    [exp.JSONBObjectAgg, renameFunc("JSON_GROUP_OBJECT")],
    [exp.DateBin, renameFunc("TIME_BUCKET")],
    [exp.BitwiseOrAgg, bitwiseAggSql("BIT_OR")],
    [exp.BitwiseAndAgg, bitwiseAggSql("BIT_AND")],
    [exp.BitwiseXorAgg, bitwiseAggSql("BIT_XOR")],
    [
      exp.BitwiseXor,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.BitwiseXor
        return gen.funcCall("XOR", [
          expr.args.this as exp.Expression,
          expr.args.expression as exp.Expression,
        ])
      },
    ],

    // Array operation transforms
    [
      exp.ArrayInsert,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.ArrayInsert
        const thisArr = expr.args.this as exp.Expression
        const positionExpr = expr.args.position as exp.Expression
        const element = expr.args.expression as exp.Expression
        const elementArray = new exp.Array({ expressions: [element] })
        const indexOffset =
          typeof expr.args.offset === "number" ? expr.args.offset : 0

        const extractedPos = tryExtractInt(positionExpr)
        if (extractedPos === undefined) {
          return gen.funcCall("ARRAY_INSERT", [thisArr, positionExpr, element])
        }

        let posValue = extractedPos
        if (posValue > 0) posValue -= indexOffset
        else if (posValue < 0) posValue += indexOffset

        let concatExprs: exp.Expression[]
        if (posValue === 0) {
          concatExprs = [elementArray, thisArr]
        } else if (posValue > 0) {
          const leftSlice = new exp.Bracket({
            this: thisArr,
            expressions: [
              new exp.Slice({
                this: exp.Literal.number(1),
                expression: exp.Literal.number(posValue),
              }),
            ],
          })
          const rightSlice = new exp.Bracket({
            this: thisArr,
            expressions: [
              new exp.Slice({ this: exp.Literal.number(posValue + 1) }),
            ],
          })
          concatExprs = [leftSlice, elementArray, rightSlice]
        } else {
          const arrLen = new exp.Length({ this: thisArr })
          const sliceEndPos = new exp.Add({
            this: arrLen,
            expression: exp.Literal.number(posValue),
          })
          const sliceStartPos = new exp.Add({
            this: sliceEndPos,
            expression: exp.Literal.number(1),
          })
          const leftSlice = new exp.Bracket({
            this: thisArr,
            expressions: [
              new exp.Slice({
                this: exp.Literal.number(1),
                expression: sliceEndPos,
              }),
            ],
          })
          const rightSlice = new exp.Bracket({
            this: thisArr,
            expressions: [new exp.Slice({ this: sliceStartPos })],
          })
          concatExprs = [leftSlice, elementArray, rightSlice]
        }

        const listConcat = gen.funcCall("LIST_CONCAT", concatExprs)
        const arrSql = gen.sql(thisArr)
        return `CASE WHEN ${arrSql} IS NULL THEN NULL ELSE ${listConcat} END`
      },
    ],

    [
      exp.ArrayRemoveAt,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.ArrayRemoveAt
        const thisArr = expr.args.this as exp.Expression
        const positionExpr = expr.args.position as exp.Expression

        const posValue = tryExtractInt(positionExpr)
        if (posValue === undefined) {
          return gen.funcCall("ARRAY_REMOVE_AT", [thisArr, positionExpr])
        }

        let resultSql: string

        if (posValue === 0) {
          resultSql = gen.sql(
            new exp.Bracket({
              this: thisArr,
              expressions: [new exp.Slice({ this: exp.Literal.number(2) })],
            }),
          )
        } else if (posValue > 0) {
          const leftSlice = new exp.Bracket({
            this: thisArr,
            expressions: [
              new exp.Slice({
                this: exp.Literal.number(1),
                expression: exp.Literal.number(posValue),
              }),
            ],
          })
          const rightSlice = new exp.Bracket({
            this: thisArr,
            expressions: [
              new exp.Slice({ this: exp.Literal.number(posValue + 2) }),
            ],
          })
          resultSql = gen.funcCall("LIST_CONCAT", [leftSlice, rightSlice])
        } else if (posValue === -1) {
          const arrLen = new exp.Length({ this: thisArr })
          const sliceEnd = new exp.Add({
            this: arrLen,
            expression: exp.Literal.number(-1),
          })
          resultSql = gen.sql(
            new exp.Bracket({
              this: thisArr,
              expressions: [
                new exp.Slice({
                  this: exp.Literal.number(1),
                  expression: sliceEnd,
                }),
              ],
            }),
          )
        } else {
          const arrLen = new exp.Length({ this: thisArr })
          const sliceEndPos = new exp.Add({
            this: arrLen,
            expression: exp.Literal.number(posValue),
          })
          const sliceStartPos = new exp.Add({
            this: sliceEndPos,
            expression: exp.Literal.number(2),
          })
          const leftSlice = new exp.Bracket({
            this: thisArr,
            expressions: [
              new exp.Slice({
                this: exp.Literal.number(1),
                expression: sliceEndPos,
              }),
            ],
          })
          const rightSlice = new exp.Bracket({
            this: thisArr,
            expressions: [new exp.Slice({ this: sliceStartPos })],
          })
          resultSql = gen.funcCall("LIST_CONCAT", [leftSlice, rightSlice])
        }

        const arrSql = gen.sql(thisArr)
        return `CASE WHEN ${arrSql} IS NULL THEN NULL ELSE ${resultSql} END`
      },
    ],

    [
      exp.ArrayRemove,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.ArrayRemove
        const lambdaId = new exp.Identifier({ this: "_u" })
        const cond = new exp.NEQ({
          this: lambdaId,
          expression: expr.args.expression as exp.Expression,
        })
        const filterExpr = new exp.ArrayFilter({
          this: expr.args.this as exp.Expression,
          expression: new exp.Lambda({ this: cond, expressions: [lambdaId] }),
        })
        const filterSql = gen.sql(filterExpr)

        if (expr.args.null_propagation) {
          const removalValue = expr.args.expression as exp.Expression
          if (
            (removalValue instanceof exp.Literal &&
              !(removalValue instanceof exp.Null)) ||
            removalValue instanceof exp.Array
          ) {
            return filterSql
          }
          const removalSql = gen.sql(removalValue)
          return `CASE WHEN ${removalSql} IS NULL THEN NULL ELSE ${filterSql} END`
        }

        return filterSql
      },
    ],

    // Simple lambdas
    [
      exp.IntDiv,
      (gen: Generator, e: exp.Expression) =>
        gen.binary_sql(e as exp.Binary, "//"),
    ],
    [exp.CurrentTime, () => "CURRENT_TIME"],
    [exp.CurrentTimestamp, () => "CURRENT_TIMESTAMP"],
    [
      exp.LogicalOr,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.LogicalOr
        return `BOOL_OR(CAST(${gen.sql(expr.args.this as exp.Expression)} AS BOOLEAN))`
      },
    ],
    [
      exp.LogicalAnd,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.LogicalAnd
        return `BOOL_AND(CAST(${gen.sql(expr.args.this as exp.Expression)} AS BOOLEAN))`
      },
    ],
    [exp.CommentColumnConstraint, () => ""],
    [
      exp.ByteLength,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.ByteLength
        return `OCTET_LENGTH(${gen.sql(expr.args.this as exp.Expression)})`
      },
    ],
    [
      exp.RegexpILike,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.RegexpILike
        const thisExpr = gen.sql(expr.args.this as exp.Expression)
        const pattern = gen.sql(expr.args.expression as exp.Expression)
        return `REGEXP_MATCHES(${thisExpr}, ${pattern}, 'i')`
      },
    ],
    [
      exp.ArrayUniqueAgg,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.ArrayUniqueAgg
        return `LIST(DISTINCT ${gen.sql(expr.args.this as exp.Expression)})`
      },
    ],
    [
      exp.Encode,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.Encode
        const charset = expr.args.charset as exp.Expression | undefined
        if (
          charset &&
          !["utf-8", "utf8"].includes(charset.name.toLowerCase())
        ) {
          gen.unsupported(`Expected utf-8 character set, got ${charset.name}.`)
        }
        return `ENCODE(${gen.sql(expr.args.this as exp.Expression)})`
      },
    ],
    [
      exp.Decode,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.Decode
        const charset = expr.args.charset as exp.Expression | undefined
        if (
          charset &&
          !["utf-8", "utf8"].includes(charset.name.toLowerCase())
        ) {
          gen.unsupported(`Expected utf-8 character set, got ${charset.name}.`)
        }
        return `DECODE(${gen.sql(expr.args.this as exp.Expression)})`
      },
    ],
    [
      exp.StrPosition,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.StrPosition
        const substr = gen.sql(expr.args.this as exp.Expression)
        const str = gen.sql(expr.args.substr as exp.Expression)
        return `STRPOS(${str}, ${substr})`
      },
    ],

    // Date/time delta → binary interval ops
    [exp.DateAdd, dateDeltaToBinaryIntervalOp],
    [exp.DateSub, dateDeltaToBinaryIntervalOp],
    [exp.DatetimeAdd, dateDeltaToBinaryIntervalOp],
    [exp.DatetimeSub, dateDeltaToBinaryIntervalOp],
    [exp.TimeAdd, dateDeltaToBinaryIntervalOp],
    [exp.TimeSub, dateDeltaToBinaryIntervalOp],
    [exp.TimestampAdd, dateDeltaToBinaryIntervalOp],
    [exp.TimestampSub, dateDeltaToBinaryIntervalOp],
    [exp.TsOrDsAdd, dateDeltaToBinaryIntervalOp],
    [
      exp.DateDiff,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.DateDiff
        const unit = (expr.text("unit") || "DAY").toUpperCase()
        const end = gen.sql(
          implicitDatetimeCast(expr.args.this as exp.Expression),
        )
        const start = gen.sql(
          implicitDatetimeCast(expr.args.expression as exp.Expression),
        )
        return `DATE_DIFF('${unit}', ${start}, ${end})`
      },
    ],
    [
      exp.DateTrunc,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.DateTrunc
        const unit = extractUnit(expr)
        const thisExpr = gen.sql(expr.args.this as exp.Expression)
        return `DATE_TRUNC('${unit}', ${thisExpr})`
      },
    ],
    [
      exp.TimestampTrunc,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.TimestampTrunc
        const unit = extractUnit(expr)
        const thisExpr = gen.sql(expr.args.this as exp.Expression)
        return `DATE_TRUNC('${unit}', ${thisExpr})`
      },
    ],
    [
      exp.TimeToStr,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.TimeToStr
        const thisExpr = gen.sql(expr.args.this as exp.Expression)
        const format = gen.formatTimeStr(e)
        return `STRFTIME(${thisExpr}, ${format})`
      },
    ],
    [
      exp.StrToTime,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.StrToTime
        const thisExpr = gen.sql(expr.args.this as exp.Expression)
        const format = gen.sql(expr.args.format as exp.Expression)
        const safe = expr.args.safe
        if (safe) {
          return `CAST(TRY_STRPTIME(${thisExpr}, ${format}) AS TIMESTAMP)`
        }
        return `STRPTIME(${thisExpr}, ${format})`
      },
    ],
    [
      exp.StrToUnix,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.StrToUnix
        const thisExpr = gen.sql(expr.args.this as exp.Expression)
        const format = gen.sql(expr.args.format as exp.Expression)
        return `EPOCH(STRPTIME(${thisExpr}, ${format}))`
      },
    ],
    [
      exp.UnixToStr,
      (gen: Generator, e: exp.Expression) => {
        const thisExpr = gen.sql(
          (e as exp.UnixToStr).args.this as exp.Expression,
        )
        const format = gen.formatTimeStr(e)
        return `STRFTIME(TO_TIMESTAMP(${thisExpr}), ${format})`
      },
    ],
    [
      exp.TimeStrToTime,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.TimeStrToTime
        const thisExpr = gen.sql(expr.args.this as exp.Expression)
        const zone = expr.args.zone
        const dataType = zone ? "TIMESTAMPTZ" : "TIMESTAMP"
        return `CAST(${thisExpr} AS ${dataType})`
      },
    ],
    [
      exp.TimeStrToDate,
      (gen: Generator, e: exp.Expression) => {
        const thisExpr = gen.sql(
          (e as exp.TimeStrToDate).args.this as exp.Expression,
        )
        return `CAST(${thisExpr} AS DATE)`
      },
    ],

    // Unix timestamp conversions
    [
      exp.UnixMicros,
      (gen: Generator, e: exp.Expression) => {
        const arg = implicitDatetimeCast(
          (e as exp.UnixMicros).args.this as exp.Expression,
        )
        return gen.funcCall("EPOCH_US", [arg])
      },
    ],
    [
      exp.UnixMillis,
      (gen: Generator, e: exp.Expression) => {
        const arg = implicitDatetimeCast(
          (e as exp.UnixMillis).args.this as exp.Expression,
        )
        return gen.funcCall("EPOCH_MS", [arg])
      },
    ],
    [
      exp.UnixSeconds,
      (gen: Generator, e: exp.Expression) => {
        const arg = implicitDatetimeCast(
          (e as exp.UnixSeconds).args.this as exp.Expression,
        )
        const epoch = gen.funcCall("EPOCH", [arg])
        return `CAST(${epoch} AS BIGINT)`
      },
    ],
    [
      exp.GenerateDateArray,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.GenerateDateArray
        const start = implicitDatetimeCast(expr.args.start as exp.Expression)
        const end = implicitDatetimeCast(expr.args.end as exp.Expression)
        const series = new exp.GenerateSeries({
          start,
          end,
          step: expr.args.step as exp.Expression | undefined,
        })
        return gen.sql(
          new exp.Cast({
            this: series,
            to: exp.DataType.build("DATE[]"),
          }),
        )
      },
    ],
    [
      exp.GenerateTimestampArray,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.GenerateTimestampArray
        const start = implicitDatetimeCast(
          expr.args.start as exp.Expression,
          "TIMESTAMP",
        )
        const end = implicitDatetimeCast(
          expr.args.end as exp.Expression,
          "TIMESTAMP",
        )
        const series = new exp.GenerateSeries({
          start,
          end,
          step: expr.args.step as exp.Expression | undefined,
        })
        return gen.sql(
          new exp.Cast({
            this: series,
            to: exp.DataType.build("TIMESTAMP[]"),
          }),
        )
      },
    ],
    [
      exp.JSONExtractArray,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.JSONExtractArray
        const jsonExtract = new exp.JSONExtract({
          this: expr.args.this as exp.Expression,
          expression: expr.args.expression as exp.Expression,
        })
        return gen.sql(
          new exp.Cast({
            this: jsonExtract,
            to: exp.DataType.build("JSON[]"),
          }),
        )
      },
    ],
    [
      exp.JSONValueArray,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.JSONValueArray
        const jsonExtract = new exp.JSONExtract({
          this: expr.args.this as exp.Expression,
          expression: expr.args.expression as exp.Expression,
        })
        return gen.sql(
          new exp.Cast({
            this: jsonExtract,
            to: exp.DataType.build("VARCHAR[]"),
          }),
        )
      },
    ],
    [
      exp.RegexpReplace,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.RegexpReplace
        return gen.funcCall(
          "REGEXP_REPLACE",
          [
            expr.args.this as exp.Expression,
            expr.args.expression as exp.Expression,
            expr.args.replacement as exp.Expression | undefined,
            regexpReplaceGlobalModifier(expr),
          ].filter((x): x is exp.Expression => x != null),
        )
      },
    ],

    // SEQ functions
    [exp.Seq1, seqSql(1)],
    [exp.Seq2, seqSql(2)],
    [exp.Seq4, seqSql(4)],
    [exp.Seq8, seqSql(8)],

    // CEIL/FLOOR with precision
    [exp.Ceil, ceilFloorSql],
    [exp.Floor, ceilFloorSql],

    // NEXT_DAY / PREVIOUS_DAY
    [exp.NextDay, dayNavigationSql],
    [exp.PreviousDay, dayNavigationSql],

    // CORR with null_on_zero_variance
    [exp.Corr, corrSql],

    // SPACE -> REPEAT
    [exp.Space, spaceSql],

    // GetExtract
    [exp.GetExtract, getExtractSql],

    // ARRAYS_ZIP
    [exp.ArraysZip, arraysZipSql],
  ])

  protected override isUnwrappedIntervalValue(expr: exp.Expression): boolean {
    return expr instanceof exp.Literal || expr instanceof exp.Paren
  }

  protected override withingroup_sql(expression: exp.WithinGroup): string {
    const func = expression.args.this as exp.Expression

    if (
      func instanceof exp.PercentileCont ||
      func instanceof exp.PercentileDisc
    ) {
      const name =
        func instanceof exp.PercentileCont ? "QUANTILE_CONT" : "QUANTILE_DISC"
      const orderCol = expression.find(exp.Ordered)
      const orderExpr = expression.args.expression as exp.Expression | undefined
      const orderSql = orderExpr ? ` ${this.sql(orderExpr)}` : ""

      if (orderCol) {
        const colSql = this.sql(orderCol.args.this as exp.Expression)
        const fracSql = this.sql(func.args.this as exp.Expression)
        return `${name}(${colSql}, ${fracSql}${orderSql})`
      }

      const fracSql = this.sql(func.args.this as exp.Expression)
      return `${name}(${fracSql}${orderSql})`
    }

    return super.withingroup_sql(expression)
  }

  protected override filter_sql(expression: exp.Filter): string {
    const inner = expression.args.this as exp.Expression
    if (inner instanceof exp.Corr && inner.args.null_on_zero_variance) {
      inner.set("null_on_zero_variance", false)
      const filterSql = super.filter_sql(expression)
      return `CASE WHEN ISNAN(${filterSql}) THEN NULL ELSE ${filterSql} END`
    }
    return super.filter_sql(expression)
  }

  protected override window_sql(expression: exp.Window): string {
    const inner = expression.args.this as exp.Expression
    const corrExpr =
      inner instanceof exp.Corr
        ? inner
        : inner instanceof exp.Filter && inner.args.this instanceof exp.Corr
          ? (inner.args.this as exp.Corr)
          : null

    if (corrExpr && corrExpr.args.null_on_zero_variance) {
      corrExpr.set("null_on_zero_variance", false)
      const windowSql = super.window_sql(expression)
      return `CASE WHEN ISNAN(${windowSql}) THEN NULL ELSE ${windowSql} END`
    }
    return super.window_sql(expression)
  }

  // DuckDB uses double quotes for identifier quoting
  protected override quoteIdentifier(name: string): string {
    return `"${name.replace(/"/g, '""')}"`
  }

  protected override columndef_sql(expression: exp.ColumnDef): string {
    if (expression.parent instanceof exp.UserDefinedFunction) {
      return this.sql(expression, "this")
    }
    return super.columndef_sql(expression)
  }

  // DuckDB-specific date part mappings for EXTRACT
  protected override extract_sql(expression: exp.Extract): string {
    const part = expression.text("this").toUpperCase()
    const expr = this.sql(expression.args.expression as exp.Expression)

    const partMappings: Record<string, string> = {
      DAYOFWEEKISO: "ISODOW",
      DAYOFYEAR: "DOY",
      WEEKOFYEAR: "WEEK",
    }

    const mappedPart = partMappings[part] || part
    return `EXTRACT(${mappedPart} FROM ${expr})`
  }

  // DuckDB uses || for string concatenation
  protected override anonymous_sql(expression: exp.Anonymous): string {
    const name = expression.name.toUpperCase()

    // CONCAT -> || (DuckDB supports both)
    if (name === "CONCAT") {
      const args = expression.expressions
      if (args.length >= 2) {
        return args.map((a) => this.sql(a)).join(" || ")
      }
    }

    // DuckDB has COALESCE, not NVL
    if (name === "NVL") {
      return `COALESCE(${this.expressions(expression.expressions)})`
    }

    return super.anonymous_sql(expression)
  }

  // DuckDB supports ILIKE
  protected override ilike_sql(expression: exp.ILike): string {
    let sql = this.binary_sql(expression, "ILIKE")
    const escapeExpr = expression.args.escape
    if (escapeExpr) {
      sql += ` ESCAPE ${this.sql(escapeExpr as exp.Expression)}`
    }
    return sql
  }

  // DuckDB uses [1, 2, 3] for array literals (not ARRAY[1, 2, 3])
  protected override array_sql(expression: exp.Array): string {
    const exprs = expression.expressions

    // For ARRAY(subquery) or ARRAY((subquery)), expressions contains a single Select/Subquery
    if (exprs.length === 1) {
      const inner = exprs[0]
      if (inner instanceof exp.Select) {
        return `ARRAY(${this.sql(inner)})`
      }
      if (inner instanceof exp.Subquery) {
        return `ARRAY(${this.sql(inner)})`
      }
    }

    // Legacy: check args.this for older AST format
    const subquery = expression.args.this
    if (subquery instanceof exp.Subquery) {
      const inner = subquery.args.this
      return `ARRAY(${this.sql(inner as exp.Expression)})`
    }

    // For array literals, use [...] without ARRAY keyword
    return `[${this.expressions(exprs)}]`
  }

  protected override hexstring_sql(expression: exp.HexString): string {
    return super.hexstring_sql(expression, "UNHEX")
  }

  // DuckDB uses ~ for regex like (same as Postgres)
  protected override regexplike_sql(expression: exp.RegexpLike): string {
    const args: exp.Expression[] = [
      expression.args.this as exp.Expression,
      expression.args.expression as exp.Expression,
    ]
    if (expression.args.flag) args.push(expression.args.flag as exp.Expression)
    return this.funcCall("REGEXP_MATCHES", args)
  }

  protected override regexpilike_sql(expression: exp.RegexpILike): string {
    return this.funcCall("REGEXP_MATCHES", [
      expression.args.this as exp.Expression,
      expression.args.expression as exp.Expression,
      exp.Literal.string("i"),
    ])
  }

  // DuckDB table sample settings
  protected override TABLESAMPLE_KEYWORDS = "USING SAMPLE"
  protected override TABLESAMPLE_SEED_KEYWORD = "REPEATABLE"

  protected override tablesample_sql(
    expression: exp.TableSample,
    tablesampleKeyword?: string,
  ): string {
    const keyword =
      expression.parent instanceof exp.Select
        ? tablesampleKeyword
        : "TABLESAMPLE"

    if (expression.args.size) {
      const method = expression.args.method
      if (
        !method ||
        (method instanceof exp.Var && method.args.this !== "RESERVOIR")
      ) {
        expression.args.method = new exp.Var({ this: "RESERVOIR" })
      }
    }

    return super.tablesample_sql(expression, keyword)
  }

  protected override create_sql(expression: exp.Create): string {
    if (expression.args.properties instanceof exp.Properties) {
      expression.set("properties", undefined)
    }
    return super.create_sql(expression)
  }

  protected generateseries_sql(expression: exp.GenerateSeries): string {
    if (expression.args.is_end_exclusive) {
      return this.funcCall("RANGE", [
        expression.args.start as exp.Expression,
        expression.args.end as exp.Expression,
        ...(expression.args.step
          ? [expression.args.step as exp.Expression]
          : []),
      ])
    }
    return this.function_fallback_sql(expression)
  }

  protected countif_sql(expression: exp.CountIf): string {
    const v = this.version
    if (v[0] > 1 || (v[0] === 1 && v[1] >= 2)) {
      return this.function_fallback_sql(expression)
    }
    const cond = expression.args.this as exp.Expression
    const ifExpr = new exp.If({
      this: cond,
      true: exp.Literal.number(1),
      false: exp.Literal.number(0),
    })
    return this.sql(new exp.Sum({ this: ifExpr }))
  }

  protected getbit_sql(expression: exp.Getbit): string {
    return this.funcCall("GET_BIT", [
      expression.args.this as exp.Expression,
      expression.args.expression as exp.Expression,
    ])
  }

  protected levenshtein_sql(expression: exp.Levenshtein): string {
    return this.funcCall("LEVENSHTEIN", [
      expression.args.this as exp.Expression,
      expression.args.expression as exp.Expression,
    ])
  }

  protected regexpextract_sql(expression: exp.RegexpExtract): string {
    let thisExpr = expression.args.this as exp.Expression
    let group = expression.args.group as exp.Expression | undefined
    const params = expression.args.parameters as exp.Expression | undefined
    const position = expression.args.position as exp.Expression | undefined
    const occurrence = expression.args.occurrence as exp.Expression | undefined

    if (
      position &&
      (!position.is_int || Number((position as exp.Literal).value) > 1)
    ) {
      thisExpr = new exp.Substring({ this: thisExpr, start: position })
    }

    if (
      !params &&
      group &&
      group instanceof exp.Literal &&
      !group.isString &&
      String(group.value) === "0"
    ) {
      group = undefined
    }

    if (
      occurrence &&
      (!occurrence.is_int || Number((occurrence as exp.Literal).value) > 1)
    ) {
      const extractAllSql = this.funcCall("REGEXP_EXTRACT_ALL", [
        thisExpr,
        expression.args.expression as exp.Expression,
        ...(group ? [group] : []),
        ...(params ? [params] : []),
      ])
      return `ARRAY_EXTRACT(${extractAllSql}, ${this.sql(occurrence as exp.Expression)})`
    }

    return this.funcCall("REGEXP_EXTRACT", [
      thisExpr,
      expression.args.expression as exp.Expression,
      ...(group ? [group] : []),
      ...(params ? [params] : []),
    ])
  }

  protected approxquantile_sql(expression: exp.ApproxQuantile): string {
    const thisExpr = expression.args.this as exp.Expression
    const quantile = expression.args.quantile as exp.Expression

    if (thisExpr instanceof exp.Distinct) {
      const innerExpr = thisExpr.expressions[0] as exp.Expression
      return `APPROX_QUANTILE(DISTINCT ${this.sql(innerExpr)}, ${this.sql(quantile)})`
    }

    let result = this.funcCall("APPROX_QUANTILE", [thisExpr, quantile])
    if (expression.isType(...exp.DataType.REAL_TYPES)) {
      result = `CAST(${result} AS DOUBLE)`
    }
    return result
  }

  protected approxquantiles_sql(expression: exp.ApproxQuantiles): string {
    let thisExpr = expression.args.this as exp.Expression
    let numQuantilesExpr: exp.Expression | undefined

    if (thisExpr instanceof exp.Distinct) {
      const exprs = thisExpr.expressions
      if (exprs.length < 2) {
        this.unsupported("APPROX_QUANTILES requires a bucket count argument")
        return this.function_fallback_sql(expression)
      }
      numQuantilesExpr = exprs[1]
      // Keep Distinct wrapping only the first expression
      thisExpr = new exp.Distinct({ expressions: [exprs[0] as exp.Expression] })
    } else {
      numQuantilesExpr = expression.args.expression as
        | exp.Expression
        | undefined
    }

    if (
      !numQuantilesExpr ||
      !(numQuantilesExpr instanceof exp.Literal) ||
      numQuantilesExpr.isString
    ) {
      this.unsupported(
        "APPROX_QUANTILES bucket count must be a positive integer",
      )
      return this.function_fallback_sql(expression)
    }

    const numQuantiles = Number(numQuantilesExpr.value)
    if (!Number.isInteger(numQuantiles) || numQuantiles <= 0) {
      this.unsupported(
        "APPROX_QUANTILES bucket count must be a positive integer",
      )
      return this.function_fallback_sql(expression)
    }

    const quantiles: exp.Expression[] = []
    for (let i = 0; i <= numQuantiles; i++) {
      quantiles.push(exp.Literal.number(i / numQuantiles))
    }

    return this.sql(
      new exp.ApproxQuantile({
        this: thisExpr,
        quantile: new exp.Array({ expressions: quantiles }),
      }),
    )
  }

  protected currentdate_sql(expression: exp.CurrentDate): string {
    const zone = expression.args.this as exp.Expression | undefined
    if (!zone) {
      return "CURRENT_DATE"
    }
    return this.sql(
      new exp.Cast({
        this: new exp.AtTimeZone({
          this: new exp.CurrentTimestamp({}),
          zone,
        }),
        to: new exp.DataType({ this: "DATE" }),
      }),
    )
  }

  protected date_sql(expression: exp.Date): string {
    let thisExpr = expression.args.this as exp.Expression | undefined
    const zone = expression.args.zone as exp.Expression | undefined

    if (zone) {
      thisExpr = new exp.AtTimeZone({
        this: new exp.AtTimeZone({
          this: new exp.Cast({
            this: thisExpr as exp.Expression,
            to: new exp.DataType({ this: "TIMESTAMP" }),
          }),
          zone: new exp.Literal({ this: "UTC", is_string: true }),
        }),
        zone,
      })
    }

    return this.sql(
      new exp.Cast({
        this: thisExpr as exp.Expression,
        to: new exp.DataType({ this: "DATE" }),
      }),
    )
  }

  protected strtodate_sql(expression: exp.StrToDate): string {
    const formattedTime = this.formatTimeStr(expression)
    const safe = expression.args.safe
    const funcName = safe ? "TRY_STRPTIME" : "STRPTIME"
    const thisExpr = this.sql(expression.args.this as exp.Expression)
    return `CAST(${funcName}(${thisExpr}, ${formattedTime}) AS DATE)`
  }

  protected datefromparts_sql(expression: exp.DateFromParts): string {
    const year = expression.args.year as exp.Expression
    const month = expression.args.month as exp.Expression
    const day = expression.args.day as exp.Expression
    return this.funcCall("MAKE_DATE", [year, month, day])
  }

  // DuckDB UnixToTime: MILLIS → EPOCH_MS, MICROS → MAKE_TIMESTAMP, SECONDS → TO_TIMESTAMP
  protected unixtotime_sql(expression: exp.UnixToTime): string {
    const scale = expression.args.scale as exp.Literal | undefined
    const timestamp = expression.args.this as exp.Expression

    // Scale is a Literal: 3=MILLIS, 6=MICROS, 0=SECONDS (or undefined)
    const scaleValue =
      scale instanceof exp.Literal ? String(scale.value) : undefined

    if (scaleValue === "3") {
      return this.funcCall("EPOCH_MS", [timestamp])
    }
    if (scaleValue === "6") {
      return this.funcCall("MAKE_TIMESTAMP", [timestamp])
    }
    // Default: seconds (scale=0 or undefined)
    return this.funcCall("TO_TIMESTAMP", [timestamp])
  }

  protected attach_sql(expression: exp.Attach): string {
    const thisExpr = this.sql(expression.args.this as exp.Expression)
    const existsSql = expression.args.exists ? " IF NOT EXISTS" : ""
    const exprs = expression.expressions
    const exprsSql = exprs.length > 0 ? ` (${this.expressions(exprs)})` : ""
    return `ATTACH${existsSql} ${thisExpr}${exprsSql}`
  }

  protected detach_sql(expression: exp.Detach): string {
    const thisExpr = this.sql(expression.args.this as exp.Expression)
    const existsSql = expression.args.exists ? " DATABASE IF EXISTS" : ""
    return `DETACH${existsSql} ${thisExpr}`
  }

  protected attachoption_sql(expression: exp.AttachOption): string {
    const thisExpr = this.sql(expression.args.this as exp.Expression)
    const value = expression.args.expression
      ? ` ${this.sql(expression.args.expression as exp.Expression)}`
      : ""
    return `${thisExpr}${value}`
  }

  protected override join_sql(expression: exp.Join): string {
    const method = expression.args.method
    const kind = expression.args.kind as string | undefined

    if (
      !expression.args.using &&
      !expression.args.on &&
      !method &&
      (!kind || kind === "INNER" || kind === "OUTER")
    ) {
      const target = expression.args.this as exp.Expression
      if (target instanceof exp.Unnest) {
        const clone = expression.copy() as exp.Join
        clone.args.on = new exp.Boolean({ this: true })
        return super.join_sql(clone)
      }
    }

    return super.join_sql(expression)
  }

  private normalizeJsonPath(path: exp.Expression): string {
    if (path instanceof exp.Literal) {
      if (!path.isString) {
        return `'$[${String(path.value)}]'`
      }
      const text = String(path.value)
      if (text.startsWith("$") || text.startsWith("/") || text.includes("[#")) {
        return this.sql(path)
      }
      return `'$.${text}'`
    }
    return this.sql(path)
  }

  private arrowJsonExtractSql(
    expression: exp.JSONExtract | exp.JSONExtractScalar,
  ): string {
    const op = expression instanceof exp.JSONExtract ? "->" : "->>"
    const pathSql = this.normalizeJsonPath(
      expression.args.expression as exp.Expression,
    )
    const arrowSql = `${this.sql(expression.args.this as exp.Expression)} ${op} ${pathSql}`
    const parent = expression.parent
    if (
      parent &&
      parent.constructor !== expression.constructor &&
      (parent instanceof exp.Binary ||
        parent instanceof exp.Bracket ||
        parent instanceof exp.In ||
        parent instanceof exp.Not)
    ) {
      return `(${arrowSql})`
    }
    return arrowSql
  }

  protected jsonextract_sql(expression: exp.JSONExtract): string {
    return this.arrowJsonExtractSql(expression)
  }

  protected jsonextractscalar_sql(expression: exp.JSONExtractScalar): string {
    if (expression.args.scalar_only) {
      const innerSql = this.funcCall("JSON_VALUE", [
        expression.args.this as exp.Expression,
        expression.args.expression as exp.Expression,
      ])
      return `${innerSql} ->> '$'`
    }
    return this.arrowJsonExtractSql(expression)
  }

  protected parsejson_sql(expression: exp.ParseJSON): string {
    const arg = expression.args.this as exp.Expression
    if (expression.args.safe) {
      return this.sql(
        new exp.Case({
          ifs: [
            new exp.If({
              this: new exp.Anonymous({
                this: "JSON_VALID",
                expressions: [arg],
              }),
              true: arg.copy(),
            }),
          ],
          default: new exp.Null({}),
        }),
      )
    }
    return this.funcCall("JSON", [arg])
  }

  protected pivot_sql(expression: exp.Pivot): string {
    if (!expression.args.this) {
      return super.pivot_sql(expression)
    }

    const direction = expression.args.unpivot ? "UNPIVOT" : "PIVOT"
    const thisExpr = this.sql(expression.args.this as exp.Expression)
    const exprs = this.expressions(expression.expressions)

    if (!exprs) {
      return this.prependCtes(expression, `UNPIVOT ${thisExpr}`)
    }

    const on = ` ON ${exprs}`
    const into = expression.args.into
      ? ` INTO ${this.sql(expression.args.into as exp.Expression)}`
      : ""
    const usingExprs = expression.args.using as exp.Expression[] | undefined
    const using = usingExprs?.length
      ? ` USING ${this.expressions(usingExprs)}`
      : ""
    const group = expression.args.group
      ? ` ${this.sql(expression.args.group as exp.Expression)}`
      : ""

    return this.prependCtes(
      expression,
      `${direction} ${thisExpr}${on}${into}${using}${group}`,
    )
  }

  protected unpivotcolumns_sql(expression: exp.UnpivotColumns): string {
    const name = this.sql(expression.args.this as exp.Expression)
    const values = this.expressions(expression.expressions)
    return `NAME ${name} VALUE ${values}`
  }

  // DuckDB SHOW statement: SHOW name
  protected show_sql(expression: exp.Show): string {
    return `SHOW ${expression.text("this")}`
  }

  // DuckDB INSTALL statement: FORCE INSTALL name FROM repo
  protected install_sql(expression: exp.Install): string {
    const force = expression.args.force ? "FORCE " : ""
    const thisExpr = this.sql(expression.args.this as exp.Expression)
    const fromExpr = expression.args.from_
    const fromClause = fromExpr
      ? ` FROM ${this.sql(fromExpr as exp.Expression)}`
      : ""
    return `${force}INSTALL ${thisExpr}${fromClause}`
  }

  // DuckDB data type output: INT[3] instead of ARRAY<INT>[3]
  protected override datatype_sql(expression: exp.DataType): string {
    const typeStr = expression.text("this").toUpperCase()

    // For ARRAY types with fixed sizes, output as INNER_TYPE[size]
    if (typeStr === "ARRAY") {
      const inner = this.expressions(expression.expressions)
      const values = expression.args.values
      if (Array.isArray(values) && values.length > 0) {
        return `${inner}[${this.expressions(values)}]`
      }
      // Empty array brackets
      return `${inner}[]`
    }

    // For TIME/TIMETZ/TIMESTAMPTZ, output just the type name without modifiers
    if (
      typeStr === "TIME" ||
      typeStr === "TIMETZ" ||
      typeStr === "TIMESTAMPTZ"
    ) {
      return typeStr
    }

    return super.datatype_sql(expression)
  }

  private isIgnoreRespectNullsWindowFunc(expr: exp.Expression): boolean {
    return DuckDBGenerator.IGNORE_RESPECT_NULLS_WINDOW_FUNCTIONS.some(
      (cls) => expr instanceof cls,
    )
  }

  protected override ignorenulls_sql(expression: exp.IgnoreNulls): string {
    const inner = expression.args.this as exp.Expression

    if (this.isIgnoreRespectNullsWindowFunc(inner)) {
      return super.ignorenulls_sql(expression)
    }

    let target = inner
    if (inner instanceof exp.First) {
      target = new exp.AnyValue({ this: inner.args.this })
    }

    if (
      !(target instanceof exp.AnyValue) &&
      !(target instanceof exp.ApproxQuantiles)
    ) {
      this.unsupported(
        "IGNORE NULLS is not supported for non-window functions.",
      )
    }

    return this.sql(target)
  }

  protected override respectnulls_sql(expression: exp.RespectNulls): string {
    const inner = expression.args.this as exp.Expression

    if (this.isIgnoreRespectNullsWindowFunc(inner)) {
      return super.respectnulls_sql(expression)
    }

    this.unsupported("RESPECT NULLS is not supported for non-window functions.")
    return this.sql(inner)
  }

  protected approxtopk_sql(expression: exp.ApproxTopK): string {
    this.unsupported(
      "APPROX_TOP_K cannot be transpiled to DuckDB due to incompatible return types.",
    )
    return this.funcCall(
      "APPROX_TOP_K",
      expression.expressions.length > 0
        ? expression.expressions
        : [expression.args.this as exp.Expression],
    )
  }

  protected override aliases_sql(expression: exp.Aliases): string {
    const thisExpr = expression.args.this as exp.Expression
    if (thisExpr instanceof exp.Posexplode) {
      return this.posexplode_sql(thisExpr)
    }
    return super.aliases_sql(expression)
  }

  protected posexplode_sql(expression: exp.Posexplode): string {
    const thisArg = expression.args.this as exp.Expression
    const parent = expression.parent

    let pos: exp.Expression = exp.toIdentifier("pos")
    let col: exp.Expression = exp.toIdentifier("col")

    if (parent instanceof exp.Aliases) {
      const exprs = parent.expressions
      if (exprs.length >= 2) {
        pos = exprs[0]!
        col = exprs[1]!
      }
    } else if (parent instanceof exp.Table) {
      const alias = parent.args.alias as exp.TableAlias | undefined
      if (alias) {
        const columns = alias.args.columns as exp.Expression[] | undefined
        if (columns && columns.length >= 2) {
          pos = columns[0]!
          col = columns[1]!
        }
        alias.pop()
      }
    }

    const unnestSql = this.sql(
      new exp.Unnest({ expressions: [thisArg], alias: col }),
    )
    const genSubscripts = this.sql(
      new exp.Alias({
        this: new exp.Sub({
          this: new exp.Anonymous({
            this: "GENERATE_SUBSCRIPTS",
            expressions: [thisArg, exp.Literal.number(1)],
          }),
          expression: exp.Literal.number(1),
        }),
        alias: pos,
      }),
    )

    const posexplodeSql = `${genSubscripts}, ${unnestSql}`

    if (
      parent instanceof exp.From ||
      (parent && parent.parent instanceof exp.From)
    ) {
      return `(SELECT ${posexplodeSql})`
    }

    return posexplodeSql
  }

  protected bitmapbucketnumber_sql(expression: exp.BitmapBucketNumber): string {
    const value = expression.args.this as exp.Expression

    // ((value - 1) // 32768) + 1
    const positiveFormula = new exp.Add({
      this: new exp.Paren({
        this: new exp.IntDiv({
          this: new exp.Paren({
            this: new exp.Sub({
              this: value,
              expression: exp.Literal.number(1),
            }),
          }),
          expression: exp.Literal.number(32768),
        }),
      }),
      expression: exp.Literal.number(1),
    })

    // value // 32768
    const nonPositiveFormula = new exp.IntDiv({
      this: value,
      expression: exp.Literal.number(32768),
    })

    // CASE WHEN value > 0 THEN ... ELSE ... END
    const caseExpr = new exp.Case({
      ifs: [
        new exp.If({
          this: new exp.GT({
            this: value,
            expression: exp.Literal.number(0),
          }),
          true: positiveFormula,
        }),
      ],
      default: nonPositiveFormula,
    })

    return this.sql(caseExpr)
  }
}

export class DuckDBDialect extends Dialect {
  static override readonly name = "duckdb"
  static override INDEX_OFFSET = 1
  static override NULL_ORDERING:
    | "nulls_are_small"
    | "nulls_are_large"
    | "nulls_are_last" = "nulls_are_last"
  static override CONCAT_COALESCE = true
  static override SAFE_DIVISION = true
  static override BYTE_START: string | null = "e'"
  static override BYTE_END: string | null = "'"
  protected static override ParserClass = DuckDBParser
  protected static override GeneratorClass = DuckDBGenerator

  override createTokenizer(): Tokenizer {
    return new Tokenizer({
      ...this.options.tokenizer,
      numbersCanBeUnderscoreSeparated: true,
      keywords: new Map([
        ["PIVOT_WIDER", TokenType.PIVOT],
        ["SUMMARIZE", TokenType.SUMMARIZE],
      ]),
    })
  }
}

// Register dialect
Dialect.register(DuckDBDialect)
