/**
 * Recursive descent SQL parser
 */

import { ErrorLevel, ParseError, type ParseErrorDetail } from "./errors.js"
import * as exp from "./expressions.js"
import { FUNCTION_BY_NAME, fromArgList } from "./expressions.js"
import { Token, TokenType, Tokenizer } from "./tokens.js"

export interface ParserOptions {
  dialect?: string
  tokenizer?: Tokenizer
}

export interface DialectSettings {
  name: string
  STRICT_STRING_CONCAT: boolean
  CONCAT_COALESCE: boolean
  LOG_BASE_FIRST: boolean | null
  ARRAY_AGG_INCLUDES_NULLS: boolean | null
  LEAST_GREATEST_IGNORES_NULLS: boolean
  INDEX_OFFSET: number
  NULL_ORDERING: "nulls_are_small" | "nulls_are_large" | "nulls_are_last"
  HEX_STRING_IS_INTEGER_TYPE: boolean
  HEX_LOWERCASE: boolean
  PRESERVE_ORIGINAL_NAMES: boolean
  TYPED_DIVISION: boolean
  SAFE_DIVISION: boolean
}

const DEFAULT_DIALECT_SETTINGS: DialectSettings = {
  name: "sqlglot",
  STRICT_STRING_CONCAT: true,
  CONCAT_COALESCE: false,
  LOG_BASE_FIRST: true,
  ARRAY_AGG_INCLUDES_NULLS: null,
  LEAST_GREATEST_IGNORES_NULLS: false,
  INDEX_OFFSET: 0,
  NULL_ORDERING: "nulls_are_small",
  HEX_STRING_IS_INTEGER_TYPE: false,
  HEX_LOWERCASE: false,
  PRESERVE_ORIGINAL_NAMES: false,
  TYPED_DIVISION: false,
  SAFE_DIVISION: false,
}

type ColumnOperatorHandler = (
  parser: Parser,
  left: exp.Expression,
  right: exp.Expression,
) => exp.Expression

const BASE_COLUMN_OPERATORS: Map<TokenType, ColumnOperatorHandler | null> =
  new Map<TokenType, ColumnOperatorHandler | null>([
    [TokenType.DOT, null], // DOT is handled specially for column references
    [
      TokenType.COLONCOLON,
      (_parser: Parser, left: exp.Expression, right: exp.Expression) =>
        new exp.Cast({ this: left, to: right as exp.DataType }),
    ],
    [
      TokenType.ARROW,
      (_parser: Parser, left: exp.Expression, right: exp.Expression) =>
        new exp.JSONExtract({ this: left, expression: right }),
    ],
    [
      TokenType.DARROW,
      (_parser: Parser, left: exp.Expression, right: exp.Expression) =>
        new exp.JSONExtractScalar({ this: left, expression: right }),
    ],
  ])

const CAST_OPERATORS = new Set([TokenType.COLONCOLON])

// Multi-word type keywords: first word -> [[remaining words, canonical name], ...]
// Ordered longest-first within each group so longest match wins.
// An empty suffix [] serves as a fallback for standalone single-word aliases.
const MULTI_WORD_TYPE_TOKENS: Map<string, [string[], string][]> = new Map([
  [
    "CHARACTER",
    [
      [["VARYING"], "VARCHAR"],
      [[], "CHAR"],
    ],
  ],
  ["CHAR", [[["VARYING"], "VARCHAR"]]],
  ["DOUBLE", [[["PRECISION"], "DOUBLE"]]],
  [
    "TIMESTAMP",
    [
      [["WITH", "LOCAL", "TIME", "ZONE"], "TIMESTAMPLTZ"],
      [["WITH", "TIME", "ZONE"], "TIMESTAMPTZ"],
      [["WITHOUT", "TIME", "ZONE"], "TIMESTAMP"],
    ],
  ],
  [
    "TIME",
    [
      [["WITH", "TIME", "ZONE"], "TIMETZ"],
      [["WITHOUT", "TIME", "ZONE"], "TIME"],
    ],
  ],
])

const STRUCT_TYPE_NAMES = new Set([
  "STRUCT",
  "OBJECT",
  "ROW",
  "NESTED",
  "UNION",
])
const NESTED_TYPE_NAMES = new Set([
  ...STRUCT_TYPE_NAMES,
  "ARRAY",
  "MAP",
  "LIST",
  "NULLABLE",
  "RANGE",
])

const IDENTIFIER_TOKENS = new Set([
  TokenType.VAR,
  TokenType.IDENTIFIER,
  // Keywords that can be used as identifiers (from Python ID_VAR_TOKENS)
  TokenType.ALL,
  TokenType.ANALYZE,
  TokenType.ANY,
  TokenType.ASC,
  TokenType.AT,
  TokenType.BEGIN,
  TokenType.CACHE,
  TokenType.COLLATE,
  TokenType.COLUMN,
  TokenType.COMMENT,
  TokenType.COMMIT,
  TokenType.CONSTRAINT,
  TokenType.COPY,
  TokenType.CUBE,
  TokenType.DEFAULT,
  TokenType.DELETE,
  TokenType.DESC,
  TokenType.END,
  TokenType.ESCAPE,
  TokenType.EXECUTE,
  TokenType.FALSE,
  TokenType.FETCH,
  TokenType.FILTER,
  TokenType.FIRST,
  TokenType.FOLLOWING,
  TokenType.FORMAT,
  TokenType.FUNCTION,
  TokenType.GLOB,
  TokenType.GLOBAL,
  TokenType.IF,
  TokenType.IGNORE,
  TokenType.INDEX,
  TokenType.INTERVAL,
  TokenType.KEY,
  TokenType.LANGUAGE,
  TokenType.LAST,
  TokenType.LATERAL,
  TokenType.LOAD,
  TokenType.LOCAL,
  TokenType.LOCK,
  TokenType.MAP,
  TokenType.MATCH,
  TokenType.MERGE,
  TokenType.MOD,
  TokenType.NEXT,
  TokenType.NULL,
  TokenType.OBJECT,
  TokenType.OFFSET,
  TokenType.ONLY,
  TokenType.OPTIONS,
  TokenType.ORDINALITY,
  TokenType.OVERWRITE,
  TokenType.PARTITION,
  TokenType.PERCENT,
  TokenType.PRECEDING,
  TokenType.PROCEDURE,
  TokenType.QUALIFY,
  TokenType.QUOTE,
  TokenType.RANGE,
  TokenType.RECURSIVE,
  TokenType.REFERENCES,
  TokenType.REFRESH,
  TokenType.REPLACE,
  TokenType.RETURNING,
  TokenType.ROLLBACK,
  TokenType.ROLLUP,
  TokenType.ROW,
  TokenType.ROWS,
  TokenType.SCHEMA,
  TokenType.SEED,
  TokenType.SEPARATOR,
  TokenType.SET,
  TokenType.SETTINGS,
  TokenType.SHOW,
  TokenType.SOME,
  TokenType.START,
  TokenType.STRAIGHT_JOIN,
  TokenType.STRUCT,
  TokenType.TABLE,
  TokenType.TEMPORARY,
  TokenType.TOP,
  TokenType.TRANSACTION,
  TokenType.TRUE,
  TokenType.TRUNCATE,
  TokenType.UNIQUE,
  TokenType.UNKNOWN,
  TokenType.UNNEST,
  TokenType.UPDATE,
  TokenType.USE,
  TokenType.VALUE,
  TokenType.VIEW,
  TokenType.VOLATILE,
  TokenType.WINDOW,
  TokenType.RLIKE,
  TokenType.ILIKE,
  TokenType.LIKE,
  TokenType.TIME,
  TokenType.DATE,
  TokenType.TIMESTAMP,
  TokenType.TIMESTAMPTZ,
  TokenType.XOR,
])

// Tokens that CANNOT be used as table aliases (join keywords, clause starters, etc.)
const TABLE_ALIAS_FORBIDDEN_TOKENS = new Set([
  TokenType.ANTI,
  TokenType.ASOF,
  TokenType.CROSS,
  TokenType.EXCEPT,
  TokenType.FULL,
  TokenType.GROUP_BY,
  TokenType.HAVING,
  TokenType.INNER,
  TokenType.INTERSECT,
  TokenType.JOIN,
  TokenType.LEFT,
  TokenType.LIMIT,
  TokenType.LOCK,
  TokenType.NATURAL,
  TokenType.OFFSET,
  TokenType.ON,
  TokenType.ORDER_BY,
  TokenType.OUTER,
  TokenType.QUALIFY,
  TokenType.RETURNING,
  TokenType.RIGHT,
  TokenType.SEMI,
  TokenType.SET,
  TokenType.UNION,
  TokenType.USING,
  TokenType.WHERE,
  TokenType.WINDOW,
])

// Tokens that CANNOT be used as expression aliases in SELECT
// (clause starters, operators, etc.)
const EXPRESSION_ALIAS_FORBIDDEN_TOKENS = new Set([
  TokenType.AND,
  TokenType.AS,
  TokenType.ASC,
  TokenType.BETWEEN,
  TokenType.BY,
  TokenType.CASE,
  TokenType.CROSS,
  TokenType.DESC,
  TokenType.ELSE,
  TokenType.END,
  TokenType.EXCEPT,
  TokenType.FOLLOWING,
  TokenType.FROM,
  TokenType.FULL,
  TokenType.GROUP_BY,
  TokenType.HAVING,
  TokenType.ILIKE,
  TokenType.IN,
  TokenType.INNER,
  TokenType.INTERSECT,
  TokenType.IS,
  TokenType.JOIN,
  TokenType.LEFT,
  TokenType.LIKE,
  TokenType.RLIKE,
  TokenType.LIMIT,
  TokenType.NOT,
  TokenType.OFFSET,
  TokenType.ON,
  TokenType.OR,
  TokenType.ORDER_BY,
  TokenType.OUTER,
  TokenType.OVER,
  TokenType.PARTITION_BY,
  TokenType.PRECEDING,
  TokenType.RANGE,
  TokenType.RIGHT,
  TokenType.ROWS,
  TokenType.THEN,
  TokenType.UNBOUNDED,
  TokenType.UNION,
  TokenType.USING,
  TokenType.WHEN,
  TokenType.WHERE,
  TokenType.WINDOW,
])

type StatementParser = (parser: Parser) => exp.Expression | undefined

type ExpressionParser<T extends exp.Expression = exp.Expression> = (
  parser: Parser,
) => T | undefined

export type FunctionBuilder = (
  args: exp.Expression[],
  dialect: DialectSettings,
) => exp.Expression

type SetItemParser = (parser: Parser) => exp.Expression | undefined

export class Parser {
  static STATEMENT_PARSERS: Map<TokenType, StatementParser> = new Map([
    [TokenType.SELECT, (p) => p.maybeParseSetOperations(p.parseSelect())],
    [TokenType.SET, (p) => p.parseSet()],
    [TokenType.WITH, (p) => p.parseWith()],
    [TokenType.FROM, (p) => p.maybeParseSetOperations(p.parseFromFirst())],
    [TokenType.CREATE, (p) => p.parseCreate()],
    [TokenType.ALTER, (p) => p.parseAlter()],
    [TokenType.COPY, (p) => p.parseCopy()],
    [TokenType.DESCRIBE, (p) => p.parseDescribe()],
    [TokenType.DELETE, (p) => p.parseDelete()],
    [TokenType.DROP, (p) => p.parseDrop()],
    [TokenType.INSERT, (p) => p.parseInsert()],
    [TokenType.UPDATE, (p) => p.parseUpdate()],
    [TokenType.USE, (p) => p.parseUse()],
    [TokenType.BEGIN, (p) => p.parseTransaction()],
    [TokenType.COMMIT, (p) => p.parseCommitOrRollback()],
    [TokenType.ROLLBACK, (p) => p.parseCommitOrRollback()],
    [TokenType.SEMICOLON, (_p) => new exp.Semicolon({})],
    [TokenType.TRUNCATE, (p) => p.parseTruncateTable()],
    [TokenType.MERGE, (p) => p.parseMerge()],
    [TokenType.SHOW, (p) => p.parseShow()],
    [TokenType.ANALYZE, (p) => p.parseAnalyze()],
  ])

  static SET_PARSERS: Map<string, SetItemParser> = new Map([
    ["GLOBAL", (p) => p.parseSetItemAssignment("GLOBAL")],
    ["LOCAL", (p) => p.parseSetItemAssignment("LOCAL")],
    ["SESSION", (p) => p.parseSetItemAssignment("SESSION")],
  ])

  static EXPRESSION_PARSERS: Map<
    new (
      ...args: unknown[]
    ) => exp.Expression,
    ExpressionParser
  > = new Map([
    // Can be extended by dialects, currently empty as placeholder
  ])

  static COLUMN_OPERATORS: Map<TokenType, ColumnOperatorHandler | null> =
    BASE_COLUMN_OPERATORS

  static DISJUNCTION: Map<TokenType, exp.ExpressionClass> = new Map([
    [TokenType.OR, exp.Or],
  ])

  static CONJUNCTION: Map<TokenType, exp.ExpressionClass> = new Map([
    [TokenType.AND, exp.And],
  ])

  static EQUALITY: Map<TokenType, exp.ExpressionClass> = new Map([
    [TokenType.EQ, exp.EQ],
    [TokenType.NEQ, exp.NEQ],
  ])

  static COMPARISON: Map<TokenType, exp.ExpressionClass> = new Map([
    [TokenType.GT, exp.GT],
    [TokenType.GTE, exp.GTE],
    [TokenType.LT, exp.LT],
    [TokenType.LTE, exp.LTE],
  ])

  static BITWISE: Map<TokenType, exp.ExpressionClass> = new Map([
    [TokenType.AMP, exp.BitwiseAnd],
    [TokenType.PIPE, exp.BitwiseOr],
    [TokenType.CARET, exp.BitwiseXor],
    [TokenType.LSHIFT, exp.BitwiseLeftShift],
    [TokenType.RSHIFT, exp.BitwiseRightShift],
  ])

  static TERM: Map<TokenType, exp.ExpressionClass> = new Map([
    [TokenType.PLUS, exp.Add],
    [TokenType.MINUS, exp.Sub],
    [TokenType.DPIPE, exp.DPipe],
  ])

  static FACTOR: Map<TokenType, exp.ExpressionClass> = new Map([
    [TokenType.STAR, exp.Mul],
    [TokenType.SLASH, exp.Div],
    [TokenType.DIV, exp.IntDiv],
    [TokenType.PERCENT, exp.Mod],
  ])

  static EXPONENT: Map<TokenType, exp.ExpressionClass> = new Map()

  static NO_PAREN_FUNCTIONS: Map<TokenType, exp.ExpressionClass> = new Map([
    [TokenType.CURRENT_DATE, exp.CurrentDate],
    [TokenType.CURRENT_DATETIME, exp.CurrentDatetime],
    [TokenType.CURRENT_TIME, exp.CurrentTime],
    [TokenType.CURRENT_TIMESTAMP, exp.CurrentTimestamp],
    [TokenType.CURRENT_USER, exp.CurrentUser],
    [TokenType.CURRENT_ROLE, exp.CurrentRole],
    [TokenType.LOCALTIME, exp.Localtime],
    [TokenType.LOCALTIMESTAMP, exp.Localtimestamp],
  ])

  static RANGE_PARSERS: Map<
    TokenType,
    (parser: Parser, left: exp.Expression) => exp.Expression
  > = new Map([
    [TokenType.BETWEEN, (p, left) => p.parseBetween(left)],
    [TokenType.IN, (p, left) => p.parseInList(left)],
    [TokenType.IS, (p, left) => p.parseIs(left)],
    [TokenType.LIKE, (p, left) => p.parseLikePattern(left, exp.Like)],
    [TokenType.ILIKE, (p, left) => p.parseLikePattern(left, exp.ILike)],
    [TokenType.GLOB, (p, left) => p.parseLikePattern(left, exp.Glob)],
    [TokenType.RLIKE, (p, left) => p.parseLikePattern(left, exp.RegexpLike)],
    [TokenType.TILDE, (p, left) => p.parseLikePattern(left, exp.RegexpLike)],
    [TokenType.IRLIKE, (p, left) => p.parseLikePattern(left, exp.RegexpILike)],
    // Postgres array operators
    [
      TokenType.AT_GT,
      (p, left) =>
        new exp.ArrayContainsAll({ this: left, expression: p.parseBitwise() }),
    ],
    [
      TokenType.LT_AT,
      (p, left) =>
        new exp.ArrayContainsAll({ this: p.parseBitwise(), expression: left }),
    ],
    // Array overlap operator &&
    [
      TokenType.DAMP,
      (p, left) =>
        new exp.ArrayOverlaps({ this: left, expression: p.parseBitwise() }),
    ],
    // ^@ (starts with)
    [
      TokenType.CARET_AT,
      (p, left) =>
        new exp.StartsWith({ this: left, expression: p.parseBitwise() }),
    ],
    // Postgres text search operator @@
    [
      TokenType.DAT,
      (p, left) =>
        new exp.MatchAgainst({ this: p.parseBitwise(), expressions: [left] }),
    ],
    // List comprehension: [x FOR x IN l IF condition]
    [TokenType.FOR, (p, left) => p.parseComprehension(left)],
  ])

  static FUNCTION_PARSERS: Map<string, (parser: Parser) => exp.Expression> =
    new Map([
      ["COUNT", (p) => p.parseCount()],
      ["TRIM", (p) => p.parseTrim()],
      ["MAP", (p) => p.parseMap()],
      ["SUBSTRING", (p) => p.parseSubstring()],
      ["OVERLAY", (p) => p.parseOverlay()],
    ])

  // Functions that can be called without parentheses (e.g., MAP {...})
  static NO_PAREN_FUNCTION_PARSERS: Map<
    string,
    (parser: Parser) => exp.Expression
  > = new Map([["MAP", (p) => p.parseMap()]])

  static FUNCTIONS: Map<string, FunctionBuilder> = (() => {
    const fns: Map<string, FunctionBuilder> = new Map()

    for (const [name, cls] of FUNCTION_BY_NAME) {
      fns.set(name, (args, _dialect) => fromArgList(cls, args))
    }

    const coalesceBuilder: FunctionBuilder = (args, _dialect) =>
      new exp.Coalesce({ this: args[0], expressions: args.slice(1) })
    fns.set("COALESCE", coalesceBuilder)
    fns.set("NVL", coalesceBuilder)
    fns.set("IFNULL", coalesceBuilder)

    fns.set(
      "CONCAT",
      (args, dialect) =>
        new exp.Concat({
          expressions: args,
          safe: !dialect.STRICT_STRING_CONCAT,
          coalesce: dialect.CONCAT_COALESCE,
        }),
    )

    fns.set(
      "CONCAT_WS",
      (args, dialect) =>
        new exp.ConcatWs({
          expressions: args,
          safe: !dialect.STRICT_STRING_CONCAT,
          coalesce: dialect.CONCAT_COALESCE,
        }),
    )

    fns.set(
      "COUNT",
      (args, _dialect) =>
        new exp.Count({
          this: args[0],
          expressions: args.slice(1),
          big_int: true,
        }),
    )

    fns.set(
      "LTRIM",
      (args, _dialect) =>
        new exp.Trim({
          this: args[0],
          expression: args[1],
          position: "LEADING",
        }),
    )
    fns.set(
      "RTRIM",
      (args, _dialect) =>
        new exp.Trim({
          this: args[0],
          expression: args[1],
          position: "TRAILING",
        }),
    )

    fns.set(
      "LPAD",
      (args, _dialect) =>
        new exp.Pad({
          this: args[0],
          expression: args[1],
          fill_pattern: args[2],
          is_left: true,
        }),
    )
    fns.set("LEFTPAD", fns.get("LPAD")!)
    fns.set(
      "RPAD",
      (args, _dialect) =>
        new exp.Pad({
          this: args[0],
          expression: args[1],
          fill_pattern: args[2],
          is_left: false,
        }),
    )
    fns.set("RIGHTPAD", fns.get("RPAD")!)

    fns.set(
      "LOG2",
      (args, _dialect) =>
        new exp.Log({
          this: new exp.Literal({ this: "2", is_string: false }),
          expression: args[0],
        }),
    )
    fns.set(
      "LOG10",
      (args, _dialect) =>
        new exp.Log({
          this: new exp.Literal({ this: "10", is_string: false }),
          expression: args[0],
        }),
    )

    fns.set("MOD", (args, _dialect) => {
      const first = args[0]
      const second = args[1]
      const wrappedFirst =
        first instanceof exp.Binary ? new exp.Paren({ this: first }) : first
      const wrappedSecond =
        second instanceof exp.Binary ? new exp.Paren({ this: second }) : second
      return new exp.Mod({ this: wrappedFirst, expression: wrappedSecond })
    })

    // Bitwise aggregate functions
    fns.set(
      "BIT_OR",
      (args, _dialect) => new exp.BitwiseOrAgg({ this: args[0] }),
    )
    fns.set(
      "BIT_AND",
      (args, _dialect) => new exp.BitwiseAndAgg({ this: args[0] }),
    )
    fns.set(
      "BIT_XOR",
      (args, _dialect) => new exp.BitwiseXorAgg({ this: args[0] }),
    )

    // Date/time functions
    fns.set("MAKE_DATE", (args, _dialect) =>
      args.length === 3
        ? new exp.DateFromParts({ year: args[0], month: args[1], day: args[2] })
        : new exp.Anonymous({ this: "MAKE_DATE", expressions: args }),
    )
    fns.set("EPOCH", (args, _dialect) => new exp.TimeToUnix({ this: args[0] }))
    fns.set(
      "EPOCH_MS",
      (args, _dialect) =>
        new exp.UnixToTime({ this: args[0], scale: exp.Literal.number("3") }),
    )
    fns.set(
      "TO_TIMESTAMP",
      (args, _dialect) => new exp.UnixToTime({ this: args[0] }),
    )

    fns.set(
      "REGEXP_LIKE",
      (args, _dialect) =>
        new exp.RegexpLike({
          this: args[0],
          expression: args[1],
          flag: args[2],
        }),
    )

    // HEX/UNHEX functions
    const hexBuilder: FunctionBuilder = (args, dialect) =>
      dialect.HEX_LOWERCASE
        ? new exp.LowerHex({ this: args[0] })
        : new exp.Hex({ this: args[0] })
    fns.set("HEX", hexBuilder)
    fns.set("TO_HEX", hexBuilder)
    fns.set("UNHEX", (args, _dialect) => new exp.Unhex({ this: args[0] }))
    fns.set("FROM_HEX", (args, _dialect) => new exp.Unhex({ this: args[0] }))

    // Binary+Func hybrids (not auto-registered via FUNCTION_BY_NAME since they extend Binary)
    const powBuilder: FunctionBuilder = (args) =>
      new exp.Pow({ this: args[0], expression: args[1] })
    fns.set("POW", powBuilder)
    fns.set("POWER", powBuilder)

    // Override auto-registered ArrayConcat: isVarLenArgs + null_propagation confuses fromArgList
    const arrayConcatBuilder: FunctionBuilder = (args) =>
      new exp.ArrayConcat({ this: args[0], expressions: args.slice(1) })
    fns.set("ARRAY_CONCAT", arrayConcatBuilder)
    fns.set("ARRAY_CAT", arrayConcatBuilder)

    fns.set("SCOPE_RESOLUTION", (args) =>
      args.length !== 2
        ? new exp.ScopeResolution({ expression: args[0] })
        : new exp.ScopeResolution({ this: args[0], expression: args[1] }),
    )

    return fns
  })()

  static TYPE_NAME_MAPPING: Map<string, string> = new Map()
  static TYPE_CONVERTERS: Map<string, (dt: exp.DataType) => exp.DataType> =
    new Map()
  static ADD_JOIN_ON_TRUE = false
  static STRICT_CAST = true
  static SET_REQUIRES_ASSIGNMENT_DELIMITER = true
  static FUNCTIONS_WITH_ALIASED_ARGS: Set<string> = new Set(["STRUCT"])

  private tokens: Token[] = []
  private index = 0
  private tokenizer: Tokenizer
  private _dialect: DialectSettings = DEFAULT_DIALECT_SETTINGS
  protected functions: Map<string, FunctionBuilder>
  protected functionParsers: Map<string, (parser: Parser) => exp.Expression>
  protected statementParsers: Map<TokenType, StatementParser>
  protected rangeParsers: Map<
    TokenType,
    (parser: Parser, left: exp.Expression) => exp.Expression
  >
  protected noParenFunctions: Map<TokenType, exp.ExpressionClass>
  protected setParsers: Map<string, SetItemParser>
  protected typeNameMapping: Map<string, string>
  protected typeConverters: Map<string, (dt: exp.DataType) => exp.DataType>

  // Error handling
  protected errorLevel: ErrorLevel = ErrorLevel.IMMEDIATE
  protected errors: ParseErrorDetail[] = []
  protected maxErrors = 3
  protected errorMessageContext = 100
  protected sql = ""

  constructor(_options: ParserOptions = {}) {
    this.tokenizer = _options.tokenizer ?? new Tokenizer()
    this.functions = new Map((this.constructor as typeof Parser).FUNCTIONS)
    this.functionParsers = new Map(
      (this.constructor as typeof Parser).FUNCTION_PARSERS,
    )
    this.statementParsers = new Map(
      (this.constructor as typeof Parser).STATEMENT_PARSERS,
    )
    this.rangeParsers = new Map(
      (this.constructor as typeof Parser).RANGE_PARSERS,
    )
    this.noParenFunctions = new Map(
      (this.constructor as typeof Parser).NO_PAREN_FUNCTIONS,
    )
    this.setParsers = new Map((this.constructor as typeof Parser).SET_PARSERS)
    this.typeNameMapping = new Map(
      (this.constructor as typeof Parser).TYPE_NAME_MAPPING,
    )
    this.typeConverters = new Map(
      (this.constructor as typeof Parser).TYPE_CONVERTERS,
    )
  }

  get dialect(): DialectSettings {
    return this._dialect
  }

  setDialect(dialect: DialectSettings): void {
    this._dialect = dialect
  }

  protected prevComments: string[] | undefined

  protected expression<T extends exp.Expression>(
    cls: exp.ExpressionClass<T>,
    args: exp.Args = {},
    comments?: string[],
  ): T {
    const instance = new cls(args)
    if (comments) {
      this.addComments(instance, comments)
    } else {
      this.addPrevComments(instance)
    }
    return instance
  }

  protected addComments(expr: exp.Expression, comments: string[]): void {
    if (expr.comments) {
      expr.comments.push(...comments)
    } else {
      expr.comments = [...comments]
    }
  }

  private addPrevComments(expr: exp.Expression): void {
    if (this.prevComments) {
      this.addComments(expr, this.prevComments)
      this.prevComments = undefined
    }
  }

  parse(sql: string): exp.Expression[] {
    this.sql = sql
    this.errors = []
    this.tokens = this.tokenizer.tokenize(sql)
    this.index = 0

    const expressions: exp.Expression[] = []

    while (!this.isEnd()) {
      const expr = this.parseStatement()
      if (expr) {
        expressions.push(expr)
      }
      this.match(TokenType.SEMICOLON)
    }

    this.checkErrors()
    return expressions
  }

  /**
   * Logs or raises any found errors, depending on the chosen error level setting.
   */
  protected checkErrors(): void {
    if (this.errorLevel === ErrorLevel.WARN) {
      for (const error of this.errors) {
        console.error(`Parse error: ${error.description}`)
      }
    } else if (this.errorLevel === ErrorLevel.RAISE && this.errors.length > 0) {
      const messages = this.errors
        .slice(0, this.maxErrors)
        .map((e) => e.description)
      const remaining = this.errors.length - this.maxErrors
      if (remaining > 0) {
        messages.push(`... and ${remaining} more`)
      }
      throw new ParseError(messages.join("\n\n"), this.errors)
    }
  }

  /**
   * Appends an error in the list of recorded errors or raises it, depending on the chosen
   * error level setting.
   */
  protected raiseError(message: string, token?: Token): void {
    const errorToken =
      token ??
      this.current ??
      this.tokens[this.index - 1] ??
      new Token(TokenType.EOF, "")
    const { line, col, start, end } = errorToken

    // Extract context around the error
    const contextStart = Math.max(0, start - this.errorMessageContext)
    const contextEnd = Math.min(
      this.sql.length,
      end + this.errorMessageContext + 1,
    )

    const startContext = this.sql.slice(contextStart, start)
    const highlight = this.sql.slice(start, end + 1)
    const endContext = this.sql.slice(end + 1, contextEnd)

    const formattedMessage = `${message}. Line ${line}, Col: ${col}.\n  ${startContext}${highlight}${endContext}`

    const errorDetail: ParseErrorDetail = {
      description: message,
      line,
      col,
      startContext,
      highlight,
      endContext,
    }

    if (this.errorLevel === ErrorLevel.IMMEDIATE) {
      throw new ParseError(formattedMessage, [errorDetail])
    }

    this.errors.push(errorDetail)
  }

  parseOne(sql: string): exp.Expression {
    const expressions = this.parse(sql)
    const [first] = expressions
    if (expressions.length !== 1 || !first) {
      throw new Error(
        `Expected exactly one expression, got ${expressions.length}`,
      )
    }
    return first
  }

  parseAs<T extends exp.Expression>(
    type: new (...args: unknown[]) => T,
  ): T | undefined {
    const parserFn = (this.constructor as typeof Parser).EXPRESSION_PARSERS.get(
      type,
    )
    if (parserFn) {
      return parserFn(this) as T | undefined
    }
    return undefined
  }

  protected get current(): Token {
    return this.tokens[this.index] ?? new Token(TokenType.EOF, "")
  }

  protected get currentTokenType(): TokenType {
    return this.current.tokenType
  }

  protected get prev(): Token {
    return this.tokens[this.index - 1] ?? new Token(TokenType.EOF, "")
  }

  protected peek(offset = 0): Token {
    return this.tokens[this.index + offset] ?? new Token(TokenType.EOF, "")
  }

  protected isConnected(): boolean {
    const prev = this.tokens[this.index - 1]
    const curr = this.tokens[this.index]
    return (
      prev !== undefined && curr !== undefined && prev.end + 1 === curr.start
    )
  }

  protected isIdentifierToken(tokenType?: TokenType): boolean {
    return IDENTIFIER_TOKENS.has(tokenType ?? this.currentTokenType)
  }

  protected isEnd(): boolean {
    return this.current.tokenType === TokenType.EOF
  }

  protected advance(): Token {
    const token = this.current
    this.index++
    this.prevComments = token.comments.length > 0 ? token.comments : undefined
    return token
  }

  protected retreat(): void {
    this.index = Math.max(0, this.index - 1)
  }

  protected match(...types: TokenType[]): boolean {
    for (const type of types) {
      if (this.current.tokenType === type) {
        this.advance()
        return true
      }
    }
    return false
  }

  protected expect(type: TokenType, message?: string): Token {
    if (this.current.tokenType !== type) {
      this.raiseError(
        message ?? `Expected ${type}, got ${this.current.tokenType}`,
      )
    }
    return this.advance()
  }

  protected expectGT(): void {
    if (this.current.tokenType === TokenType.RSHIFT) {
      // >> tokenized as RSHIFT — consume one > by replacing with GT (don't advance)
      // The next expectGT/expect(GT) will consume the remaining >
      this.tokens[this.index] = new Token(TokenType.GT, ">")
      return
    }
    this.expect(TokenType.GT)
  }

  protected matchText(...texts: string[]): boolean {
    const upper = this.current.text.toUpperCase()
    for (const text of texts) {
      if (upper === text.toUpperCase()) {
        this.advance()
        return true
      }
    }
    return false
  }

  protected matchPair(token1: TokenType, token2: TokenType): boolean {
    if (this.current.tokenType === token1 && this.peek().tokenType === token2) {
      this.advance()
      this.advance()
      return true
    }
    return false
  }

  protected matchTextSeq(...texts: string[]): boolean {
    const index = this.index
    for (const text of texts) {
      if (
        this.current.tokenType !== TokenType.STRING &&
        this.current.text.toUpperCase() === text.toUpperCase()
      ) {
        this.advance()
      } else {
        this.index = index
        return false
      }
    }
    return true
  }

  // Statement parsing
  protected parseStatement(): exp.Expression | undefined {
    const parser = this.statementParsers.get(this.current.tokenType)
    if (parser) {
      this.advance()
      return parser(this)
    }

    // Try to parse as an expression first (like Python's _parse_statement lines 2072-2074)
    // This handles cases like "STRING_TO_ARRAY('xx', 'yy')" as a standalone expression
    if (!this.isEnd() && this.current.tokenType !== TokenType.SEMICOLON) {
      const index = this.index
      try {
        let expr = this.parseExpression()
        // Handle set operations (UNION/EXCEPT/INTERSECT) after parenthesized select
        if (expr instanceof exp.Subquery) {
          expr = this.maybeParseSetOperations(expr)
        }
        // Check for implicit alias (e.g., "0 b1010" → "0 AS b1010")
        if (
          expr &&
          !this.isEnd() &&
          (this.current.tokenType as string) !== TokenType.SEMICOLON
        ) {
          if (this.match(TokenType.AS)) {
            const alias = this.parseIdentifier()
            expr = new exp.Alias({ this: expr, alias })
          } else if (
            IDENTIFIER_TOKENS.has(this.current.tokenType) &&
            !EXPRESSION_ALIAS_FORBIDDEN_TOKENS.has(this.current.tokenType)
          ) {
            const alias = this.parseIdentifier()
            expr = new exp.Alias({ this: expr, alias })
          }
        }
        // If we successfully parsed and we're at end/semicolon, return the expression
        const currentToken = this.current // Re-read after parsing to avoid type narrowing issues
        if (
          expr &&
          (this.isEnd() || currentToken.tokenType === TokenType.SEMICOLON)
        ) {
          return expr
        }
      } catch (e) {
        // If we consumed tokens before failing, re-throw (expression was partially parsed)
        if (this.index > index) {
          throw e
        }
        // Otherwise fall through to command parsing
      }
      // Reset and fall through to command parsing
      this.index = index
      return this.parseCommand()
    }

    return undefined
  }

  // SELECT parsing
  parseSelect(): exp.Select {
    const args: exp.Args = {}

    // AS STRUCT / AS VALUE (BigQuery)
    if (this.match(TokenType.AS)) {
      if (this.matchText("STRUCT", "VALUE")) {
        args.kind = this.prev.text.toUpperCase()
      } else {
        this.retreat()
      }
    }

    // DISTINCT [ON (expr, ...)]
    if (this.match(TokenType.DISTINCT)) {
      let on: exp.Expression | undefined
      if (this.match(TokenType.ON)) {
        this.expect(TokenType.L_PAREN)
        on = new exp.Tuple({
          expressions: this.parseCSV(() => this.parseExpression()),
        })
        this.expect(TokenType.R_PAREN)
      }
      args.distinct = new exp.Distinct({ on })
    }

    // ALL (ignore)
    this.match(TokenType.ALL)

    // Select expressions
    args.expressions = this.parseSelectExpressions()

    // FROM
    if (this.match(TokenType.FROM)) {
      args.from_ = this.parseFrom()
    }

    const select = new exp.Select(args)
    return this.parseQueryModifiers(select)
  }

  parseFromFirst(): exp.Expression {
    const from = this.parseFrom()
    // Parse JOINs and comma-separated tables before checking for SELECT
    const joins: exp.Join[] = []
    while (this.isJoinKeyword() || this.current.tokenType === TokenType.COMMA) {
      const join = this.parseJoin()
      if (join) joins.push(join)
    }
    if (this.match(TokenType.SELECT)) {
      const select = this.parseSelect()
      if (!select.args.from_) {
        select.args.from_ = from
        from.parent = select
      }
      if (joins.length > 0) {
        select.args.joins = [
          ...((select.args.joins as exp.Join[]) || []),
          ...joins,
        ]
        for (const j of joins) j.parent = select
      }
      return select
    }
    // No SELECT follows: create SELECT * FROM ...
    const star = new exp.Star({})
    const select = new exp.Select({
      expressions: [star],
      from_: from,
      ...(joins.length > 0 ? { joins } : {}),
    })
    return this.parseQueryModifiers(select)
  }

  parseWith(): exp.Expression {
    const recursive = this.match(TokenType.RECURSIVE)

    const ctes: exp.CTE[] = []
    do {
      const cte = this.parseCTE()
      if (cte) ctes.push(cte)
    } while (this.match(TokenType.COMMA))

    const withExpr = new exp.With({
      expressions: ctes,
      recursive: recursive || undefined,
    })

    // Parse the statement following the CTEs — dispatch via STATEMENT_PARSERS
    const statementParsers = (this.constructor as typeof Parser)
      .STATEMENT_PARSERS
    const parser = statementParsers.get(this.currentTokenType)
    if (parser) {
      this.advance()
      const stmt = parser(this)
      if (
        stmt &&
        "with_" in (stmt.constructor as typeof exp.Expression).argTypes
      ) {
        stmt.args.with_ = withExpr
        withExpr.parent = stmt
        return stmt
      }
      if (stmt) return stmt
    }

    // Fallback: collect remaining tokens as command
    const parts: string[] = []
    while (!this.isEnd() && this.current.tokenType !== TokenType.SEMICOLON) {
      parts.push(this.advance().text)
    }
    if (parts.length > 0) {
      const command = new exp.Command({
        this: new exp.Literal({ this: parts.join(" "), isString: false }),
      })
      command.args.with_ = withExpr
      return command
    }

    return withExpr
  }

  protected parseCTE(): exp.CTE | undefined {
    // Parse table alias: name(col1, col2, ...)
    const alias = this.parseTableAlias()
    if (!alias) return undefined

    // Parse optional USING KEY (col1, col2, ...)
    let keyExpressions: exp.Expression[] | undefined
    if (this.matchTextSeq("USING", "KEY")) {
      this.expect(TokenType.L_PAREN)
      keyExpressions = this.parseCSV(() => this.parseIdentifier())
      this.expect(TokenType.R_PAREN)
    }

    // Expect AS
    if (!this.match(TokenType.AS)) {
      return undefined
    }

    // Parse optional MATERIALIZED
    let materialized: boolean | undefined
    if (this.matchTextSeq("NOT", "MATERIALIZED")) {
      materialized = false
    } else if (this.matchText("MATERIALIZED")) {
      materialized = true
    }

    // Parse wrapped statement
    this.expect(TokenType.L_PAREN)
    const query = this.parseWrappedCTEQuery()
    this.expect(TokenType.R_PAREN)

    return new exp.CTE({
      this: query,
      alias,
      key_expressions: keyExpressions,
      materialized,
    })
  }

  private parseTableAlias(): exp.TableAlias | undefined {
    let name: exp.Identifier | undefined
    if (IDENTIFIER_TOKENS.has(this.current.tokenType)) {
      name = this.parseIdentifier()
    } else if (this.current.tokenType === TokenType.STRING) {
      const raw = this.advance().text
      const inner = raw.slice(1, -1)
      name = new exp.Identifier({ this: inner, quoted: true })
    }
    if (!name) return undefined

    let columns: exp.Expression[] | undefined
    if (this.match(TokenType.L_PAREN)) {
      columns = this.parseCSV(() => this.parseIdentifier())
      this.expect(TokenType.R_PAREN)
    }

    return new exp.TableAlias({ this: name, columns })
  }

  protected parseWrappedCTEQuery(): exp.Expression {
    if (this.match(TokenType.SELECT)) {
      return this.maybeParseSetOperations(this.parseSelect())
    }
    if (this.match(TokenType.FROM)) {
      return this.maybeParseSetOperations(this.parseFromFirst())
    }
    // Check STATEMENT_PARSERS for dialect-specific entries (e.g. PIVOT/UNPIVOT)
    const statementParsers = (this.constructor as typeof Parser)
      .STATEMENT_PARSERS
    const parser = statementParsers.get(this.currentTokenType)
    if (parser) {
      this.advance()
      const stmt = parser(this)
      if (stmt) return stmt
    }
    // VALUES or other statements in CTEs
    return this.parseExpression()
  }

  protected maybeParseSetOperations(result: exp.Expression): exp.Expression {
    let current = result
    while (true) {
      let cls: exp.ExpressionClass | undefined
      if (this.match(TokenType.UNION)) {
        cls = exp.Union
      } else if (this.match(TokenType.EXCEPT)) {
        cls = exp.Except
      } else if (this.match(TokenType.INTERSECT)) {
        cls = exp.Intersect
      } else {
        break
      }

      let distinct: boolean | undefined
      if (this.match(TokenType.ALL)) {
        distinct = false
      } else if (this.match(TokenType.DISTINCT)) {
        distinct = true
      }

      const byName = this.matchTextSeq("BY", "NAME")

      let right: exp.Expression
      if (this.match(TokenType.FROM)) {
        right = this.parseFromFirst()
      } else if (this.match(TokenType.L_PAREN)) {
        right = this.parseSubquery()
        this.expect(TokenType.R_PAREN)
      } else {
        this.expect(TokenType.SELECT)
        right = this.parseSelect()
      }

      current = new cls({
        this: current,
        expression: right,
        distinct,
        ...(byName ? { by_name: true } : {}),
      })
    }
    return current
  }

  private parseSelectExpressions(): exp.Expression[] {
    return this.parseCSV(() => this.parseSelectExpression())
  }

  private parseSelectExpression(): exp.Expression {
    // Check for *
    if (this.match(TokenType.STAR)) {
      return new exp.Star({})
    }

    // Check for table.*
    if (
      this.current.tokenType === TokenType.VAR &&
      this.peek(1).tokenType === TokenType.DOT
    ) {
      const table = this.advance()
      this.advance() // .
      if (this.match(TokenType.STAR)) {
        return new exp.Column({
          this: new exp.Star({}),
          table: new exp.Identifier({ this: table.text }),
        })
      }
      this.retreat()
      this.retreat()
    }

    const expr = this.parseExpression()

    // Check for alias
    if (this.match(TokenType.AS)) {
      if (this.match(TokenType.L_PAREN)) {
        const expressions = this.parseCSV(() => this.parseIdentifier())
        this.expect(TokenType.R_PAREN)
        return new exp.Aliases({ this: expr, expressions })
      }
      const alias = this.parseIdentifier()
      return new exp.Alias({ this: expr, alias })
    }

    // Implicit alias (identifier after expression)
    // Must be an identifier token but NOT a forbidden expression alias token
    const tokenType = this.current.tokenType
    if (
      IDENTIFIER_TOKENS.has(tokenType) &&
      !EXPRESSION_ALIAS_FORBIDDEN_TOKENS.has(tokenType)
    ) {
      const alias = this.parseIdentifier()
      return new exp.Alias({ this: expr, alias })
    }

    return expr
  }

  private parseGroupBy(): exp.Group {
    const groupArgs: exp.Args = {}
    const expressions: exp.Expression[] = []
    const cubeList: exp.Cube[] = []
    const rollupList: exp.Rollup[] = []
    const groupingSetsList: exp.GroupingSets[] = []

    const parseGroupByItem = (): exp.Expression => {
      if (this.match(TokenType.CUBE)) {
        this.expect(TokenType.L_PAREN)
        const exprs = this.parseCSV(() => this.parseExpression())
        this.expect(TokenType.R_PAREN)
        return new exp.Cube({ expressions: exprs })
      }
      if (this.match(TokenType.ROLLUP)) {
        this.expect(TokenType.L_PAREN)
        const exprs = this.parseCSV(() => this.parseExpression())
        this.expect(TokenType.R_PAREN)
        return new exp.Rollup({ expressions: exprs })
      }
      if (this.match(TokenType.GROUPING_SETS)) {
        this.matchText("SETS")
        this.expect(TokenType.L_PAREN)
        const exprs = this.parseCSV(() => this.parseExpression())
        this.expect(TokenType.R_PAREN)
        return new exp.GroupingSets({ expressions: exprs })
      }
      return this.parseExpression()
    }

    const items = this.parseCSV(parseGroupByItem)
    for (const item of items) {
      if (item instanceof exp.Cube) cubeList.push(item)
      else if (item instanceof exp.Rollup) rollupList.push(item)
      else if (item instanceof exp.GroupingSets) groupingSetsList.push(item)
      else expressions.push(item)
    }

    if (expressions.length > 0) groupArgs.expressions = expressions
    if (cubeList.length > 0) groupArgs.cube = cubeList
    if (rollupList.length > 0) groupArgs.rollup = rollupList
    if (groupingSetsList.length > 0) groupArgs.grouping_sets = groupingSetsList

    return new exp.Group(groupArgs)
  }

  private parseFrom(): exp.From {
    const table = this.parseTableExpression()
    return new exp.From({ this: table })
  }

  private parseMatchRecognizeMeasure(): exp.MatchRecognizeMeasure {
    const upper = this.current.text.toUpperCase()
    let windowFrame: string | undefined
    if (upper === "FINAL" || upper === "RUNNING") {
      windowFrame = upper
      this.advance()
    }
    let thisExpr: exp.Expression = this.parseExpression()
    if (this.match(TokenType.AS)) {
      const alias = this.parseIdentifier()
      thisExpr = new exp.Alias({ this: thisExpr, alias })
    }
    return new exp.MatchRecognizeMeasure({
      this: thisExpr,
      window_frame: windowFrame,
    })
  }

  protected parseMatchRecognize(): exp.MatchRecognize {
    this.expect(TokenType.L_PAREN)

    // PARTITION BY
    let partitionBy: exp.Expression[] | undefined
    if (this.match(TokenType.PARTITION_BY)) {
      partitionBy = this.parseCSV(() => this.parseDisjunction())
    }

    // ORDER BY
    let order: exp.Order | undefined
    if (this.match(TokenType.ORDER_BY)) {
      order = this.parseOrder()
    }

    // MEASURES
    let measures: exp.MatchRecognizeMeasure[] | undefined
    if (this.matchTextSeq("MEASURES")) {
      measures = this.parseCSV(() => this.parseMatchRecognizeMeasure())
    }

    // ONE ROW PER MATCH / ALL ROWS PER MATCH
    let rows: exp.Var | undefined
    if (this.matchTextSeq("ONE", "ROW", "PER", "MATCH")) {
      rows = new exp.Var({ this: "ONE ROW PER MATCH" })
    } else if (this.matchTextSeq("ALL", "ROWS", "PER", "MATCH")) {
      let text = "ALL ROWS PER MATCH"
      if (this.matchTextSeq("SHOW", "EMPTY", "MATCHES")) {
        text += " SHOW EMPTY MATCHES"
      } else if (this.matchTextSeq("OMIT", "EMPTY", "MATCHES")) {
        text += " OMIT EMPTY MATCHES"
      } else if (this.matchTextSeq("WITH", "UNMATCHED", "ROWS")) {
        text += " WITH UNMATCHED ROWS"
      }
      rows = new exp.Var({ this: text })
    }

    // AFTER MATCH SKIP
    let after: exp.Var | undefined
    if (this.matchTextSeq("AFTER", "MATCH", "SKIP")) {
      let text = "AFTER MATCH SKIP"
      if (this.matchTextSeq("PAST", "LAST", "ROW")) {
        text += " PAST LAST ROW"
      } else if (this.matchTextSeq("TO", "NEXT", "ROW")) {
        text += " TO NEXT ROW"
      } else if (this.matchTextSeq("TO", "FIRST")) {
        text += ` TO FIRST ${this.advance().text}`
      } else if (this.matchTextSeq("TO", "LAST")) {
        text += ` TO LAST ${this.advance().text}`
      }
      after = new exp.Var({ this: text })
    }

    // PATTERN (...)
    let pattern: exp.Var | undefined
    if (this.matchTextSeq("PATTERN")) {
      this.expect(TokenType.L_PAREN)

      if (this.isEnd()) {
        this.raiseError("Expecting )")
      }

      let paren = 1
      const start = this.current

      while (!this.isEnd() && paren > 0) {
        if (this.current.tokenType === TokenType.L_PAREN) {
          paren++
        }
        if (this.current.tokenType === TokenType.R_PAREN) {
          paren--
        }

        const end = this.prev
        this.advance()

        if (paren === 0) {
          pattern = new exp.Var({
            this: this.sql.slice(start.start, end.end),
          })
        }
      }

      if (paren > 0) {
        this.raiseError("Expecting )")
      }
    }

    // DEFINE
    let define: exp.Expression[] | undefined
    if (this.matchTextSeq("DEFINE")) {
      define = this.parseCSV(() => this.parseNameAsExpression())
    }

    this.expect(TokenType.R_PAREN)

    const alias = this.parseTableAlias()

    return new exp.MatchRecognize({
      partition_by: partitionBy,
      order,
      measures,
      rows,
      after,
      pattern,
      define,
      alias,
    })
  }

  private parseNameAsExpression(): exp.Expression {
    const thisExpr =
      this.current.tokenType === TokenType.STRING
        ? new exp.Identifier({
            this: this.advance().text.slice(1, -1),
            quoted: true,
          })
        : new exp.Identifier({ this: this.advance().text })
    if (this.match(TokenType.AS)) {
      return new exp.Alias({ alias: thisExpr, this: this.parseDisjunction() })
    }
    return thisExpr
  }

  static QUERY_MODIFIER_PARSERS: Partial<
    Record<
      TokenType,
      (
        parser: Parser,
      ) => [string, exp.Expression | exp.Expression[] | undefined]
    >
  > = {
    [TokenType.MATCH_RECOGNIZE]: (p) => ["match", p.parseMatchRecognize()],
    [TokenType.WHERE]: (p) => ["where", p.parseWhere()],
    [TokenType.GROUP_BY]: (p) => ["group", p.parseGroupBy()],
    [TokenType.HAVING]: (p) => ["having", p.parseHaving()],
    [TokenType.QUALIFY]: (p) => ["qualify", p.parseQualify()],
    [TokenType.WINDOW]: (p) => ["windows", p.parseWindowClause()],
    [TokenType.ORDER_BY]: (p) => ["order", p.parseOrder()],
    [TokenType.LIMIT]: (p) => ["limit", p.parseLimit()],
    [TokenType.OFFSET]: (p) => ["offset", p.parseOffset()],
    [TokenType.FETCH]: (p) => ["limit", p.parseFetch()],
  }

  protected parseQueryModifiers(select: exp.Select): exp.Select {
    // Parse JOINs
    while (this.isJoinKeyword() || this.current.tokenType === TokenType.COMMA) {
      const join = this.parseJoin()
      if (join) {
        select.append("joins", join)
      } else {
        break
      }
    }

    // Parse LATERALs
    while (true) {
      const lateral = this.parseLateral()
      if (!lateral) break
      select.append("laterals", lateral)
    }

    // Parse query modifiers (WHERE, GROUP BY, HAVING, etc.)
    const modifierParsers = (this.constructor as typeof Parser)
      .QUERY_MODIFIER_PARSERS
    while (true) {
      const tokenType = this.current.tokenType
      const parser = modifierParsers[tokenType]
      if (!parser) break
      this.advance()
      const [key, value] = parser(this)
      if (value) {
        select.set(key, value)
      } else {
        break
      }
    }

    // USING SAMPLE / TABLESAMPLE (handled separately — parseTableSample does its own token matching)
    const sample = this.parseTableSample(true)
    if (sample) {
      select.set("sample", sample)
    }

    // FOR UPDATE / FOR SHARE / LOCK IN SHARE MODE (handled separately — parseLocks does its own FOR matching)
    const locks = this.parseLocks()
    if (locks.length > 0) {
      select.set("locks", locks)
    }

    return select
  }

  protected parseWhere(): exp.Where {
    return new exp.Where({ this: this.parseExpression() })
  }

  protected parseHaving(): exp.Having {
    return new exp.Having({ this: this.parseExpression() })
  }

  protected parseQualify(): exp.Qualify {
    return new exp.Qualify({ this: this.parseExpression() })
  }

  protected parseWindowClause(): exp.Expression[] {
    return this.parseCSV(() => this.parseNamedWindow())
  }

  protected parseOrder(): exp.Order {
    return new exp.Order({
      expressions: this.parseCSV(() => this.parseOrdered()),
    })
  }

  protected parseLimitOptions(): exp.LimitOptions | undefined {
    const percent = this.match(TokenType.PERCENT, TokenType.MOD)
    const rows = this.match(TokenType.ROW, TokenType.ROWS)
    this.matchText("ONLY")
    const withTies = this.matchTextSeq("WITH", "TIES")
    if (!percent && !rows && !withTies) return undefined
    return new exp.LimitOptions({ percent, rows, with_ties: withTies })
  }

  protected parseLimit(): exp.Limit {
    // LIMIT x% (PERCENT) parses as Mod if we use parseTerm — backtrack and use parseUnary
    const savedIndex = this.index
    let expression: exp.Expression
    try {
      expression = this.parseTerm()
      if (expression instanceof exp.Mod) {
        this.index = savedIndex
        expression = this.parseUnary()
      }
    } catch {
      this.index = savedIndex
      expression = this.parseUnary()
    }
    const limitOptions = this.parseLimitOptions()
    return new exp.Limit({ this: expression, limit_options: limitOptions })
  }

  protected parseOffset(): exp.Offset {
    return new exp.Offset({ this: this.parsePrimary() })
  }

  protected parseFetch(): exp.Fetch {
    const direction = this.match(TokenType.FIRST, TokenType.NEXT)
      ? this.prev.text.toUpperCase()
      : "FIRST"
    const count = this.parseTerm()
    const limitOptions = this.parseLimitOptions()
    return new exp.Fetch({ direction, count, limit_options: limitOptions })
  }

  protected parseTableExpression(): exp.Expression {
    const table = this.parseTableAtom()

    // Parse table-level TABLESAMPLE (after table name and alias)
    const sample = this.parseTableSample(false)
    if (sample) {
      const target = table instanceof exp.Alias ? table.this : table
      if (target instanceof exp.Table || target instanceof exp.Subquery) {
        target.args.sample = sample
        sample.parent = target
      }
    }

    // Parse PIVOT/UNPIVOT clauses
    const pivots = this.parsePivots()
    if (pivots) {
      table.args.pivots = pivots
    }

    return table
  }

  protected parseTableAtom(): exp.Expression {
    // Subquery
    if (this.match(TokenType.L_PAREN)) {
      const subquery = this.parseSubquery()
      this.expect(TokenType.R_PAREN)
      return this.maybeParseAlias(subquery)
    }

    // UNNEST: parsed specially with alias stored on Unnest (like Python)
    const unnest = this.parseUnnest()
    if (unnest) return unnest

    // String literal as table reference (e.g., FROM 'x.y' in DuckDB)
    if (this.current.tokenType === TokenType.STRING) {
      const text = this.advance().text
      const inner = text.slice(1, -1)
      const ident = new exp.Identifier({ this: inner, quoted: true })
      const table = new exp.Table({ this: ident })
      return this.maybeParseAlias(table)
    }

    // Table reference
    const name = this.parseTableName()

    const historicalData = this.parseHistoricalData()
    if (historicalData) {
      name.args.when = historicalData
      historicalData.parent = name
    }

    return this.maybeParseAlias(name)
  }

  protected parseUnnest(): exp.Unnest | undefined {
    if (this.current.tokenType !== TokenType.UNNEST) return undefined
    this.advance() // consume UNNEST
    this.expect(TokenType.L_PAREN)
    const expressions = this.parseCSV(() => this.parseExpression())
    this.expect(TokenType.R_PAREN)

    const ordinality = this.matchTextSeq("WITH", "ORDINALITY")

    this.match(TokenType.AS)
    const alias = this.parseTableAlias()

    let offset: boolean | exp.Expression | undefined = ordinality || undefined
    if (!ordinality && this.matchTextSeq("WITH", "OFFSET")) {
      this.match(TokenType.AS)
      if (IDENTIFIER_TOKENS.has(this.current.tokenType)) {
        offset = new exp.Identifier({ this: this.advance().text })
      } else {
        offset = new exp.Identifier({ this: "offset" })
      }
    }

    return new exp.Unnest({ expressions, alias, offset })
  }

  private static readonly HISTORICAL_DATA_PREFIX = new Set([
    "AT",
    "BEFORE",
    "END",
  ])
  private static readonly HISTORICAL_DATA_KIND = new Set([
    "OFFSET",
    "STATEMENT",
    "STREAM",
    "TIMESTAMP",
    "VERSION",
  ])

  protected parseHistoricalData(): exp.HistoricalData | undefined {
    const index = this.index
    const upper = this.current.text.toUpperCase()
    if (!Parser.HISTORICAL_DATA_PREFIX.has(upper)) return undefined
    this.advance()
    const thisText = upper
    if (!this.match(TokenType.L_PAREN)) {
      this.index = index
      return undefined
    }
    const kindUpper = this.current.text.toUpperCase()
    if (!Parser.HISTORICAL_DATA_KIND.has(kindUpper)) {
      this.index = index
      return undefined
    }
    this.advance()
    if (!this.match(TokenType.FARROW)) {
      this.index = index
      return undefined
    }
    const expression = this.parseBitwise()
    this.expect(TokenType.R_PAREN)
    return new exp.HistoricalData({
      this: new exp.Var({ this: thisText }),
      kind: new exp.Var({ this: kindUpper }),
      expression,
    })
  }

  protected parsePivots(): exp.Pivot[] | undefined {
    const pivots: exp.Pivot[] = []
    let pivot = this.parsePivot()
    while (pivot) {
      pivots.push(pivot)
      pivot = this.parsePivot()
    }
    return pivots.length > 0 ? pivots : undefined
  }

  protected parsePivot(): exp.Pivot | undefined {
    const index = this.index
    let unpivot: boolean

    if (this.match(TokenType.PIVOT)) {
      unpivot = false
    } else if (this.match(TokenType.UNPIVOT)) {
      unpivot = true
    } else {
      return undefined
    }

    if (!this.match(TokenType.L_PAREN)) {
      this.index = index
      return undefined
    }

    let expressions: exp.Expression[]
    if (unpivot) {
      expressions = this.parseCSV(() => this.parseExpression())
    } else {
      expressions = this.parsePivotAggregations()
    }

    if (!this.match(TokenType.FOR)) {
      this.raiseError("Expecting FOR in PIVOT")
    }

    const fields: exp.In[] = []
    const tryParsePivotIn = (): exp.In | undefined => {
      const savedIndex = this.index
      try {
        return this.parsePivotIn()
      } catch {
        this.index = savedIndex
        return undefined
      }
    }
    let field = tryParsePivotIn()
    while (field) {
      fields.push(field)
      field = tryParsePivotIn()
    }

    const group =
      this.match(TokenType.GROUP_BY) || this.match(TokenType.GROUP)
        ? (this.matchText("BY"), this.parseGroupBy())
        : undefined

    this.expect(TokenType.R_PAREN)

    const pivot = new exp.Pivot({
      expressions,
      fields,
      unpivot: unpivot || undefined,
      group,
    })

    if (
      this.currentTokenType !== TokenType.PIVOT &&
      this.currentTokenType !== TokenType.UNPIVOT
    ) {
      const alias = this.parseTableAlias()
      if (alias) {
        pivot.args.alias = alias
      }
    }

    return pivot
  }

  private parsePivotAggregations(): exp.Expression[] {
    const results: exp.Expression[] = []
    while (this.currentTokenType !== TokenType.FOR && !this.isEnd()) {
      const name = this.current.text
      this.advance()
      if (this.currentTokenType !== TokenType.L_PAREN) {
        this.raiseError("Expecting function call in PIVOT aggregation")
      }
      this.advance() // consume L_PAREN
      const func = this.parseFunction(name)
      results.push(this.maybeParseAlias(func))
      this.match(TokenType.COMMA)
    }
    return results
  }

  private parsePivotIn(): exp.In {
    const value = this.parseBitwise()

    if (!this.match(TokenType.IN)) {
      this.raiseError("Expecting IN in PIVOT")
    }

    if (this.match(TokenType.L_PAREN)) {
      const expressions = this.parseCSV(() => {
        const thisExpr = this.parseExpression()
        if (this.match(TokenType.ALIAS)) {
          const alias = this.parseBitwise()
          if (alias instanceof exp.Column && !alias.args.table) {
            return new exp.PivotAlias({
              this: thisExpr,
              alias: alias.args.this,
            })
          }
          return new exp.PivotAlias({ this: thisExpr, alias })
        }
        return thisExpr
      })
      this.expect(TokenType.R_PAREN)
      return new exp.In({ this: value, expressions })
    }

    return new exp.In({ this: value, field: this.parseIdentifier() })
  }

  protected parseSimplifiedPivot(isUnpivot?: boolean): exp.Pivot {
    const table = this.parseTableExpression()
    const onExpressions = this.matchText("ON")
      ? this.parseCSV(() => {
          const thisExpr = this.parseBitwise()
          if (this.match(TokenType.IN)) {
            return this.parseInList(thisExpr)
          }
          if (this.currentTokenType === TokenType.ALIAS) {
            return this.maybeParseAlias(thisExpr)
          }
          return thisExpr
        })
      : undefined

    const using = this.match(TokenType.USING)
      ? this.parseCSV(() => this.maybeParseAlias(this.parseExpression()))
      : undefined

    const group =
      this.match(TokenType.GROUP_BY) || this.match(TokenType.GROUP)
        ? (this.matchText("BY"), this.parseGroupBy())
        : undefined

    return new exp.Pivot({
      this: table,
      expressions: onExpressions,
      using,
      group,
      unpivot: isUnpivot || undefined,
    })
  }

  protected parseTableName(): exp.Table {
    const parts: exp.Identifier[] = []

    do {
      parts.push(this.parseIdentifier())
    } while (this.match(TokenType.DOT))

    // Check for function call: table_func() used as table source (e.g., PIVOT duckdb_functions())
    const lastPart = parts[parts.length - 1]
    if (lastPart && this.match(TokenType.L_PAREN)) {
      const func = this.parseFunction(lastPart.name)
      if (parts.length === 1) {
        return new exp.Table({ this: func })
      }
      // schema.func() - wrap with Dot
      const prefix = parts.slice(0, -1)
      let left: exp.Expression
      if (prefix.length === 1) {
        left = new exp.Column({ this: prefix[0] })
      } else {
        left = new exp.Column({ this: prefix[1], table: prefix[0] })
      }
      return new exp.Table({
        this: new exp.Dot({ this: left, expression: func }),
      })
    }

    if (parts.length === 1) {
      return new exp.Table({ this: parts[0] })
    }
    if (parts.length === 2) {
      return new exp.Table({ this: parts[1], db: parts[0] })
    }
    if (parts.length === 3) {
      return new exp.Table({ this: parts[2], db: parts[1], catalog: parts[0] })
    }

    throw new Error("Invalid table name")
  }

  protected parseTableNameSchema(): exp.Expression {
    const parts: exp.Identifier[] = []
    do {
      parts.push(this.parseIdentifier())
    } while (this.match(TokenType.DOT))

    let table: exp.Table
    if (parts.length === 1) {
      table = new exp.Table({ this: parts[0] })
    } else if (parts.length === 2) {
      table = new exp.Table({ this: parts[1], db: parts[0] })
    } else if (parts.length === 3) {
      table = new exp.Table({ this: parts[2], db: parts[1], catalog: parts[0] })
    } else {
      throw new Error("Invalid table name")
    }

    if (this.match(TokenType.L_PAREN)) {
      const columns = this.parseCSV(() => this.parseIdentifier())
      this.expect(TokenType.R_PAREN)
      return new exp.Schema({ this: table, expressions: columns })
    }

    return table
  }

  protected maybeParseAlias(expr: exp.Expression): exp.Expression {
    let alias: exp.Identifier | undefined
    if (this.match(TokenType.AS)) {
      if (this.current.tokenType === TokenType.L_PAREN) {
        this.advance()
        const columns = this.parseCSV(() => this.parseIdentifier())
        this.expect(TokenType.R_PAREN)
        const tableAlias = new exp.TableAlias({ columns })
        if (expr instanceof exp.Subquery || expr instanceof exp.Table) {
          expr.args.alias = tableAlias
          tableAlias.parent = expr
          return expr
        }
        return new exp.Alias({ this: expr, alias: tableAlias })
      }
      alias = this.parseIdentifier()
    } else {
      // Check if current token can be an implicit alias
      // It must be an identifier token but NOT a forbidden table alias token
      const tokenType = this.current.tokenType
      if (
        IDENTIFIER_TOKENS.has(tokenType) &&
        !TABLE_ALIAS_FORBIDDEN_TOKENS.has(tokenType) &&
        !(
          this.current.text.toUpperCase() === "POSITIONAL" &&
          this.peek(1).tokenType === TokenType.JOIN
        )
      ) {
        alias = this.parseIdentifier()
      }
    }

    if (!alias) return expr

    // Check for column list: AS t(a, b)
    if (this.match(TokenType.L_PAREN)) {
      const columns = this.parseCSV(() => this.parseIdentifier())
      this.expect(TokenType.R_PAREN)
      const tableAlias = new exp.TableAlias({ this: alias, columns })
      if (expr instanceof exp.Subquery) {
        expr.args.alias = tableAlias
        tableAlias.parent = expr
        return expr
      }
      return new exp.Alias({ this: expr, alias: tableAlias })
    }

    return new exp.Alias({ this: expr, alias })
  }

  protected parseSubquery(): exp.Subquery {
    if (this.match(TokenType.SELECT)) {
      const select = this.parseSelect()
      return new exp.Subquery({ this: this.maybeParseSetOperations(select) })
    }
    if (this.match(TokenType.FROM)) {
      const fromFirst = this.parseFromFirst()
      return new exp.Subquery({ this: this.maybeParseSetOperations(fromFirst) })
    }
    if (this.matchText("VALUES")) {
      const values = this.parseValues()
      return new exp.Subquery({ this: values })
    }
    // Check STATEMENT_PARSERS for dialect-specific subquery starts (e.g. PIVOT/UNPIVOT)
    const statementParsers = (this.constructor as typeof Parser)
      .STATEMENT_PARSERS
    const parser = statementParsers.get(this.currentTokenType)
    if (parser) {
      this.advance()
      const stmt = parser(this)
      if (stmt) return new exp.Subquery({ this: stmt })
    }
    this.raiseError("Expected SELECT, FROM, or VALUES in subquery")
    return new exp.Subquery({})
  }

  protected parseValues(): exp.Values {
    const tuples: exp.Tuple[] = []
    do {
      this.expect(TokenType.L_PAREN)
      const values = this.parseCSV(() => this.parseExpression())
      this.expect(TokenType.R_PAREN)
      tuples.push(new exp.Tuple({ expressions: values }))
    } while (this.match(TokenType.COMMA))
    return new exp.Values({ expressions: tuples })
  }

  protected isJoinKeyword(): boolean {
    const type = this.current.tokenType
    return (
      type === TokenType.JOIN ||
      type === TokenType.INNER ||
      type === TokenType.LEFT ||
      type === TokenType.RIGHT ||
      type === TokenType.FULL ||
      type === TokenType.CROSS ||
      type === TokenType.NATURAL ||
      type === TokenType.SEMI ||
      type === TokenType.ANTI ||
      type === TokenType.ASOF ||
      this.current.text.toUpperCase() === "POSITIONAL"
    )
  }

  protected parseJoin(): exp.Join | undefined {
    // Comma join: "FROM tbl1, tbl2" → implicit cross join
    if (this.match(TokenType.COMMA)) {
      const table = this.parseTableAtom()
      const sample = this.parseTableSample(false)
      if (sample) {
        const target = table instanceof exp.Alias ? table.this : table
        if (target instanceof exp.Table || target instanceof exp.Subquery) {
          target.args.sample = sample
          sample.parent = target
        }
      }
      return new exp.Join({ this: table })
    }

    const args: exp.Args = {}

    // Parse join modifiers
    // method: NATURAL, LATERAL
    let method: string | undefined
    // side: LEFT, RIGHT, FULL
    let side: string | undefined
    // kind: INNER, OUTER, CROSS, SEMI, ANTI
    let kind: string | undefined

    // NATURAL/POSITIONAL/ASOF is a method, not kind
    if (this.match(TokenType.NATURAL)) {
      method = "NATURAL"
    } else if (this.match(TokenType.ASOF)) {
      method = "ASOF"
    } else if (this.current.text.toUpperCase() === "POSITIONAL") {
      method = "POSITIONAL"
      this.advance()
    }

    // side: LEFT, RIGHT, FULL
    if (this.match(TokenType.LEFT)) {
      side = "LEFT"
    } else if (this.match(TokenType.RIGHT)) {
      side = "RIGHT"
    } else if (this.match(TokenType.FULL)) {
      side = "FULL"
    }

    // kind: OUTER, INNER, CROSS, SEMI, ANTI
    if (this.match(TokenType.OUTER)) {
      kind = "OUTER"
    } else if (this.match(TokenType.INNER)) {
      kind = "INNER"
    } else if (this.match(TokenType.CROSS)) {
      kind = "CROSS"
    } else if (this.match(TokenType.SEMI)) {
      kind = "SEMI"
    } else if (this.match(TokenType.ANTI)) {
      kind = "ANTI"
    }

    if (!this.match(TokenType.JOIN)) {
      return undefined
    }

    const right = this.parseTableAtom()

    if (this.match(TokenType.ON)) {
      args.on = this.parseExpression()
    } else if (this.match(TokenType.USING)) {
      this.expect(TokenType.L_PAREN)
      args.using = this.parseCSV(() => this.parseIdentifier())
      this.expect(TokenType.R_PAREN)
    }

    args.this = right
    if (method) args.method = method
    if (side) args.side = side
    if (kind) args.kind = kind

    if (
      (this.constructor as typeof Parser).ADD_JOIN_ON_TRUE &&
      !args.on &&
      !args.using &&
      !method &&
      (kind === undefined || kind === "INNER" || kind === "OUTER")
    ) {
      args.on = new exp.Boolean({ this: true })
    }

    return new exp.Join(args)
  }

  private parseOrdered(): exp.Ordered {
    const expr = this.parseExpression()
    const args: exp.Args = { this: expr }

    let desc: boolean | undefined
    if (this.match(TokenType.ASC)) {
      desc = false
      args.desc = false
    } else if (this.match(TokenType.DESC)) {
      desc = true
      args.desc = true
    }

    const isNullsFirst = this.matchTextSeq("NULLS", "FIRST")
    const isNullsLast = !isNullsFirst && this.matchTextSeq("NULLS", "LAST")
    const explicitlyNullOrdered = isNullsFirst || isNullsLast

    let nullsFirst = isNullsFirst ? true : false
    if (
      !explicitlyNullOrdered &&
      ((!desc && this.dialect.NULL_ORDERING === "nulls_are_small") ||
        (desc && this.dialect.NULL_ORDERING !== "nulls_are_small")) &&
      this.dialect.NULL_ORDERING !== "nulls_are_last"
    ) {
      nullsFirst = true
    }

    args.nulls_first = nullsFirst

    return new exp.Ordered(args)
  }

  // Expression parsing with hierarchical precedence
  protected parseExpression(): exp.Expression {
    return this.parseDisjunction()
  }

  private parseDisjunction(): exp.Expression {
    return this.parseBinaryOps(
      (this.constructor as typeof Parser).DISJUNCTION,
      () => this.parseConjunction(),
    )
  }

  private parseConjunction(): exp.Expression {
    return this.parseBinaryOps(
      (this.constructor as typeof Parser).CONJUNCTION,
      () => this.parseNot(),
    )
  }

  private parseNot(): exp.Expression {
    if (this.match(TokenType.NOT)) {
      return new exp.Not({ this: this.parseNot() })
    }
    return this.parseEquality()
  }

  protected parseEquality(): exp.Expression {
    return this.parseBinaryOps(
      (this.constructor as typeof Parser).EQUALITY,
      () => this.parseComparison(),
    )
  }

  private parseComparison(): exp.Expression {
    const left = this.parseBinaryOps(
      (this.constructor as typeof Parser).COMPARISON,
      () => this.parseBitwise(),
    )
    return this.parsePostfixExpression(left)
  }

  protected parseBitwise(): exp.Expression {
    return this.parseBinaryOps(
      (this.constructor as typeof Parser).BITWISE,
      () => this.parseTerm(),
    )
  }

  private parseTerm(): exp.Expression {
    return this.parseBinaryOps((this.constructor as typeof Parser).TERM, () =>
      this.parseFactor(),
    )
  }

  private parseFactor(): exp.Expression {
    const exponent = (this.constructor as typeof Parser).EXPONENT
    const parseInner =
      exponent.size > 0
        ? () =>
            this.parseAtTimeZone(
              this.parseBinaryOps(exponent, () => this.parseUnary()),
            )
        : () => this.parseAtTimeZone(this.parseUnary())
    const result = this.parseBinaryOps(
      (this.constructor as typeof Parser).FACTOR,
      parseInner,
    )

    if (result instanceof exp.Div) {
      result.set("typed", this.dialect.TYPED_DIVISION)
      result.set("safe", this.dialect.SAFE_DIVISION)
    }

    return result
  }

  private parseAtTimeZone(this_: exp.Expression): exp.Expression {
    if (!this.matchTextSeq("AT", "TIME", "ZONE")) {
      return this_
    }
    return this.parseAtTimeZone(
      this.expression(exp.AtTimeZone, { this: this_, zone: this.parseUnary() }),
    )
  }

  protected parseUnary(): exp.Expression {
    if (this.match(TokenType.MINUS)) {
      return new exp.Neg({ this: this.parseUnary() })
    }
    if (this.match(TokenType.TILDE)) {
      return new exp.BitwiseNot({ this: this.parseUnary() })
    }
    // Postgres prefix operators: |/ (sqrt), ||/ (cbrt)
    if (this.match(TokenType.PIPE_SLASH)) {
      return new exp.Sqrt({ this: this.parseUnary() })
    }
    if (this.match(TokenType.DPIPE_SLASH)) {
      return new exp.Cbrt({ this: this.parseUnary() })
    }
    // DuckDB @ operator: @x = ABS(x)
    if (this.match(TokenType.AT)) {
      return new exp.Abs({ this: this.parseBitwise() })
    }
    if (this.match(TokenType.EXISTS)) {
      this.expect(TokenType.L_PAREN)
      const subquery = this.parseSubquery()
      this.expect(TokenType.R_PAREN)
      return new exp.Exists({ this: subquery })
    }
    const primary = this.parsePrimary()
    return this.parseColumnOps(primary)
  }

  private parseBinaryOps(
    operators: Map<TokenType, exp.ExpressionClass>,
    parseHigher: () => exp.Expression,
  ): exp.Expression {
    let left = parseHigher()

    while (true) {
      const cls = operators.get(this.current.tokenType)
      if (!cls) break
      this.advance()
      const right = parseHigher()
      left = new cls({ this: left, expression: right })
    }

    return left
  }

  private parsePostfixExpression(left: exp.Expression): exp.Expression {
    // IS [NOT] NULL / IS [NOT] TRUE / IS [NOT] FALSE
    if (this.match(TokenType.IS)) {
      const not = this.match(TokenType.NOT)
      let right: exp.Expression

      if (this.match(TokenType.NULL)) {
        right = new exp.Null({})
      } else if (this.match(TokenType.TRUE)) {
        right = exp.Boolean.true_()
      } else if (this.match(TokenType.FALSE)) {
        right = exp.Boolean.false_()
      } else {
        this.raiseError("Expected NULL, TRUE, or FALSE after IS")
        right = new exp.Null({})
      }

      const is = new exp.Is({ this: left, expression: right })
      return not ? new exp.Not({ this: is }) : is
    }

    // Handle SIMILAR TO (two-word operator)
    if (this.matchText("SIMILAR") && this.matchText("TO")) {
      const pattern = this.parseBitwise()
      return new exp.SimilarTo({ this: left, expression: pattern })
    }

    // Check for NOT prefix (handles both keyword NOT and symbol !)
    const negate = this.match(TokenType.NOT)

    // Handle SIMILAR TO after NOT
    if (negate && this.matchText("SIMILAR") && this.matchText("TO")) {
      const pattern = this.parseBitwise()
      return new exp.Not({
        this: new exp.SimilarTo({ this: left, expression: pattern }),
      })
    }

    // Check RANGE_PARSERS
    const rangeParser = this.rangeParsers.get(this.current.tokenType)
    if (rangeParser) {
      this.advance()
      const result = rangeParser(this, left)
      return negate ? new exp.Not({ this: result }) : result
    }

    // If we consumed NOT but didn't find a range operator, retreat
    if (negate) {
      this.retreat()
    }

    return left
  }

  private parseBetween(left: exp.Expression): exp.Expression {
    let symmetric: boolean | undefined
    if (this.matchText("SYMMETRIC")) {
      symmetric = true
    } else if (this.matchText("ASYMMETRIC")) {
      symmetric = false
    }

    const low = this.parseBitwise()
    this.expect(TokenType.AND)
    const high = this.parseBitwise()
    return new exp.Between({ this: left, low, high, symmetric })
  }

  protected parseInList(left: exp.Expression): exp.Expression {
    if (this.match(TokenType.L_PAREN)) {
      if (this.current.tokenType === TokenType.SELECT) {
        this.advance()
        const query = this.parseSelect()
        this.expect(TokenType.R_PAREN)
        return new exp.In({
          this: left,
          query: new exp.Subquery({ this: query }),
        })
      }

      const expressions = this.parseCSV(() => this.parseExpression())
      this.expect(TokenType.R_PAREN)
      return new exp.In({ this: left, expressions })
    }

    const parts: exp.Identifier[] = []
    do {
      parts.push(this.parseIdentifier())
    } while (this.match(TokenType.DOT))
    let col: exp.Column
    if (parts.length === 1) {
      col = new exp.Column({ this: parts[0] })
    } else if (parts.length === 2) {
      col = new exp.Column({ this: parts[1], table: parts[0] })
    } else {
      col = new exp.Column({
        this: parts[parts.length - 1],
        table: parts[parts.length - 2],
      })
    }
    return new exp.In({ this: left, field: col })
  }

  // List comprehension: [x FOR x IN l IF condition]
  // Python's _parse_comprehension
  private parseComprehension(thisExpr: exp.Expression): exp.Expression {
    const index = this.index
    // Parse loop variable (e.g., x or x, i for value and index)
    const expression = this.parseIdentifier()
    let position: exp.Expression | undefined
    if (this.match(TokenType.COMMA)) {
      position = this.parseIdentifier()
    }

    if (!this.match(TokenType.IN)) {
      this.index = index - 1
      return thisExpr
    }

    // Iterator can be any expression (array literal, column, function call, etc.)
    const iterator = this.parsePrimary()
    let condition: exp.Expression | undefined
    if (this.matchText("IF")) {
      condition = this.parseDisjunction()
    }

    return new exp.Comprehension({
      this: thisExpr,
      expression,
      position,
      iterator,
      condition,
    })
  }

  parseLikePattern(
    left: exp.Expression,
    cls: exp.ExpressionClass,
  ): exp.Expression {
    let pattern: exp.Expression
    if (this.match(TokenType.ANY) || this.matchText("SOME")) {
      pattern = new exp.Any({ this: this.parseBitwise() })
    } else if (this.match(TokenType.ALL)) {
      pattern = new exp.All({ this: this.parseBitwise() })
    } else {
      pattern = this.parseBitwise()
    }
    const args: exp.Args = { this: left, expression: pattern }

    if (this.matchText("ESCAPE")) {
      args.escape = this.parseBitwise()
    }

    return new cls(args)
  }

  protected parsePrimary(): exp.Expression {
    // NO_PAREN_FUNCTIONS - functions that don't require parentheses
    const noParenCls = this.noParenFunctions.get(this.current.tokenType)
    if (noParenCls) {
      // If next token is L_PAREN with args, parse as regular function call
      const peekNext = this.peek(1)
      if (peekNext.tokenType === TokenType.L_PAREN) {
        const peekAfter = this.peek(2)
        if (peekAfter.tokenType !== TokenType.R_PAREN) {
          const funcName = this.current.text
          this.advance() // past function name
          this.advance() // past L_PAREN
          return this.parseFunction(funcName)
        }
      }
      this.advance()
      // Some dialects allow optional empty parentheses
      if (this.match(TokenType.L_PAREN)) {
        this.expect(TokenType.R_PAREN)
      }
      return new noParenCls({})
    }

    // Inline type constructor: STRUCT<TYPE>(values) / ARRAY<TYPE>(values) → CAST
    {
      const typeConstructorResult = this.parseTypeConstructor()
      if (typeConstructorResult) return typeConstructorResult
    }

    // Array literal [1, 2, 3] or Struct literal {'x': 1} - use parseBracket
    if (
      this.current.tokenType === TokenType.L_BRACKET ||
      this.current.tokenType === TokenType.L_BRACE
    ) {
      const result = this.parseBracket(undefined)
      if (result) return result
    }

    // ARRAY[1, 2, 3] or ARRAY(subquery) or ARRAY(expr, ...)
    if (this.match(TokenType.ARRAY)) {
      // Use parseBracket for ARRAY[...]
      const bracketResult = this.parseBracket(undefined)
      if (bracketResult) return bracketResult

      if (this.match(TokenType.L_PAREN)) {
        // ARRAY(SELECT...) - direct subquery without parens
        if (this.current.tokenType === TokenType.SELECT) {
          this.advance()
          const select = this.parseSelect()
          this.expect(TokenType.R_PAREN)
          return new exp.Array({ expressions: [select] })
        }
        // ARRAY((SELECT...)) or ARRAY(expr, ...) - parse as expressions
        const exprs = this.parseCSV(() => this.parseLambda())
        this.expect(TokenType.R_PAREN)
        return new exp.Array({ expressions: exprs })
      }
      // Just ARRAY keyword without brackets - retreat and let it be parsed as identifier
      this.retreat()
    }

    // Parenthesized expression, tuple, or subquery
    if (this.match(TokenType.L_PAREN)) {
      if (this.current.tokenType === TokenType.SELECT) {
        this.advance()
        const select = this.maybeParseSetOperations(this.parseSelect())
        this.expect(TokenType.R_PAREN)
        return new exp.Subquery({ this: select })
      }
      if (this.current.tokenType === TokenType.FROM) {
        this.advance()
        const fromFirst = this.maybeParseSetOperations(this.parseFromFirst())
        this.expect(TokenType.R_PAREN)
        return new exp.Subquery({ this: fromFirst })
      }
      if (this.matchText("VALUES")) {
        const values = this.parseValues()
        const setOps = this.maybeParseSetOperations(values)
        const modded = this.parseQueryModifiers(setOps as exp.Select)
        this.expect(TokenType.R_PAREN)
        return new exp.Subquery({ this: modded })
      }

      const first = this.parseExpression()

      // If the expression is a Subquery or Values, apply set operations and query modifiers
      // (Python: _parse_paren lines 6011-6015 — handles INTERSECT/UNION after parenthesized subquery)
      if (first instanceof exp.Subquery || first instanceof exp.Values) {
        let result: exp.Expression = this.maybeParseSetOperations(first)
        result = this.parseQueryModifiers(result as exp.Select)
        this.expect(TokenType.R_PAREN)
        return new exp.Subquery({ this: result })
      }

      if (this.match(TokenType.COMMA)) {
        const exprs = [first, ...this.parseCSV(() => this.parseExpression())]
        this.expect(TokenType.R_PAREN)
        return new exp.Tuple({ expressions: exprs })
      }
      this.expect(TokenType.R_PAREN)
      return new exp.Paren({ this: first })
    }

    // NULL
    if (this.match(TokenType.NULL)) {
      return new exp.Null({})
    }

    // TRUE / FALSE
    if (this.match(TokenType.TRUE)) {
      return exp.Boolean.true_()
    }
    if (this.match(TokenType.FALSE)) {
      return exp.Boolean.false_()
    }

    // Number
    if (this.match(TokenType.NUMBER)) {
      return exp.Literal.number(this.prev.text)
    }

    // String
    if (this.match(TokenType.STRING)) {
      // Remove quotes
      const text = this.prev.text
      const inner = text.slice(1, -1).replace(/''/g, "'").replace(/""/g, '"')
      return exp.Literal.string(inner)
    }

    // National string (N'...')
    // Token text contains just the content (quotes and prefix removed by tokenizer)
    if (this.match(TokenType.NATIONAL_STRING)) {
      const content = this.prev.text.replace(/''/g, "'").replace(/""/g, '"')
      return new exp.National({ this: exp.Literal.string(content) })
    }

    // Bit string (B'...' or 0b...)
    // Token text contains just the binary digits (prefix and quotes removed by tokenizer)
    if (this.match(TokenType.BIT_STRING)) {
      const content = this.prev.text
      return new exp.BitString({ this: exp.Literal.string(content) })
    }

    // Hex string (X'...' or 0x...)
    // Token text contains just the hex digits (prefix and quotes removed by tokenizer)
    if (this.match(TokenType.HEX_STRING)) {
      const content = this.prev.text
      const isInteger = this._dialect?.HEX_STRING_IS_INTEGER_TYPE ?? false
      return new exp.HexString({
        this: exp.Literal.string(content),
        is_integer: isInteger || undefined,
      })
    }

    // Escape string (e'...')
    if (this.match(TokenType.BYTE_STRING)) {
      // Token already contains just the content
      const inner = this.prev.text
      return new exp.ByteString({ this: exp.Literal.string(inner) })
    }

    // CASE expression
    if (this.match(TokenType.CASE)) {
      return this.parseCase()
    }

    // CAST / TRY_CAST
    if (this.match(TokenType.CAST)) {
      return this.parseCast(!(this.constructor as typeof Parser).STRICT_CAST)
    }
    if (this.match(TokenType.TRY_CAST)) {
      return this.parseCast(true)
    }

    // INTERVAL
    if (this.match(TokenType.INTERVAL)) {
      return this.parseInterval()
    }

    // Star with optional EXCLUDE/EXCEPT/REPLACE, or *COLUMNS(...)
    if (this.match(TokenType.STAR)) {
      if (
        this.current.text.toUpperCase() === "COLUMNS" &&
        this.peek(1).tokenType === TokenType.L_PAREN
      ) {
        const name = this.advance().text
        this.advance() // consume L_PAREN
        const func = this.parseFunction(name)
        if (func instanceof exp.Columns) {
          func.args.unpack = true
        }
        return func
      }
      const except_ = this.parseStarOp("EXCEPT", "EXCLUDE")
      const replace = this.parseStarOp("REPLACE")
      return new exp.Star({ except_, replace })
    }

    // Type literals: DATE 'x', TIME 'x', TIMESTAMP 'x', TIMESTAMPTZ 'x'
    if (
      this.match(TokenType.DATE) ||
      this.match(TokenType.TIME) ||
      this.match(TokenType.TIMESTAMP) ||
      this.match(TokenType.TIMESTAMPTZ)
    ) {
      const rawTypeName = this.prev.text.toUpperCase()
      const typeName = this.typeNameMapping.get(rawTypeName) ?? rawTypeName
      if (this.current.tokenType === TokenType.STRING) {
        const literal = this.parsePrimary()
        return new exp.Cast({
          this: literal,
          to: new exp.DataType({ this: typeName }),
        })
      }
      // Not followed by string - retreat and let it be parsed as identifier
      this.retreat()
    }

    // Identifier (could be column, function, etc.)
    if (IDENTIFIER_TOKENS.has(this.current.tokenType)) {
      return this.parseIdentifierOrFunction()
    }

    // Placeholder ?
    if (this.match(TokenType.QMARK)) {
      return new exp.Placeholder({})
    }

    // Parameter $1, :name, etc.
    if (this.match(TokenType.DOLLAR) || this.match(TokenType.COLON)) {
      const kind = this.prev.text
      // Re-check for VAR or NUMBER since we advanced past the $ or :
      const nextType = this.current.tokenType as TokenType
      if (nextType === TokenType.VAR || nextType === TokenType.NUMBER) {
        const name = this.advance().text
        return new exp.Parameter({ this: name, kind })
      }
      return new exp.Placeholder({ kind })
    }

    // EXTRACT function
    if (this.match(TokenType.EXTRACT)) {
      return this.parseExtract()
    }

    this.raiseError(
      `Unexpected token ${this.current.tokenType} (${this.current.text})`,
    )
    return new exp.Null({})
  }

  protected parseColumnOps(expr: exp.Expression): exp.Expression {
    // Handle bracket/brace subscripts first (uses parseBracket)
    let result = this.parseBracket(expr) ?? expr

    // Handle colon variant extraction (Snowflake: data:value:nested)
    result = this.parseColonAsVariantExtract(result)

    // Handle column operators (::, ->, etc.)
    while (true) {
      const opType = this.current.tokenType
      const handler = (this.constructor as typeof Parser).COLUMN_OPERATORS.get(
        opType,
      )

      if (handler === undefined) {
        break // Not a column operator
      }

      this.advance() // consume the operator

      if (handler === null) {
        // DOT operator - handled elsewhere for column references
        this.retreat()
        break
      }

      // Parse right operand
      let right: exp.Expression
      if (CAST_OPERATORS.has(opType)) {
        right = this.parseDataType()
      } else {
        right = this.parsePrimary()
      }

      result = handler(this, result, right)

      // Continue parsing brackets after operator
      result = this.parseBracket(result) ?? result
    }

    return result
  }

  protected parseColonAsVariantExtract(
    thisExpr: exp.Expression,
  ): exp.Expression {
    return thisExpr
  }

  // Follows Python's _parse_bracket - handles both [bracket] and {brace} subscript/literal syntax
  private parseBracket(
    base: exp.Expression | undefined,
  ): exp.Expression | undefined {
    if (!this.match(TokenType.L_BRACKET) && !this.match(TokenType.L_BRACE)) {
      return base
    }

    const bracketKind = this.prev.tokenType

    // Parse contents as CSV of bracket key-values (which may include slices and aliases)
    const expressions: exp.Expression[] = []
    const endToken =
      bracketKind === TokenType.L_BRACKET
        ? TokenType.R_BRACKET
        : TokenType.R_BRACE

    if (this.current.tokenType !== endToken) {
      expressions.push(
        ...this.parseCSV(() => this.parseBracketKeyValue(bracketKind)),
      )
    }

    if (bracketKind === TokenType.L_BRACKET) {
      this.expect(TokenType.R_BRACKET)
    } else {
      this.expect(TokenType.R_BRACE)
    }

    let result: exp.Expression

    // Brace literal creates a Struct
    if (bracketKind === TokenType.L_BRACE) {
      result = new exp.Struct({
        expressions: this.kvToPropEq(expressions),
      })
    } else if (!base) {
      // Standalone [1, 2, 3] without preceding expression is Array
      result = new exp.Array({ expressions })
    } else {
      // base[index] is Bracket subscript
      const adjusted = exp.applyIndexOffset(
        expressions,
        -this._dialect.INDEX_OFFSET,
      )
      result = new exp.Bracket({
        this: base,
        expressions: adjusted,
      })
    }

    // Recursively handle chained brackets
    return this.parseBracket(result)
  }

  // Parse a single element inside brackets/braces - supports slice and alias syntax
  private parseBracketKeyValue(bracketKind: TokenType): exp.Expression {
    // For braces, parse as alias (allows 'key': value syntax)
    const isMap = bracketKind === TokenType.L_BRACE
    if (isMap) {
      return this.parseSlice(this.parseAliasExpression())
    }
    // For brackets, if we see a colon first it's the start of a slice with no start value
    if (this.current.tokenType === TokenType.COLON) {
      return this.parseSlice(undefined)
    }
    return this.parseSlice(this.parseExpression())
  }

  // Parse alias expression like 'key': value or key AS value
  private parseAliasExpression(): exp.Expression {
    const expr = this.parseDisjunction()

    // Check for 'key': value syntax (colon as alias separator in struct literals)
    if (this.match(TokenType.COLON)) {
      const value = this.parseDisjunction()
      return new exp.Alias({ this: value, alias: expr })
    }

    // Check for AS alias
    if (this.match(TokenType.ALIAS)) {
      const alias = this.parseIdentifier()
      return new exp.Alias({ this: expr, alias })
    }

    return expr
  }

  // Follows Python's _parse_slice - handles array slicing syntax [start:end:step]
  private parseSlice(expr: exp.Expression | undefined): exp.Expression {
    if (!this.match(TokenType.COLON)) {
      return expr ?? new exp.Slice({})
    }

    // Handle special case: :-: (DASH followed by COLON) means end=-1
    let end: exp.Expression | undefined
    if (
      this.current.tokenType === TokenType.MINUS &&
      this.peek(1).tokenType === TokenType.COLON
    ) {
      this.advance() // consume DASH
      end = new exp.Neg({ this: exp.Literal.number("1") })
    } else if (
      this.current.tokenType !== TokenType.COLON &&
      this.current.tokenType !== TokenType.R_BRACKET
    ) {
      end = this.parseExpression()
    }

    // Check for step
    let step: exp.Expression | undefined
    if (this.match(TokenType.COLON)) {
      if (
        this.current.tokenType !== TokenType.R_BRACKET &&
        this.current.tokenType !== TokenType.R_BRACE
      ) {
        step = this.parseUnary()
      }
    }

    return new exp.Slice({ this: expr, expression: end, step })
  }

  // Convert key-value pairs to PropertyEQ expressions for struct literals
  // Follows Python's _kv_to_prop_eq
  private kvToPropEq(expressions: exp.Expression[]): exp.Expression[] {
    return expressions.map((e, _index) => {
      if (e instanceof exp.Alias) {
        // 'key': value -> PropertyEQ(this=Identifier(key), expression=value)
        const key = e.args.alias as exp.Expression
        let keyExpr: exp.Expression
        if (key instanceof exp.Identifier) {
          keyExpr = new exp.Identifier({ this: key.name })
        } else if (key instanceof exp.Literal && key.isString) {
          keyExpr = new exp.Identifier({ this: String(key.args.this ?? "") })
        } else {
          keyExpr = key
        }
        return new exp.PropertyEQ({
          this: keyExpr,
          expression: e.args.this as exp.Expression,
        })
      }
      if (e instanceof exp.EQ) {
        return new exp.PropertyEQ({
          this: e.args.this as exp.Expression,
          expression: e.args.expression as exp.Expression,
        })
      }
      if (e instanceof exp.PropertyEQ || e instanceof exp.Slice) {
        return e
      }
      // Positional value - convert to PropertyEQ with index
      return e
    })
  }

  private parseIdentifierOrFunction(): exp.Expression {
    // Parse first identifier
    const first = this.parseIdentifier()

    // Check if it's a single function call (no dot)
    if (this.match(TokenType.L_PAREN)) {
      return this.parseFunction(first.name)
    }

    // Check for no-paren function (e.g., MAP {...})
    const noParenParser = (
      this.constructor as typeof Parser
    ).NO_PAREN_FUNCTION_PARSERS.get(first.name.toUpperCase())
    if (noParenParser) {
      return noParenParser(this)
    }

    // If no dot follows, it's a simple column reference
    if (!this.match(TokenType.DOT)) {
      return new exp.Column({ this: first })
    }

    // We have a dot - could be:
    // 1. Method call: x.foo() -> Dot(x, foo())
    // 2. Column reference: schema.table or table.column

    // Collect all parts: schema.table.column or x.method()
    const parts: exp.Identifier[] = [first]

    do {
      // Check for table.* pattern
      if (
        this.current.tokenType === TokenType.STAR ||
        !IDENTIFIER_TOKENS.has(this.current.tokenType)
      ) {
        break
      }
      parts.push(this.parseIdentifier())
    } while (this.match(TokenType.DOT))

    // Check if it's a method call (last part followed by parentheses)
    if (this.match(TokenType.L_PAREN)) {
      // It's a method call: x.foo() or schema.func()
      // Method calls always create Anonymous to preserve original name
      const methodName = parts[parts.length - 1]!
      const func = this.parseMethodCall(methodName.name)

      // If only two parts, it's object.method()
      if (parts.length === 2) {
        return new exp.Dot({ this: first, expression: func })
      }

      // Multiple parts: could be schema.table.method() or x.y.z.method()
      // Build column from all but last part, then Dot with method
      const columnParts = parts.slice(0, -1)
      let column: exp.Expression
      if (columnParts.length === 1) {
        column = new exp.Column({ this: columnParts[0] })
      } else if (columnParts.length === 2) {
        column = new exp.Column({ this: columnParts[1], table: columnParts[0] })
      } else if (columnParts.length === 3) {
        column = new exp.Column({
          this: columnParts[2],
          table: columnParts[1],
          db: columnParts[0],
        })
      } else {
        column = new exp.Column({
          this: columnParts[3],
          table: columnParts[2],
          db: columnParts[1],
          catalog: columnParts[0],
        })
      }
      return new exp.Dot({ this: column, expression: func })
    }

    // It's a column reference (schema.table.column)
    if (parts.length === 2) {
      return new exp.Column({ this: parts[1], table: parts[0] })
    }
    if (parts.length === 3) {
      return new exp.Column({ this: parts[2], table: parts[1], db: parts[0] })
    }
    if (parts.length === 4) {
      return new exp.Column({
        this: parts[3],
        table: parts[2],
        db: parts[1],
        catalog: parts[0],
      })
    }

    throw new Error("Invalid column reference")
  }

  private parseFunction(name: string): exp.Expression {
    const upperName = name.toUpperCase()

    const specialParser = this.functionParsers.get(upperName)
    if (specialParser) {
      const result = specialParser(this)
      if (this._dialect.PRESERVE_ORIGINAL_NAMES) {
        if (!result._meta) result._meta = {}
        result._meta["name"] = name
      }
      return this.parseWindow(result)
    }

    // Parse arguments using parseLambda (follows Python's _parse_function_args)
    const aliasedArgs = (
      this.constructor as typeof Parser
    ).FUNCTIONS_WITH_ALIASED_ARGS.has(upperName)
    const args: exp.Expression[] = []

    if (this.current.tokenType !== TokenType.R_PAREN) {
      args.push(...this.parseCSV(() => this.parseLambda(aliasedArgs)))
    }

    this.expect(TokenType.R_PAREN)

    // Convert Alias → PropertyEQ for functions with aliased args (e.g. STRUCT)
    const finalArgs = aliasedArgs ? this.kvToPropEq(args) : args

    // Build the function expression (pass original name to preserve case for Anonymous)
    const func = this.buildFunction(name, finalArgs)

    if (this._dialect.PRESERVE_ORIGINAL_NAMES) {
      if (!func._meta) func._meta = {}
      func._meta["name"] = name
    }

    // parseWindow handles: WITHIN GROUP, FILTER, IGNORE NULLS normalization, postfix IGNORE NULLS, OVER
    return this.parseWindow(func)
  }

  private parseMethodCall(name: string): exp.Expression {
    const args: exp.Expression[] = []
    if (this.current.tokenType !== TokenType.R_PAREN) {
      args.push(...this.parseCSV(() => this.parseLambda()))
    }
    this.expect(TokenType.R_PAREN)
    const func = new exp.Anonymous({ this: name, expressions: args })
    return this.parseWindow(func)
  }

  private buildFunction(name: string, args: exp.Expression[]): exp.Expression {
    const builder = this.functions.get(name.toUpperCase())
    if (builder) {
      return builder(args, this.dialect)
    }

    return new exp.Anonymous({ this: name, expressions: args })
  }

  private parseCount(): exp.Expression {
    const args: exp.Args = {}

    if (this.match(TokenType.DISTINCT)) {
      args.distinct = true
    }

    if (this.match(TokenType.STAR)) {
      // COUNT(*)
    } else if (this.current.tokenType !== TokenType.R_PAREN) {
      const exprs = this.parseCSV(() => this.parseExpression())
      if (exprs.length === 1) {
        args.this = exprs[0]
      } else {
        args.expressions = exprs
      }
    }

    this.expect(TokenType.R_PAREN)

    return new exp.Count(args)
  }

  protected parseStringAgg(): exp.GroupConcat {
    let args: (exp.Expression | undefined)[]
    if (this.match(TokenType.DISTINCT)) {
      args = [
        new exp.Distinct({
          expressions: [this.parseDisjunction()],
        }),
      ]
      if (this.match(TokenType.COMMA)) {
        args.push(...this.parseCSV(() => this.parseDisjunction()))
      }
    } else {
      args = this.parseCSV(() => this.parseDisjunction())
    }

    if (!this.match(TokenType.R_PAREN) && args.length > 0) {
      // ORDER BY inside parens: STRING_AGG(expr, sep ORDER BY col)
      if (this.current.tokenType === TokenType.ORDER_BY) {
        this.advance()
        const order = new exp.Order({
          this: args[0],
          expressions: this.parseCSV(() => this.parseOrdered()),
        })
        this.expect(TokenType.R_PAREN)
        return new exp.GroupConcat({ this: order, separator: args[1] })
      }
      this.expect(TokenType.R_PAREN)
      return new exp.GroupConcat({ this: args[0], separator: args[1] })
    }

    // WITHIN GROUP (ORDER BY ...) after R_PAREN
    if (this.matchTextSeq("WITHIN", "GROUP")) {
      this.expect(TokenType.L_PAREN)
      let orderExpr: exp.Expression | undefined = args[0]
      if (this.match(TokenType.ORDER_BY)) {
        orderExpr = new exp.Order({
          this: args[0],
          expressions: this.parseCSV(() => this.parseOrdered()),
        })
      }
      this.expect(TokenType.R_PAREN)
      return new exp.GroupConcat({ this: orderExpr, separator: args[1] })
    }

    return new exp.GroupConcat({ this: args[0], separator: args[1] })
  }

  // Follows Python's _parse_lambda - parses function arguments with lambda support
  protected parseLambda(alias = false): exp.Expression {
    const index = this.index

    // DuckDB LAMBDA syntax: LAMBDA x : expr or LAMBDA x, y : expr
    if (this.matchText("LAMBDA")) {
      const lambdaExpressions = this.parseCSV(() => this.parseLambdaArg())
      if (this.match(TokenType.COLON)) {
        const body = this.parseDisjunction()
        return new exp.Lambda({
          this: body,
          expressions: lambdaExpressions,
          colon: true,
        })
      }
      // Not a valid LAMBDA syntax - retreat
      this.index = index
    }

    // Try to parse lambda parameters: (x, y) or single x
    let expressions: exp.Expression[] = []

    if (this.match(TokenType.L_PAREN)) {
      // Only try to parse as lambda args if content looks like identifiers
      // Skip if next token is SELECT, L_PAREN (subquery), etc.
      if (IDENTIFIER_TOKENS.has(this.current.tokenType)) {
        expressions = this.parseCSV(() => this.parseLambdaArg())

        if (!this.match(TokenType.R_PAREN)) {
          this.index = index
          expressions = []
        }
      } else {
        // Not lambda args - retreat and parse as expression
        this.index = index
      }
    } else if (IDENTIFIER_TOKENS.has(this.current.tokenType)) {
      const arg = this.parseLambdaArg()
      if (arg) {
        expressions = [arg]
      }
    }

    // Check if next token is a lambda operator (->)
    if (expressions.length > 0 && this.match(TokenType.ARROW)) {
      // Parse lambda body
      const body = this.parseDisjunction()
      return new exp.Lambda({
        this: body,
        expressions: expressions,
      })
    }

    // Not a lambda - retreat and parse as normal expression
    this.index = index

    // DISTINCT inside function args (Python: line 6298 in _parse_lambda)
    if (this.match(TokenType.DISTINCT)) {
      const distinctExprs = this.parseCSV(() => this.parseDisjunction())
      return new exp.Distinct({ expressions: distinctExprs })
    }

    // If current token can't start an expression (e.g. ORDER BY inside function parens like
    // CUME_DIST( ORDER BY foo)), skip expression parsing and let modifiers handle it.
    // Python: _parse_select_or_expression returns None, then _parse_order picks up ORDER_BY.
    let expr: exp.Expression | undefined
    if (this.current.tokenType === TokenType.ORDER_BY) {
      expr = undefined
    } else {
      expr = this.parseExpression()
    }

    // When alias=true, parse explicit AS alias (Python: _parse_select_or_expression(alias=True))
    if (expr && alias && this.match(TokenType.AS)) {
      const aliasId = this.parseIdentifier()
      expr = new exp.Alias({ this: expr, alias: aliasId })
    }

    // Check for named parameter syntax: name := value
    if (expr && this.match(TokenType.COLON_EQ)) {
      const value = this.parseExpression()
      // Unwrap single-part Column to Identifier (Python: _parse_assignment line 5158-5159)
      let left: exp.Expression = expr
      if (
        left instanceof exp.Column &&
        !left.args.table &&
        left.args.this instanceof exp.Identifier
      ) {
        left = left.args.this
      }
      return new exp.PropertyEQ({ this: left, expression: value })
    }

    // Chain modifiers: IGNORE NULLS, ORDER BY, LIMIT (Python: _parse_limit(_parse_order(...)))
    let result: exp.Expression | undefined = expr
      ? this.parseRespectOrIgnoreNulls(expr)
      : undefined
    if (this.match(TokenType.ORDER_BY)) {
      result = new exp.Order({
        this: result,
        expressions: this.parseCSV(() => this.parseOrdered()),
      })
    }
    return result as exp.Expression
  }

  private parseLambdaArg(): exp.Expression {
    return this.parseIdentifier()
  }

  private parseRespectOrIgnoreNulls(expr: exp.Expression): exp.Expression {
    if (this.matchTextSeq("IGNORE", "NULLS")) {
      return new exp.IgnoreNulls({ this: expr })
    }
    if (this.matchTextSeq("RESPECT", "NULLS")) {
      return new exp.RespectNulls({ this: expr })
    }
    return expr
  }

  // Follows Python's _parse_window - handles post-function modifiers
  private parseWindow(func: exp.Expression): exp.Expression {
    let result = func

    // 1. WITHIN GROUP (Python lines 7398-7400)
    if (this.matchTextSeq("WITHIN", "GROUP")) {
      this.expect(TokenType.L_PAREN)
      let order: exp.Order | undefined
      if (this.match(TokenType.ORDER_BY)) {
        order = new exp.Order({
          expressions: this.parseCSV(() => this.parseOrdered()),
        })
      }
      this.expect(TokenType.R_PAREN)
      result = new exp.WithinGroup({ this: result, expression: order })
    }

    // 2. FILTER clause (Python lines 7402-7407)
    if (this.match(TokenType.FILTER)) {
      this.expect(TokenType.L_PAREN)
      this.match(TokenType.WHERE)
      const condition = this.parseExpression()
      this.expect(TokenType.R_PAREN)
      result = new exp.Filter({
        this: result,
        expression: new exp.Where({ this: condition }),
      })
    }

    // 3. Normalize IGNORE NULLS position (Python lines 7423-7428)
    // If IGNORE/RESPECT NULLS was parsed inside the function argument,
    // move it to wrap the entire function
    if (result instanceof exp.AggFunc || result instanceof exp.Anonymous) {
      const ignoreNulls = this.findIgnoreRespectNulls(result)
      if (ignoreNulls && ignoreNulls !== result) {
        // Replace the IgnoreNulls/RespectNulls node with its inner expression
        this.replaceIgnoreRespectNulls(result, ignoreNulls)
        // Wrap the entire function with IgnoreNulls/RespectNulls
        result =
          ignoreNulls instanceof exp.IgnoreNulls
            ? new exp.IgnoreNulls({ this: result })
            : new exp.RespectNulls({ this: result })
      }
    }

    // 4. Parse postfix IGNORE NULLS / RESPECT NULLS (Python line 7430)
    result = this.parseRespectOrIgnoreNulls(result)

    // 5. Parse OVER clause (Python lines 7432-7503)
    if (this.match(TokenType.OVER)) {
      return this.parseWindowSpec_over(result)
    }

    return result
  }

  // Helper: Find IgnoreNulls/RespectNulls in function args
  private findIgnoreRespectNulls(
    expr: exp.Expression,
  ): exp.IgnoreNulls | exp.RespectNulls | undefined {
    for (const arg of Object.values(expr.args)) {
      if (arg instanceof exp.IgnoreNulls || arg instanceof exp.RespectNulls) {
        return arg
      }
      if (Array.isArray(arg)) {
        for (const item of arg) {
          if (
            item instanceof exp.IgnoreNulls ||
            item instanceof exp.RespectNulls
          ) {
            return item
          }
        }
      }
    }
    return undefined
  }

  // Helper: Replace IgnoreNulls/RespectNulls with its inner expression
  private replaceIgnoreRespectNulls(
    parent: exp.Expression,
    target: exp.IgnoreNulls | exp.RespectNulls,
  ): void {
    const inner = target.args.this as exp.Expression
    for (const [key, value] of Object.entries(parent.args)) {
      if (value === target) {
        parent.args[key] = inner
        return
      }
      if (Array.isArray(value)) {
        const idx = value.indexOf(target)
        if (idx >= 0) {
          value[idx] = inner
          return
        }
      }
    }
  }

  private parseNamedWindow(): exp.Window {
    const name = this.parseIdentifier()
    this.match(TokenType.AS) // AS
    return this.parseWindowSpec_over(name)
  }

  // Renamed from parseWindowFunctionWithExpr to follow Python naming
  private parseWindowSpec_over(func: exp.Expression): exp.Window {
    const args: exp.Args = { this: func }

    if (!this.match(TokenType.L_PAREN)) {
      // OVER window_name (named reference, no parens)
      const alias = this.parseIdentifier()
      args.alias = alias
      return new exp.Window(args)
    }

    // PARTITION BY
    if (this.match(TokenType.PARTITION_BY)) {
      args.partition_by = this.parseCSV(() => this.parseExpression())
    }

    // ORDER BY
    if (this.match(TokenType.ORDER_BY)) {
      args.order = new exp.Order({
        expressions: this.parseCSV(() => this.parseOrdered()),
      })
    }

    // Frame specification (ROWS/RANGE BETWEEN ... AND ...)
    if (this.match(TokenType.ROWS) || this.match(TokenType.RANGE)) {
      const kind = this.prev.text
      args.spec = this.parseWindowFrameSpec(kind)
    }

    this.expect(TokenType.R_PAREN)

    return new exp.Window(args)
  }

  private parseWindowFrameSpec(kind: string): exp.WindowSpec {
    const args: exp.Args = { kind }

    if (this.match(TokenType.BETWEEN)) {
      Object.assign(args, this.parseWindowBound("start"))
      this.expect(TokenType.AND)
      Object.assign(args, this.parseWindowBound("end"))
    } else {
      Object.assign(args, this.parseWindowBound("start"))
    }

    return new exp.WindowSpec(args)
  }

  private parseWindowBound(prefix: "start" | "end"): exp.Args {
    const args: exp.Args = {}

    if (this.match(TokenType.UNBOUNDED)) {
      args[prefix] = "UNBOUNDED"
      if (this.match(TokenType.PRECEDING)) {
        args[`${prefix}_side`] = "PRECEDING"
      } else if (this.match(TokenType.FOLLOWING)) {
        args[`${prefix}_side`] = "FOLLOWING"
      }
    } else if (this.matchText("CURRENT")) {
      this.matchText("ROW")
      args[prefix] = "CURRENT ROW"
    } else {
      args[prefix] = this.parsePrimary()
      if (this.match(TokenType.PRECEDING)) {
        args[`${prefix}_side`] = "PRECEDING"
      } else if (this.match(TokenType.FOLLOWING)) {
        args[`${prefix}_side`] = "FOLLOWING"
      }
    }

    return args
  }

  private parseCase(): exp.Case {
    const args: exp.Args = {}
    const ifs: exp.If[] = []

    // CASE expr WHEN ... or CASE WHEN ...
    if (this.current.tokenType !== TokenType.WHEN) {
      args.this = this.parseExpression()
    }

    while (this.match(TokenType.WHEN)) {
      const cond = this.parseExpression()
      this.expect(TokenType.THEN)
      const then = this.parseExpression()
      ifs.push(new exp.If({ this: cond, true: then }))
    }

    if (this.match(TokenType.ELSE)) {
      args.default = this.parseExpression()
    }

    this.expect(TokenType.END)

    args.ifs = ifs
    return new exp.Case(args)
  }

  private parseCast(safe: boolean): exp.Expression {
    this.expect(TokenType.L_PAREN)
    const expr = this.parseExpression()
    this.expect(TokenType.AS)
    const dataType = this.parseDataType()
    this.expect(TokenType.R_PAREN)

    const args: exp.Args = { this: expr, to: dataType }
    if (safe) {
      args.safe = true
    }

    return safe ? new exp.TryCast(args) : new exp.Cast(args)
  }

  protected parseDataType(): exp.DataType {
    let name = this.advance().text.toUpperCase()

    // When a dialect maps a keyword to a different token type (e.g., Presto: ROW → STRUCT),
    // use the canonical token type name instead of the original text
    if (this.prev.tokenType === TokenType.STRUCT && name !== "STRUCT") {
      name = "STRUCT"
    }

    // Handle multi-word type keywords
    const multiWordSuffix = MULTI_WORD_TYPE_TOKENS.get(name)
    if (multiWordSuffix) {
      for (const [words, canonicalName] of multiWordSuffix) {
        if (this.matchTextSeq(...words)) {
          name = canonicalName
          break
        }
      }
    }

    // Apply dialect-specific type name mapping
    name = this.typeNameMapping.get(name) ?? name

    const args: exp.Args = { this: name }

    // Parse size/precision or nested types
    if (this.match(TokenType.L_PAREN)) {
      const expressions: exp.Expression[] = []

      // For STRUCT, MAP, etc. - parse as column definitions (name TYPE pairs)
      if (STRUCT_TYPE_NAMES.has(name)) {
        // Parse column definitions: name TYPE, name TYPE, ...
        do {
          const colName = this.parseIdentifier()
          const colType = this.parseDataType()
          expressions.push(
            new exp.ColumnDef({
              this: colName,
              kind: colType,
            }),
          )
        } while (this.match(TokenType.COMMA))
      } else {
        // For other types, parse as expressions (size/precision)
        expressions.push(this.parsePrimary())
        while (this.match(TokenType.COMMA)) {
          expressions.push(this.parsePrimary())
        }
      }

      this.expect(TokenType.R_PAREN)
      args.expressions = expressions
    }

    // Parse angle bracket type params: STRUCT<col TYPE>, ARRAY<TYPE>
    if (NESTED_TYPE_NAMES.has(name) && this.match(TokenType.LT)) {
      if (STRUCT_TYPE_NAMES.has(name)) {
        args.expressions = this.parseCSV(() => this.parseStructType())
      } else {
        args.expressions = this.parseCSV(() => this.parseDataType())
      }
      args.nested = true
      this.expectGT()
    }

    // Parse array brackets (INT[], VARCHAR(100)[], INT[3], etc.)
    // Supports fixed-size arrays like INT[3] and multi-dimensional arrays
    let result: exp.DataType | undefined

    while (this.match(TokenType.L_BRACKET)) {
      // Check if there's a size expression before the closing bracket
      let values: exp.Expression[] | undefined
      if (this.current.tokenType !== TokenType.R_BRACKET) {
        // Parse size expression(s): INT[3] or INT[3][4]
        values = this.parseCSV(() => this.parseBitwise())
      }
      this.expect(TokenType.R_BRACKET)

      const inner = result ?? new exp.DataType(args)
      result = new exp.DataType({
        this: "ARRAY",
        expressions: [inner],
        values,
        nested: true,
      })
    }

    let dataType = result ?? new exp.DataType(args)
    if (this.typeConverters.size > 0) {
      const typeName = dataType.text("this").toUpperCase()
      const converter = this.typeConverters.get(typeName)
      if (converter) {
        dataType = converter(dataType)
      }
    }
    return dataType
  }

  protected parseTypeConstructor(): exp.Expression | undefined {
    if (
      (this.current.tokenType === TokenType.STRUCT ||
        this.current.tokenType === TokenType.ARRAY) &&
      this.peek(1).tokenType === TokenType.LT
    ) {
      const index = this.index
      const dataType = this.parseDataType()
      const nextType = this.currentTokenType
      if (nextType === TokenType.L_PAREN || nextType === TokenType.L_BRACKET) {
        const isStruct = STRUCT_TYPE_NAMES.has(dataType.text("this"))
        const closingToken =
          nextType === TokenType.L_BRACKET
            ? TokenType.R_BRACKET
            : TokenType.R_PAREN
        this.advance()
        const values = this.parseCSV(() => this.parseDisjunction())
        this.expect(closingToken)
        const cls = isStruct ? exp.Struct : exp.Array
        return this.parseColumnOps(
          exp.cast(new cls({ expressions: values }), dataType),
        )
      }
      this.index = index
    }
    return undefined
  }

  protected parseStructType(): exp.Expression {
    const saved = this.index

    // Try parsing as a data type (handles ARRAY<STRING>, INT, etc.)
    const dt = this.parseDataType()

    // If followed by comma or GT, this is a type-only field (e.g., STRUCT<ARRAY<STRING>>)
    if (
      this.current.tokenType === TokenType.COMMA ||
      this.current.tokenType === TokenType.GT ||
      this.current.tokenType === TokenType.RSHIFT
    ) {
      return dt
    }

    // Otherwise it was a field name followed by a type: retreat and parse as name + type
    this.index = saved
    const colName = this.parseIdentifier()
    this.match(TokenType.COLON)
    const colType = this.parseDataType()
    return new exp.ColumnDef({ this: colName, kind: colType })
  }

  private parseInterval(): exp.Interval {
    let value =
      this.current.tokenType === TokenType.STRING
        ? this.parsePrimary()
        : this.parseTerm()
    let unit: string | undefined

    // Parse unit (DAY, HOUR, etc.)
    if (
      this.current.tokenType === TokenType.VAR ||
      this.current.tokenType === TokenType.IDENTIFIER
    ) {
      unit = this.advance().text.toUpperCase()
    }

    // Normalize number-valued intervals to string literals (Python: _parse_interval_span lines 5339-5340)
    // INTERVAL -1 DAY → INTERVAL '-1' DAY, INTERVAL 5 DAY → INTERVAL '5' DAY
    if (value instanceof exp.Literal && value.isNumber) {
      value = exp.Literal.string(String(value.value))
    } else if (value instanceof exp.Neg) {
      const inner = value.args.this
      if (inner instanceof exp.Literal && inner.isNumber) {
        value = exp.Literal.string(`-${inner.value}`)
      }
    }

    // Normalize combined string intervals like '1 hour' -> '1', HOUR
    // Pattern matches: number (optional negative/decimal), whitespace, unit letters
    if (value instanceof exp.Literal && value.args.is_string) {
      const INTERVAL_STRING_RE = /^\s*(-?[0-9]+(?:\.[0-9]+)?)\s+([a-zA-Z]+)\s*$/
      const literalValue = value.args.this as string
      const match = INTERVAL_STRING_RE.exec(literalValue)
      if (match && match[1] && match[2]) {
        value = new exp.Literal({ this: match[1], is_string: true })
        unit = match[2].toUpperCase()
      }
    }

    return new exp.Interval({ this: value, unit })
  }

  parseExtract(): exp.Expression {
    this.expect(TokenType.L_PAREN)
    const unit = this.advance().text.toUpperCase()
    this.matchText("FROM")
    const expr = this.parseExpression()
    this.expect(TokenType.R_PAREN)

    return new exp.Extract({ this: unit, expression: expr })
  }

  private parseTrim(): exp.Expression {
    let position: string | undefined
    if (
      this.match(TokenType.LEADING) ||
      this.match(TokenType.TRAILING) ||
      this.match(TokenType.BOTH)
    ) {
      position = this.prev.text.toUpperCase()
    }

    const first = this.parseExpression()

    if (this.matchText("FROM")) {
      const expression = first
      const second = this.parseExpression()
      this.expect(TokenType.R_PAREN)
      return new exp.Trim({ this: second, expression, position })
    }
    if (this.match(TokenType.COMMA)) {
      const expression = this.parseExpression()
      this.expect(TokenType.R_PAREN)
      return new exp.Trim({ this: first, expression, position })
    }

    this.expect(TokenType.R_PAREN)
    return new exp.Trim({ this: first, position })
  }

  // Parse SUBSTRING - handles both standard and Postgres FROM/FOR syntax
  // SUBSTRING(string, start, length) or SUBSTRING(string FROM start FOR length)
  private parseSubstring(): exp.Expression {
    // Parse initial CSV arguments
    const args = this.parseCSV(() => this.parseBitwise())

    let start: exp.Expression | undefined
    let length: exp.Expression | undefined

    // Parse optional FROM/FOR syntax (Postgres style)
    while (this.current.tokenType !== TokenType.R_PAREN) {
      if (this.matchText("FROM") || this.match(TokenType.FROM)) {
        start = this.parseBitwise()
      } else if (this.matchText("FOR") || this.match(TokenType.FOR)) {
        if (!start) {
          // Default start to 1 if only FOR is specified
          start = exp.Literal.number(1)
        }
        length = this.parseBitwise()
      } else {
        break
      }
    }

    if (start) {
      args.push(start)
    }
    if (length) {
      args.push(length)
    }

    this.expect(TokenType.R_PAREN)
    return exp.fromArgList(exp.Substring, args)
  }

  // Parse OVERLAY - handles OVERLAY(string PLACING replacement FROM start FOR length)
  private parseOverlay(): exp.Expression {
    const str = this.parseBitwise()

    let replacement: exp.Expression | undefined
    let fromPos: exp.Expression | undefined
    let forLen: exp.Expression | undefined

    // Parse PLACING, FROM, FOR in any order (comma also works as separator)
    while (this.current.tokenType !== TokenType.R_PAREN) {
      if (this.match(TokenType.COMMA) || this.matchText("PLACING")) {
        replacement = this.parseBitwise()
      } else if (this.matchText("FROM") || this.match(TokenType.FROM)) {
        fromPos = this.parseBitwise()
      } else if (this.matchText("FOR") || this.match(TokenType.FOR)) {
        forLen = this.parseBitwise()
      } else {
        break
      }
    }

    this.expect(TokenType.R_PAREN)
    return new exp.Overlay({
      this: str,
      expression: replacement,
      from_: fromPos,
      for_: forLen,
    })
  }

  // Parse MAP function - handles both MAP(keys, values) and MAP {key: value} syntax
  private parseMap(): exp.Expression {
    // Check for brace syntax: MAP {...}
    if (this.current.tokenType === TokenType.L_BRACE) {
      const struct = this.parseBracket(undefined)
      return new exp.ToMap({ this: struct })
    }

    // Standard MAP(keys, values) syntax
    const args = this.parseCSV(() => this.parseExpression())
    this.expect(TokenType.R_PAREN)
    return new exp.Map({ keys: args[0], values: args[1] })
  }

  private parseIs(left: exp.Expression): exp.Expression {
    const negate = this.match(TokenType.NOT)

    if (this.matchTextSeq("DISTINCT", "FROM")) {
      const klass = negate ? exp.NullSafeEQ : exp.NullSafeNEQ
      return new klass({ this: left, expression: this.parseBitwise() })
    }

    const expression = this.parseNull() ?? this.parseBitwise()
    let result: exp.Expression = new exp.Is({ this: left, expression })
    if (negate) {
      result = new exp.Not({ this: result })
    }
    return result
  }

  private parseNull(): exp.Null | undefined {
    if (this.match(TokenType.NULL)) {
      return new exp.Null({})
    }
    return undefined
  }

  protected parseIdentifier(): exp.Identifier {
    if (this.current.tokenType === TokenType.VAR) {
      return new exp.Identifier({ this: this.advance().text })
    }

    if (this.current.tokenType === TokenType.IDENTIFIER) {
      // Quoted identifier - remove quotes
      const text = this.advance().text
      const quote = text.charAt(0)
      const inner = text
        .slice(1, -1)
        .replace(new RegExp(`${quote}${quote}`, "g"), quote)
      return new exp.Identifier({ this: inner, quoted: true })
    }

    if (IDENTIFIER_TOKENS.has(this.current.tokenType)) {
      return new exp.Identifier({ this: this.advance().text })
    }

    this.raiseError(`Expected identifier, got ${this.current.tokenType}`)
    return new exp.Identifier({ this: "" })
  }

  protected parseAnyIdentifier(): exp.Identifier {
    if (this.current.tokenType === TokenType.IDENTIFIER) {
      const text = this.advance().text
      const quote = text.charAt(0)
      const inner = text
        .slice(1, -1)
        .replace(new RegExp(`${quote}${quote}`, "g"), quote)
      return new exp.Identifier({ this: inner, quoted: true })
    }
    return new exp.Identifier({ this: this.advance().text })
  }

  private parseStarOp(...keywords: string[]): exp.Expression[] | undefined {
    if (!this.matchText(...keywords)) return undefined
    const parseAliasedExpr = () => {
      const expr = this.parseExpression()
      if (this.match(TokenType.ALIAS) || this.match(TokenType.AS)) {
        const alias = this.parseIdentifier()
        return new exp.Alias({ this: expr, alias })
      }
      return expr
    }
    if (this.match(TokenType.L_PAREN)) {
      const exprs = this.parseCSV(parseAliasedExpr)
      this.expect(TokenType.R_PAREN)
      return exprs
    }
    return [parseAliasedExpr()]
  }

  protected parseCSV<T>(parser: () => T): T[] {
    const results: T[] = []

    do {
      results.push(parser())
    } while (this.match(TokenType.COMMA))

    return results
  }

  parseCreate(): exp.Expression {
    const fallbackIndex = this.index
    const replace = this.matchTextSeq("OR", "REPLACE") || undefined

    const createKind = this.parseCreateKind()
    if (!createKind) {
      return this.parseAsCommand("CREATE")
    }

    const exists = this.matchTextSeq("IF", "NOT", "EXISTS") || undefined

    if (createKind.endsWith("SEQUENCE")) {
      const name = this.parseTableName()
      const properties = this.parseSequenceProperties()
      return this.expression(exp.Create, {
        this: name,
        kind: createKind,
        exists,
        replace,
        properties,
      })
    }

    if (
      createKind.endsWith("FUNCTION") ||
      createKind.endsWith("PROCEDURE") ||
      createKind === "MACRO"
    ) {
      const name = this.parseAnyIdentifier()

      let expressions: exp.Expression[] | undefined
      let wrapped = false
      if (this.match(TokenType.L_PAREN)) {
        expressions = this.parseFunctionParameters()
        this.expect(TokenType.R_PAREN)
        wrapped = true
      }

      const udf = this.expression(exp.UserDefinedFunction, {
        this: name,
        expressions,
        wrapped: wrapped || undefined,
      })

      let body: exp.Expression | undefined
      if (this.match(TokenType.ALIAS, TokenType.AS)) {
        body = this.parseUDFExpression()
      }

      return this.expression(exp.Create, {
        this: udf,
        kind: createKind,
        expression: body,
        exists,
        replace,
      })
    }

    // TABLE, VIEW, INDEX, SCHEMA
    const thisExpr = this.parseCreateSchema()

    // Parse properties between schema and AS (e.g., USING, PARTITIONED BY, etc.)
    const properties = this.parseCreateProperties()

    // Parse AS SELECT or (SELECT ...) — only match AS if followed by a DDL select start
    let body: exp.Expression | undefined
    if (
      this.currentTokenType === TokenType.ALIAS ||
      this.currentTokenType === TokenType.AS
    ) {
      const nextTT = this.peek(1).tokenType
      if (
        nextTT === TokenType.SELECT ||
        nextTT === TokenType.FROM ||
        nextTT === TokenType.L_PAREN ||
        nextTT === TokenType.WITH
      ) {
        this.advance() // consume AS
      }
    }
    body = this.parseDdlSelect()

    // Parse trailing properties (after AS SELECT)
    const trailingProps = this.parseCreateProperties()
    const allProperties = this.mergeProperties(properties, trailingProps)

    // Parse CLONE
    let clone: exp.Expression | undefined
    if (this.matchText("CLONE") || this.matchText("COPY")) {
      const cloneTarget = this.parseTableName()
      clone = new exp.Clone({ this: cloneTarget })
    }

    // If there are unparsed tokens, fall back to Command to preserve original SQL
    if (!this.isEnd() && this.currentTokenType !== TokenType.SEMICOLON) {
      this.index = fallbackIndex
      return this.parseAsCommand("CREATE")
    }

    return this.expression(exp.Create, {
      this: thisExpr,
      kind: createKind,
      expression: body,
      exists,
      replace,
      properties: allProperties,
      clone,
    })
  }

  protected parseCreateProperties(): exp.Properties | undefined {
    const props: exp.Expression[] = []

    while (!this.isEnd() && this.currentTokenType !== TokenType.SEMICOLON) {
      if (this.matchText("USING")) {
        props.push(
          new exp.FileFormatProperty({ this: this.parseAnyIdentifier() }),
        )
      } else if (this.matchTextSeq("STORED", "AS")) {
        props.push(new exp.FileFormatProperty({ this: this.parsePrimary() }))
      } else if (
        this.currentTokenType === TokenType.WITH &&
        this.peek(1).tokenType === TokenType.L_PAREN
      ) {
        this.advance() // consume WITH
        this.advance() // consume L_PAREN
        const withProps = this.parseCSV(() => this.parseWithProperty())
        this.expect(TokenType.R_PAREN)
        props.push(...withProps)
      } else if (this.matchTextSeq("PARTITIONED", "BY")) {
        if (this.match(TokenType.L_PAREN)) {
          const exprs = this.parseCSV(() => this.parseFieldDef())
          this.expect(TokenType.R_PAREN)
          props.push(
            new exp.PartitionedByProperty({
              this: new exp.Schema({ expressions: exprs }),
            }),
          )
        }
      } else if (this.matchTextSeq("CLUSTER", "BY")) {
        if (this.match(TokenType.L_PAREN)) {
          const exprs = this.parseCSV(() => this.parseExpression())
          this.expect(TokenType.R_PAREN)
          props.push(new exp.ClusteredByProperty({ expressions: exprs }))
        }
      } else if (this.matchTextSeq("SORT", "BY")) {
        if (this.match(TokenType.L_PAREN)) {
          const exprs = this.parseCSV(() => this.parseExpression())
          this.expect(TokenType.R_PAREN)
          props.push(
            new exp.SortKeyProperty({
              this: new exp.Tuple({ expressions: exprs }),
            }),
          )
        }
      } else if (this.matchText("LOCATION")) {
        props.push(new exp.LocationProperty({ this: this.parsePrimary() }))
      } else if (this.matchText("COMMENT")) {
        props.push(new exp.SchemaCommentProperty({ this: this.parsePrimary() }))
      } else if (this.matchText("TBLPROPERTIES") || this.matchText("OPTIONS")) {
        if (this.match(TokenType.L_PAREN)) {
          const exprs = this.parseCSV(() => this.parseProperty())
          this.expect(TokenType.R_PAREN)
          props.push(...exprs)
        }
      } else {
        break
      }
    }

    if (props.length === 0) return undefined
    return new exp.Properties({ expressions: props })
  }

  protected parseProperty(): exp.Expression {
    const key = this.parseExpression()
    if (this.match(TokenType.EQ) || this.match(TokenType.ALIAS)) {
      const value = this.parseExpression()
      return new exp.Property({ this: key, value })
    }
    return new exp.Property({ this: key })
  }

  protected parseWithProperty(): exp.Expression {
    const key = this.parseAnyIdentifier()
    const keyName = key.name.toUpperCase()
    if (this.match(TokenType.EQ) || this.match(TokenType.ALIAS)) {
      const value = this.parseExpression()
      if (keyName === "FORMAT") {
        return new exp.FileFormatProperty({ this: value })
      }
      if (keyName === "PARTITIONED_BY") {
        return new exp.PartitionedByProperty({
          this: new exp.Schema({
            expressions:
              value instanceof exp.Array ? value.expressions : [value],
          }),
        })
      }
      return new exp.Property({ this: key, value })
    }
    return new exp.Property({ this: key })
  }

  protected parseCreateSchema(): exp.Expression {
    // Parse table name parts (db.schema.table)
    const parts: exp.Identifier[] = []
    do {
      parts.push(this.parseIdentifier())
    } while (this.match(TokenType.DOT))

    let table: exp.Table
    if (parts.length === 1) {
      table = new exp.Table({ this: parts[0] })
    } else if (parts.length === 2) {
      table = new exp.Table({ this: parts[1], db: parts[0] })
    } else if (parts.length === 3) {
      table = new exp.Table({ this: parts[2], db: parts[1], catalog: parts[0] })
    } else {
      table = new exp.Table({ this: parts[0] })
    }

    // Check if `(` follows — could be column definitions or subquery
    if (this.currentTokenType !== TokenType.L_PAREN) return table

    // Disambiguate: peek after L_PAREN to check for SELECT/WITH (subquery, not column defs)
    const nextToken = this.peek(1).tokenType
    if (
      nextToken === TokenType.SELECT ||
      nextToken === TokenType.WITH ||
      nextToken === TokenType.FROM
    ) {
      return table
    }

    this.advance() // consume L_PAREN
    const columns: exp.Expression[] = []
    while (!this.isEnd()) {
      const tt = this.current.tokenType
      if (tt === TokenType.R_PAREN) break
      columns.push(this.parseFieldDef())
      if (!this.match(TokenType.COMMA)) break
    }
    this.expect(TokenType.R_PAREN)
    return new exp.Schema({ this: table, expressions: columns })
  }

  protected parseFieldDef(): exp.Expression {
    const name = this.parseAnyIdentifier()

    // If next token is , or ) — no type, just a column name
    if (
      this.currentTokenType === TokenType.COMMA ||
      this.currentTokenType === TokenType.R_PAREN
    ) {
      return name
    }

    // Try to parse a data type
    const kind = this.parseDataType()

    // Parse basic column constraints
    const constraints: exp.Expression[] = []
    let depth = 0
    while (!this.isEnd()) {
      const tt = this.current.tokenType
      if (tt === TokenType.L_PAREN) {
        depth++
        this.advance()
      } else if (tt === TokenType.R_PAREN) {
        if (depth === 0) break
        depth--
        this.advance()
      } else if (tt === TokenType.COMMA && depth === 0) {
        break
      } else if (depth === 0 && this.matchTextSeq("NOT", "NULL")) {
        constraints.push(new exp.NotNullColumnConstraint({}))
      } else if (depth === 0 && this.matchTextSeq("PRIMARY", "KEY")) {
        constraints.push(new exp.PrimaryKeyColumnConstraint({}))
      } else if (depth === 0 && this.matchText("UNIQUE")) {
        constraints.push(new exp.UniqueColumnConstraint({}))
      } else if (depth === 0 && this.matchText("DEFAULT")) {
        constraints.push(
          new exp.DefaultColumnConstraint({ this: this.parsePrimary() }),
        )
      } else if (depth === 0 && this.matchText("COMMENT")) {
        constraints.push(
          new exp.CommentColumnConstraint({ this: this.parsePrimary() }),
        )
      } else {
        // Skip unknown column modifiers (UNSIGNED, AUTO_INCREMENT, REFERENCES, CHECK, etc.)
        this.advance()
      }
    }

    return new exp.ColumnDef({
      this: name,
      kind,
      constraints: constraints.length > 0 ? constraints : undefined,
    })
  }

  protected mergeProperties(
    a: exp.Properties | undefined,
    b: exp.Properties | undefined,
  ): exp.Properties | undefined {
    if (!a && !b) return undefined
    if (!a) return b
    if (!b) return a
    return new exp.Properties({
      expressions: [...a.expressions, ...b.expressions],
    })
  }

  private parseSequenceProperties(): exp.SequenceProperties | undefined {
    const args: exp.Args = {}
    const options: exp.Expression[] = []
    const startIndex = this.index

    while (!this.isEnd() && this.current.tokenType !== TokenType.SEMICOLON) {
      if (this.matchTextSeq("INCREMENT")) {
        this.matchText("BY")
        args.increment = this.parseTerm()
      } else if (this.matchText("MINVALUE")) {
        args.minvalue = this.parseTerm()
      } else if (this.matchText("MAXVALUE")) {
        args.maxvalue = this.parseTerm()
      } else if (this.match(TokenType.START)) {
        this.matchText("WITH")
        args.start = this.parseTerm()
      } else if (this.matchText("CACHE")) {
        const num =
          this.current.tokenType === TokenType.NUMBER
            ? this.parsePrimary()
            : undefined
        args.cache = num ?? true
      } else if (this.matchTextSeq("OWNED", "BY")) {
        args.owned = this.matchText("NONE") ? undefined : this.parsePrimary()
      } else if (this.matchTextSeq("NO", "CYCLE")) {
        options.push(new exp.Var({ this: "NO CYCLE" }))
      } else if (this.matchText("CYCLE")) {
        options.push(new exp.Var({ this: "CYCLE" }))
      } else {
        break
      }
    }

    if (options.length > 0) {
      args.options = options
    }

    if (this.index === startIndex) {
      return undefined
    }

    return new exp.SequenceProperties(args)
  }

  static ALTERABLES: Set<TokenType> = new Set([
    TokenType.INDEX,
    TokenType.TABLE,
    TokenType.VIEW,
  ])

  parseAlter(): exp.Expression {
    const start = this.prev
    const fallbackIndex = this.index

    const alterables = (this.constructor as typeof Parser).ALTERABLES
    let alterToken: Token | undefined
    if (alterables.has(this.current.tokenType)) {
      alterToken = this.advance()
    }
    if (!alterToken) {
      this.index = fallbackIndex
      return this.parseAsCommand(start.text)
    }

    const exists = this.matchTextSeq("IF", "EXISTS") || undefined
    const only = this.matchText("ONLY") || undefined
    const table = this.parseTableName()

    if (!this.current || this.isEnd()) {
      this.index = fallbackIndex
      return this.parseAsCommand(start.text)
    }
    this.advance()

    const actionText = this.prev.text.toUpperCase()
    const actions = this.parseAlterAction(actionText)

    if (
      actions &&
      actions.length > 0 &&
      (this.isEnd() || this.current.tokenType === TokenType.SEMICOLON)
    ) {
      return this.expression(exp.Alter, {
        this: table,
        kind: alterToken.text.toUpperCase(),
        exists,
        only,
        actions,
      })
    }

    this.index = fallbackIndex
    return this.parseAsCommand(start.text)
  }

  protected parseAlterAction(actionText: string): exp.Expression[] | undefined {
    switch (actionText) {
      case "ADD": {
        this.match(TokenType.COLUMN)
        const addExists = this.matchTextSeq("IF", "NOT", "EXISTS") || undefined
        const colName = this.parseIdentifier()
        const colType =
          this.isEnd() || this.current.tokenType === TokenType.SEMICOLON
            ? undefined
            : this.parseDataType()
        const colDef = new exp.ColumnDef({
          this: colName,
          kind: colType,
          exists: addExists,
        })
        return [colDef]
      }
      case "DROP": {
        this.match(TokenType.COLUMN)
        const dropExists = this.matchTextSeq("IF", "EXISTS") || undefined
        const column = this.parseIdentifier()
        return [
          new exp.Drop({ this: column, kind: "COLUMN", exists: dropExists }),
        ]
      }
      case "ALTER": {
        this.match(TokenType.COLUMN)
        const column = this.parseIdentifier()
        if (
          this.matchText("TYPE") ||
          this.matchTextSeq("SET", "DATA", "TYPE")
        ) {
          const dtype = this.parseDataType()
          return [new exp.AlterColumn({ this: column, dtype })]
        }
        if (this.matchTextSeq("SET", "DEFAULT")) {
          const defaultVal = this.parseExpression()
          return [new exp.AlterColumn({ this: column, default: defaultVal })]
        }
        if (this.matchTextSeq("DROP", "DEFAULT")) {
          return [new exp.AlterColumn({ this: column, drop: true })]
        }
        if (this.matchTextSeq("DROP", "NOT", "NULL")) {
          return [
            new exp.AlterColumn({ this: column, drop: true, allow_null: true }),
          ]
        }
        if (this.matchTextSeq("SET", "NOT", "NULL")) {
          return [new exp.AlterColumn({ this: column, allow_null: false })]
        }
        // TSQL-style: ALTER COLUMN col TYPE [COLLATE collation]
        if (!this.isEnd() && this.current.tokenType !== TokenType.SEMICOLON) {
          const dtype = this.parseDataType()
          const collate = this.match(TokenType.COLLATE)
            ? this.parseTerm()
            : undefined
          return [new exp.AlterColumn({ this: column, dtype, collate })]
        }
        return undefined
      }
      case "RENAME": {
        const action = this.parseAlterTableRename()
        return action ? [action] : undefined
      }
      case "SET": {
        const props = this.parseCSV(() => this.parseSetItemAssignment())
        const setExprs = props.filter(
          (p): p is exp.Expression => p instanceof exp.Expression,
        )
        if (setExprs.length > 0) {
          return [new exp.AlterSet({ expressions: setExprs })]
        }
        return undefined
      }
      default:
        return undefined
    }
  }

  protected parseAlterTableRename(): exp.AlterRename | undefined {
    this.matchText("TO")
    const target = this.parseTableName()
    return this.expression(exp.AlterRename, { this: target })
  }

  static ANALYZE_STYLES: Set<string> = new Set([
    "VERBOSE",
    "SKIP_LOCKED",
    "BUFFER_USAGE_LIMIT",
  ])

  protected parseAnalyze(): exp.Expression {
    const fallbackIndex = this.index
    const options: string[] = []
    const analyzeStyles = (this.constructor as typeof Parser).ANALYZE_STYLES
    while (
      !this.isEnd() &&
      this.current.tokenType !== TokenType.SEMICOLON &&
      analyzeStyles.has(this.current.text.toUpperCase())
    ) {
      const style = this.advance().text.toUpperCase()
      if (style === "BUFFER_USAGE_LIMIT") {
        const num = this.advance().text
        options.push(`${style} ${num}`)
      } else {
        options.push(style)
      }
    }

    const kind = this.match(TokenType.TABLE) ? "TABLE" : undefined
    let table: exp.Expression | undefined
    if (!this.isEnd() && this.current.tokenType !== TokenType.SEMICOLON) {
      table = this.parseTableName()
      if (this.match(TokenType.L_PAREN)) {
        const cols = this.parseCSV(() => this.parseIdentifier())
        this.expect(TokenType.R_PAREN)
        table = new exp.Table({
          this: (table as exp.Table).args.this,
          expressions: cols,
        })
      }
    }

    if (!this.isEnd() && this.current.tokenType !== TokenType.SEMICOLON) {
      this.index = fallbackIndex
      return this.parseAsCommand("ANALYZE")
    }

    return new exp.Analyze({
      this: table,
      kind,
      options:
        options.length > 0
          ? options.map((o) => new exp.Var({ this: o }))
          : undefined,
    })
  }

  protected parseShow(): exp.Expression {
    const fallbackIndex = this.index

    try {
      const parts: string[] = []
      if (this.matchText("FULL")) parts.push("FULL")
      if (this.matchText("TERSE")) parts.push("TERSE")

      if (this.isEnd() || this.current.tokenType === TokenType.SEMICOLON) {
        this.index = fallbackIndex
        return this.parseAsCommand("SHOW")
      }

      const name = this.current.text.toUpperCase()
      this.advance()

      let like: exp.Expression | undefined
      let db: exp.Expression | undefined
      let where: exp.Expression | undefined

      // Multi-word SHOW names: PRIMARY KEYS, IMPORTED KEYS, UNIQUE KEYS, FILE FORMATS
      let fullName = name
      if (["PRIMARY", "IMPORTED", "UNIQUE", "FILE"].includes(name)) {
        if (!this.isEnd()) {
          const next = this.current.text.toUpperCase()
          if (next === "KEYS" || next === "FORMATS") {
            fullName = `${name} ${next}`
            this.advance()
          }
        }
      }

      if (this.match(TokenType.FROM) || this.match(TokenType.IN)) {
        db = this.parseTableName()
      }

      if (this.match(TokenType.LIKE)) {
        like = this.parsePrimary()
      }

      if (!db && (this.match(TokenType.FROM) || this.match(TokenType.IN))) {
        db = this.parseTableName()
      }

      if (this.match(TokenType.WHERE)) {
        where = this.parseWhere()
      }

      if (
        !this.isEnd() &&
        (this.current.tokenType as TokenType) !== TokenType.SEMICOLON
      ) {
        this.index = fallbackIndex
        return this.parseAsCommand("SHOW")
      }

      return new exp.Show({
        this: fullName,
        full: parts.includes("FULL") || undefined,
        terse: parts.includes("TERSE") || undefined,
        like,
        db,
        where,
      })
    } catch {
      this.index = fallbackIndex
      return this.parseAsCommand("SHOW")
    }
  }

  static CREATABLES: Set<TokenType> = new Set([
    TokenType.TABLE,
    TokenType.VIEW,
    TokenType.INDEX,
    TokenType.SCHEMA,
    TokenType.FUNCTION,
    TokenType.PROCEDURE,
  ])

  protected parseCreateKind(): string | undefined {
    // Handle optional TEMPORARY/TEMP/MATERIALIZED/VOLATILE prefix
    const temporary =
      this.match(TokenType.TEMPORARY) || this.match(TokenType.TEMP)
    const materialized = this.matchText("MATERIALIZED")
    const volatile = this.matchText("VOLATILE")

    if (
      this.match(
        TokenType.TABLE,
        TokenType.VIEW,
        TokenType.INDEX,
        TokenType.SCHEMA,
        TokenType.FUNCTION,
        TokenType.PROCEDURE,
      )
    ) {
      const kindText = this.prev.text.toUpperCase()
      return (
        (temporary ? "TEMPORARY " : "") +
        (materialized ? "MATERIALIZED " : "") +
        (volatile ? "VOLATILE " : "") +
        kindText
      )
    }
    if (this.matchText("SEQUENCE") || this.matchText("MACRO")) {
      const kindText = this.prev.text.toUpperCase()
      return (temporary ? "TEMPORARY " : "") + kindText
    }
    return undefined
  }

  protected parseFunctionParameters(): exp.Expression[] {
    if (this.currentTokenType === TokenType.R_PAREN) return []
    return this.parseCSV(() => this.parseFunctionParameter())
  }

  protected parseFunctionParameter(): exp.Expression {
    const name = this.parseAnyIdentifier()
    if (
      this.currentTokenType === TokenType.COMMA ||
      this.currentTokenType === TokenType.R_PAREN
    ) {
      return new exp.ColumnDef({ this: name })
    }
    const kind = this.parseDataType()
    return new exp.ColumnDef({ this: name, kind })
  }

  protected parseUDFExpression(): exp.Expression | undefined {
    return this.parseExpression()
  }

  protected parseCopy(): exp.Expression {
    this.match(TokenType.INTO)

    let thisExpr: exp.Expression
    if (this.match(TokenType.L_PAREN)) {
      const subquery = this.parseSubquery()
      this.expect(TokenType.R_PAREN)
      thisExpr = subquery
    } else {
      thisExpr = this.parseTableNameSchema()
    }

    const kind = this.match(TokenType.FROM) || !this.matchText("TO")
    const files = this.parseCSV(() => this.parsePrimary())

    this.matchText("WITH")

    let params: exp.CopyParameter[] | undefined
    if (this.match(TokenType.L_PAREN)) {
      params = this.parseCopyParameters()
      this.expect(TokenType.R_PAREN)
    }

    if (!this.isEnd() && this.currentTokenType !== TokenType.SEMICOLON) {
      return this.parseAsCommand("COPY")
    }

    return new exp.Copy({
      this: thisExpr,
      kind: kind || undefined,
      files: files.length > 0 ? files : undefined,
      params,
    })
  }

  protected parseDescribe(): exp.Expression {
    const statementParsers = (this.constructor as typeof Parser)
      .STATEMENT_PARSERS
    let thisExpr: exp.Expression
    if (statementParsers.has(this.currentTokenType)) {
      thisExpr = this.parseStatement() as exp.Expression
    } else {
      thisExpr = this.parseTableName()
    }
    return new exp.Describe({ this: thisExpr })
  }

  // ==================== DML Parsers ====================

  protected parseDelete(): exp.Delete {
    let tables: exp.Expression[] | undefined
    if (this.currentTokenType !== TokenType.FROM) {
      tables = this.parseCSV(() => this.parseTableExpression()) || undefined
    }

    const returning = this.parseReturning()

    const this_ = this.match(TokenType.FROM)
      ? this.parseTableExpression()
      : undefined
    const using = this.matchText("USING")
      ? this.parseCSV(() => this.parseTableExpression())
      : undefined

    return new exp.Delete({
      tables: tables && tables.length > 0 ? tables : undefined,
      this: this_,
      using: using && using.length > 0 ? using : undefined,
      where:
        this.currentTokenType === TokenType.WHERE
          ? (this.advance(), this.parseWhere())
          : undefined,
      returning: returning || this.parseReturning(),
      order:
        this.currentTokenType === TokenType.ORDER_BY
          ? (this.advance(), this.parseOrder())
          : undefined,
      limit:
        this.currentTokenType === TokenType.LIMIT
          ? (this.advance(), this.parseLimit())
          : undefined,
    })
  }

  protected parseDrop(): exp.Expression {
    const temporary =
      this.match(TokenType.TEMPORARY) || this.match(TokenType.TEMP) || undefined
    const materialized = this.matchText("MATERIALIZED") || undefined

    const kind = this.parseDropKind()
    if (!kind) {
      return this.parseAsCommand("DROP")
    }

    const concurrently = this.matchText("CONCURRENTLY") || undefined
    const exists = this.parseIfExists()

    const this_ = this.parseTableName()

    let expressions: exp.Expression[] | undefined
    if (this.match(TokenType.L_PAREN)) {
      expressions = this.parseCSV(() => this.parseDataType())
      this.expect(TokenType.R_PAREN)
    }

    return new exp.Drop({
      exists,
      this: this_,
      expressions:
        expressions && expressions.length > 0 ? expressions : undefined,
      kind,
      temporary,
      materialized,
      cascade: this.matchText("CASCADE") || undefined,
      constraints: this.matchText("CONSTRAINTS") || undefined,
      purge: this.matchText("PURGE") || undefined,
      concurrently,
    })
  }

  protected parseDropKind(): string | undefined {
    if (this.match(TokenType.TABLE)) return "TABLE"
    if (this.match(TokenType.VIEW)) return "VIEW"
    if (this.match(TokenType.INDEX)) return "INDEX"
    if (this.match(TokenType.SCHEMA)) return "SCHEMA"
    if (this.match(TokenType.FUNCTION)) return "FUNCTION"
    if (this.match(TokenType.PROCEDURE)) return "PROCEDURE"
    if (this.matchText("DATABASE")) return "DATABASE"
    if (this.matchText("SEQUENCE")) return "SEQUENCE"
    if (this.matchText("MODEL")) return "MODEL"
    if (this.matchText("TYPE")) return "TYPE"
    if (this.matchText("TRIGGER")) return "TRIGGER"
    return undefined
  }

  protected parseIfExists(): boolean | undefined {
    return this.matchTextSeq("IF", "EXISTS") || undefined
  }

  protected parseInsert(): exp.Expression {
    const overwrite = this.match(TokenType.OVERWRITE) || undefined
    const ignore = this.match(TokenType.IGNORE) || undefined

    let alternative: string | undefined
    if (this.matchText("OR")) {
      if (this.matchText("REPLACE")) alternative = "REPLACE"
      else if (this.matchText("IGNORE")) alternative = "IGNORE"
      else if (this.matchText("ABORT")) alternative = "ABORT"
      else if (this.matchText("ROLLBACK")) alternative = "ROLLBACK"
      else if (this.matchText("FAIL")) alternative = "FAIL"
    }

    this.match(TokenType.INTO)
    this.match(TokenType.TABLE)

    const isFunction = this.match(TokenType.FUNCTION) || undefined
    const this_ = isFunction ? this.parsePrimary() : this.parseInsertTable()

    const returning = this.parseReturning()

    const byName = this.matchTextSeq("BY", "NAME") || undefined
    const exists = this.parseIfExists()

    const partition = this.match(TokenType.PARTITION_BY)
      ? this.parseExpression()
      : undefined

    const defaultValues = this.matchTextSeq("DEFAULT", "VALUES") || undefined
    const expression = !defaultValues
      ? this.parseDerivedTableValues() || this.parseDdlSelect()
      : undefined
    const conflict = this.parseOnConflict()

    return new exp.Insert({
      is_function: isFunction,
      this: this_,
      by_name: byName,
      exists,
      default: defaultValues,
      expression,
      conflict,
      returning: returning || this.parseReturning(),
      overwrite,
      alternative,
      ignore,
      partition,
    })
  }

  protected parseInsertTable(): exp.Expression {
    const table = this.parseTableName()
    if (this.match(TokenType.L_PAREN)) {
      const columns = this.parseCSV(() => this.parseAnyIdentifier())
      this.expect(TokenType.R_PAREN)
      return new exp.Schema({ this: table, expressions: columns })
    }
    return table
  }

  protected parseDdlSelect(): exp.Expression | undefined {
    if (
      this.currentTokenType === TokenType.SELECT ||
      this.currentTokenType === TokenType.FROM
    ) {
      return this.parseStatement()
    }
    // WITH starts a CTE only if followed by an identifier (not a paren which indicates properties)
    if (
      this.currentTokenType === TokenType.WITH &&
      this.peek(1).tokenType !== TokenType.L_PAREN
    ) {
      return this.parseStatement()
    }
    // Handle (SELECT ...) wrapped in parens — preserve as Subquery
    if (this.currentTokenType === TokenType.L_PAREN) {
      const nextToken = this.peek(1).tokenType
      if (
        nextToken === TokenType.SELECT ||
        nextToken === TokenType.WITH ||
        nextToken === TokenType.FROM
      ) {
        this.advance() // consume L_PAREN
        const result = this.parseStatement()
        this.expect(TokenType.R_PAREN)
        return new exp.Subquery({ this: result })
      }
    }
    return undefined
  }

  protected parseDerivedTableValues(): exp.Expression | undefined {
    if (!this.matchText("VALUES")) return undefined
    const tuples: exp.Tuple[] = []
    do {
      this.expect(TokenType.L_PAREN)
      const values = this.parseCSV(() => this.parseExpression())
      this.expect(TokenType.R_PAREN)
      tuples.push(new exp.Tuple({ expressions: values }))
    } while (this.match(TokenType.COMMA))
    return new exp.Values({ expressions: tuples })
  }

  protected parseOnConflict(): exp.OnConflict | undefined {
    const conflict = this.matchTextSeq("ON", "CONFLICT")
    const duplicate = this.matchTextSeq("ON", "DUPLICATE", "KEY")

    if (!conflict && !duplicate) return undefined

    let conflictKeys: exp.Expression[] | undefined
    let constraint: exp.Expression | undefined

    if (conflict) {
      if (this.matchTextSeq("ON", "CONSTRAINT")) {
        constraint = this.parseIdentifier()
      } else if (this.match(TokenType.L_PAREN)) {
        conflictKeys = this.parseCSV(() => this.parseIdentifier())
        this.expect(TokenType.R_PAREN)
      }
    }

    const indexPredicate =
      this.currentTokenType === TokenType.WHERE
        ? (this.advance(), this.parseWhere())
        : undefined

    let action: string | undefined
    if (this.matchText("DO")) {
      if (this.matchText("NOTHING")) action = "NOTHING"
      else if (this.match(TokenType.UPDATE)) action = "UPDATE"
    } else if (this.match(TokenType.UPDATE)) {
      action = "UPDATE"
    }

    let expressions: exp.Expression[] | undefined
    if (action === "UPDATE" && this.match(TokenType.SET)) {
      expressions = this.parseCSV(() => this.parseEquality())
    }

    const where =
      this.currentTokenType === TokenType.WHERE
        ? (this.advance(), this.parseWhere())
        : undefined

    return new exp.OnConflict({
      duplicate: duplicate || undefined,
      expressions:
        expressions && expressions.length > 0 ? expressions : undefined,
      action,
      conflict_keys:
        conflictKeys && conflictKeys.length > 0 ? conflictKeys : undefined,
      index_predicate: indexPredicate,
      constraint,
      where,
    })
  }

  protected parseReturning(): exp.Returning | undefined {
    if (!this.match(TokenType.RETURNING)) return undefined
    return new exp.Returning({
      expressions: this.parseCSV(() => this.parseExpression()),
      into: this.match(TokenType.INTO) ? this.parseTableName() : undefined,
    })
  }

  protected parseUpdate(): exp.Update {
    const args: exp.Args = {
      this: this.parseTableExpression(),
    }
    while (!this.isEnd() && this.currentTokenType !== TokenType.SEMICOLON) {
      if (this.match(TokenType.SET)) {
        args.expressions = this.parseCSV(() => this.parseEquality())
      } else if (this.currentTokenType === TokenType.RETURNING) {
        args.returning = this.parseReturning()
      } else if (this.currentTokenType === TokenType.FROM) {
        this.advance()
        args.from_ = new exp.From({ this: this.parseTableExpression() })
      } else if (this.currentTokenType === TokenType.WHERE) {
        this.advance()
        args.where = this.parseWhere()
      } else if (this.currentTokenType === TokenType.ORDER_BY) {
        this.advance()
        args.order = this.parseOrder()
      } else if (this.currentTokenType === TokenType.LIMIT) {
        this.advance()
        args.limit = this.parseLimit()
      } else {
        break
      }
    }
    return new exp.Update(args)
  }

  protected parseUse(): exp.Use {
    let kind: string | undefined
    if (
      this.matchText("DATABASE") ||
      this.matchText("CATALOG") ||
      this.matchText("SCHEMA") ||
      this.matchText("WAREHOUSE") ||
      this.matchText("ROLE")
    ) {
      kind = this.prev.text.toUpperCase()
    }
    const this_ = this.parseTableName()
    return new exp.Use({ kind, this: this_ })
  }

  protected parseTransaction(): exp.Transaction {
    let this_: string | undefined
    if (
      this.matchText("DEFERRED") ||
      this.matchText("IMMEDIATE") ||
      this.matchText("EXCLUSIVE")
    ) {
      this_ = this.prev.text.toUpperCase()
    }
    this.matchText("TRANSACTION")
    this.matchText("WORK")

    const modes: exp.Var[] = []
    while (true) {
      const mode: string[] = []
      while (
        this.current.tokenType === TokenType.VAR ||
        this.current.tokenType === TokenType.NOT
      ) {
        mode.push(this.advance().text)
      }
      if (mode.length > 0) {
        modes.push(new exp.Var({ this: mode.join(" ") }))
      }
      if (!this.match(TokenType.COMMA)) break
    }

    return new exp.Transaction({
      this: this_,
      modes: modes.length > 0 ? modes : undefined,
    })
  }

  protected parseCommitOrRollback(): exp.Commit | exp.Rollback {
    const isRollback = this.prev.tokenType === TokenType.ROLLBACK

    this.matchText("TRANSACTION")
    this.matchText("WORK")

    let savepoint: exp.Expression | undefined
    if (this.matchText("TO")) {
      this.matchText("SAVEPOINT")
      savepoint = this.parseIdentifier()
    }

    let chain: boolean | undefined
    if (this.match(TokenType.AND)) {
      chain = !this.matchText("NO")
      this.matchText("CHAIN")
    }

    if (isRollback) {
      return new exp.Rollback({ savepoint })
    }
    return new exp.Commit({ chain })
  }

  protected parseTruncateTable(): exp.TruncateTable {
    const isDatabase = this.matchText("DATABASE") || undefined
    this.match(TokenType.TABLE)
    const exists = this.parseIfExists()
    const expressions = this.parseCSV(() => this.parseTableExpression())
    return new exp.TruncateTable({
      expressions,
      is_database: isDatabase,
      exists,
    })
  }

  protected parseMerge(): exp.Merge {
    this.matchText("INTO")
    const target = this.parseTableExpression()

    this.matchText("USING")
    const using = this.parseTableExpression()

    let on: exp.Expression | undefined
    let usingCond: exp.Expression[] | undefined
    if (this.match(TokenType.ON)) {
      on = this.parseExpression()
    } else if (this.match(TokenType.USING)) {
      this.expect(TokenType.L_PAREN)
      usingCond = this.parseCSV(() => this.parseIdentifier())
      this.expect(TokenType.R_PAREN)
    }

    const whens: exp.When[] = []
    while (this.matchText("WHEN")) {
      const not = this.match(TokenType.NOT)
      this.matchText("MATCHED")
      const matched = !not
      const source = this.matchTextSeq("BY", "SOURCE") || undefined
      let condition: exp.Expression | undefined
      if (this.match(TokenType.AND)) {
        condition = this.parseExpression()
      }
      this.matchText("THEN")
      const then = this.parseMergeThen(matched)
      whens.push(new exp.When({ matched, source, condition, then }))
    }

    return new exp.Merge({
      this: target,
      using,
      on,
      using_cond: usingCond,
      whens:
        whens.length > 0 ? new exp.Whens({ expressions: whens }) : undefined,
    })
  }

  protected parseMergeThen(_matched: boolean): exp.Expression {
    if (this.match(TokenType.INSERT)) {
      let this_: exp.Expression | undefined
      if (this.match(TokenType.L_PAREN)) {
        const columns = this.parseCSV(() => this.parseAnyIdentifier())
        this.expect(TokenType.R_PAREN)
        this_ = new exp.Schema({ expressions: columns })
      }
      let expression: exp.Expression | undefined
      if (this.matchText("VALUES")) {
        this.expect(TokenType.L_PAREN)
        const values = this.parseCSV(() => this.parseExpression())
        this.expect(TokenType.R_PAREN)
        expression = new exp.Tuple({ expressions: values })
      }
      return new exp.Insert({ this: this_, expression })
    }
    if (this.match(TokenType.UPDATE)) {
      if (this.match(TokenType.SET)) {
        const expressions = this.parseCSV(() => this.parseEquality())
        return new exp.Update({ expressions })
      }
      return new exp.Update({})
    }
    if (this.match(TokenType.DELETE)) {
      return new exp.Delete({})
    }
    return this.parseExpression()
  }

  // ==================== End DML Parsers ====================

  private parseCopyParameters(): exp.CopyParameter[] {
    const options: exp.CopyParameter[] = []
    while (this.currentTokenType !== TokenType.R_PAREN && !this.isEnd()) {
      // Parse parameter name (any token acts as a Var)
      const nameText = this.current.text.toUpperCase()
      this.advance()
      const option = new exp.Var({ this: nameText })

      const param = new exp.CopyParameter({ this: option })

      // Parse value
      const value =
        this.parsePrimary() ||
        (this.current.tokenType === TokenType.L_BRACE
          ? this.parseBracket(undefined)
          : undefined)
      if (value) {
        param.args.expression = value
      }

      options.push(param)
      this.match(TokenType.COMMA)
    }
    return options
  }

  private parseCommand(): exp.Command {
    const tokens: Token[] = []
    const expressions: exp.Expression[] = []
    const placeholderPositions: number[] = []
    while (!this.isEnd() && this.current.tokenType !== TokenType.SEMICOLON) {
      if (
        this.current.tokenType === TokenType.L_PAREN &&
        (this.peek(1).tokenType === TokenType.SELECT ||
          this.peek(1).tokenType === TokenType.FROM)
      ) {
        this.advance() // consume L_PAREN
        const subquery = this.parseSubquery()
        this.expect(TokenType.R_PAREN)
        expressions.push(subquery)
        placeholderPositions.push(tokens.length)
        tokens.push(
          new Token(TokenType.VAR, `\0${expressions.length - 1}`, 0, 0, 0, 0),
        )
      } else {
        tokens.push(this.current)
        this.advance()
      }
    }
    const cmd = new exp.Command({ this: joinTokens(tokens) })
    if (expressions.length > 0) {
      cmd.args.expressions = expressions
    }
    return cmd
  }

  protected parseTableSample(asModifier: boolean): exp.TableSample | undefined {
    if (!this.match(TokenType.TABLESAMPLE)) {
      if (asModifier && this.matchTextSeq("USING", "SAMPLE")) {
        // Matched USING SAMPLE
      } else {
        return undefined
      }
    }

    let method: exp.Var | undefined
    let percent: exp.Expression | undefined
    let size: exp.Expression | undefined
    let seed: exp.Expression | undefined

    // Parse optional method (RESERVOIR, SYSTEM, BERNOULLI)
    if (
      this.current.tokenType === TokenType.VAR ||
      this.current.tokenType === TokenType.ROWS
    ) {
      method = new exp.Var({ this: this.advance().text.toUpperCase() })
    }

    const hasParen = this.match(TokenType.L_PAREN)
    if (
      this.current.tokenType === TokenType.NUMBER ||
      this.current.tokenType === TokenType.VAR
    ) {
      const num = this.parsePrimary()

      if (this.match(TokenType.PERCENT) || this.match(TokenType.MOD)) {
        percent = num
      } else if (this.match(TokenType.ROWS)) {
        size = num
      } else {
        size = num
      }
    }

    if (hasParen) {
      this.expect(TokenType.R_PAREN)
    }

    // Parse optional (method) or (method, seed)
    if (this.match(TokenType.L_PAREN)) {
      method = new exp.Var({ this: this.advance().text.toUpperCase() })
      if (this.match(TokenType.COMMA)) {
        seed = this.parsePrimary()
      }
      this.expect(TokenType.R_PAREN)
    }

    // Parse REPEATABLE(seed) or SEED(seed)
    if (this.matchText("REPEATABLE") || this.matchText("SEED")) {
      this.expect(TokenType.L_PAREN)
      seed = this.parsePrimary()
      this.expect(TokenType.R_PAREN)
    }

    return new exp.TableSample({
      method,
      percent,
      size,
      seed,
    })
  }

  protected parseVarFromOptions(
    options: Record<string, Array<string | string[]>>,
    raiseUnmatched = true,
  ): exp.Var | undefined {
    const start = this.current
    if (this.isEnd()) return undefined

    const option = start.text.toUpperCase()
    const continuations = options[option]

    const index = this.index
    this.advance()

    for (const keywords of continuations ?? []) {
      const kws = typeof keywords === "string" ? [keywords] : keywords
      if (this.matchTextSeq(...kws)) {
        return new exp.Var({ this: `${option} ${kws.join(" ")}` })
      }
    }

    // Python for...else semantics: else block runs when loop completes without break
    // continuations is truthy (non-empty array) or continuations is undefined (key not in options)
    if (
      (continuations !== undefined && continuations.length > 0) ||
      continuations === undefined
    ) {
      if (raiseUnmatched) {
        this.raiseError(`Unknown option ${option}`)
      }
      this.index = index
      return undefined
    }

    return new exp.Var({ this: option })
  }

  protected parseLocks(): exp.Lock[] {
    const locks: exp.Lock[] = []
    while (true) {
      let update: boolean | undefined
      let key: boolean | undefined
      if (this.matchTextSeq("FOR", "UPDATE")) {
        update = true
      } else if (
        this.matchTextSeq("FOR", "SHARE") ||
        this.matchTextSeq("LOCK", "IN", "SHARE", "MODE")
      ) {
        update = false
      } else if (this.matchTextSeq("FOR", "KEY", "SHARE")) {
        update = false
        key = true
      } else if (this.matchTextSeq("FOR", "NO", "KEY", "UPDATE")) {
        update = true
        key = true
      } else {
        break
      }
      let expressions: exp.Expression[] | undefined
      if (this.matchText("OF")) {
        expressions = this.parseCSV(() => this.parseTableNameSchema())
      }
      let wait: boolean | exp.Expression | undefined
      if (this.matchText("NOWAIT")) {
        wait = true
      } else if (this.matchText("WAIT")) {
        wait = this.parsePrimary()
      } else if (this.matchTextSeq("SKIP", "LOCKED")) {
        wait = false
      }
      locks.push(new exp.Lock({ update, expressions, wait, key }))
    }
    return locks
  }

  protected parseLateral(): exp.Lateral | undefined {
    let crossApply: boolean | undefined
    if (this.matchPair(TokenType.CROSS, TokenType.APPLY)) {
      crossApply = true
    } else if (this.matchPair(TokenType.OUTER, TokenType.APPLY)) {
      crossApply = false
    }

    if (crossApply !== undefined) {
      // CROSS/OUTER APPLY expr
      const thisExpr = this.parsePrimary()
      const alias = this.parseTableAlias()
      return new exp.Lateral({ this: thisExpr, alias, cross_apply: crossApply })
    }

    if (!this.match(TokenType.LATERAL)) return undefined

    const view = this.match(TokenType.VIEW) || undefined
    const outer = this.match(TokenType.OUTER) || undefined

    // LATERAL [VIEW] [OUTER] expr
    const thisExpr = this.parsePrimary()

    let alias: exp.TableAlias | undefined
    if (view) {
      const table = this.parseIdentifier()
      const columns = this.match(TokenType.ALIAS)
        ? this.parseCSV(() => this.parseIdentifier())
        : []
      alias = new exp.TableAlias({ this: table, columns })
    } else {
      const ordinality = this.matchPair(TokenType.WITH, TokenType.ORDINALITY)
      alias = this.parseTableAlias()
      if (ordinality) {
        return new exp.Lateral({
          this: thisExpr,
          view,
          outer,
          alias,
          ordinality,
        })
      }
    }

    return new exp.Lateral({ this: thisExpr, view, outer, alias })
  }

  parseSet(): exp.Set | exp.Command {
    const index = this.index
    const items = this.parseCSV(() => this.parseSetItem())
    const expressions = items.filter(
      (e): e is exp.Expression => e !== undefined,
    )
    const set = this.expression(exp.Set, { expressions })

    if (!this.isEnd() && this.currentTokenType !== TokenType.SEMICOLON) {
      this.index = index
      return this.parseAsCommand("SET")
    }

    return set
  }

  protected parseSetItem(): exp.Expression {
    const upper = this.current.text.toUpperCase()
    const parser = this.setParsers.get(upper)
    if (parser) {
      this.advance()
      const result = parser(this)
      if (result) return result
    }
    return this.parseSetItemAssignment() ?? this.parsePrimary()
  }

  parseSetItemAssignment(kind?: string): exp.Expression | undefined {
    const index = this.index

    const left = this.parsePrimary()
    if (!left) {
      this.index = index
      return undefined
    }

    const hasDelimiter =
      this.match(TokenType.EQ) || this.matchText("TO") || this.matchText(":=")

    if (
      !hasDelimiter &&
      (this.constructor as typeof Parser).SET_REQUIRES_ASSIGNMENT_DELIMITER
    ) {
      this.index = index
      return undefined
    }

    const right = this.parseSetValue()
    const eq = this.expression(exp.EQ, { this: left, expression: right })
    return this.expression(exp.SetItem, { this: eq, kind })
  }

  private parseSetValue(): exp.Expression {
    if (this.match(TokenType.L_PAREN)) {
      if (this.match(TokenType.SELECT)) {
        const select = this.parseSelect()
        this.expect(TokenType.R_PAREN)
        return new exp.Subquery({ this: select })
      }
      const expr = this.parseExpression()
      this.expect(TokenType.R_PAREN)
      return new exp.Paren({ this: expr })
    }
    return this.parsePrimary()
  }

  parseNextValueFor(): exp.Expression | undefined {
    if (!this.matchTextSeq("VALUE", "FOR")) {
      this.retreat()
      return undefined
    }
    const parts: exp.Identifier[] = [this.parseIdentifier()]
    while (this.match(TokenType.DOT)) {
      parts.push(this.parseIdentifier())
    }
    let thisExpr: exp.Expression
    if (parts.length === 1) {
      thisExpr = new exp.Column({ this: parts[0] })
    } else if (parts.length === 2) {
      thisExpr = new exp.Column({ this: parts[1], table: parts[0] })
    } else if (parts.length === 3) {
      thisExpr = new exp.Column({
        this: parts[2],
        table: parts[1],
        db: parts[0],
      })
    } else {
      thisExpr = new exp.Column({
        this: parts[parts.length - 1],
        table: parts[parts.length - 2],
        db: parts[parts.length - 3],
        catalog: parts[parts.length - 4],
      })
    }
    let order: exp.Expression | undefined
    if (this.match(TokenType.OVER)) {
      this.match(TokenType.L_PAREN)
      if (this.match(TokenType.ORDER_BY)) {
        order = this.parseOrder()
      }
      this.match(TokenType.R_PAREN)
    }
    return new exp.NextValueFor({ this: thisExpr, order })
  }

  protected parseAsCommand(prefix: string): exp.Command {
    const tokens: Token[] = []
    const expressions: exp.Expression[] = []
    while (!this.isEnd() && this.currentTokenType !== TokenType.SEMICOLON) {
      if (
        this.currentTokenType === TokenType.L_PAREN &&
        (this.peek(1).tokenType === TokenType.SELECT ||
          this.peek(1).tokenType === TokenType.FROM)
      ) {
        this.advance()
        const subquery = this.parseSubquery()
        this.expect(TokenType.R_PAREN)
        expressions.push(subquery)
        tokens.push(
          new Token(TokenType.VAR, `\0${expressions.length - 1}`, 0, 0, 0, 0),
        )
      } else {
        tokens.push(this.current)
        this.advance()
      }
    }
    const text = tokens.length > 0 ? `${prefix} ${joinTokens(tokens)}` : prefix
    const cmd = new exp.Command({ this: text })
    if (expressions.length > 0) {
      cmd.args.expressions = expressions
    }
    return cmd
  }
}

function joinTokens(tokens: Token[]): string {
  let text = ""
  for (let i = 0; i < tokens.length; i++) {
    const tok = tokens[i]!
    if (i > 0) {
      const prev = tokens[i - 1]!
      if (prev.end > 0 && tok.start > 0) {
        const gap = tok.start - prev.end
        text += gap > 0 ? " ".repeat(gap) : ""
      } else {
        text += " "
      }
    }
    text += tok.text
  }
  return text
}
