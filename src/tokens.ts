/**
 * Token types and tokenizer for SQL parsing
 */

import { type Trie, TrieResult, inTrie, newTrie } from "./trie.js"

export enum TokenType {
  // Literals
  NUMBER = "NUMBER",
  STRING = "STRING",
  BIT_STRING = "BIT_STRING",
  HEX_STRING = "HEX_STRING",
  BYTE_STRING = "BYTE_STRING",
  NATIONAL_STRING = "NATIONAL_STRING",
  RAW_STRING = "RAW_STRING",

  // Identifiers
  VAR = "VAR",
  IDENTIFIER = "IDENTIFIER",
  PARAMETER = "PARAMETER",
  PLACEHOLDER = "PLACEHOLDER",

  // Keywords
  ABORT = "ABORT",
  ADD = "ADD",
  ALIAS = "ALIAS",
  ALL = "ALL",
  ALTER = "ALTER",
  ANALYZE = "ANALYZE",
  AND = "AND",
  ANTI = "ANTI",
  ANY = "ANY",
  APPLY = "APPLY",
  ARRAY = "ARRAY",
  ATTACH = "ATTACH",
  AS = "AS",
  ASC = "ASC",
  ASOF = "ASOF",
  AT = "AT", // Also operator
  AUTO_INCREMENT = "AUTO_INCREMENT",
  BEGIN = "BEGIN",
  BETWEEN = "BETWEEN",
  BOTH = "BOTH",
  BUCKET = "BUCKET",
  BY = "BY",
  CACHE = "CACHE",
  CALL = "CALL",
  CASE = "CASE",
  CAST = "CAST",
  CHARACTER_SET = "CHARACTER_SET",
  CLUSTER = "CLUSTER",
  CLUSTER_BY = "CLUSTER_BY",
  COLLATE = "COLLATE",
  COLUMN = "COLUMN",
  COMMENT = "COMMENT", // Also for block comments
  COMMIT = "COMMIT",
  CONNECT = "CONNECT",
  CONSTRAINT = "CONSTRAINT",
  COPY = "COPY",
  CREATE = "CREATE",
  CROSS = "CROSS",
  CUBE = "CUBE",
  CURRENT_DATE = "CURRENT_DATE",
  CURRENT_DATETIME = "CURRENT_DATETIME",
  DATE = "DATE",
  CURRENT_ROW = "CURRENT_ROW",
  CURRENT_TIME = "CURRENT_TIME",
  CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP",
  CURRENT_USER = "CURRENT_USER",
  CURRENT_ROLE = "CURRENT_ROLE",
  LOCALTIME = "LOCALTIME",
  LOCALTIMESTAMP = "LOCALTIMESTAMP",
  DATABASE = "DATABASE",
  DEFAULT = "DEFAULT",
  DELETE = "DELETE",
  DESC = "DESC",
  DETACH = "DETACH",
  DESCRIBE = "DESCRIBE",
  DISTINCT = "DISTINCT",
  DISTRIBUTE = "DISTRIBUTE",
  DISTRIBUTE_BY = "DISTRIBUTE_BY",
  DIV = "DIV",
  DROP = "DROP",
  ELSE = "ELSE",
  END = "END",
  ESCAPE = "ESCAPE",
  EXCEPT = "EXCEPT",
  EXECUTE = "EXECUTE",
  EXISTS = "EXISTS",
  EXPLAIN = "EXPLAIN",
  EXTRACT = "EXTRACT",
  FALSE = "FALSE",
  FETCH = "FETCH",
  FILTER = "FILTER",
  FINAL = "FINAL",
  FIRST = "FIRST",
  FOLLOWING = "FOLLOWING",
  FOR = "FOR",
  FOREIGN = "FOREIGN",
  FORMAT = "FORMAT",
  FROM = "FROM",
  FULL = "FULL",
  FUNCTION = "FUNCTION",
  GLOB = "GLOB",
  GLOBAL = "GLOBAL",
  GROUP = "GROUP",
  GROUP_BY = "GROUP_BY",
  GROUPING_SETS = "GROUPING_SETS",
  HAVING = "HAVING",
  HINT = "HINT",
  IF = "IF",
  IGNORE = "IGNORE",
  ILIKE = "ILIKE",
  IMMUTABLE = "IMMUTABLE",
  IN = "IN",
  INDEX = "INDEX",
  INNER = "INNER",
  INSERT = "INSERT",
  INTERSECT = "INTERSECT",
  INTERVAL = "INTERVAL",
  INTO = "INTO",
  IS = "IS",
  ISNULL = "ISNULL",
  JOIN = "JOIN",
  KEY = "KEY",
  LANGUAGE = "LANGUAGE",
  LAST = "LAST",
  LATERAL = "LATERAL",
  LAZY = "LAZY",
  LEADING = "LEADING",
  LEFT = "LEFT",
  LIKE = "LIKE",
  LIMIT = "LIMIT",
  LOAD = "LOAD",
  LOCAL = "LOCAL",
  LOCK = "LOCK",
  MAP = "MAP",
  MATCH = "MATCH",
  MATCH_CONDITION = "MATCH_CONDITION",
  MATCH_RECOGNIZE = "MATCH_RECOGNIZE",
  MATERIALIZED = "MATERIALIZED",
  MERGE = "MERGE",
  MOD = "MOD",
  NATURAL = "NATURAL",
  NEXT = "NEXT",
  NO = "NO",
  NOT = "NOT",
  NOTNULL = "NOTNULL",
  NULL = "NULL",
  OBJECT = "OBJECT",
  OFFSET = "OFFSET",
  ON = "ON",
  ONLY = "ONLY",
  OPTION = "OPTION",
  OPTIONS = "OPTIONS",
  OR = "OR",
  ORDER = "ORDER",
  ORDER_BY = "ORDER_BY",
  ORDINALITY = "ORDINALITY",
  OUTER = "OUTER",
  OUT = "OUT",
  OVER = "OVER",
  OVERLAPS = "OVERLAPS",
  OVERWRITE = "OVERWRITE",
  PARTITION = "PARTITION",
  PARTITION_BY = "PARTITION_BY",
  PERCENT = "PERCENT",
  PIVOT = "PIVOT",
  PRAGMA = "PRAGMA",
  PRECEDING = "PRECEDING",
  PRIMARY = "PRIMARY",
  PROCEDURE = "PROCEDURE",
  QUALIFY = "QUALIFY",
  QUOTE = "QUOTE",
  RANGE = "RANGE",
  RECURSIVE = "RECURSIVE",
  REFERENCES = "REFERENCES",
  REFRESH = "REFRESH",
  REPLACE = "REPLACE",
  RESPECT = "RESPECT",
  INSTALL = "INSTALL",
  FORCE = "FORCE",
  RETURN = "RETURN",
  RETURNING = "RETURNING",
  RIGHT = "RIGHT",
  RLIKE = "RLIKE",
  IRLIKE = "IRLIKE",
  ROLLBACK = "ROLLBACK",
  ROLLUP = "ROLLUP",
  ROW = "ROW",
  ROWS = "ROWS",
  SCHEMA = "SCHEMA",
  SEED = "SEED",
  SELECT = "SELECT",
  SEMI = "SEMI",
  SEPARATOR = "SEPARATOR",
  SERDE = "SERDE",
  SERDE_PROPERTIES = "SERDE_PROPERTIES",
  SESSION_USER = "SESSION_USER",
  SET = "SET",
  SETTINGS = "SETTINGS",
  SHOW = "SHOW",
  SIMILAR = "SIMILAR",
  SOME = "SOME",
  SORT = "SORT",
  SORT_BY = "SORT_BY",
  START = "START",
  STORAGE_INTEGRATION = "STORAGE_INTEGRATION",
  STRAIGHT_JOIN = "STRAIGHT_JOIN",
  STRUCT = "STRUCT",
  SUMMARIZE = "SUMMARIZE",
  TABLE = "TABLE",
  TABLESAMPLE = "TABLESAMPLE",
  TEMP = "TEMP",
  TEMPORARY = "TEMPORARY",
  THEN = "THEN",
  TIME = "TIME",
  TIMESTAMP = "TIMESTAMP",
  TIMESTAMPTZ = "TIMESTAMPTZ",
  TOP = "TOP",
  TRAILING = "TRAILING",
  TRANSACTION = "TRANSACTION",
  TRUE = "TRUE",
  TRUNCATE = "TRUNCATE",
  TRY_CAST = "TRY_CAST",
  TYPE = "TYPE",
  UNBOUNDED = "UNBOUNDED",
  UNCACHE = "UNCACHE",
  UNION = "UNION",
  UNIQUE = "UNIQUE",
  UNKNOWN = "UNKNOWN",
  UNNEST = "UNNEST",
  UNPIVOT = "UNPIVOT",
  UPDATE = "UPDATE",
  USE = "USE",
  USING = "USING",
  VALUE = "VALUE",
  VALUES = "VALUES",
  VIEW = "VIEW",
  VOLATILE = "VOLATILE",
  WHEN = "WHEN",
  WHERE = "WHERE",
  WINDOW = "WINDOW",
  WITH = "WITH",
  WITHIN = "WITHIN",
  WITHOUT = "WITHOUT",
  XOR = "XOR",
  ZONE = "ZONE",

  // Operators
  AMP = "AMP",
  ARROW = "ARROW",
  DARROW = "DARROW",
  FARROW = "FARROW",
  HASH_ARROW = "HASH_ARROW",
  DHASH_ARROW = "DHASH_ARROW",
  CARET = "CARET",
  COLON = "COLON",
  COLON_EQ = "COLON_EQ",
  COLONCOLON = "COLONCOLON",
  COMMA = "COMMA",
  CONCAT = "CONCAT",
  DAMP = "DAMP",
  DOT = "DOT",
  DPIPE = "DPIPE",
  DQMARK = "DQMARK",
  DSTAR = "DSTAR",
  EQ = "EQ",
  NULLSAFE_EQ = "NULLSAFE_EQ",
  GT = "GT",
  GTE = "GTE",
  LT = "LT",
  LTE = "LTE",
  LSHIFT = "LSHIFT",
  RSHIFT = "RSHIFT",
  MINUS = "MINUS",
  NEQ = "NEQ",
  PIPE = "PIPE",
  PLUS = "PLUS",
  QMARK = "QMARK",
  SLASH = "SLASH",
  STAR = "STAR",
  TILDE = "TILDE",
  HASH = "HASH",
  DOLLAR = "DOLLAR",

  // Postgres-specific operators
  DAT = "DAT", // @@ (text search match)
  AT_GT = "AT_GT", // @> (array contains all)
  LT_AT = "LT_AT", // <@ (array contained by)
  CARET_AT = "CARET_AT", // ^@ (starts with)
  PIPE_SLASH = "PIPE_SLASH", // |/ (square root)
  DPIPE_SLASH = "DPIPE_SLASH", // ||/ (cube root)
  QMARK_AMP = "QMARK_AMP", // ?& (JSON has all keys)
  QMARK_PIPE = "QMARK_PIPE", // ?| (JSON has any key)
  HASH_DASH = "HASH_DASH", // #- (JSON delete path)

  // Delimiters
  L_PAREN = "L_PAREN",
  R_PAREN = "R_PAREN",
  L_BRACKET = "L_BRACKET",
  R_BRACKET = "R_BRACKET",
  L_BRACE = "L_BRACE",
  R_BRACE = "R_BRACE",
  SEMICOLON = "SEMICOLON",

  // Comments (COMMENT already declared as keyword)
  BLOCK_COMMENT = "BLOCK_COMMENT",

  // Special
  BREAK = "BREAK",
  EOF = "EOF",
}

export class Token {
  constructor(
    public readonly tokenType: TokenType,
    public readonly text: string,
    public readonly line: number = 1,
    public readonly col: number = 0,
    public readonly start: number = 0,
    public readonly end: number = 0,
    public readonly comments: string[] = [],
  ) {}

  toString(): string {
    return `Token(${this.tokenType}, ${JSON.stringify(this.text)})`
  }
}

const KEYWORDS: Map<string, TokenType> = new Map([
  ["ABORT", TokenType.ABORT],
  ["ADD", TokenType.ADD],
  ["ALL", TokenType.ALL],
  ["ALTER", TokenType.ALTER],
  ["ANALYZE", TokenType.ANALYZE],
  ["AND", TokenType.AND],
  ["ANTI", TokenType.ANTI],
  ["ANY", TokenType.ANY],
  ["APPLY", TokenType.APPLY],
  ["ARRAY", TokenType.ARRAY],
  ["ATTACH", TokenType.ATTACH],
  ["AS", TokenType.AS],
  ["ASC", TokenType.ASC],
  ["ASOF", TokenType.ASOF],
  ["AT", TokenType.AT],
  ["AUTO_INCREMENT", TokenType.AUTO_INCREMENT],
  ["BEGIN", TokenType.BEGIN],
  ["BETWEEN", TokenType.BETWEEN],
  ["BOTH", TokenType.BOTH],
  ["BUCKET", TokenType.BUCKET],
  ["BY", TokenType.BY],
  ["CACHE", TokenType.CACHE],
  ["CALL", TokenType.CALL],
  ["CASE", TokenType.CASE],
  ["CAST", TokenType.CAST],
  ["CHARACTER", TokenType.CHARACTER_SET],
  ["CLUSTER", TokenType.CLUSTER],
  ["COLLATE", TokenType.COLLATE],
  ["COLUMN", TokenType.COLUMN],
  ["COMMENT", TokenType.COMMENT],
  ["COMMIT", TokenType.COMMIT],
  ["CONNECT", TokenType.CONNECT],
  ["CONSTRAINT", TokenType.CONSTRAINT],
  ["COPY", TokenType.COPY],
  ["CREATE", TokenType.CREATE],
  ["CROSS", TokenType.CROSS],
  ["CUBE", TokenType.CUBE],
  ["CURRENT_DATE", TokenType.CURRENT_DATE],
  ["CURRENT_DATETIME", TokenType.CURRENT_DATETIME],
  ["CURRENT_TIME", TokenType.CURRENT_TIME],
  ["CURRENT_TIMESTAMP", TokenType.CURRENT_TIMESTAMP],
  ["CURRENT_USER", TokenType.CURRENT_USER],
  ["CURRENT_ROLE", TokenType.CURRENT_ROLE],
  ["LOCALTIME", TokenType.LOCALTIME],
  ["LOCALTIMESTAMP", TokenType.LOCALTIMESTAMP],
  ["DATE", TokenType.DATE],
  ["DATABASE", TokenType.DATABASE],
  ["DEFAULT", TokenType.DEFAULT],
  ["DELETE", TokenType.DELETE],
  ["DESC", TokenType.DESC],
  ["DETACH", TokenType.DETACH],
  ["DESCRIBE", TokenType.DESCRIBE],
  ["DISTINCT", TokenType.DISTINCT],
  ["DISTRIBUTE", TokenType.DISTRIBUTE],
  ["DIV", TokenType.DIV],
  ["DROP", TokenType.DROP],
  ["ELSE", TokenType.ELSE],
  ["END", TokenType.END],
  ["ESCAPE", TokenType.ESCAPE],
  ["EXCEPT", TokenType.EXCEPT],
  ["EXECUTE", TokenType.EXECUTE],
  ["EXISTS", TokenType.EXISTS],
  ["EXPLAIN", TokenType.EXPLAIN],
  ["EXTRACT", TokenType.EXTRACT],
  ["FALSE", TokenType.FALSE],
  ["FETCH", TokenType.FETCH],
  ["FILTER", TokenType.FILTER],
  ["FINAL", TokenType.FINAL],
  ["FIRST", TokenType.FIRST],
  ["FOLLOWING", TokenType.FOLLOWING],
  ["FOR", TokenType.FOR],
  ["FORCE", TokenType.FORCE],
  ["FOREIGN", TokenType.FOREIGN],
  ["FORMAT", TokenType.FORMAT],
  ["FROM", TokenType.FROM],
  ["FULL", TokenType.FULL],
  ["FUNCTION", TokenType.FUNCTION],
  ["GLOB", TokenType.GLOB],
  ["GLOBAL", TokenType.GLOBAL],
  ["GROUP", TokenType.GROUP],
  ["GROUPING", TokenType.GROUPING_SETS],
  ["HAVING", TokenType.HAVING],
  ["HINT", TokenType.HINT],
  ["IF", TokenType.IF],
  ["IGNORE", TokenType.IGNORE],
  ["ILIKE", TokenType.ILIKE],
  ["IMMUTABLE", TokenType.IMMUTABLE],
  ["IN", TokenType.IN],
  ["INDEX", TokenType.INDEX],
  ["INNER", TokenType.INNER],
  ["INSERT", TokenType.INSERT],
  ["INSTALL", TokenType.INSTALL],
  ["INTERSECT", TokenType.INTERSECT],
  ["INTERVAL", TokenType.INTERVAL],
  ["INTO", TokenType.INTO],
  ["IS", TokenType.IS],
  ["ISNULL", TokenType.ISNULL],
  ["JOIN", TokenType.JOIN],
  ["KEY", TokenType.KEY],
  ["LANGUAGE", TokenType.LANGUAGE],
  ["LAST", TokenType.LAST],
  ["LATERAL", TokenType.LATERAL],
  ["LAZY", TokenType.LAZY],
  ["LEADING", TokenType.LEADING],
  ["LEFT", TokenType.LEFT],
  ["LIKE", TokenType.LIKE],
  ["LIMIT", TokenType.LIMIT],
  ["LOAD", TokenType.LOAD],
  ["LOCAL", TokenType.LOCAL],
  ["LOCK", TokenType.LOCK],
  ["MAP", TokenType.MAP],
  ["MATCH", TokenType.MATCH],
  ["MATERIALIZED", TokenType.MATERIALIZED],
  ["MERGE", TokenType.MERGE],
  ["MOD", TokenType.MOD],
  ["NATURAL", TokenType.NATURAL],
  ["NEXT", TokenType.NEXT],
  ["NO", TokenType.NO],
  ["NOT", TokenType.NOT],
  ["NOTNULL", TokenType.NOTNULL],
  ["NULL", TokenType.NULL],
  ["OBJECT", TokenType.OBJECT],
  ["OFFSET", TokenType.OFFSET],
  ["ON", TokenType.ON],
  ["ONLY", TokenType.ONLY],
  ["OPTION", TokenType.OPTION],
  ["OPTIONS", TokenType.OPTIONS],
  ["OR", TokenType.OR],
  ["ORDER", TokenType.ORDER],
  ["ORDINALITY", TokenType.ORDINALITY],
  ["OUTER", TokenType.OUTER],
  ["OUT", TokenType.OUT],
  ["OVER", TokenType.OVER],
  ["OVERLAPS", TokenType.OVERLAPS],
  ["OVERWRITE", TokenType.OVERWRITE],
  ["PARTITION", TokenType.PARTITION],
  ["PERCENT", TokenType.PERCENT],
  ["PIVOT", TokenType.PIVOT],
  ["PRAGMA", TokenType.PRAGMA],
  ["PRECEDING", TokenType.PRECEDING],
  ["PRIMARY", TokenType.PRIMARY],
  ["PROCEDURE", TokenType.PROCEDURE],
  ["QUALIFY", TokenType.QUALIFY],
  ["QUOTE", TokenType.QUOTE],
  ["RANGE", TokenType.RANGE],
  ["RECURSIVE", TokenType.RECURSIVE],
  ["REFERENCES", TokenType.REFERENCES],
  ["REFRESH", TokenType.REFRESH],
  ["REPLACE", TokenType.REPLACE],
  ["RESPECT", TokenType.RESPECT],
  ["RETURN", TokenType.RETURN],
  ["RETURNING", TokenType.RETURNING],
  ["RIGHT", TokenType.RIGHT],
  ["REGEXP", TokenType.RLIKE],
  ["RLIKE", TokenType.RLIKE],
  ["ROLLBACK", TokenType.ROLLBACK],
  ["ROLLUP", TokenType.ROLLUP],
  ["ROW", TokenType.ROW],
  ["ROWS", TokenType.ROWS],
  ["SCHEMA", TokenType.SCHEMA],
  ["SEED", TokenType.SEED],
  ["SELECT", TokenType.SELECT],
  ["SEMI", TokenType.SEMI],
  ["SEPARATOR", TokenType.SEPARATOR],
  ["SERDE", TokenType.SERDE],
  ["SESSION_USER", TokenType.SESSION_USER],
  ["SET", TokenType.SET],
  ["SETTINGS", TokenType.SETTINGS],
  ["SHOW", TokenType.SHOW],
  ["SIMILAR", TokenType.SIMILAR],
  ["SOME", TokenType.SOME],
  ["SORT", TokenType.SORT],
  ["START", TokenType.START],
  ["STRAIGHT_JOIN", TokenType.STRAIGHT_JOIN],
  ["STRUCT", TokenType.STRUCT],
  ["TABLE", TokenType.TABLE],
  ["TABLESAMPLE", TokenType.TABLESAMPLE],
  ["TEMP", TokenType.TEMP],
  ["TEMPORARY", TokenType.TEMPORARY],
  ["THEN", TokenType.THEN],
  ["TIME", TokenType.TIME],
  ["TIMESTAMP", TokenType.TIMESTAMP],
  ["TIMESTAMPTZ", TokenType.TIMESTAMPTZ],
  ["TOP", TokenType.TOP],
  ["TRAILING", TokenType.TRAILING],
  ["TRANSACTION", TokenType.TRANSACTION],
  ["TRUE", TokenType.TRUE],
  ["TRUNCATE", TokenType.TRUNCATE],
  ["TRY_CAST", TokenType.TRY_CAST],
  ["TYPE", TokenType.TYPE],
  ["UNBOUNDED", TokenType.UNBOUNDED],
  ["UNCACHE", TokenType.UNCACHE],
  ["UNION", TokenType.UNION],
  ["UNIQUE", TokenType.UNIQUE],
  ["UNKNOWN", TokenType.UNKNOWN],
  ["UNNEST", TokenType.UNNEST],
  ["UNPIVOT", TokenType.UNPIVOT],
  ["UPDATE", TokenType.UPDATE],
  ["USE", TokenType.USE],
  ["USING", TokenType.USING],
  ["VALUE", TokenType.VALUE],
  ["VALUES", TokenType.VALUES],
  ["VIEW", TokenType.VIEW],
  ["VOLATILE", TokenType.VOLATILE],
  ["WHEN", TokenType.WHEN],
  ["WHERE", TokenType.WHERE],
  ["WINDOW", TokenType.WINDOW],
  ["WITH", TokenType.WITH],
  ["WITHIN", TokenType.WITHIN],
  ["WITHOUT", TokenType.WITHOUT],
  ["XOR", TokenType.XOR],
  ["ZONE", TokenType.ZONE],
])

const MULTI_WORD_KEYWORDS: Map<string, TokenType> = new Map([
  ["GROUP BY", TokenType.GROUP_BY],
  ["ORDER BY", TokenType.ORDER_BY],
  ["PARTITION BY", TokenType.PARTITION_BY],
  ["CLUSTER BY", TokenType.CLUSTER_BY],
  ["DISTRIBUTE BY", TokenType.DISTRIBUTE_BY],
  ["SORT BY", TokenType.SORT_BY],
])

const SINGLE_TOKENS: Map<string, TokenType> = new Map([
  ["(", TokenType.L_PAREN],
  [")", TokenType.R_PAREN],
  ["[", TokenType.L_BRACKET],
  ["]", TokenType.R_BRACKET],
  ["{", TokenType.L_BRACE],
  ["}", TokenType.R_BRACE],
  [",", TokenType.COMMA],
  [";", TokenType.SEMICOLON],
  [".", TokenType.DOT],
  ["+", TokenType.PLUS],
  ["-", TokenType.MINUS],
  ["*", TokenType.STAR],
  ["/", TokenType.SLASH],
  ["%", TokenType.PERCENT],
  ["&", TokenType.AMP],
  ["|", TokenType.PIPE],
  ["^", TokenType.CARET],
  // ["~", TokenType.TILDE], - handled in multi-char operator section (RLIKE, IRLIKE, LIKE, ILIKE)
  ["<", TokenType.LT],
  [">", TokenType.GT],
  // "=" handled in multi-char section (for =>)
  ["?", TokenType.QMARK],
  [":", TokenType.COLON],
  ["@", TokenType.AT],
  ["#", TokenType.HASH],
  ["$", TokenType.DOLLAR],
])

// Format string configuration: [prefix, end_quote, token_type]
// e.g., ["N'", "'", TokenType.NATIONAL_STRING] or ["0x", "", TokenType.HEX_STRING]
export type FormatStringConfig = [string, string, TokenType]

export interface TokenizerOptions {
  dialect?: string
  identifiers?: string[] // e.g., ['"', '`', '[']
  quotes?: string[] // e.g., ["'", '"']
  keywords?: Map<string, TokenType>
  numbersCanBeUnderscoreSeparated?: boolean
  bitStrings?: Array<[string, string]> // e.g., [["b'", "'"], ["0b", ""]]
  hexStrings?: Array<[string, string]> // e.g., [["x'", "'"], ["0x", ""]]
  stringEscapes?: string[] // e.g., ["'"] for base, ["\\"] for Hive
  unescapedSequences?: Record<string, string> // e.g., {"\\n": "\n", ...}
}

export class Tokenizer {
  private sql = ""
  private pos = 0
  private line = 1
  private col = 0
  private tokens: Token[] = []
  private comments: string[] = []
  private keywords: Map<string, TokenType>
  private singleTokens: Map<string, TokenType>
  private keywordTrie: Trie
  private identifierChars: Set<string>
  private quoteChars: Set<string>
  private numbersCanBeUnderscoreSeparated: boolean
  private formatStrings: Map<string, [string, TokenType]> // prefix -> [end_quote, token_type]
  private hasBitStrings: boolean
  private hasHexStrings: boolean
  private stringEscapes: Set<string>
  private unescapedSequences: Record<string, string>

  constructor(options: TokenizerOptions = {}) {
    this.keywords = new Map(KEYWORDS)
    if (options.keywords) {
      for (const [k, v] of options.keywords) {
        this.keywords.set(k, v)
      }
    }
    this.singleTokens = new Map(SINGLE_TOKENS)
    this.keywordTrie = newTrie(
      [...MULTI_WORD_KEYWORDS.keys()].map((k) => k.toUpperCase()),
    )
    this.identifierChars = new Set(options.identifiers ?? ['"', "`"])
    this.quoteChars = new Set(options.quotes ?? ["'"])
    this.numbersCanBeUnderscoreSeparated =
      options.numbersCanBeUnderscoreSeparated ?? false

    // Build format strings map: prefix -> [end_quote, token_type]
    this.formatStrings = new Map()

    // Add N'...' for all quote chars (NATIONAL_STRING)
    for (const quote of this.quoteChars) {
      for (const prefix of ["n", "N"]) {
        this.formatStrings.set(prefix + quote, [
          quote,
          TokenType.NATIONAL_STRING,
        ])
      }
    }

    // Add bit strings
    if (options.bitStrings) {
      for (const [prefix, endQuote] of options.bitStrings) {
        this.formatStrings.set(prefix, [endQuote, TokenType.BIT_STRING])
      }
    }

    // Add hex strings
    if (options.hexStrings) {
      for (const [prefix, endQuote] of options.hexStrings) {
        this.formatStrings.set(prefix, [endQuote, TokenType.HEX_STRING])
      }
    }

    this.hasBitStrings = (options.bitStrings?.length ?? 0) > 0
    this.hasHexStrings = (options.hexStrings?.length ?? 0) > 0
    this.stringEscapes = new Set(options.stringEscapes ?? ["'"])
    this.unescapedSequences = options.unescapedSequences ?? {}
  }

  tokenize(sql: string): Token[] {
    this.sql = sql
    this.pos = 0
    this.line = 1
    this.col = 0
    this.tokens = []
    this.comments = []

    while (this.pos < this.sql.length) {
      this.scanToken()
    }

    // Attach trailing comments to the last real token (Python behavior)
    const lastToken = this.tokens[this.tokens.length - 1]
    if (this.comments.length > 0 && lastToken) {
      lastToken.comments.push(...this.comments)
      this.comments = []
    }

    this.addToken(TokenType.EOF, "")
    return this.tokens
  }

  private get current(): string {
    return this.sql[this.pos] ?? ""
  }

  private get peek(): string {
    return this.sql[this.pos + 1] ?? ""
  }

  private advance(): string {
    const ch = this.current
    this.pos++
    if (ch === "\n") {
      this.line++
      this.col = 0
    } else {
      this.col++
    }
    return ch
  }

  private addToken(type: TokenType, text: string, start?: number): void {
    // Comments before ; go to the preceding token (Python behavior)
    if (
      type === TokenType.SEMICOLON &&
      this.comments.length > 0 &&
      this.tokens.length > 0
    ) {
      const prev = this.tokens[this.tokens.length - 1]
      if (prev) {
        prev.comments.push(...this.comments)
        this.comments = []
      }
    }

    const token = new Token(
      type,
      text,
      this.line,
      this.col,
      start ?? this.pos - text.length,
      this.pos,
      this.comments,
    )
    this.tokens.push(token)
    this.comments = []
  }

  private scanToken(): void {
    this.skipWhitespace()

    if (this.pos >= this.sql.length) {
      return
    }

    const ch = this.current
    const start = this.pos

    // Single-line comment
    if (ch === "-" && this.peek === "-") {
      this.scanLineComment()
      return
    }

    // Block comment
    if (ch === "/" && this.peek === "*") {
      this.scanBlockComment()
      return
    }

    // String - check configurable quotes (also checks format strings like N'...')
    if (this.quoteChars.has(ch)) {
      this.scanString(ch, TokenType.STRING)
      return
    }

    // Quoted identifier - check configurable delimiters
    if (this.identifierChars.has(ch)) {
      if (ch === "[") {
        this.scanBracketIdentifier()
      } else {
        this.scanQuotedIdentifier(ch)
      }
      return
    }

    // Number
    if (this.isDigit(ch) || (ch === "." && this.isDigit(this.peek))) {
      this.scanNumber()
      return
    }

    // Identifier or keyword
    if (this.isAlpha(ch) || ch === "_") {
      this.scanIdentifier()
      return
    }

    // Multi-character operators
    if (ch === "<") {
      this.advance()
      if (this.current === "=") {
        this.advance()
        this.addToken(TokenType.LTE, "<=", start)
      } else if (this.current === ">") {
        this.advance()
        this.addToken(TokenType.NEQ, "<>", start)
      } else if (this.current === "<") {
        this.advance()
        this.addToken(TokenType.LSHIFT, "<<", start)
      } else if (this.current === "@") {
        // <@ (array contained by) - Postgres
        this.advance()
        this.addToken(TokenType.LT_AT, "<@", start)
      } else {
        this.addToken(TokenType.LT, "<", start)
      }
      return
    }

    if (ch === ">") {
      this.advance()
      if (this.current === "=") {
        this.advance()
        this.addToken(TokenType.GTE, ">=", start)
      } else if (this.current === ">") {
        this.advance()
        this.addToken(TokenType.RSHIFT, ">>", start)
      } else {
        this.addToken(TokenType.GT, ">", start)
      }
      return
    }

    if (ch === "!") {
      this.advance()
      if (this.current === "=") {
        this.advance()
        this.addToken(TokenType.NEQ, "!=", start)
      } else {
        // In SQL, ! alone is typically NOT (e.g., Postgres !~ for negated regex)
        this.addToken(TokenType.NOT, "!", start)
      }
      return
    }

    if (ch === "|") {
      this.advance()
      const ch2 = this.current
      if (ch2 === "|") {
        this.advance()
        const ch3 = this.current
        // Check for ||/ (cube root) - Postgres
        if (ch3 === "/") {
          this.advance()
          this.addToken(TokenType.DPIPE_SLASH, "||/", start)
        } else {
          this.addToken(TokenType.DPIPE, "||", start)
        }
      } else if (ch2 === "/") {
        // |/ (square root) - Postgres
        this.advance()
        this.addToken(TokenType.PIPE_SLASH, "|/", start)
      } else {
        this.addToken(TokenType.PIPE, "|", start)
      }
      return
    }

    if (ch === "&") {
      this.advance()
      if (this.current === "&") {
        this.advance()
        this.addToken(TokenType.DAMP, "&&", start)
      } else {
        this.addToken(TokenType.AMP, "&", start)
      }
      return
    }

    if (ch === ":") {
      this.advance()
      if (this.current === ":") {
        this.advance()
        this.addToken(TokenType.COLONCOLON, "::", start)
      } else if (this.current === "=") {
        this.advance()
        this.addToken(TokenType.COLON_EQ, ":=", start)
      } else {
        this.addToken(TokenType.COLON, ":", start)
      }
      return
    }

    if (ch === "*") {
      this.advance()
      if (this.current === "*") {
        this.advance()
        this.addToken(TokenType.DSTAR, "**", start)
      } else {
        this.addToken(TokenType.STAR, "*", start)
      }
      return
    }

    if (ch === "=") {
      this.advance()
      if (this.current === ">") {
        this.advance()
        this.addToken(TokenType.FARROW, "=>", start)
      } else {
        this.addToken(TokenType.EQ, "=", start)
      }
      return
    }

    if (ch === "-") {
      this.advance()
      if (this.current === ">") {
        this.advance()
        if (this.current === ">") {
          this.advance()
          this.addToken(TokenType.DARROW, "->>", start)
        } else {
          this.addToken(TokenType.ARROW, "->", start)
        }
      } else {
        this.addToken(TokenType.MINUS, "-", start)
      }
      return
    }

    // Dollar-quoted strings: $$...$$ or $tag$...$tag$ (Postgres)
    if (ch === "$") {
      if (
        this.peek === "$" ||
        this.isAlpha(this.peek) ||
        this.peek === "_" ||
        this.peek.charCodeAt(0) > 127
      ) {
        this.scanDollarQuotedString()
        return
      }
    }

    // Tilde-based operators: ~~*, ~~, ~*, ~
    // Used for regex and LIKE operators in Postgres/DuckDB
    if (ch === "~") {
      this.advance()
      const ch2 = this.current
      if (ch2 === "~") {
        this.advance()
        const ch3 = this.current
        if (ch3 === "~") {
          this.advance()
          this.addToken(TokenType.GLOB, "~~~", start)
        } else if (ch3 === "*") {
          this.advance()
          this.addToken(TokenType.ILIKE, "~~*", start)
        } else {
          this.addToken(TokenType.LIKE, "~~", start)
        }
      } else if (ch2 === "*") {
        this.advance()
        this.addToken(TokenType.IRLIKE, "~*", start)
      } else {
        this.addToken(TokenType.TILDE, "~", start)
      }
      return
    }

    // Postgres-specific operators: @@, @>
    if (ch === "@") {
      this.advance()
      const ch2 = this.current
      if (ch2 === "@") {
        this.advance()
        this.addToken(TokenType.DAT, "@@", start)
      } else if (ch2 === ">") {
        this.advance()
        this.addToken(TokenType.AT_GT, "@>", start)
      } else {
        this.addToken(TokenType.AT, "@", start)
      }
      return
    }

    // Postgres-specific operators: ?&, ?|
    if (ch === "?") {
      this.advance()
      const ch2 = this.current
      if (ch2 === "&") {
        this.advance()
        this.addToken(TokenType.QMARK_AMP, "?&", start)
      } else if (ch2 === "|") {
        this.advance()
        this.addToken(TokenType.QMARK_PIPE, "?|", start)
      } else {
        this.addToken(TokenType.QMARK, "?", start)
      }
      return
    }

    // Postgres-specific operator: #-
    if (ch === "#") {
      this.advance()
      const ch2 = this.current
      if (ch2 === "-") {
        this.advance()
        this.addToken(TokenType.HASH_DASH, "#-", start)
      } else if (ch2 === ">") {
        this.advance()
        this.addToken(TokenType.HASH_ARROW, "#>", start)
      } else {
        this.addToken(TokenType.HASH, "#", start)
      }
      return
    }

    // Integer division: // (DuckDB)
    if (ch === "/" && this.peek === "/") {
      this.advance()
      this.advance()
      this.addToken(TokenType.DIV, "//", start)
      return
    }

    // ^@ (starts with)
    if (ch === "^" && this.peek === "@") {
      this.advance()
      this.advance()
      this.addToken(TokenType.CARET_AT, "^@", start)
      return
    }

    // Single-character tokens
    const singleType = this.singleTokens.get(ch)
    if (singleType) {
      this.advance()
      this.addToken(singleType, ch, start)
      return
    }

    throw new Error(
      `Unexpected character '${ch}' at line ${this.line}, col ${this.col}`,
    )
  }

  private skipWhitespace(): void {
    while (this.pos < this.sql.length && /\s/.test(this.current)) {
      this.advance()
    }
  }

  private scanLineComment(): void {
    this.advance() // -
    this.advance() // -

    const start = this.pos
    while (this.pos < this.sql.length && this.current !== "\n") {
      this.advance()
    }

    this.comments.push(this.sql.slice(start, this.pos))
  }

  private scanBlockComment(): void {
    this.advance() // /
    this.advance() // *

    const start = this.pos
    let depth = 1

    while (this.pos < this.sql.length && depth > 0) {
      if (this.current === "/" && this.peek === "*") {
        depth++
        this.advance()
        this.advance()
      } else if (this.current === "*" && this.peek === "/") {
        depth--
        this.advance()
        this.advance()
      } else {
        this.advance()
      }
    }

    if (depth > 0) {
      throw new Error(`Unterminated block comment at line ${this.line}`)
    }

    this.comments.push(this.sql.slice(start, this.pos - 2))
  }

  private scanString(
    quote: string,
    tokenType: TokenType = TokenType.STRING,
    endQuote?: string,
  ): void {
    const start = this.pos
    this.advance() // Opening quote

    const delimiter = endQuote ?? quote
    const escapes = this.stringEscapes
    const hasUnescapedSeqs = Object.keys(this.unescapedSequences).length > 0
    let text = ""

    while (this.pos < this.sql.length) {
      // Check UNESCAPED_SEQUENCES first (e.g., \n → newline)
      if (hasUnescapedSeqs && escapes.has(this.current) && this.peek) {
        const seq = this.current + this.peek
        const unescaped = this.unescapedSequences[seq]
        if (unescaped !== undefined) {
          text += unescaped
          this.advance()
          this.advance()
          continue
        }
      }

      // Escape char handling: escape + delimiter → delimiter char
      if (
        escapes.has(this.current) &&
        (this.peek === delimiter || escapes.has(this.peek))
      ) {
        if (this.current === this.peek && this.quoteChars.has(this.current)) {
          // Quote doubling (e.g., '' → ')
          text += this.current
        } else if (this.peek === delimiter) {
          // Escape + delimiter (e.g., \' → ')
          text += this.peek
        } else {
          // Escape + escape (e.g., \\ when not in UNESCAPED_SEQUENCES)
          text += this.current + this.peek
        }
        this.advance()
        this.advance()
        continue
      }

      // Check for delimiter (end of string)
      if (this.current === delimiter) {
        break
      }

      // Regular character
      text += this.current
      this.advance()
    }

    if (this.current !== delimiter) {
      throw new Error(`Unterminated string at line ${this.line}`)
    }

    this.advance() // Closing quote

    this.addToken(tokenType, text, start)
  }

  // Scan 0b101010 or 0B101010 (binary literal)
  private scanBitString(): void {
    const start = this.pos
    this.advance() // Skip '0'
    this.advance() // Skip 'b' or 'B'

    while (this.current === "0" || this.current === "1") {
      this.advance()
    }

    // Extract just the binary digits (without 0b prefix)
    const value = this.sql.slice(start + 2, this.pos)
    this.addToken(TokenType.BIT_STRING, value, start)
  }

  // Scan 0xDEADBEEF or 0XDEADBEEF (hex literal)
  private scanHexString(): void {
    const start = this.pos
    this.advance() // Skip '0'
    this.advance() // Skip 'x' or 'X'

    while (this.isHexDigit(this.current)) {
      this.advance()
    }

    // Extract just the hex digits (without 0x prefix)
    const value = this.sql.slice(start + 2, this.pos)

    // Validate it's a valid hex number
    if (value.length === 0 || !value.match(/^[0-9a-fA-F]+$/)) {
      // Invalid hex - treat as NUMBER + VAR
      this.pos = start + 1
      this.addToken(TokenType.NUMBER, "0", start)
      return
    }

    this.addToken(TokenType.HEX_STRING, value, start)
  }

  private scanNumber(): void {
    const start = this.pos

    // Handle 0b/0x prefix if dialect has bit/hex strings enabled
    if (this.current === "0") {
      const next = this.peek
      if ((next === "b" || next === "B") && this.hasBitStrings) {
        this.scanBitString()
        return
      }
      if ((next === "x" || next === "X") && this.hasHexStrings) {
        this.scanHexString()
        return
      }
    }

    // Integer part
    while (
      this.isDigit(this.current) ||
      (this.current === "_" && this.numbersCanBeUnderscoreSeparated)
    ) {
      this.advance()
    }

    // Decimal part
    if (this.current === "." && this.isDigit(this.peek)) {
      this.advance() // .
      while (this.isDigit(this.current)) {
        this.advance()
      }
    }

    // Exponent
    const ch = this.current
    if (ch === "e" || ch === "E") {
      this.advance()
      const sign = this.current
      if (sign === "+" || sign === "-") {
        this.advance()
      }
      while (
        this.isDigit(this.current) ||
        (this.current === "_" && this.numbersCanBeUnderscoreSeparated)
      ) {
        this.advance()
      }
    }

    const text = this.sql.slice(start, this.pos)
    this.addToken(TokenType.NUMBER, text, start)
  }

  private scanIdentifier(): void {
    const start = this.pos

    // Check for escape string prefix: e'...' or E'...'
    const ch = this.current
    if ((ch === "e" || ch === "E") && this.peek === "'") {
      this.scanEscapeString()
      return
    }

    // Check for format strings: N'...', B'...', X'...'
    // Need to check 1-char or 2-char prefixes
    const nextChar = this.peek
    if (this.quoteChars.has(nextChar)) {
      // Single char + quote: check N'
      const prefix = ch + nextChar
      const formatEntry = this.formatStrings.get(prefix)
      if (formatEntry) {
        const [endQuote, tokenType] = formatEntry
        this.advance() // skip prefix char
        this.scanString(nextChar, tokenType, endQuote)
        return
      }
    }

    while (this.isAlphaNumeric(this.current) || this.current === "_") {
      this.advance()
    }

    const text = this.sql.slice(start, this.pos)
    const upper = text.toUpperCase()

    if (this.tryMultiWordKeyword(upper, start)) {
      return
    }

    const keywordType = this.keywords.get(upper)
    if (keywordType) {
      this.addToken(keywordType, text, start)
    } else {
      this.addToken(TokenType.VAR, text, start)
    }
  }

  private tryMultiWordKeyword(firstWord: string, start: number): boolean {
    const [result] = inTrie(this.keywordTrie, firstWord)
    if (result === TrieResult.FAILED) {
      return false
    }

    const savedPos = this.pos
    const savedLine = this.line
    const savedCol = this.col

    let accumulated = firstWord

    while (true) {
      const wsStart = this.pos
      while (this.pos < this.sql.length && /[ \t]/.test(this.current)) {
        this.advance()
      }

      if (!this.isAlpha(this.current) && this.current !== "_") {
        this.pos = wsStart
        this.line = savedLine
        this.col = savedCol + (wsStart - savedPos)
        break
      }

      const wordStart = this.pos
      while (this.isAlphaNumeric(this.current) || this.current === "_") {
        this.advance()
      }
      const nextWord = this.sql.slice(wordStart, this.pos).toUpperCase()

      const candidate = `${accumulated} ${nextWord}`
      const [candidateResult] = inTrie(this.keywordTrie, candidate)

      if (candidateResult === TrieResult.EXISTS) {
        const kwType = MULTI_WORD_KEYWORDS.get(candidate)
        if (kwType) {
          const fullText = this.sql.slice(start, this.pos)
          this.addToken(kwType, fullText, start)
          return true
        }
      } else if (candidateResult === TrieResult.PREFIX) {
        accumulated = candidate
        continue
      }

      break
    }

    this.pos = savedPos
    this.line = savedLine
    this.col = savedCol
    return false
  }

  private scanQuotedIdentifier(quote: string): void {
    const start = this.pos
    this.advance() // Opening quote

    while (this.pos < this.sql.length && this.current !== quote) {
      if (this.current === quote && this.peek === quote) {
        this.advance()
      }
      this.advance()
    }

    if (this.current !== quote) {
      throw new Error(`Unterminated identifier at line ${this.line}`)
    }

    this.advance() // Closing quote

    const text = this.sql.slice(start, this.pos)
    this.addToken(TokenType.IDENTIFIER, text, start)
  }

  private scanBracketIdentifier(): void {
    const start = this.pos
    this.advance() // Opening [

    while (this.pos < this.sql.length) {
      if (this.current === "]") {
        if (this.peek === "]") {
          this.advance()
          this.advance()
        } else {
          break
        }
      } else {
        this.advance()
      }
    }

    if (this.current !== "]") {
      throw new Error(`Unterminated identifier at line ${this.line}`)
    }

    this.advance() // Closing ]

    const text = this.sql.slice(start, this.pos)
    this.addToken(TokenType.IDENTIFIER, text, start)
  }

  // Postgres escape strings: e'...' or E'...'
  // Handles escape sequences like \n, \t, \\, \'
  // Decodes escape sequences so AST contains actual characters
  private scanEscapeString(): void {
    const start = this.pos
    this.advance() // Skip 'e' or 'E'
    this.advance() // Skip opening quote

    let content = ""
    while (this.pos < this.sql.length) {
      if (this.current === "'") {
        if (this.peek === "'") {
          // Escaped quote '' -> single quote
          content += "'"
          this.advance()
          this.advance()
        } else {
          break
        }
      } else if (this.current === "\\") {
        // Escape sequence - decode it
        this.advance() // Skip backslash
        const escaped: string = this.current
        this.advance() // Skip escaped char
        if (escaped === "n") {
          content += "\n"
        } else if (escaped === "t") {
          content += "\t"
        } else if (escaped === "r") {
          content += "\r"
        } else if (escaped === "'") {
          content += "'"
        } else if (escaped === "\\") {
          content += "\\"
        } else {
          // Unknown escape - keep backslash and char
          content += "\\" + escaped
        }
      } else {
        content += this.current
        this.advance()
      }
    }

    if (this.current !== "'") {
      throw new Error(`Unterminated escape string at line ${this.line}`)
    }

    this.advance() // Closing quote

    this.addToken(TokenType.BYTE_STRING, content, start)
  }

  // Postgres dollar-quoted strings: $$...$$ or $tag$...$tag$
  private scanDollarQuotedString(): void {
    const start = this.pos
    this.advance() // Skip first $

    // Extract tag (empty for $$, or identifier for $tag$)
    let tag = ""
    if (this.current !== "$") {
      const tagStart = this.pos
      while (
        this.pos < this.sql.length &&
        (this.isAlphaNumeric(this.current) ||
          this.current === "_" ||
          this.current.charCodeAt(0) > 127)
      ) {
        this.advance()
      }
      tag = this.sql.slice(tagStart, this.pos)
    }

    if (this.current !== "$") {
      // Not a valid dollar quote - fall back to DOLLAR token
      this.pos = start
      this.advance()
      this.addToken(TokenType.DOLLAR, "$", start)
      return
    }

    this.advance() // Skip closing $ of opening delimiter

    // Build closing delimiter
    const closeDelim = `$${tag}$`

    // Find closing delimiter
    while (this.pos < this.sql.length) {
      if (this.current === "$") {
        // Check if this is the closing delimiter
        const remaining = this.sql.slice(this.pos, this.pos + closeDelim.length)
        if (remaining === closeDelim) {
          break
        }
      }
      this.advance()
    }

    if (this.pos >= this.sql.length) {
      throw new Error(`Unterminated dollar-quoted string at line ${this.line}`)
    }

    // Extract content between opening and closing delimiters
    const contentStart = start + closeDelim.length
    const content = this.sql.slice(contentStart, this.pos)

    // Skip closing delimiter
    for (let i = 0; i < closeDelim.length; i++) {
      this.advance()
    }

    // Store inner content only (no quotes) - generator will add them
    this.addToken(TokenType.STRING, content, start)
  }

  private isDigit(ch: string): boolean {
    return ch >= "0" && ch <= "9"
  }

  private isHexDigit(ch: string): boolean {
    return (
      this.isDigit(ch) || (ch >= "a" && ch <= "f") || (ch >= "A" && ch <= "F")
    )
  }

  private isAlpha(ch: string): boolean {
    return (ch >= "a" && ch <= "z") || (ch >= "A" && ch <= "Z")
  }

  private isAlphaNumeric(ch: string): boolean {
    return this.isAlpha(ch) || this.isDigit(ch)
  }
}
