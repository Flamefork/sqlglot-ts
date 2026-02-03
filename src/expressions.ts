/**
 * Expression AST node classes for SQL parsing
 */

// Re-export base types
export {
  Expression,
  type ArgValue,
  type Args,
  type ExpressionClass,
  type ExpressionConstructor,
  maybeParse,
  convert,
  _applyBuilder,
  _applyListBuilder,
  _applyChildListBuilder,
  _applyConjunctionBuilder,
} from "./expression-base.js"
import {
  type Args,
  Expression,
  type ExpressionClass,
  type ExpressionConstructor,
  _applyChildListBuilder,
  maybeParse,
  setAliasFactory,
  setAndConstructor,
  setConvertCtors,
} from "./expression-base.js"

// Re-export all generated classes
export * from "./expressions.generated.js"

// Import specific classes needed for type definitions and helpers
import {
  Alias,
  And,
  Anonymous,
  Boolean as BooleanExpr,
  CTE,
  Cast,
  Column,
  Except,
  Func,
  GENERATED_CLASSES,
  DataType as GeneratedDataType,
  Extract as GeneratedExtract,
  Identifier,
  Intersect,
  Literal,
  Not,
  Null,
  Or,
  Paren,
  Select,
  Subquery,
  Table,
  TableAlias,
  Union,
  With,
} from "./expressions.generated.js"

// DataTypeSize - custom class for data type size specifications (e.g., VARCHAR(255), MAX)
export class DataTypeSize extends Expression {
  static readonly argTypes: Record<string, boolean> = {
    this: true,
    max: false,
  }
  get key(): string {
    return "datatypesize"
  }
}

// Extend DataType with static Type enum
export class DataType extends GeneratedDataType {
  static readonly Type = {
    CHAR: "CHAR",
    NCHAR: "NCHAR",
    VARCHAR: "VARCHAR",
    NVARCHAR: "NVARCHAR",
    TEXT: "TEXT",
    BINARY: "BINARY",
    VARBINARY: "VARBINARY",
    INT: "INT",
    TINYINT: "TINYINT",
    SMALLINT: "SMALLINT",
    BIGINT: "BIGINT",
    FLOAT: "FLOAT",
    DOUBLE: "DOUBLE",
    DECIMAL: "DECIMAL",
    BOOLEAN: "BOOLEAN",
    DATE: "DATE",
    DATETIME: "DATETIME",
    TIME: "TIME",
    TIMESTAMP: "TIMESTAMP",
    TIMESTAMPTZ: "TIMESTAMPTZ",
    INTERVAL: "INTERVAL",
    ARRAY: "ARRAY",
    MAP: "MAP",
    JSON: "JSON",
    STRUCT: "STRUCT",
    NULL: "NULL",
    UNKNOWN: "UNKNOWN",
  } as const

  static readonly TEXT_TYPES = new Set([
    DataType.Type.CHAR,
    DataType.Type.NCHAR,
    DataType.Type.VARCHAR,
    DataType.Type.NVARCHAR,
    DataType.Type.TEXT,
  ])

  static readonly FLOAT_TYPES = new Set([
    DataType.Type.DOUBLE,
    DataType.Type.FLOAT,
  ])

  static readonly REAL_TYPES = new Set([
    ...DataType.FLOAT_TYPES,
    DataType.Type.DECIMAL,
  ])

  static readonly INTEGER_TYPES = new Set([
    DataType.Type.INT,
    DataType.Type.TINYINT,
    DataType.Type.SMALLINT,
    DataType.Type.BIGINT,
  ])

  static readonly NUMERIC_TYPES = new Set([
    ...DataType.INTEGER_TYPES,
    ...DataType.REAL_TYPES,
  ])

  static readonly TEMPORAL_TYPES = new Set([
    DataType.Type.DATE,
    DataType.Type.DATETIME,
    DataType.Type.TIME,
    DataType.Type.TIMESTAMP,
    DataType.Type.TIMESTAMPTZ,
  ])

  isType(...dtypes: string[]): boolean {
    return dtypes.includes(this.text("this"))
  }

  static build(dtype: string): DataType {
    return new DataType({ this: dtype.toUpperCase() })
  }
}

export class Extract extends GeneratedExtract {
  override get name(): string {
    return this.text("this")
  }

  get expression(): Expression | undefined {
    return this.args.expression as Expression | undefined
  }
}

// Expression registry for lookup by key
const EXPRESSION_CLASSES: Map<string, ExpressionClass> = new Map()

export function registerExpression(cls: ExpressionClass): void {
  const instance = new cls({})
  EXPRESSION_CLASSES.set(instance.key, cls)
}

export function getExpressionClass(key: string): ExpressionClass | undefined {
  return EXPRESSION_CLASSES.get(key)
}

// Register all generated classes
for (const cls of GENERATED_CLASSES) {
  registerExpression(cls as unknown as ExpressionClass)
}

// Register enhanced/custom classes (overwrites generated versions)
registerExpression(DataType)
registerExpression(DataTypeSize)
registerExpression(Extract)

// Register And constructor for conjunction builder (avoids circular deps)
setAndConstructor(And)

// Register constructors for convert() (avoids circular deps)
setConvertCtors(Literal, BooleanExpr, Null)

// Register alias factory for Expression.as_() (avoids circular deps)
setAliasFactory(
  (expr, alias, quoted) =>
    new Alias({ this: expr, alias: new Identifier({ this: alias, quoted }) }),
)

// Helper functions for building expressions
export function column(
  name: string,
  table?: string,
  db?: string,
  catalog?: string,
): Column {
  return new Column({
    this: new Identifier({ this: name }),
    table: table ? new Identifier({ this: table }) : undefined,
    db: db ? new Identifier({ this: db }) : undefined,
    catalog: catalog ? new Identifier({ this: catalog }) : undefined,
  })
}

export function table_(name: string, db?: string, catalog?: string): Table {
  return new Table({
    this: new Identifier({ this: name }),
    db: db ? new Identifier({ this: db }) : undefined,
    catalog: catalog ? new Identifier({ this: catalog }) : undefined,
  })
}

export function alias_(expression: Expression, name: string): Alias {
  return new Alias({
    this: expression,
    alias: new Identifier({ this: name }),
  })
}

export function and_(...expressions: Expression[]): Expression | undefined {
  if (expressions.length === 0) return undefined
  if (expressions.length === 1) return expressions[0]
  return expressions.reduce(
    (acc, expr) => new And({ this: acc, expression: expr }),
  )
}

export function or_(...expressions: Expression[]): Expression | undefined {
  if (expressions.length === 0) return undefined
  if (expressions.length === 1) return expressions[0]
  return expressions.reduce(
    (acc, expr) => new Or({ this: acc, expression: expr }),
  )
}

export function not_(expression: Expression): Not {
  return new Not({ this: expression })
}

export function func(name: string, ...args: Expression[]): Anonymous {
  return new Anonymous({ this: name, expressions: args })
}

function camelToSnakeCase(name: string): string {
  return name.replace(/([a-z])([A-Z])/g, "$1_$2").toUpperCase()
}

interface FuncClass extends ExpressionClass {
  readonly sqlNames?: readonly string[]
  readonly isVarLenArgs?: boolean
}

export function getSqlNames(cls: FuncClass): string[] {
  if (cls.sqlNames) {
    return [...cls.sqlNames]
  }
  return [camelToSnakeCase(cls.name)]
}

export function fromArgList<T extends Expression>(
  cls: FuncClass & (new (args?: Args) => T),
  args: Expression[],
): T {
  const argTypes = cls.argTypes
  const argKeys = Object.keys(argTypes)
  const isVarLen = cls.isVarLenArgs ?? false

  if (isVarLen) {
    const nonVarKeys = argKeys.slice(0, -1)
    const varKey = argKeys.at(-1)
    if (!varKey) {
      throw new Error(`No argTypes defined for ${cls.name}`)
    }
    const argsDict: Args = {}

    nonVarKeys.forEach((key, i) => {
      if (i < args.length) {
        argsDict[key] = args[i]
      }
    })
    argsDict[varKey] = args.slice(nonVarKeys.length)

    return new cls(argsDict) as T
  }

  const argsDict: Args = {}
  argKeys.forEach((key, i) => {
    if (i < args.length) {
      argsDict[key] = args[i]
    }
  })
  return new cls(argsDict) as T
}

function isFunc(cls: ExpressionClass): boolean {
  let current: object | null = cls
  while (current) {
    if (current === Func) return true
    current = Object.getPrototypeOf(current) as object | null
  }
  return false
}

export const FUNCTION_BY_NAME: Map<string, ExpressionClass> = new Map()

for (const cls of GENERATED_CLASSES) {
  if (isFunc(cls as ExpressionClass)) {
    const names = getSqlNames(cls as FuncClass)
    for (const name of names) {
      FUNCTION_BY_NAME.set(name.toUpperCase(), cls as ExpressionClass)
    }
  }
}

export const indexOffsetLogs: string[] = []

export function applyIndexOffset(
  expressions: Expression[],
  offset: number,
): Expression[] {
  if (!offset || expressions.length !== 1) {
    return expressions
  }

  const expression = expressions[0]
  if (expression instanceof Literal && expression.isNumber) {
    indexOffsetLogs.push(`Applying array index offset (${offset})`)
    const adjusted = Number(expression.value) + offset
    return [Literal.number(adjusted)]
  }

  return expressions
}

export function select(...expressions: (string | Expression)[]): Select {
  return new Select({}).select(...expressions)
}

export function from_(expression: string | Expression): Select {
  return new Select({}).from(expression)
}

export function toIdentifier(name: string, quoted?: boolean): Identifier {
  return new Identifier({ this: name, quoted })
}

export function toColumn(name: string, table?: string): Column {
  return new Column({
    this: toIdentifier(name),
    table: table ? toIdentifier(table) : undefined,
  })
}

export function cast(expr: string | Expression, to: string | DataType): Cast {
  const thisExpr = typeof expr === "string" ? maybeParse(expr) : expr
  const toExpr =
    typeof to === "string" ? new DataType({ this: to, expressions: [] }) : to
  return new Cast({ this: thisExpr, to: toExpr })
}

export function paren(expr: Expression): Paren {
  return new Paren({ this: expr })
}

export function true_(): BooleanExpr {
  return new BooleanExpr({ this: true })
}

export function false_(): BooleanExpr {
  return new BooleanExpr({ this: false })
}

export function null_(): Null {
  return new Null({})
}

export function _applyCteBuilder(
  alias: string | Expression,
  as_: string | Expression,
  instance: Expression,
  options?: {
    recursive?: boolean
    materialized?: boolean
    append?: boolean
    copy?: boolean
    dialect?: string
    scalar?: boolean
  },
): Expression {
  const aliasExpr = maybeParse(alias, {
    dialect: options?.dialect,
    into: TableAlias,
  })
  let asExpr = maybeParse(as_, {
    dialect: options?.dialect,
    copy: options?.copy,
  })
  if (options?.scalar && !(asExpr instanceof Subquery)) {
    asExpr = new Subquery({ this: asExpr })
  }
  const cte = new CTE({
    this: asExpr,
    alias: aliasExpr,
    materialized: options?.materialized,
    scalar: options?.scalar,
  })
  return _applyChildListBuilder([cte], instance, "with_", {
    append: options?.append ?? true,
    copy: options?.copy ?? true,
    into: With,
    properties: options?.recursive ? { recursive: options.recursive } : {},
  })
}

function _applySetOperation(
  expressions: (string | Expression)[],
  setCtor: ExpressionConstructor,
  options?: {
    distinct?: boolean
    dialect?: string
    copy?: boolean
  },
): Expression {
  const parsed = expressions.map((e) =>
    maybeParse(e, { dialect: options?.dialect, copy: options?.copy }),
  )
  let result = parsed[0]!
  for (let i = 1; i < parsed.length; i++) {
    result = new setCtor({
      this: result,
      expression: parsed[i],
      distinct: options?.distinct ?? true,
    })
  }
  return result
}

export function union(...expressions: (string | Expression)[]): Expression {
  return _applySetOperation(expressions, Union)
}

export function intersect(...expressions: (string | Expression)[]): Expression {
  return _applySetOperation(expressions, Intersect)
}

export function except_(...expressions: (string | Expression)[]): Expression {
  return _applySetOperation(expressions, Except)
}
