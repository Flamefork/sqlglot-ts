/**
 * Expression AST node classes for SQL parsing
 */

// Re-export base types
export {
  _applyBuilder,
  _applyChildListBuilder,
  _applyConjunctionBuilder,
  _applyListBuilder,
  _extractBuilderArgs,
  _wrap,
  type Args,
  type ArgValue,
  type BuilderOptions,
  camelToSnakeCase,
  convert,
  Expression,
  type ExpressionClass,
  type ExpressionConstructor,
  maybeParse,
} from "./expression-base.js"

import {
  _applyBuilder,
  _applyChildListBuilder,
  _applyConjunctionBuilder,
  _applyListBuilder,
  _extractBuilderArgs,
  _wrap,
  type Args,
  type BuilderOptions,
  camelToSnakeCase,
  convert,
  Expression,
  type ExpressionClass,
  type ExpressionConstructor,
  maybeParse,
  setAliasFactory,
  setAndConstructor,
  setBinaryConstructor,
  setConnectorConstructor,
  setConvertCtors,
  setExpressionFluentConstructors,
  setExpressionParser,
  setOrConstructor,
  setParenConstructor,
} from "./expression-base.js"

// Re-export all generated classes
export * from "./expressions.generated.js"

// Import specific classes needed for type definitions and helpers
import {
  Alias,
  Alter,
  AlterRename,
  And,
  Anonymous,
  Array as ArrayExpr,
  Between,
  Binary,
  Boolean as BooleanExpr,
  Case,
  Cast,
  Column,
  Condition,
  Connector,
  CTE,
  Delete,
  Div,
  EQ,
  Except,
  From,
  Func,
  GENERATED_CLASSES,
  DataType as GeneratedDataType,
  Extract as GeneratedExtract,
  Identifier,
  ILike,
  In,
  Insert,
  Intersect,
  Interval,
  Is,
  Like,
  Literal,
  Merge,
  NEQ,
  Not,
  Null,
  Or,
  Ordered,
  Paren,
  Placeholder,
  RegexpLike,
  RenameColumn,
  Schema,
  Select,
  Subquery,
  Table,
  TableAlias,
  Tuple,
  Union,
  Unnest,
  Update,
  Values,
  Var,
  When,
  Whens,
  Where,
  With,
  Xor,
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

// Extend DataType with static Type enum (generated) and type sets
import {
  DATA_TYPE_TYPES,
  type DataTypeType,
  STRUCT_TYPES,
  ARRAY_TYPES,
  NESTED_TYPES,
  TEXT_TYPES as GEN_TEXT_TYPES,
  SIGNED_INTEGER_TYPES,
  UNSIGNED_INTEGER_TYPES,
  INTEGER_TYPES as GEN_INTEGER_TYPES,
  FLOAT_TYPES as GEN_FLOAT_TYPES,
  REAL_TYPES as GEN_REAL_TYPES,
  NUMERIC_TYPES as GEN_NUMERIC_TYPES,
  TEMPORAL_TYPES as GEN_TEMPORAL_TYPES,
} from "./expressions.generated.js"

export { DATA_TYPE_TYPES, type DataTypeType }

export class DataType extends GeneratedDataType {
  static readonly Type: typeof DATA_TYPE_TYPES = DATA_TYPE_TYPES

  static readonly STRUCT_TYPES: ReadonlySet<string> = STRUCT_TYPES
  static readonly ARRAY_TYPES: ReadonlySet<string> = ARRAY_TYPES
  static readonly NESTED_TYPES: ReadonlySet<string> = NESTED_TYPES
  static readonly TEXT_TYPES: ReadonlySet<string> = GEN_TEXT_TYPES
  static readonly SIGNED_INTEGER_TYPES: ReadonlySet<string> =
    SIGNED_INTEGER_TYPES
  static readonly UNSIGNED_INTEGER_TYPES: ReadonlySet<string> =
    UNSIGNED_INTEGER_TYPES
  static readonly INTEGER_TYPES: ReadonlySet<string> = GEN_INTEGER_TYPES
  static readonly FLOAT_TYPES: ReadonlySet<string> = GEN_FLOAT_TYPES
  static readonly REAL_TYPES: ReadonlySet<string> = GEN_REAL_TYPES
  static readonly NUMERIC_TYPES: ReadonlySet<string> = GEN_NUMERIC_TYPES
  static readonly TEMPORAL_TYPES: ReadonlySet<string> = GEN_TEMPORAL_TYPES

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

// Register And/Connector/Paren/Binary constructors for conjunction builder (avoids circular deps)
setAndConstructor(And)
setOrConstructor(Or)
setBinaryConstructor(Binary)
setConnectorConstructor(Connector)
setParenConstructor(Paren)
setExpressionFluentConstructors({
  between: Between,
  div: Div,
  eq: EQ,
  ilike: ILike,
  in_: In,
  is_: Is,
  like: Like,
  neq: NEQ,
  not_: Not,
  ordered: Ordered,
  regexpLike: RegexpLike,
  select: Select,
  subquery: Subquery,
  unnest: Unnest,
})

// Register constructors for convert() (avoids circular deps)
setConvertCtors(Literal, BooleanExpr, Null)

// Register Expression parser for parseAsExpression mode (uses Condition as concrete proxy)
setExpressionParser(Condition)

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

export function alias_(
  expression: Expression,
  name: string,
  options?: { table?: boolean },
): Alias | Expression {
  const alias = new Identifier({ this: name })
  if (options?.table) {
    const tableAlias = new TableAlias({ this: alias })
    expression.set("alias", tableAlias)
    return expression
  }
  return new Alias({
    this: expression,
    alias,
  })
}

function _combine(
  expressions: (string | Expression | null | undefined)[],
  operator: ExpressionConstructor,
  options?: BuilderOptions,
  wrap = true,
): Expression | undefined {
  const condOpts: { dialect?: string; copy?: boolean } = {}
  if (options?.dialect !== undefined) condOpts.dialect = options.dialect
  if (options?.copy !== undefined) condOpts.copy = options.copy
  const conditions = expressions
    .filter((e): e is string | Expression => e !== null && e !== undefined)
    .map((e) => condition(e, condOpts))
  if (conditions.length === 0) return undefined
  let [this_, ...rest] = conditions
  if (rest.length > 0 && wrap) {
    this_ = _wrap(this_!, Connector) as Condition
  }
  for (const expr of rest) {
    this_ = new operator({
      this: this_,
      expression: wrap ? _wrap(expr, Connector) : expr,
    }) as Condition
  }
  return this_
}

export function and_(
  ...args: (string | Expression | null | undefined | BuilderOptions)[]
): Expression | undefined {
  let options: BuilderOptions | undefined
  const expressions: (string | Expression | null | undefined)[] = []
  for (const arg of args) {
    if (
      typeof arg === "object" &&
      arg !== null &&
      !(arg instanceof Expression)
    ) {
      options = arg as BuilderOptions
    } else {
      expressions.push(arg as string | Expression | null | undefined)
    }
  }
  return _combine(expressions, And, options)
}

export function or_(
  ...args: (string | Expression | null | undefined | BuilderOptions)[]
): Expression | undefined {
  let options: BuilderOptions | undefined
  const expressions: (string | Expression | null | undefined)[] = []
  for (const arg of args) {
    if (
      typeof arg === "object" &&
      arg !== null &&
      !(arg instanceof Expression)
    ) {
      options = arg as BuilderOptions
    } else {
      expressions.push(arg as string | Expression | null | undefined)
    }
  }
  return _combine(expressions, Or, options)
}

export function not_(expression: string | Expression): Not {
  const this_ = condition(expression)
  return new Not({ this: _wrap(this_, Connector) })
}

export function func(name: string, ...args: Expression[]): Anonymous {
  return new Anonymous({ this: name, expressions: args })
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
  return new Select({}).from_(expression)
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

function _extractSetOpArgs(
  args: (string | Expression | { distinct?: boolean })[],
): { expressions: (string | Expression)[]; distinct: boolean } {
  let distinct = true
  const expressions: (string | Expression)[] = []
  for (const arg of args) {
    if (
      typeof arg === "object" &&
      arg !== null &&
      !(arg instanceof Expression) &&
      "distinct" in arg
    ) {
      distinct = arg.distinct ?? true
    } else {
      expressions.push(arg as string | Expression)
    }
  }
  return { expressions, distinct }
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

export function union(
  ...args: (string | Expression | { distinct?: boolean })[]
): Expression {
  const { expressions, distinct } = _extractSetOpArgs(args)
  return _applySetOperation(expressions, Union, { distinct })
}

export function intersect(
  ...args: (string | Expression | { distinct?: boolean })[]
): Expression {
  const { expressions, distinct } = _extractSetOpArgs(args)
  return _applySetOperation(expressions, Intersect, { distinct })
}

export function except_(
  ...args: (string | Expression | { distinct?: boolean })[]
): Expression {
  const { expressions, distinct } = _extractSetOpArgs(args)
  return _applySetOperation(expressions, Except, { distinct })
}

export function condition(
  expression: string | Expression,
  options?: { dialect?: string; copy?: boolean },
): Condition {
  return maybeParse(expression, {
    into: Condition,
    dialect: options?.dialect,
    copy: options?.copy,
  }) as Condition
}

export function case_(expression?: string | Expression): Case {
  const this_ = expression ? maybeParse(expression) : undefined
  return new Case({ this: this_, ifs: [] })
}

export function values(
  valuesList: unknown[][],
  alias?: string,
  columns?: string[],
): Values {
  const tuples = valuesList.map(
    (row) => new Tuple({ expressions: row.map((v) => convert(v)) }),
  )
  return new Values({
    expressions: tuples,
    alias: columns
      ? new TableAlias({
          this: toIdentifier(alias!),
          columns: columns.map((c) => toIdentifier(c)),
        })
      : alias
        ? new TableAlias({ this: toIdentifier(alias) })
        : undefined,
  })
}

export function delete_(
  table: string | Expression,
  options?: {
    where?: string | Expression
    returning?: string | Expression
    dialect?: string
  },
): Delete {
  let result = new Delete({}).delete_(table, { copy: false })
  if (options?.where) result = result.where(options.where) as Delete
  if (options?.returning)
    result = result.returning(options.returning, {
      copy: false,
    }) as Delete
  return result
}

export function update(
  table: string | Expression,
  properties?: Record<string, unknown>,
  options?: {
    where?: string | Expression
    from_?: string | Expression
    with_?: Record<string, string>
    dialect?: string
  },
): Update {
  const updateExpr = new Update({
    this: maybeParse(table, { into: Table, dialect: options?.dialect }),
  })
  if (properties) {
    const eqs = Object.entries(properties).map(
      ([k, v]) =>
        new EQ({
          this: maybeParse(k, { dialect: options?.dialect }),
          expression: convert(v),
        }),
    )
    updateExpr.set("expressions", eqs)
  }
  if (options?.from_) {
    updateExpr.set(
      "from_",
      maybeParse(options.from_, {
        into: From,
        dialect: options?.dialect,
        prefix: "FROM",
      }),
    )
  }
  if (options?.where) {
    const whereExpr =
      options.where instanceof Condition
        ? new Where({ this: options.where })
        : maybeParse(options.where, {
            into: Where,
            dialect: options?.dialect,
            prefix: "WHERE",
          })
    updateExpr.set("where", whereExpr)
  }
  if (options?.with_) {
    const cteList = Object.entries(options.with_).map(([a, qry]) =>
      alias_(
        new CTE({ this: maybeParse(qry, { dialect: options?.dialect }) }),
        a,
      ),
    )
    updateExpr.set("with_", new With({ expressions: cteList }))
  }
  return updateExpr
}

export function insert(
  expression: string | Expression,
  into: string | Expression,
  options?: {
    columns?: string[]
    overwrite?: boolean
    returning?: string | Expression
    dialect?: string
  },
): Insert {
  const expr = maybeParse(expression, { dialect: options?.dialect })
  let tableExpr: Expression = maybeParse(into, {
    into: Table,
    dialect: options?.dialect,
  })
  if (options?.columns) {
    tableExpr = new Schema({
      this: tableExpr,
      expressions: options.columns.map((c) => toIdentifier(c)),
    })
  }
  const result = new Insert({
    this: tableExpr,
    expression: expr,
    overwrite: options?.overwrite,
  })
  if (options?.returning)
    return result.returning(options.returning, { copy: false })
  return result
}

export function subquery(
  expression: string | Expression,
  alias?: string,
): Select {
  const parsed = maybeParse(expression)
  const sub = new Subquery({ this: parsed })
  if (alias) {
    sub.set("alias", new TableAlias({ this: toIdentifier(alias) }))
  }
  return new Select({}).from_(sub)
}

export function renameColumn(
  table: string,
  oldName: string,
  newName: string,
  exists?: boolean,
): Alter {
  return new Alter({
    this: maybeParse(table, { into: Table }),
    kind: "TABLE",
    actions: [
      new RenameColumn({
        this: toColumn(oldName),
        to: toColumn(newName),
        exists,
      }),
    ],
  })
}

export function merge(
  ...whenExprs: (
    | string
    | Expression
    | {
        into: string | Expression
        using: string | Expression
        on: string | Expression
        returning?: string | Expression
        dialect?: string
      }
  )[]
): Merge {
  const last = whenExprs[whenExprs.length - 1]
  const hasOptions = typeof last === "object" && !(last instanceof Expression)
  const options = hasOptions
    ? (last as {
        into: string | Expression
        using: string | Expression
        on: string | Expression
        returning?: string | Expression
        dialect?: string
      })
    : undefined
  const whens = hasOptions ? whenExprs.slice(0, -1) : whenExprs

  const whenList: Expression[] = []
  for (const w of whens as (string | Expression)[]) {
    const parsed = maybeParse(w, {
      dialect: options?.dialect,
      into: Whens,
    })
    if (parsed instanceof When) whenList.push(parsed)
    else whenList.push(...parsed.expressions)
  }

  const result = new Merge({
    this: maybeParse(options!.into, { dialect: options?.dialect }),
    using: maybeParse(options!.using, { dialect: options?.dialect }),
    on: maybeParse(options!.on, { dialect: options?.dialect }),
    whens: new Whens({ expressions: whenList }),
  })
  if (options?.returning) result.returning(options.returning, { copy: false })

  const usingClause = result.args["using"]
  if (usingClause instanceof Alias) {
    const inner = usingClause.this
    const aliasName = (usingClause.args["alias"] as Expression).text("this")
    if (inner instanceof Expression) {
      usingClause.replace(alias_(inner, aliasName, { table: true }))
    }
  }

  return result
}

// ============================================================================
// Top-level utility functions
// ============================================================================

export function toBool(
  value: string | boolean | null | undefined,
): string | boolean | null | undefined {
  if (typeof value === "boolean" || value === null || value === undefined) {
    return value
  }
  const lower = value.toLowerCase()
  if (lower === "true" || lower === "1") return true
  if (lower === "false" || lower === "0") return false
  return value
}

export function splitNumWords(
  value: string,
  sep: string,
  minNumWords: number,
  fillFromStart = true,
): (string | undefined)[] {
  const words = value.split(sep)
  if (fillFromStart) {
    const padding: undefined[] = Array(
      Math.max(0, minNumWords - words.length),
    ).fill(undefined)
    return [...padding, ...words]
  }
  const padding: undefined[] = Array(
    Math.max(0, minNumWords - words.length),
  ).fill(undefined)
  return [...words, ...padding]
}

export function columnTableNames(
  expression: Expression,
  exclude = "",
): Set<string> {
  return new Set(
    expression
      .findAll(Column)
      .map((c) => c.text("table"))
      .filter((t) => t && t !== exclude),
  )
}

export function parseIdentifier(
  name: string | Expression,
  dialect?: string,
): Identifier {
  try {
    return maybeParse(name, { dialect, into: Identifier }) as Identifier
  } catch {
    return toIdentifier(typeof name === "string" ? name : name.name)
  }
}

export function renameTable(
  oldName: string | Expression,
  newName: string | Expression,
  dialect?: string,
): Alter {
  const oldTable = toTable(oldName, dialect)
  const newTable = toTable(newName, dialect)
  return new Alter({
    this: oldTable,
    kind: "TABLE",
    actions: [new AlterRename({ this: newTable })],
  })
}

export function tableName(
  table: Expression | string,
  dialect?: string,
): string {
  const tableExpr = maybeParse(table, { into: Table, dialect }) as InstanceType<
    typeof Table
  >
  const parts = tableExpr.args.catalog
    ? [tableExpr.args.catalog, tableExpr.args.db, tableExpr.args.this]
    : tableExpr.args.db
      ? [tableExpr.args.db, tableExpr.args.this]
      : [tableExpr.args.this]
  return parts
    .filter((p): p is Expression => p instanceof Expression)
    .map((p) => p.name)
    .join(".")
}

export function toInterval(interval: string | Expression): Interval {
  if (interval instanceof Expression) {
    if (!interval.is_string) throw new Error("Invalid interval string.")
    interval = interval.name
  }
  return maybeParse(`INTERVAL ${interval}`) as Interval
}

export function toTable(
  sqlPath: string | Expression,
  dialect?: string,
): InstanceType<typeof Table> {
  if (sqlPath instanceof Table) return sqlPath as InstanceType<typeof Table>
  try {
    return maybeParse(sqlPath, { into: Table, dialect }) as InstanceType<
      typeof Table
    >
  } catch {
    const [catalog, db, name] = splitNumWords(sqlPath as string, ".", 3)
    if (!name) throw new Error(`Cannot parse table: ${sqlPath}`)
    return table_(name, db, catalog) as InstanceType<typeof Table>
  }
}

export function replacePlaceholders(
  expression: Expression,
  ...args: unknown[]
): Expression {
  const kwargs: Record<string, unknown> = {}
  const positional: unknown[] = []
  for (const arg of args) {
    if (
      typeof arg === "object" &&
      arg !== null &&
      !(arg instanceof Expression)
    ) {
      Object.assign(kwargs, arg)
    } else {
      positional.push(arg)
    }
  }
  const iter = positional[Symbol.iterator]()
  return expression.transform((node) => {
    if (node instanceof Placeholder) {
      const name = node.name
      if (name) {
        const val = kwargs[name]
        if (val !== undefined) return convert(val)
      } else {
        const next = iter.next()
        if (!next.done) return convert(next.value)
      }
    }
    return node
  })!
}

export function array_(
  ...expressions: (string | Expression)[]
): InstanceType<typeof ArrayExpr> {
  return new ArrayExpr({
    expressions: expressions.map((e) => maybeParse(e)),
  })
}

export function tuple_(
  ...expressions: (string | Expression)[]
): InstanceType<typeof Tuple> {
  return new Tuple({
    expressions: expressions.map((e) => maybeParse(e)),
  })
}

export function var_(name: string | Expression): InstanceType<typeof Var> {
  if (!name) throw new Error("Cannot convert empty name into var.")
  const n = name instanceof Expression ? name.name : name
  return new Var({ this: n })
}

export function xor(
  ...args: (string | Expression | null | undefined | BuilderOptions)[]
): Expression | undefined {
  let options: BuilderOptions | undefined
  const expressions: (string | Expression | null | undefined)[] = []
  for (const arg of args) {
    if (
      typeof arg === "object" &&
      arg !== null &&
      !(arg instanceof Expression)
    ) {
      options = arg as BuilderOptions
    } else {
      expressions.push(arg as string | Expression | null | undefined)
    }
  }
  return _combine(expressions, Xor, options)
}
