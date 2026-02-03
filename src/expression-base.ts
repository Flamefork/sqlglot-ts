/**
 * Base Expression class and types for SQL AST nodes
 */

export type ArgValue =
  | Expression
  | Expression[]
  | string
  | number
  | boolean
  | null
  | undefined
export type Args = Record<string, ArgValue>

export interface ExpressionClass<T extends Expression = Expression> {
  new (args?: Args): T
  readonly argTypes: Record<string, boolean>
  readonly required_args: Set<string>
}

/**
 * Base class for all SQL AST nodes
 */
export abstract class Expression {
  static readonly argTypes: Record<string, boolean> = { this: true }

  static get required_args(): Set<string> {
    const required = new Set<string>()
    for (const [key, isRequired] of Object.entries(this.argTypes)) {
      if (isRequired) {
        required.add(key)
      }
    }
    return required
  }

  readonly args: Args
  parent: Expression | undefined
  argKey: string | undefined
  index: number | undefined
  comments: string[] | undefined
  _type: Expression | undefined
  _meta: Record<string, unknown> | undefined

  get type(): Expression | undefined {
    if (this.key === "cast" || this.key === "trycast") {
      return this._type || (this.args.to as Expression | undefined)
    }
    return this._type
  }

  set type(dtype: Expression | undefined) {
    this._type = dtype
  }

  isType(...dtypes: string[]): boolean {
    if (this.key === "cast" || this.key === "trycast") {
      const to = this.args.to as Expression | undefined
      if (!to) return false
      return dtypes.includes(to.text("this"))
    }
    const type = this.type
    if (!type) return false
    return dtypes.includes(type.text("this"))
  }

  constructor(args: Args = {}) {
    this.args = args
    this.setParents()
  }

  get key(): string {
    return this.constructor.name.toLowerCase()
  }

  get this(): ArgValue {
    return this.args.this
  }

  get expression(): ArgValue {
    return this.args.expression
  }

  get expressions(): Expression[] {
    const val = this.args.expressions
    return Array.isArray(val) ? val : []
  }

  private setParents(): void {
    for (const [key, value] of Object.entries(this.args)) {
      if (value instanceof Expression) {
        value.parent = this
        value.argKey = key
      } else if (Array.isArray(value)) {
        value.forEach((item, idx) => {
          if (item instanceof Expression) {
            item.parent = this
            item.argKey = key
            item.index = idx
          }
        })
      }
    }
  }

  text(key: string): string {
    const field = this.args[key]
    if (typeof field === "string") {
      return field
    }
    if (field instanceof Expression) {
      // Python checks: Identifier, Literal, Var → this; Star → ""; Null → "null"
      if (
        field.key === "var" ||
        field.key === "identifier" ||
        field.key === "literal"
      ) {
        const val = field.args.this
        return typeof val === "string" ? val : ""
      }
      if (field.key === "star") return ""
      if (field.key === "null") return "null"
      const val = field.args.this
      return typeof val === "string" ? val : ""
    }
    return ""
  }

  copy(): this {
    const clone = new (this.constructor as ExpressionClass)(
      this.deepCopyArgs(),
    ) as this
    clone.comments = this.comments ? [...this.comments] : undefined
    if (this._type !== undefined) {
      clone._type = this._type.copy()
    }
    if (this._meta !== undefined) {
      clone._meta = { ...this._meta }
    }
    return clone
  }

  private deepCopyArgs(): Args {
    const result: Args = {}
    for (const [key, value] of Object.entries(this.args)) {
      if (value instanceof Expression) {
        result[key] = value.copy()
      } else if (Array.isArray(value)) {
        result[key] = value.map((item) =>
          item instanceof Expression ? item.copy() : item,
        )
      } else {
        result[key] = value
      }
    }
    return result
  }

  *iterExpressions(reverse = false): Generator<Expression> {
    const entries = Object.entries(this.args)
    const ordered = reverse ? entries.reverse() : entries
    for (const [, value] of ordered) {
      if (value instanceof Expression) {
        yield value
      } else if (Array.isArray(value)) {
        const items = reverse ? [...value].reverse() : value
        for (const item of items) {
          if (item instanceof Expression) {
            yield item
          }
        }
      }
    }
  }

  *bfs(prune?: (node: Expression) => boolean): Generator<Expression> {
    const queue: Expression[] = [this]
    while (queue.length > 0) {
      const node = queue.shift()!
      yield node
      if (prune && prune(node)) continue
      for (const child of node.iterExpressions()) {
        queue.push(child)
      }
    }
  }

  *dfs(prune?: (node: Expression) => boolean): Generator<Expression> {
    const stack: Expression[] = [this]
    while (stack.length > 0) {
      const node = stack.pop()!
      yield node
      if (prune && prune(node)) continue
      for (const child of node.iterExpressions(true)) {
        stack.push(child)
      }
    }
  }

  *walk(
    bfs = true,
    prune?: (node: Expression) => boolean,
  ): Generator<Expression> {
    if (bfs) {
      yield* this.bfs(prune)
    } else {
      yield* this.dfs(prune)
    }
  }

  find<T extends Expression>(...types: ExpressionClass<T>[]): T | undefined
  find<T extends Expression>(
    bfs: boolean,
    ...types: ExpressionClass<T>[]
  ): T | undefined
  find<T extends Expression>(
    first: boolean | ExpressionClass<T>,
    ...rest: ExpressionClass<T>[]
  ): T | undefined {
    const bfs = typeof first === "boolean" ? first : true
    const types = typeof first === "boolean" ? rest : [first, ...rest]
    for (const node of this.walk(bfs)) {
      for (const type of types) {
        if (node instanceof type) {
          return node as T
        }
      }
    }
    return undefined
  }

  findAll<T extends Expression>(...types: ExpressionClass<T>[]): T[]
  findAll<T extends Expression>(
    bfs: boolean,
    ...types: ExpressionClass<T>[]
  ): T[]
  findAll<T extends Expression>(
    first: boolean | ExpressionClass<T>,
    ...rest: ExpressionClass<T>[]
  ): T[] {
    const bfs = typeof first === "boolean" ? first : true
    const types = typeof first === "boolean" ? rest : [first, ...rest]
    const results: T[] = []
    for (const node of this.walk(bfs)) {
      for (const type of types) {
        if (node instanceof type) {
          results.push(node as T)
          break
        }
      }
    }
    return results
  }

  findAncestor<T extends Expression>(
    ...types: ExpressionClass<T>[]
  ): T | undefined {
    let ancestor = this.parent
    while (ancestor) {
      for (const type of types) {
        if (ancestor instanceof type) {
          return ancestor as T
        }
      }
      ancestor = ancestor.parent
    }
    return undefined
  }

  get depth(): number {
    let d = 0
    let node: Expression | undefined = this
    while (node.parent) {
      d++
      node = node.parent
    }
    return d
  }

  root(): Expression {
    let root: Expression = this
    while (root.parent) {
      root = root.parent
    }
    return root
  }

  set(argKey: string, value: ArgValue, index?: number, overwrite = true): void {
    if (index !== undefined) {
      const expressions = (this.args[argKey] ?? []) as Expression[]
      if (index < 0 || index >= expressions.length) {
        return
      }
      if (value === null || value === undefined) {
        expressions.splice(index, 1)
        for (let i = index; i < expressions.length; i++) {
          const expr = expressions[i]
          if (expr instanceof Expression) {
            expr.index = i
          }
        }
        return
      }
      if (Array.isArray(value)) {
        expressions.splice(index, 1, ...(value as Expression[]))
        value = expressions
      } else if (overwrite) {
        expressions[index] = value as Expression
      } else {
        expressions.splice(index, 0, value as Expression)
      }
      value = expressions
    } else if (value === null || value === undefined) {
      delete this.args[argKey]
      return
    }

    this.args[argKey] = value
    this.setParentForValue(argKey, value, index)
  }

  append(argKey: string, value: ArgValue): void {
    if (!(argKey in this.args)) {
      this.args[argKey] = []
    }
    const arr = this.args[argKey] as Expression[]
    arr.push(value as Expression)
    this.setParentForValue(argKey, value, arr.length - 1)
  }

  setParentForValue(argKey: string, value: ArgValue, index?: number): void {
    if (value instanceof Expression) {
      value.parent = this
      value.argKey = argKey
      value.index = index
    } else if (Array.isArray(value)) {
      for (let i = 0; i < value.length; i++) {
        const v = value[i]
        if (v instanceof Expression) {
          v.parent = this
          v.argKey = argKey
          v.index = i
        }
      }
    }
  }

  replace(expression: Expression): Expression {
    const parent = this.parent

    if (!parent || parent === expression) {
      return expression
    }

    const key = this.argKey!
    parent.set(key, expression, this.index)

    if (expression !== (this as Expression)) {
      this.parent = undefined
      this.argKey = undefined
      this.index = undefined
    }

    return expression
  }

  pop(): Expression {
    const parent = this.parent
    if (!parent || !this.argKey) {
      return this
    }
    const key = this.argKey
    const value = parent.args[key]
    if (Array.isArray(value) && this.index !== undefined) {
      value.splice(this.index, 1)
      for (let i = this.index; i < value.length; i++) {
        const expr = value[i]
        if (expr instanceof Expression) {
          expr.index = i
        }
      }
    } else {
      parent.args[key] = undefined
    }
    this.parent = undefined
    this.argKey = undefined
    this.index = undefined
    return this
  }

  *flatten(unnest = true): Generator<Expression> {
    const unwrap = (e: Expression): Expression => (unnest ? e.unnest() : e)

    const stack: Expression[] = [unwrap(this)]
    while (stack.length > 0) {
      const node = stack.pop()!
      if (node.constructor === this.constructor) {
        const right = node.args.expression as Expression | undefined
        const left = node.args.this as Expression | undefined
        if (right) stack.push(unwrap(right))
        if (left) stack.push(unwrap(left))
      } else {
        yield node
      }
    }
  }

  unnest(): Expression {
    let expression: Expression = this
    while (expression.key === "paren") {
      const inner = expression.args.this
      if (inner instanceof Expression) {
        expression = inner
      } else {
        break
      }
    }
    return expression
  }

  unalias(): Expression {
    if (this.key === "alias") {
      const inner = this.args.this
      return inner instanceof Expression ? inner : this
    }
    return this
  }

  assertIs<T extends Expression>(type: ExpressionClass<T>): T {
    if (!(this instanceof type)) {
      throw new Error(`${this.key} is not ${type.name}`)
    }
    return this as T
  }

  transform(
    fn: (node: Expression) => Expression | null,
    copy = true,
  ): Expression | null {
    const root = copy ? this.copy() : this
    return root._transformInPlace(fn)
  }

  private _transformInPlace(
    fn: (node: Expression) => Expression | null,
  ): Expression | null {
    const result = fn(this)
    if (result === null) {
      return null
    }
    if (result !== this) {
      return result
    }

    for (const [key, value] of Object.entries(this.args)) {
      if (value instanceof Expression) {
        const transformed = value._transformInPlace(fn)
        if (transformed !== value) {
          this.args[key] = transformed ?? undefined
        }
      } else if (Array.isArray(value)) {
        const newArray: ArgValue[] = []
        for (const item of value) {
          if (item instanceof Expression) {
            const transformed = item._transformInPlace(fn)
            if (transformed !== null) {
              newArray.push(transformed)
            }
          } else {
            newArray.push(item)
          }
        }
        this.args[key] = newArray as Expression[]
      }
    }

    this.setParents()
    return this
  }

  // parseImpl() method - implementation provided by index.ts to avoid circular deps
  private static _parseImpl:
    | ((sql: string, options?: { dialect?: string }) => Expression)
    | undefined

  static setParseImpl(
    impl: (sql: string, options?: { dialect?: string }) => Expression,
  ): void {
    Expression._parseImpl = impl
  }

  static parseImpl(sql: string, options?: { dialect?: string }): Expression {
    if (!Expression._parseImpl) {
      throw new Error(
        "parseImpl() requires initialization - import from sqlglot-ts",
      )
    }
    return Expression._parseImpl(sql, options)
  }

  // sql() method - implementation provided by index.ts to avoid circular deps
  private static _sqlImpl:
    | ((
        expr: Expression,
        options?: {
          dialect?: unknown
          pretty?: boolean
          unsupportedLevel?: string
        },
      ) => string)
    | undefined

  static setSqlImpl(
    impl: (
      expr: Expression,
      options?: {
        dialect?: unknown
        pretty?: boolean
        unsupportedLevel?: string
      },
    ) => string,
  ): void {
    Expression._sqlImpl = impl
  }

  sql(options?: {
    dialect?: unknown
    pretty?: boolean
    unsupportedLevel?: string
  }): string {
    if (!Expression._sqlImpl) {
      throw new Error("sql() requires initialization - import from sqlglot-ts")
    }
    return Expression._sqlImpl(this, options)
  }

  private static _dumpImpl:
    | ((expr: Expression) => Record<string, unknown>[])
    | undefined
  private static _loadImpl:
    | ((payloads: Record<string, unknown>[]) => Expression)
    | undefined

  static setSerdeImpl(
    dumpFn: (expr: Expression) => Record<string, unknown>[],
    loadFn: (payloads: Record<string, unknown>[]) => Expression,
  ): void {
    Expression._dumpImpl = dumpFn
    Expression._loadImpl = loadFn
  }

  dump(): Record<string, unknown>[] {
    if (!Expression._dumpImpl) {
      throw new Error("dump() requires initialization - import from sqlglot-ts")
    }
    return Expression._dumpImpl(this)
  }

  static load(payloads: Record<string, unknown>[]): Expression {
    if (!Expression._loadImpl) {
      throw new Error("load() requires initialization - import from sqlglot-ts")
    }
    return Expression._loadImpl(payloads)
  }

  toString(): string {
    return `${this.constructor.name}(${JSON.stringify(this.args)})`
  }

  get alias(): string {
    const aliasExpr = this.args.alias
    if (aliasExpr instanceof Expression) {
      const thisVal = aliasExpr.args.this
      return typeof thisVal === "string" ? thisVal : ""
    }
    if (typeof aliasExpr === "string") {
      return aliasExpr
    }
    return ""
  }

  get name(): string {
    return this.text("this")
  }

  get aliasOrName(): string {
    return this.alias || this.name
  }

  get isStar(): boolean {
    return false // overridden in Star, Column
  }

  /** Python-style snake_case alias for isStar */
  get is_star(): boolean {
    return this.isStar
  }

  get isLeaf(): boolean {
    for (const value of Object.values(this.args)) {
      if (value instanceof Expression) return false
      if (Array.isArray(value) && value.some((v) => v instanceof Expression))
        return false
    }
    return true
  }

  /** Checks whether a Literal expression is a string */
  get is_string(): boolean {
    if (this.key !== "literal") return false
    return !!this.args.is_string
  }

  /** Checks whether a Literal expression is a number (or Neg of a number) */
  get is_number(): boolean {
    if (this.key === "literal") {
      return !this.args.is_string
    }
    if (this.key === "neg") {
      const inner = this.args.this
      return inner instanceof Expression && inner.is_number
    }
    return false
  }

  /** Checks whether an expression is an integer */
  get is_int(): boolean {
    if (!this.is_number) return false
    const raw = this.args.this
    if (typeof raw === "string") {
      const n = Number(raw)
      return !isNaN(n) && Number.isInteger(n)
    }
    if (typeof raw === "number") {
      return Number.isInteger(raw)
    }
    if (raw instanceof Expression) {
      return raw.is_int
    }
    return false
  }

  /** Returns a TypeScript value equivalent of the SQL node */
  toPy(): string | number {
    throw new Error(`${this.key} cannot be converted to a TypeScript value`)
  }

  get outputName(): string {
    return this.alias || this.text("this")
  }

  as_(alias: string, quoted = false): Expression {
    if (!_aliasFactory) {
      throw new Error("as_() requires initialization - import from sqlglot-ts")
    }
    return _aliasFactory(this, alias, quoted)
  }

  protected _binop(
    klass: new (args: Args) => Expression,
    other: unknown,
  ): Expression {
    return new klass({ this: this, expression: convert(other) })
  }
}

export type ExpressionConstructor<T extends Expression = Expression> = new (
  args?: Args,
) => T

interface MaybeParseOptions {
  prefix?: string | undefined
  dialect?: string | undefined
  copy?: boolean | undefined
  into?: ExpressionConstructor | undefined
}

export function maybeParse(
  sqlOrExpr: string | Expression,
  options?: MaybeParseOptions,
): Expression {
  if (sqlOrExpr instanceof Expression) {
    return options?.copy ? sqlOrExpr.copy() : sqlOrExpr
  }
  let sql = sqlOrExpr
  const prefix = options?.prefix
  if (prefix) {
    sql = `${prefix} ${sql}`
  }
  const dialect = options?.dialect
  const parsed = Expression.parseImpl(sql, dialect ? { dialect } : undefined)

  // If 'into' is specified, extract that node type from the parsed tree
  if (options?.into) {
    const found = parsed.find(options.into as never)
    if (found) {
      return found
    }
  }

  return parsed
}

function _isWrongExpression(
  expression: Expression,
  into: ExpressionConstructor,
): boolean {
  return !(expression instanceof into)
}

export function _applyBuilder(
  expression: string | Expression,
  instance: Expression,
  arg: string,
  options?: {
    copy?: boolean | undefined
    prefix?: string | undefined
    into?: ExpressionConstructor | undefined
    dialect?: string | undefined
    intoArg?: string | undefined
  },
): Expression {
  const into = options?.into
  const intoArg = options?.intoArg ?? "this"
  if (
    into &&
    expression instanceof Expression &&
    _isWrongExpression(expression, into)
  ) {
    expression = new into({ [intoArg]: expression })
  }
  const inst = options?.copy ? instance.copy() : instance
  const parsed = maybeParse(expression, options)
  inst.set(arg, parsed)
  return inst
}

export function _applyListBuilder(
  expressions: (string | Expression | undefined)[],
  instance: Expression,
  arg: string,
  options?: {
    append?: boolean | undefined
    copy?: boolean | undefined
    prefix?: string | undefined
    into?: ExpressionConstructor | undefined
    dialect?: string | undefined
  },
): Expression {
  const inst = options?.copy ? instance.copy() : instance
  const append = options?.append ?? true

  const parsed: Expression[] = expressions
    .filter((e): e is string | Expression => e !== undefined && e !== null)
    .map((e) => maybeParse(e, options))

  const existing = inst.args[arg]
  const final =
    append && Array.isArray(existing) ? [...existing, ...parsed] : parsed
  inst.set(arg, final)
  return inst
}

export function _applyChildListBuilder(
  expressions: (string | Expression | undefined)[],
  instance: Expression,
  arg: string,
  options?: {
    append?: boolean | undefined
    copy?: boolean | undefined
    prefix?: string | undefined
    into?: ExpressionConstructor | undefined
    dialect?: string | undefined
    properties?: Args | undefined
  },
): Expression {
  const inst = options?.copy ? instance.copy() : instance
  const append = options?.append ?? true
  const into = options?.into
  const properties: Args = options?.properties ?? {}

  const parsed: Expression[] = []
  for (const expression of expressions) {
    if (expression === undefined || expression === null) continue
    let expr: Expression
    if (
      into &&
      expression instanceof Expression &&
      _isWrongExpression(expression, into)
    ) {
      expr = new into({ expressions: [expression] })
    } else {
      expr = maybeParse(expression, options)
    }
    const exprList = expr.args.expressions
    if (Array.isArray(exprList)) {
      parsed.push(...exprList)
    }
    for (const [k, v] of Object.entries(expr.args)) {
      if (k !== "expressions") {
        properties[k] = v
      }
    }
  }

  const existing = inst.args[arg]
  const finalExprs =
    append && existing instanceof Expression
      ? [...(existing.expressions || []), ...parsed]
      : parsed

  if (into) {
    const child = new into({ expressions: finalExprs })
    for (const [k, v] of Object.entries(properties)) {
      child.set(k, v)
    }
    inst.set(arg, child)
  }

  return inst
}

// Late-bound alias factory to avoid circular deps (Alias/Identifier in generated code)
let _aliasFactory:
  | ((expr: Expression, alias: string, quoted: boolean) => Expression)
  | undefined

export function setAliasFactory(
  factory: (expr: Expression, alias: string, quoted: boolean) => Expression,
): void {
  _aliasFactory = factory
}

// Late-bound And constructor to avoid circular deps
let _AndCtor: ExpressionConstructor | undefined

export function setAndConstructor(ctor: ExpressionConstructor): void {
  _AndCtor = ctor
}

// Late-bound constructors for convert()
let _LiteralCtor:
  | { string(val: string): Expression; number(val: number): Expression }
  | undefined
let _BooleanCtor: { true_(): Expression; false_(): Expression } | undefined
let _NullCtor: ExpressionConstructor | undefined

export function setConvertCtors(
  literal: { string(val: string): Expression; number(val: number): Expression },
  boolean_: { true_(): Expression; false_(): Expression },
  null_: ExpressionConstructor,
): void {
  _LiteralCtor = literal
  _BooleanCtor = boolean_
  _NullCtor = null_
}

export function convert(value: unknown): Expression {
  if (value instanceof Expression) return value
  if (typeof value === "string") {
    if (!_LiteralCtor) throw new Error("convert() requires initialization")
    return _LiteralCtor.string(value)
  }
  if (typeof value === "number") {
    if (!_LiteralCtor) throw new Error("convert() requires initialization")
    return _LiteralCtor.number(value)
  }
  if (typeof value === "boolean") {
    if (!_BooleanCtor) throw new Error("convert() requires initialization")
    return value ? _BooleanCtor.true_() : _BooleanCtor.false_()
  }
  if (value === null || value === undefined) {
    if (!_NullCtor) throw new Error("convert() requires initialization")
    return new _NullCtor({})
  }
  throw new Error(`Cannot convert ${typeof value} to Expression`)
}

export function _applyConjunctionBuilder(
  expressions: (string | Expression | undefined)[],
  instance: Expression,
  arg: string,
  options?: {
    append?: boolean | undefined
    copy?: boolean | undefined
    into?: ExpressionConstructor | undefined
    dialect?: string | undefined
  },
): Expression {
  const filtered = expressions.filter(
    (e): e is string | Expression => e !== undefined && e !== null && e !== "",
  )
  if (filtered.length === 0) return instance

  const inst = options?.copy ? instance.copy() : instance
  const append = options?.append ?? true
  const into = options?.into

  const existing = inst.args[arg]
  const allExprs: (string | Expression)[] = []
  if (append && existing instanceof Expression) {
    allExprs.push(into ? (existing.args.this as Expression) : existing)
  }
  allExprs.push(...filtered)

  const parsed = allExprs.map((e) => maybeParse(e, options))
  let node: Expression = parsed[0] as Expression
  if (_AndCtor) {
    for (let i = 1; i < parsed.length; i++) {
      node = new _AndCtor({ this: node, expression: parsed[i] })
    }
  }

  inst.set(arg, into ? new into({ this: node }) : node)
  return inst
}

/**
 * Generate unique name by appending numbers if name is taken.
 * Example: "pos" → "pos_2" → "pos_3" if "pos" and "pos_2" are taken.
 */
export function findNewName(taken: Set<string>, base: string): string {
  if (!taken.has(base)) {
    return base
  }

  let i = 2
  let newName = `${base}_${i}`
  while (taken.has(newName)) {
    i++
    newName = `${base}_${i}`
  }
  return newName
}
