/**
 * Base Expression class and types for SQL AST nodes
 */

import type {
  Between,
  Div,
  EQ,
  ILike,
  In,
  Is,
  Like,
  NEQ,
  Not,
  Ordered,
  RegexpLike,
  Select,
  Subquery,
  Unnest,
} from "./expressions.generated.js"

export type ArgValue =
  | Expression
  | Expression[]
  | string
  | number
  | boolean
  | null
  | undefined
export type Args = Record<string, ArgValue>

export interface BuilderOptions {
  append?: boolean
  dialect?: string
  copy?: boolean
}

export function _extractBuilderArgs(
  args: (string | Expression | null | undefined | BuilderOptions)[],
): { expressions: (string | Expression)[]; options: BuilderOptions } {
  let options: BuilderOptions = {}
  const expressions: (string | Expression)[] = []
  for (const arg of args) {
    if (arg === null || arg === undefined) continue
    if (typeof arg === "object" && !(arg instanceof Expression)) {
      options = arg
    } else {
      expressions.push(arg as string | Expression)
    }
  }
  return { expressions, options }
}

export interface ExpressionClass<T extends Expression = Expression> {
  new (args?: Args): T
  readonly argTypes: Record<string, boolean>
}

/**
 * Base class for all SQL AST nodes
 */
export abstract class Expression {
  static readonly argTypes: Record<string, boolean> = { this: true }

  readonly args: Args
  parent: Expression | undefined
  argKey: string | undefined
  index: number | undefined
  comments: string[] | undefined
  _type: Expression | undefined
  _meta: Record<string, unknown> | undefined
  private _hash: number | undefined

  equals(other: Expression): boolean {
    return (
      this === other ||
      (this.constructor === other.constructor &&
        this.hashCode() === other.hashCode())
    )
  }

  hashCode(): number {
    if (this._hash !== undefined) {
      return this._hash
    }

    const nodes: Expression[] = []
    const queue: Expression[] = [this]

    while (queue.length > 0) {
      const node = queue.shift()!
      nodes.push(node)
      for (const child of node.iterExpressions()) {
        if (child._hash === undefined) {
          queue.push(child)
        }
      }
    }

    for (let i = nodes.length - 1; i >= 0; i--) {
      const node = nodes[i]!
      let h = _stringHash(node.key)
      const isLeafLike = node.key === "literal" || node.key === "identifier"

      const sortedKeys = Object.keys(node.args).sort()
      for (const k of sortedKeys) {
        const v = node.args[k]
        if (isLeafLike) {
          if (v !== undefined && v !== null && v !== false) {
            h = _combineHash(h, _stringHash(k))
            h = _combineHash(h, _argValueHash(v, false))
          }
        } else if (Array.isArray(v)) {
          for (const item of v) {
            if (item != null) {
              h = _combineHash(h, _stringHash(k))
              h = _combineHash(h, _argValueHash(item, true))
            } else {
              h = _combineHash(h, _stringHash(k))
            }
          }
        } else if (v !== undefined && v !== null && v !== false) {
          h = _combineHash(h, _stringHash(k))
          h = _combineHash(h, _argValueHash(v, true))
        }
      }

      node._hash = h
    }

    return this._hash!
  }

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
    let depth = 0
    let parent = this.parent
    while (parent) {
      depth++
      parent = parent.parent
    }
    return depth
  }

  root(): Expression {
    let parent = this.parent
    if (!parent) {
      return this
    }
    while (parent.parent) {
      parent = parent.parent
    }
    return parent
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
    if (this.key !== "paren") {
      return this
    }

    let inner = this.args.this
    if (!(inner instanceof Expression)) {
      return this
    }

    while (inner.key === "paren") {
      const nextInner: ArgValue = inner.args.this
      if (!(nextInner instanceof Expression)) {
        return inner
      }
      inner = nextInner
    }

    return inner
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
    | ((
        sql: string,
        options?: {
          dialect?: string
          into?:
            | ExpressionConstructor
            | ExpressionConstructor[]
            | ExpressionClass
            | ExpressionClass[]
        },
      ) => Expression)
    | undefined

  static setParseImpl(
    impl: (
      sql: string,
      options?: {
        dialect?: string
        into?: ExpressionConstructor | ExpressionConstructor[]
      },
    ) => Expression,
  ): void {
    Expression._parseImpl = impl
  }

  static parseImpl(
    sql: string,
    options?: {
      dialect?: string
      into?:
        | ExpressionConstructor
        | ExpressionConstructor[]
        | ExpressionClass
        | ExpressionClass[]
    },
  ): Expression {
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
          identify?: boolean | "safe"
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
        identify?: boolean | "safe"
        unsupportedLevel?: string
      },
    ) => string,
  ): void {
    Expression._sqlImpl = impl
  }

  sql(options?: {
    dialect?: unknown
    pretty?: boolean
    identify?: boolean | "safe"
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

  and_(
    ...args: (string | Expression | null | undefined | BuilderOptions)[]
  ): Expression {
    return _combineConditions(this, args, _AndCtor)
  }

  or_(
    ...args: (string | Expression | null | undefined | BuilderOptions)[]
  ): Expression {
    return _combineConditions(this, args, _OrCtor)
  }

  not_(): Not {
    if (!_NotCtor) {
      throw new Error("Expression.not_() requires initialization")
    }
    return new _NotCtor({
      this: _wrap(_parseConditionExpression(this), _ConnectorCtor),
    })
  }

  eq(other: unknown): EQ {
    if (!_EQCtor) {
      throw new Error("Expression.eq() requires initialization")
    }
    return this._binop(_EQCtor, other) as EQ
  }

  neq(other: unknown): NEQ {
    if (!_NEQCtor) {
      throw new Error("Expression.neq() requires initialization")
    }
    return this._binop(_NEQCtor, other) as NEQ
  }

  is_(other: unknown): Is {
    if (!_IsCtor) {
      throw new Error("Expression.is_() requires initialization")
    }
    return this._binop(_IsCtor, other) as Is
  }

  like(other: unknown): Like {
    if (!_LikeCtor) {
      throw new Error("Expression.like() requires initialization")
    }
    return this._binop(_LikeCtor, other) as Like
  }

  ilike(other: unknown): ILike {
    if (!_ILikeCtor) {
      throw new Error("Expression.ilike() requires initialization")
    }
    return this._binop(_ILikeCtor, other) as ILike
  }

  rlike(other: unknown): RegexpLike {
    if (!_RegexpLikeCtor) {
      throw new Error("Expression.rlike() requires initialization")
    }
    return this._binop(_RegexpLikeCtor, other) as RegexpLike
  }

  isin(
    ...args: (
      | unknown
      | {
          query?: string | Expression
          unnest?: string | Expression | (string | Expression)[]
        }
    )[]
  ): In {
    if (!_InCtor || !_SubqueryCtor || !_UnnestCtor) {
      throw new Error("Expression.isin() requires initialization")
    }
    let query: Expression | undefined
    let unnestExpr: Expression | undefined
    const values: unknown[] = []
    for (const arg of args) {
      if (
        typeof arg === "object" &&
        arg !== null &&
        !(arg instanceof Expression)
      ) {
        const queryArg = "query" in arg ? arg.query : undefined
        if (queryArg !== undefined) {
          if (
            typeof queryArg !== "string" &&
            !(queryArg instanceof Expression)
          ) {
            throw new TypeError(
              "Expression.isin({ query }) expects a string or Expression",
            )
          }
          query = maybeParse(queryArg)
          if (!(query instanceof _SubqueryCtor)) {
            query = new _SubqueryCtor({ this: query })
          }
        }
        const unnestArg = "unnest" in arg ? arg.unnest : undefined
        if (unnestArg !== undefined) {
          const unnestList = Array.isArray(unnestArg) ? unnestArg : [unnestArg]
          unnestExpr = new _UnnestCtor({
            expressions: unnestList.map((expression: unknown) => {
              if (
                typeof expression !== "string" &&
                !(expression instanceof Expression)
              ) {
                throw new TypeError(
                  "Expression.isin({ unnest }) expects string or Expression values",
                )
              }
              return maybeParse(expression)
            }),
          })
        }
      } else {
        values.push(arg)
      }
    }
    return new _InCtor({
      this: this.copy(),
      expressions: values.map((value) => convert(value)),
      query,
      unnest: unnestExpr,
    })
  }

  between(
    low: unknown,
    high: unknown,
    options?: { symmetric?: boolean },
  ): Between {
    if (!_BetweenCtor) {
      throw new Error("Expression.between() requires initialization")
    }
    const result = new _BetweenCtor({
      this: this.copy(),
      low: convert(low),
      high: convert(high),
    })
    if (options?.symmetric !== undefined) {
      result.set("symmetric", options.symmetric)
    }
    return result
  }

  div(other: unknown, options?: { typed?: boolean; safe?: boolean }): Div {
    if (!_DivCtor) {
      throw new Error("Expression.div() requires initialization")
    }
    const result = this._binop(_DivCtor, other) as Div
    if (options?.typed) {
      result.set("typed", options.typed)
    }
    if (options?.safe) {
      result.set("safe", options.safe)
    }
    return result
  }

  desc(nullsFirst = false): Ordered {
    if (!_OrderedCtor) {
      throw new Error("Expression.desc() requires initialization")
    }
    return new _OrderedCtor({
      this: this,
      desc: true,
      nulls_first: nullsFirst,
    })
  }

  asc(nullsFirst = true): Ordered {
    if (!_OrderedCtor) {
      throw new Error("Expression.asc() requires initialization")
    }
    return new _OrderedCtor({ this: this, nulls_first: nullsFirst })
  }

  get parentSelect(): Select | undefined {
    if (!_SelectCtor) {
      return undefined
    }
    return this.findAncestor(_SelectCtor)
  }

  get unnestOperands(): Expression[] {
    return [...this.iterExpressions()].map((expression) => expression.unnest())
  }

  popComments(): string[] {
    const comments = this.comments ?? []
    this.comments = undefined
    return comments
  }

  get aliasColumnNames(): string[] {
    const tableAlias = this.args.alias
    if (!(tableAlias instanceof Expression)) {
      return []
    }
    const columns = tableAlias.args.columns
    if (!Array.isArray(columns)) {
      return []
    }
    return columns
      .filter((column): column is Expression => column instanceof Expression)
      .map((column) => column.name)
  }

  addComments(comments?: string[], prepend = false): void {
    if (this.comments === undefined) {
      this.comments = []
    }
    if (!comments) {
      return
    }
    for (const comment of comments) {
      const [, ...meta] = comment.split("SQLGLOT_META")
      if (meta.length > 0) {
        for (const kv of meta.join("").split(",")) {
          const [key, ...valueParts] = kv.split("=")
          const value =
            valueParts.length > 0 ? _toBool(valueParts[0]?.trim()) : true
          if (!this._meta) {
            this._meta = {}
          }
          const metaKey = key?.trim()
          if (metaKey) {
            this._meta[metaKey] = value
          }
        }
      }
      if (!prepend) {
        this.comments.push(comment)
      }
    }
    if (prepend) {
      this.comments = [...comments, ...this.comments]
    }
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
    let this_: Expression = this.copy()
    let other_: Expression = convert(other, true)
    if (!(this_ instanceof klass) && !(other_ instanceof klass)) {
      this_ = _wrap(this_, _BinaryCtor)
      other_ = _wrap(other_, _BinaryCtor)
    }
    return new klass({ this: this_, expression: other_ })
  }
}

export type ExpressionConstructor<T extends Expression = Expression> = new (
  args?: Args,
) => T

interface MaybeParseOptions {
  prefix?: string | undefined
  dialect?: string | undefined
  copy?: boolean | undefined
  into?: ExpressionConstructor | ExpressionClass | undefined
  parseAsExpression?: boolean | undefined
  extractFromPrefix?: boolean | undefined
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
  const extractFromPrefix = options?.extractFromPrefix
  if (prefix) {
    sql = `${prefix} ${sql}`
  }
  const dialect = options?.dialect
  const into = options?.into

  // parseAsExpression: parse as expression context (avoids statement parser for keywords like BEGIN)
  if (options?.parseAsExpression && !into && _ExpressionParser) {
    return Expression.parseImpl(sql, {
      ...(dialect ? { dialect } : {}),
      into: _ExpressionParser,
    })
  }

  const result = Expression.parseImpl(sql, {
    ...(dialect ? { dialect } : {}),
    ...(into ? { into } : {}),
  })

  if (extractFromPrefix && prefix) {
    const exprs = result.args["expressions"]
    if (Array.isArray(exprs) && exprs.length === 1) {
      return exprs[0] as Expression
    }
  }

  return result
}

// Late-bound Expression class reference for parseAsExpression
let _ExpressionParser: ExpressionClass | undefined
export function setExpressionParser(cls: ExpressionClass): void {
  _ExpressionParser = cls
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
    parseAsExpression?: boolean | undefined
    extractFromPrefix?: boolean | undefined
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

// Late-bound And/Connector/Paren/Binary constructors to avoid circular deps
let _AndCtor: ExpressionConstructor | undefined
let _OrCtor: ExpressionConstructor | undefined
let _ConnectorCtor: ExpressionConstructor | undefined
let _ParenCtor: ExpressionConstructor | undefined
let _BinaryCtor: ExpressionConstructor | undefined
let _BetweenCtor: ExpressionConstructor<Between> | undefined
let _DivCtor: ExpressionConstructor<Div> | undefined
let _EQCtor: ExpressionConstructor<EQ> | undefined
let _ILikeCtor: ExpressionConstructor<ILike> | undefined
let _InCtor: ExpressionConstructor<In> | undefined
let _IsCtor: ExpressionConstructor<Is> | undefined
let _LikeCtor: ExpressionConstructor<Like> | undefined
let _NEQCtor: ExpressionConstructor<NEQ> | undefined
let _NotCtor: ExpressionConstructor<Not> | undefined
let _OrderedCtor: ExpressionConstructor<Ordered> | undefined
let _RegexpLikeCtor: ExpressionConstructor<RegexpLike> | undefined
let _SelectCtor: ExpressionClass<Select> | undefined
let _SubqueryCtor: ExpressionConstructor<Subquery> | undefined
let _UnnestCtor: ExpressionConstructor<Unnest> | undefined

export function setAndConstructor(ctor: ExpressionConstructor): void {
  _AndCtor = ctor
}

export function setOrConstructor(ctor: ExpressionConstructor): void {
  _OrCtor = ctor
}

export function setConnectorConstructor(ctor: ExpressionConstructor): void {
  _ConnectorCtor = ctor
}

export function setParenConstructor(ctor: ExpressionConstructor): void {
  _ParenCtor = ctor
}

export function setBinaryConstructor(ctor: ExpressionConstructor): void {
  _BinaryCtor = ctor
}

export function setExpressionFluentConstructors(ctors: {
  between: ExpressionConstructor<Between>
  div: ExpressionConstructor<Div>
  eq: ExpressionConstructor<EQ>
  ilike: ExpressionConstructor<ILike>
  in_: ExpressionConstructor<In>
  is_: ExpressionConstructor<Is>
  like: ExpressionConstructor<Like>
  neq: ExpressionConstructor<NEQ>
  not_: ExpressionConstructor<Not>
  ordered: ExpressionConstructor<Ordered>
  regexpLike: ExpressionConstructor<RegexpLike>
  select: ExpressionClass<Select>
  subquery: ExpressionConstructor<Subquery>
  unnest: ExpressionConstructor<Unnest>
}): void {
  _BetweenCtor = ctors.between
  _DivCtor = ctors.div
  _EQCtor = ctors.eq
  _ILikeCtor = ctors.ilike
  _InCtor = ctors.in_
  _IsCtor = ctors.is_
  _LikeCtor = ctors.like
  _NEQCtor = ctors.neq
  _NotCtor = ctors.not_
  _OrderedCtor = ctors.ordered
  _RegexpLikeCtor = ctors.regexpLike
  _SelectCtor = ctors.select
  _SubqueryCtor = ctors.subquery
  _UnnestCtor = ctors.unnest
}

export function _wrap(
  expression: Expression,
  kind: ExpressionConstructor | undefined,
): Expression {
  if (kind && _ParenCtor && expression instanceof kind) {
    return new _ParenCtor({ this: expression })
  }
  return expression
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

export function convert(value: unknown, copy = false): Expression {
  if (value instanceof Expression) return copy ? value.copy() : value
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
  expressions: (string | Expression | boolean | null | undefined)[],
  instance: Expression,
  arg: string,
  options?: {
    append?: boolean | undefined
    copy?: boolean | undefined
    into?: ExpressionConstructor | undefined
    dialect?: string | undefined
  },
): Expression {
  const filtered: (string | Expression)[] = []
  for (const e of expressions) {
    if (e === undefined || e === null || e === "") continue
    if (typeof e === "boolean") {
      filtered.push(convert(e))
    } else {
      filtered.push(e)
    }
  }
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

  const parseOpts = options ? { dialect: options.dialect } : undefined
  const parsed = allExprs.map((e) => maybeParse(e, parseOpts))
  let node: Expression = parsed[0] as Expression
  if (_AndCtor) {
    if (parsed.length > 1 && _ConnectorCtor && _ParenCtor) {
      node =
        node instanceof _ConnectorCtor ? new _ParenCtor({ this: node }) : node
    }
    for (let i = 1; i < parsed.length; i++) {
      let expr = parsed[i] as Expression
      if (_ConnectorCtor && _ParenCtor && expr instanceof _ConnectorCtor) {
        expr = new _ParenCtor({ this: expr })
      }
      node = new _AndCtor({ this: node, expression: expr })
    }
  }

  inst.set(arg, into ? new into({ this: node }) : node)
  return inst
}

/**
 * Generate unique name by appending numbers if name is taken.
 * Example: "pos" → "pos_2" → "pos_3" if "pos" and "pos_2" are taken.
 */
export function camelToSnakeCase(s: string): string {
  return s.replace(/([a-z])([A-Z])/g, "$1_$2").toUpperCase()
}

function _parseConditionExpression(
  expression: string | Expression,
  options?: { dialect?: string; copy?: boolean },
): Expression {
  if (!_ExpressionParser) {
    throw new Error("Condition parsing requires initialization")
  }
  return maybeParse(expression, {
    into: _ExpressionParser,
    dialect: options?.dialect,
    copy: options?.copy,
  })
}

function _combineConditions(
  base: Expression,
  args: (string | Expression | null | undefined | BuilderOptions)[],
  operator: ExpressionConstructor | undefined,
): Expression {
  if (!operator) {
    throw new Error("Condition builder requires initialization")
  }
  let options: BuilderOptions | undefined
  const expressions: (string | Expression | null | undefined)[] = [base]
  for (const arg of args) {
    if (
      typeof arg === "object" &&
      arg !== null &&
      !(arg instanceof Expression)
    ) {
      options = arg
    } else {
      expressions.push(arg as string | Expression | null | undefined)
    }
  }
  const parsed = expressions
    .filter(
      (expression): expression is string | Expression =>
        expression !== null && expression !== undefined,
    )
    .map((expression) => {
      const parseOptions = {
        ...(options?.dialect !== undefined ? { dialect: options.dialect } : {}),
        ...(options?.copy !== undefined ? { copy: options.copy } : {}),
      }
      return _parseConditionExpression(expression, parseOptions)
    })

  let node = parsed[0]
  if (!node) {
    return base
  }
  if (
    parsed.length > 1 &&
    _ConnectorCtor &&
    _ParenCtor &&
    node instanceof _ConnectorCtor
  ) {
    node = new _ParenCtor({ this: node })
  }
  for (const parsedExpression of parsed.slice(1)) {
    let expression = parsedExpression
    if (_ConnectorCtor && _ParenCtor && expression instanceof _ConnectorCtor) {
      expression = new _ParenCtor({ this: expression })
    }
    node = new operator({ this: node, expression })
  }
  return node
}

function _toBool(
  value: string | boolean | null | undefined,
): string | boolean | null | undefined {
  if (typeof value === "boolean" || value === null || value === undefined) {
    return value
  }
  const lower = value.toLowerCase()
  if (lower === "true" || lower === "1") {
    return true
  }
  if (lower === "false" || lower === "0") {
    return false
  }
  return value
}

function _stringHash(s: string): number {
  let h = 0
  for (let i = 0; i < s.length; i++) {
    h = (Math.imul(31, h) + s.charCodeAt(i)) | 0
  }
  return h
}

function _combineHash(a: number, b: number): number {
  return (Math.imul(31, a) ^ b) | 0
}

function _argValueHash(v: ArgValue, foldCase: boolean): number {
  if (v instanceof Expression) {
    return v.hashCode()
  }
  if (typeof v === "string") {
    return _stringHash(foldCase ? v.toLowerCase() : v)
  }
  if (typeof v === "number") {
    return v | 0
  }
  if (typeof v === "boolean") {
    return v ? 1 : 0
  }
  return 0
}

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
