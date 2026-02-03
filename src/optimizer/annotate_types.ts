import type { Expression } from "../expression-base.js"
import {
  Alias,
  CTE,
  Column,
  ColumnDef,
  Identifier,
  Join,
  type Literal,
  PropertyEQ,
  Array as SQLArray,
  Select,
  Struct,
  TableAlias,
  Unnest,
} from "../expressions.generated.js"
import { DataType } from "../expressions.js"

const RETURN_TYPES: Map<string, string> = new Map([
  ["approxquantile", "DOUBLE"],
  ["avg", "DOUBLE"],
  ["cbrt", "DOUBLE"],
  ["cos", "DOUBLE"],
  ["exp", "DOUBLE"],
  ["ln", "DOUBLE"],
  ["log", "DOUBLE"],
  ["pow", "DOUBLE"],
  ["radians", "DOUBLE"],
  ["round", "DOUBLE"],
  ["safedivide", "DOUBLE"],
  ["sin", "DOUBLE"],
  ["sqrt", "DOUBLE"],
  ["stddev", "DOUBLE"],
  ["stddevpop", "DOUBLE"],
  ["stddevsamp", "DOUBLE"],
  ["tan", "DOUBLE"],
  ["toduble", "DOUBLE"],
  ["variance", "DOUBLE"],
  ["variancepop", "DOUBLE"],

  ["approxdistinct", "BIGINT"],
  ["arraysize", "BIGINT"],
  ["countif", "BIGINT"],

  ["ascii", "INT"],
  ["ceil", "INT"],
  ["count", "INT"],
  ["datediff", "INT"],
  ["length", "INT"],
  ["sign", "INT"],
  ["strposition", "INT"],

  ["boolean", "BOOLEAN"],
  ["between", "BOOLEAN"],
  ["in", "BOOLEAN"],
  ["exists", "BOOLEAN"],
  ["regexplike", "BOOLEAN"],

  ["currentdate", "DATE"],
  ["date", "DATE"],
  ["lastday", "DATE"],
  ["strtodate", "DATE"],
  ["tsordstodate", "DATE"],

  ["currentdatetime", "DATETIME"],
  ["datetime", "DATETIME"],
  ["datetimeadd", "DATETIME"],
  ["datetimesub", "DATETIME"],

  ["currenttime", "TIME"],
  ["time", "TIME"],
  ["timeadd", "TIME"],
  ["timesub", "TIME"],

  ["currenttimestamp", "TIMESTAMP"],
  ["strtotime", "TIMESTAMP"],
  ["timestampadd", "TIMESTAMP"],
  ["timestampsub", "TIMESTAMP"],
  ["unixtotime", "TIMESTAMP"],

  ["interval", "INTERVAL"],

  ["arraytostring", "VARCHAR"],
  ["concat", "VARCHAR"],
  ["concatws", "VARCHAR"],
  ["chr", "VARCHAR"],
  ["dpipe", "VARCHAR"],
  ["groupconcat", "VARCHAR"],
  ["initcap", "VARCHAR"],
  ["lower", "VARCHAR"],
  ["md5", "VARCHAR"],
  ["substring", "VARCHAR"],
  ["string", "VARCHAR"],
  ["timetostr", "VARCHAR"],
  ["trim", "VARCHAR"],
  ["tobase64", "VARCHAR"],
  ["upper", "VARCHAR"],
  ["replace", "VARCHAR"],

  ["null", "NULL"],
  ["parsejson", "JSON"],
])

function annotateStruct(expr: Struct): void {
  const colDefs: Expression[] = []
  for (const child of expr.expressions) {
    if (child instanceof PropertyEQ) {
      const key = child.args.this as Expression
      const value = child.args.expression as Expression
      const keyName =
        key instanceof Identifier
          ? (key.args.this as string)
          : String(key.args.this ?? "")
      colDefs.push(
        new ColumnDef({
          this: new Identifier({ this: keyName }),
          kind: value._type ?? new DataType({ this: "UNKNOWN" }),
        }),
      )
    }
  }
  expr._type = new DataType({ this: "STRUCT", expressions: colDefs })
}

function annotateArray(expr: SQLArray): void {
  const children = expr.expressions
  if (children.length > 0) {
    const firstType = children[0]?._type
    if (firstType) {
      expr._type = new DataType({ this: "ARRAY", expressions: [firstType] })
      return
    }
  }
  expr._type = new DataType({ this: "ARRAY" })
}

export function annotateTypes(expression: Expression): Expression {
  // Phase 1: Bottom-up type annotation
  const stack: [Expression, boolean][] = [[expression, false]]

  while (stack.length > 0) {
    const top = stack.pop()!
    const expr = top[0]
    const childrenDone = top[1]

    if (expr._type) continue

    if (!childrenDone) {
      stack.push([expr, true])
      for (const child of expr.iterExpressions()) {
        stack.push([child, false])
      }
      continue
    }

    const key = expr.key

    if (key === "cast" || key === "trycast") {
      const to = expr.args.to as Expression | undefined
      if (to) expr._type = to
      continue
    }

    if (key === "literal") {
      const lit = expr as Literal
      if (lit.isString) {
        expr._type = new DataType({ this: "VARCHAR" })
      } else {
        const val = String(expr.args.this)
        const isInt = !val.includes(".") && !val.toLowerCase().includes("e")
        expr._type = new DataType({ this: isInt ? "INT" : "DOUBLE" })
      }
      continue
    }

    if (expr instanceof Struct) {
      annotateStruct(expr)
      continue
    }

    if (expr instanceof SQLArray) {
      annotateArray(expr)
      continue
    }

    if (expr instanceof PropertyEQ) {
      const value = expr.args.expression as Expression | undefined
      expr._type = value?._type ?? new DataType({ this: "UNKNOWN" })
      continue
    }

    const returnType = RETURN_TYPES.get(key)
    if (returnType) {
      expr._type = new DataType({ this: returnType })
      continue
    }

    if (key === "alias" || key === "paren" || key === "neg") {
      const child = expr.args.this as Expression | undefined
      if (child?._type) {
        expr._type = child._type
      } else {
        expr._type = new DataType({ this: "UNKNOWN" })
      }
      continue
    }

    if (key === "not") {
      expr._type = new DataType({ this: "BOOLEAN" })
      continue
    }

    // Don't set UNKNOWN for complex nodes (Select, From, etc.) — leave them untyped
  }

  // Phase 2: Mini scope resolution for CTE columns and UNNEST aliases
  resolveScopes(expression)

  return expression
}

function resolveScopes(expression: Expression): void {
  // Step 1: Build CTE column type map (cte_alias_name → column_name → type)
  const cteTypes = new Map<string, Map<string, Expression>>()

  for (const node of expression.bfs()) {
    if (node instanceof CTE) {
      const alias = node.args.alias as TableAlias | undefined
      const aliasName = alias
        ? String(
            alias.args.this instanceof Identifier
              ? alias.args.this.args.this
              : (alias.args.this ?? ""),
          )
        : ""
      const select = node.args.this
      if (select instanceof Select && aliasName) {
        const columnTypes = new Map<string, Expression>()
        for (const selectExpr of select.expressions) {
          const name = selectExpr.alias || selectExpr.text("this")
          if (name && selectExpr._type) {
            columnTypes.set(name, selectExpr._type)
          }
        }
        cteTypes.set(aliasName, columnTypes)
      }
    }
  }

  if (cteTypes.size === 0) return

  // Step 2: Resolve UNNEST types and build alias map
  const unnestAliasTypes = new Map<string, Expression>()

  for (const node of expression.bfs()) {
    if (!(node instanceof Unnest)) continue

    // Try to resolve source column type from CTEs
    for (const sourceExpr of node.expressions) {
      if (!(sourceExpr instanceof Column)) continue
      const tableName = sourceExpr.text("table")
      const colName = sourceExpr.text("this")
      const cteColTypes = cteTypes.get(tableName)
      if (!cteColTypes) continue
      const sourceType = cteColTypes.get(colName)
      if (!sourceType || sourceType.text("this") !== "ARRAY") continue
      // Unwrap ARRAY → element type
      const elementType = (sourceType as DataType).expressions[0]
      if (elementType) {
        node._type = elementType
      }
    }

    if (!node._type) continue

    // Find the UNNEST's alias: check parent Join or From for alias info
    // The Unnest might be wrapped in an Alias, or the Join might have the alias
    const parent = node.parent
    if (parent instanceof Join) {
      // The alias is typically on the join's "this" which wraps the Unnest
      // Actually, the parser wraps UNNEST AS t(col) → the alias info might be on Unnest itself
      const unnestAlias = node.args.alias as TableAlias | undefined
      if (unnestAlias) {
        const aName = String(
          unnestAlias.args.this instanceof Identifier
            ? unnestAlias.args.this.args.this
            : (unnestAlias.args.this ?? ""),
        )
        if (aName) unnestAliasTypes.set(aName, node._type)
      }
    }
    // Also check if the UNNEST itself has alias directly
    if (!node.args.alias && parent) {
      // Check if parent has alias (e.g., Alias wrapping Unnest)
      if (parent instanceof Alias) {
        const aliasNode = parent.args.alias
        if (aliasNode instanceof TableAlias) {
          const aName = String(
            aliasNode.args.this instanceof Identifier
              ? aliasNode.args.this.args.this
              : (aliasNode.args.this ?? ""),
          )
          if (aName) unnestAliasTypes.set(aName, node._type)
        } else if (aliasNode instanceof Identifier) {
          const aName = String(aliasNode.args.this ?? "")
          if (aName) unnestAliasTypes.set(aName, node._type)
        }
      }
    }
  }

  if (unnestAliasTypes.size === 0) return

  // Step 3: Annotate columns that reference UNNEST aliases
  for (const node of expression.bfs()) {
    if (!(node instanceof Column) || node._type) continue
    const tableName = node.text("table")
    if (!tableName) continue
    const aliasType = unnestAliasTypes.get(tableName)
    if (aliasType) {
      node._type = aliasType
    }
  }
}
