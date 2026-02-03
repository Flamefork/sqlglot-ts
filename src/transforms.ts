import { findNewName } from "./expression-base.js"
import * as exp from "./expressions.js"
import type { Generator } from "./generator.js"
import { findAllInScope } from "./scope.js"

type Transform = (generator: Generator, expression: exp.Expression) => string

export class UnsupportedError extends Error {
  constructor(message: string) {
    super(message)
    this.name = "UnsupportedError"
  }
}

export function preprocess(
  transforms: ((expression: exp.Expression) => exp.Expression)[],
  generator?: Transform,
): Transform {
  return (self: Generator, expression: exp.Expression): string => {
    const expressionType = expression.constructor

    for (const transform of transforms) {
      expression = transform(expression)
    }

    if (generator) {
      return generator(self, expression)
    }

    const handlerKey = expression.key + "_sql"
    const sqlHandler = (self as unknown as Record<string, Function>)[handlerKey]
    if (sqlHandler) {
      return sqlHandler.call(self, expression)
    }

    const transformsHandler = (
      self as unknown as { transforms: Map<Function, Transform> }
    ).transforms.get(expression.constructor)
    if (transformsHandler) {
      if (expressionType === expression.constructor) {
        if (expression instanceof exp.Func) {
          return self.funcCall(
            (expression as exp.Func).name,
            (expression as exp.Func).expressions,
          )
        }
        throw new Error(
          `Expression type ${expression.constructor.name} requires a _sql method in order to be transformed.`,
        )
      }
      return transformsHandler(self, expression)
    }

    throw new Error(
      `Unsupported expression type ${expression.constructor.name}.`,
    )
  }
}

function tryLiteralNumber(e: exp.Expression): number | null {
  if (e instanceof exp.Literal && !e.isString) {
    return Number(e.args.this)
  }
  if (e instanceof exp.Neg) {
    const inner = e.args.this as exp.Expression
    if (inner instanceof exp.Literal && !inner.isString) {
      return -Number(inner.args.this)
    }
  }
  return null
}

export function sequenceSql(gen: Generator, e: exp.Expression): string {
  const expression = e as exp.GenerateSeries
  const start = expression.args.start as exp.Expression | undefined
  const end = expression.args.end as exp.Expression | undefined
  const step = expression.args.step as exp.Expression | undefined

  if (start && end && expression.args.is_end_exclusive) {
    const stepValue = step ?? exp.Literal.number(1)

    const startNum = tryLiteralNumber(start)
    const endNum = tryLiteralNumber(end)
    const stepNum = tryLiteralNumber(stepValue)

    if (startNum !== null && endNum !== null && stepNum !== null) {
      const isEmpty =
        stepNum === 0 ||
        (stepNum > 0 && startNum >= endNum) ||
        (stepNum < 0 && startNum <= endNum)
      if (isEmpty) {
        return gen.sql(new exp.Array({ expressions: [] }))
      }
      const adjustedEndNum = endNum - stepNum
      const adjustedEnd =
        adjustedEndNum < 0
          ? new exp.Neg({ this: exp.Literal.number(Math.abs(adjustedEndNum)) })
          : exp.Literal.number(adjustedEndNum)
      const sequenceArgs: exp.Expression[] = [start, adjustedEnd]
      if (step) sequenceArgs.push(step)
      return gen.funcCall("SEQUENCE", sequenceArgs)
    }

    const adjustedEnd = new exp.Paren({
      this: new exp.Sub({ this: end, expression: stepValue }),
    })
    const sequenceArgs = [start, adjustedEnd]
    if (step) sequenceArgs.push(step)
    const sequenceCall = new exp.Anonymous({
      this: "SEQUENCE",
      expressions: sequenceArgs,
    })

    let shouldReturnEmpty: exp.Expression
    if (stepNum !== null) {
      if (stepNum === 0) {
        return gen.sql(new exp.Array({ expressions: [] }))
      }
      shouldReturnEmpty =
        stepNum > 0
          ? new exp.LTE({ this: adjustedEnd.copy(), expression: start.copy() })
          : new exp.GTE({ this: adjustedEnd.copy(), expression: start.copy() })
    } else {
      const zero = exp.Literal.number(0)
      shouldReturnEmpty = new exp.Or({
        this: new exp.EQ({ this: stepValue.copy(), expression: zero.copy() }),
        expression: new exp.Or({
          this: new exp.And({
            this: new exp.GT({
              this: stepValue.copy(),
              expression: zero.copy(),
            }),
            expression: new exp.GTE({
              this: start.copy(),
              expression: adjustedEnd.copy(),
            }),
          }),
          expression: new exp.And({
            this: new exp.LT({
              this: stepValue.copy(),
              expression: zero.copy(),
            }),
            expression: new exp.LTE({
              this: start.copy(),
              expression: adjustedEnd.copy(),
            }),
          }),
        }),
      })
    }

    const ifExpr = new exp.If({
      this: shouldReturnEmpty,
      true: new exp.Array({ expressions: [] }),
      false: sequenceCall,
    })

    return gen.sql(ifExpr)
  }

  const args: exp.Expression[] = []
  if (start) args.push(start)
  if (end) args.push(end)
  if (step) args.push(step)
  return gen.funcCall("SEQUENCE", args)
}

export function eliminateSemiAndAntiJoins(
  expression: exp.Expression,
): exp.Expression {
  if (expression instanceof exp.Select) {
    const joins = expression.args.joins as exp.Join[] | undefined
    if (joins) {
      const toRemove: exp.Join[] = []
      for (const join of joins) {
        const on = join.args.on as exp.Expression | undefined
        const kind = join.args.kind as string | undefined
        if (on && (kind === "SEMI" || kind === "ANTI")) {
          const subquery = new exp.Select({
            expressions: [exp.Literal.number(1)],
            from_: new exp.From({
              this: join.args.this as exp.Expression,
            }),
            where: new exp.Where({ this: on }),
          })
          let exists: exp.Expression = new exp.Exists({ this: subquery })
          if (kind === "ANTI") {
            exists = new exp.Not({ this: exists })
          }

          toRemove.push(join)

          const existingWhere = expression.args.where as exp.Where | undefined
          if (existingWhere) {
            existingWhere.set(
              "this",
              new exp.And({
                this: existingWhere.args.this as exp.Expression,
                expression: exists,
              }),
            )
          } else {
            expression.set("where", new exp.Where({ this: exists }))
          }
        }
      }
      if (toRemove.length > 0) {
        expression.args.joins = joins.filter((j) => !toRemove.includes(j))
        if ((expression.args.joins as exp.Join[]).length === 0) {
          delete expression.args.joins
        }
      }
    }
  }
  return expression
}

/**
 * Convert Explode/Posexplode projections to UNNEST with GENERATE_ARRAY.
 * Used by BigQuery, Presto, Snowflake for cross-dialect UNNEST transpilation.
 *
 * @param indexOffset - 0 for BigQuery/Snowflake, 1 for Presto
 */
export function explodeProjectionToUnnest(
  indexOffset = 0,
): (expression: exp.Expression) => exp.Expression {
  return function _explodeProjectionToUnnest(
    expression: exp.Expression,
  ): exp.Expression {
    if (!(expression instanceof exp.Select)) {
      return expression
    }

    const takenSelectNames = new Set(expression.namedSelects)
    const takenSourceNames = collectSourceNames(expression)

    function newName(names: Set<string>, base: string): string {
      const name = findNewName(names, base)
      names.add(name)
      return name
    }

    const arrays: exp.Expression[] = []
    const seriesAlias = newName(takenSelectNames, "pos")
    const seriesTableAlias = newName(takenSourceNames, "_u")

    // Create: UNNEST(GENERATE_SERIES(indexOffset)) AS _u(pos)
    const unnestNode = new exp.Unnest({
      expressions: [
        new exp.GenerateSeries({ start: exp.Literal.number(indexOffset) }),
      ],
    })
    const tableAlias = new exp.TableAlias({
      this: exp.toIdentifier(seriesTableAlias),
      columns: [exp.toIdentifier(seriesAlias)],
    })
    unnestNode.set("alias", tableAlias)
    const series = unnestNode

    // Use list() copy because expression.selects mutates during loop
    const selectsList = [...expression.selects]
    for (const select of selectsList) {
      let explode = select.find(exp.Explode)

      if (explode) {
        let posAlias = ""
        let explodeAlias = ""
        let alias: exp.Expression

        if (select instanceof exp.Alias) {
          const aliasArg = select.args.alias
          explodeAlias =
            typeof aliasArg === "string"
              ? aliasArg
              : aliasArg instanceof exp.Identifier
                ? aliasArg.name
                : ""
          alias = select
        } else if (select instanceof exp.Aliases) {
          const exprsArg = select.expressions
          if (Array.isArray(exprsArg) && exprsArg.length >= 2) {
            posAlias = (exprsArg[0] as exp.Identifier).name
            explodeAlias = (exprsArg[1] as exp.Identifier).name
          }
          const thisArg = select.args.this as exp.Expression
          const aliasNode = exp.alias_(thisArg.copy(), "")
          alias = select.replace(aliasNode)
          const newExplode = alias.find(exp.Explode)
          if (!newExplode) {
            throw new Error("Expected Explode node after replacement")
          }
          explode = newExplode
        } else {
          const aliasNode = exp.alias_(select.copy(), "")
          alias = select.replace(aliasNode)
          // Re-find explode in the new alias
          const newExplode = alias.find(exp.Explode)
          if (!newExplode) {
            throw new Error("Expected Explode node after replacement")
          }
          explode = newExplode
        }

        const isPosexplode = explode instanceof exp.Posexplode
        let explodeArg = explode.this as exp.Expression

        if (explode instanceof exp.ExplodeOuter) {
          const bracket = explodeArg instanceof exp.Bracket ? explodeArg : null
          if (bracket) {
            bracket.set("safe", true)
            bracket.set("offset", true)
            const arraySizeFunc = exp.func(
              "ARRAY_SIZE",
              exp.func("COALESCE", explodeArg, new exp.Array({})),
            )
            const eqZero = new exp.EQ({
              this: arraySizeFunc,
              expression: exp.Literal.number(0),
            })
            const arrayExpr = new exp.Array({ expressions: [bracket] })
            explodeArg = exp.func("IF", eqZero, arrayExpr, explodeArg)
          }
        }

        // Prevent using Explode's argument as new selection
        if (explodeArg instanceof exp.Column) {
          takenSelectNames.add(explodeArg.outputName)
        }

        const unnestSourceAlias = newName(takenSourceNames, "_u")

        if (!explodeAlias) {
          explodeAlias = newName(takenSelectNames, "col")

          if (isPosexplode) {
            posAlias = newName(takenSelectNames, "pos")
          }
        }

        if (!posAlias) {
          posAlias = newName(takenSelectNames, "pos")
        }

        alias.set("alias", exp.toIdentifier(explodeAlias))

        const seriesCol = exp.column(seriesAlias, seriesTableAlias)
        const posCol = exp.column(posAlias, unnestSourceAlias)
        const eqCondition = new exp.EQ({ this: seriesCol, expression: posCol })
        const column = new exp.If({
          this: eqCondition,
          true: exp.column(explodeAlias, unnestSourceAlias),
        })

        explode.replace(column)

        if (isPosexplode) {
          const expressions = expression.expressions as exp.Expression[]
          const aliasIndex = expressions.indexOf(alias)
          const seriesCol2 = exp.column(seriesAlias, seriesTableAlias)
          const posCol2 = exp.column(posAlias, unnestSourceAlias)
          const eqCondition2 = new exp.EQ({
            this: seriesCol2,
            expression: posCol2,
          })
          const posIf = new exp.If({
            this: eqCondition2,
            true: exp.column(posAlias, unnestSourceAlias),
          })
          const posAliasExpr = exp.alias_(posIf, posAlias)
          expressions.splice(aliasIndex + 1, 0, posAliasExpr)
          expression.set("expressions", expressions)
        }

        if (arrays.length === 0) {
          if (expression.args.from_) {
            expression.join(series, { copy: false, joinType: "CROSS" })
          } else {
            expression.from(series, false)
          }
        }

        const size = new exp.ArraySize({ this: explodeArg.copy() })
        arrays.push(size)

        // Create: UNNEST(array) AS _u(col) WITH OFFSET AS pos
        const unnestExpr = new exp.Unnest({
          expressions: [explodeArg.copy()],
          offset: exp.toIdentifier(posAlias),
        })
        const tableAlias2 = new exp.TableAlias({
          this: exp.toIdentifier(unnestSourceAlias),
          columns: [exp.toIdentifier(explodeAlias)],
        })
        unnestExpr.set("alias", tableAlias2)

        expression.join(unnestExpr, { joinType: "CROSS", copy: false })

        let sizeExpr: exp.Expression = size
        if (indexOffset !== 1) {
          sizeExpr = new exp.Sub({
            this: size,
            expression: exp.Literal.number(1),
          })
        }

        const wrapBinary = (e: exp.Expression): exp.Expression =>
          e instanceof exp.Binary ? new exp.Paren({ this: e }) : e

        const seriesCol3 = exp.column(seriesAlias, seriesTableAlias)
        const posCol3 = exp.column(posAlias, unnestSourceAlias)
        const eqCond = new exp.EQ({ this: seriesCol3, expression: posCol3 })
        const gtCond = new exp.GT({
          this: wrapBinary(seriesCol3.copy()),
          expression: wrapBinary(sizeExpr),
        })
        const eqSizeCond = new exp.EQ({
          this: wrapBinary(posCol3.copy()),
          expression: wrapBinary(sizeExpr.copy()),
        })
        const andCond = new exp.And({ this: gtCond, expression: eqSizeCond })
        const orCond = new exp.Or({
          this: eqCond,
          expression: new exp.Paren({ this: andCond }),
        })

        // Manually append to WHERE (copy=false in Python)
        const existingWhere = expression.args.where as exp.Where | undefined
        if (existingWhere) {
          const existing = existingWhere.args.this as exp.Expression
          const wrappedExisting =
            existing instanceof exp.And || existing instanceof exp.Or
              ? new exp.Paren({ this: existing })
              : existing
          const newCondition = new exp.And({
            this: wrappedExisting,
            expression: new exp.Paren({ this: orCond }),
          })
          existingWhere.set("this", newCondition)
        } else {
          expression.set("where", new exp.Where({ this: orCond }))
        }
      }
    }

    if (arrays.length > 0) {
      let end: exp.Expression = new exp.Greatest({
        this: arrays[0],
        expressions: arrays.slice(1),
      })

      if (indexOffset !== 1) {
        end = new exp.Sub({
          this: end,
          expression: exp.Literal.number(1 - indexOffset),
        })
      }
      ;(series.expressions[0] as exp.GenerateSeries).set("end", end)
    }

    return expression
  }
}

/**
 * Collect table/subquery aliases from FROM and JOIN clauses.
 * Lightweight alternative to Python's Scope(expression).references.
 */
function collectSourceNames(select: exp.Select): Set<string> {
  const names = new Set<string>()

  const from = select.args.from_ as exp.From | undefined
  if (from && from.args.this instanceof exp.Expression) {
    const alias = extractAlias(from.args.this)
    if (alias) names.add(alias)
  }

  const joins = select.args.joins as exp.Join[] | undefined
  if (joins) {
    for (const join of joins) {
      if (join.args.this instanceof exp.Expression) {
        const alias = extractAlias(join.args.this)
        if (alias) names.add(alias)
      }
    }
  }

  return names
}

function extractAlias(expr: exp.Expression): string | null {
  if (expr instanceof exp.Alias) {
    const aliasNode = expr.args.alias
    if (typeof aliasNode === "string") return aliasNode
    if (aliasNode instanceof exp.Identifier) return aliasNode.name
    return null
  }
  if (expr instanceof exp.Table) {
    const tableAlias = expr.args.alias as exp.TableAlias | undefined
    if (tableAlias) {
      const aliasId = tableAlias.args.this as exp.Identifier | undefined
      return aliasId ? aliasId.name : null
    }
    const tableName = expr.args.this as exp.Identifier | undefined
    if (tableName) return tableName.name
  }
  if (expr instanceof exp.Subquery) {
    const tableAlias = expr.args.alias as exp.TableAlias | undefined
    if (tableAlias) {
      const aliasId = tableAlias.args.this as exp.Identifier | undefined
      return aliasId ? aliasId.name : null
    }
  }
  return null
}

/**
 * Eliminates the WINDOW clause by inlining each named window.
 *
 * Example:
 *   SELECT x, SUM(y) OVER w FROM t WINDOW w AS (PARTITION BY x)
 *   becomes:
 *   SELECT x, SUM(y) OVER (PARTITION BY x) FROM t
 */
export function eliminateWindowClause(
  expression: exp.Expression,
): exp.Expression {
  if (!(expression instanceof exp.Select)) {
    return expression
  }

  const windowsArg = expression.args.windows as exp.WindowSpec[] | undefined
  if (!windowsArg || windowsArg.length === 0) {
    return expression
  }

  // Remove the WINDOW clause from the Select
  expression.set("windows", null)

  // Build map of window name -> window spec
  const windowExpression = new Map<string, exp.WindowSpec>()

  /**
   * Inline inherited window spec into a Window node.
   * If the window has an "alias" arg that references a named window,
   * copy that window's partition_by, order, and spec args.
   */
  function inlineInheritedWindow(window: exp.Expression): void {
    const aliasArg = window.args.alias
    if (!aliasArg) {
      return
    }

    // Extract alias reference (can be string or Identifier)
    let alias: string
    if (typeof aliasArg === "string") {
      alias = aliasArg.toLowerCase()
    } else if (aliasArg instanceof exp.Identifier) {
      alias = aliasArg.name.toLowerCase()
    } else {
      return
    }

    const inheritedWindow = windowExpression.get(alias)
    if (!inheritedWindow) {
      return
    }

    // Clear the "alias" reference
    window.set("alias", null)

    // Copy partition_by, order, and spec from inherited window
    for (const key of ["partition_by", "order", "spec"] as const) {
      const arg = inheritedWindow.args[key]
      if (arg instanceof exp.Expression) {
        window.set(key, arg.copy())
      } else if (Array.isArray(arg) && arg.length > 0) {
        window.set(
          key,
          arg.map((e: exp.Expression) => e.copy()),
        )
      }
    }
  }

  // First pass: process named window definitions
  for (const windowDef of windowsArg) {
    inlineInheritedWindow(windowDef)

    // Extract window name from "this" arg
    const thisArg = windowDef.args.this
    let name: string
    if (typeof thisArg === "string") {
      name = thisArg.toLowerCase()
    } else if (thisArg instanceof exp.Identifier) {
      name = thisArg.name.toLowerCase()
    } else {
      continue
    }

    windowExpression.set(name, windowDef)
  }

  // Second pass: inline into Window expressions in the query
  for (const window of findAllInScope(expression, [exp.Window], true)) {
    inlineInheritedWindow(window)
  }

  return expression
}

/**
 * Convert SELECT statements that contain the QUALIFY clause into subqueries, filtered equivalently.
 *
 * The idea behind this transformation can be seen in Snowflake's documentation for QUALIFY:
 * https://docs.snowflake.com/en/sql-reference/constructs/qualify
 *
 * Some dialects don't support window functions in the WHERE clause, so we need to include them as
 * projections in the subquery, in order to refer to them in the outer filter using aliases. Also,
 * if a column is referenced in the QUALIFY clause but is not selected, we need to include it too,
 * otherwise we won't be able to refer to it in the outer query's WHERE clause. Finally, if a
 * newly aliased projection is referenced in the QUALIFY clause, it will be replaced by the
 * corresponding expression to avoid creating invalid column references.
 */
export function eliminateQualify(expression: exp.Expression): exp.Expression {
  if (!(expression instanceof exp.Select)) {
    return expression
  }

  const qualifyArg = expression.args["qualify"]
  if (!(qualifyArg instanceof exp.Expression)) {
    return expression
  }

  // Ensure all selects have aliases
  const taken = new Set(expression.namedSelects)
  for (const select of expression.selects) {
    if (!select.aliasOrName) {
      const alias = findNewName(taken, "_c")
      select.replace(exp.alias_(select, alias))
      taken.add(alias)
    }
  }

  // Build outer SELECT that references inner columns by alias
  function selectAliasOrName(select: exp.Expression): string | exp.Column {
    const aliasOrName = select.aliasOrName
    const identifier = select.args["alias"] || select.args["this"]
    if (identifier instanceof exp.Identifier) {
      const quoted = identifier.args["quoted"]
      return new exp.Column({
        this: new exp.Identifier({
          this: aliasOrName,
          quoted: quoted,
        }),
      })
    }
    return aliasOrName
  }

  const outerSelectExprs = expression.selects.map(selectAliasOrName)
  const outerSelects = exp.select(...outerSelectExprs)

  // Pop qualify filters from the inner query
  let qualifyFilters = qualifyArg.pop().args["this"] as exp.Expression

  // Build map of alias -> expression for column replacement
  const expressionByAlias = new Map<string, exp.Expression>()
  for (const select of expression.selects) {
    if (select instanceof exp.Alias) {
      const aliasNode = select.args["alias"]
      const aliasName =
        typeof aliasNode === "string"
          ? aliasNode
          : aliasNode instanceof exp.Identifier
            ? aliasNode.name
            : ""
      if (aliasName) {
        expressionByAlias.set(aliasName, select.args["this"] as exp.Expression)
      }
    }
  }

  // Find Window and Column nodes in qualify filters
  const selectCandidates = expression.isStar
    ? [exp.Window]
    : [exp.Window, exp.Column]

  const candidateNodes: exp.Expression[] = []
  for (const type of selectCandidates) {
    candidateNodes.push(...qualifyFilters.findAll(type as any))
  }

  for (const selectCandidate of candidateNodes) {
    if (selectCandidate instanceof exp.Window) {
      // Replace column references in window with their expressions
      if (expressionByAlias.size > 0) {
        for (const column of selectCandidate.findAll(exp.Column)) {
          const expr = expressionByAlias.get(column.name)
          if (expr) {
            column.replace(expr.copy())
          }
        }
      }

      // Add window to inner select with new alias
      const alias = findNewName(new Set(expression.namedSelects), "_w")
      const column = exp.column(alias)

      // Replace window in qualify filters first (before moving it)
      if (selectCandidate.parent instanceof exp.Qualify) {
        qualifyFilters = column
      } else {
        selectCandidate.replace(column)
      }

      // Now add the window (which has been replaced) to select
      const currentExprs = expression.args["expressions"] as exp.Expression[]
      expression.args["expressions"] = [
        ...currentExprs,
        exp.alias_(selectCandidate, alias),
      ]
    } else if (selectCandidate instanceof exp.Column) {
      // Add column to select if not already there
      if (!expression.namedSelects.includes(selectCandidate.name)) {
        const currentExprs = expression.args["expressions"] as exp.Expression[]
        expression.args["expressions"] = [
          ...currentExprs,
          selectCandidate.copy(),
        ]
      }
    }
  }

  // Wrap in subquery and add WHERE clause
  return outerSelects
    .from(expression.subquery("_t"), false)
    .where(qualifyFilters)
}

/**
 * Convert CROSS JOIN UNNEST into LATERAL VIEW EXPLODE.
 * Used by Hive and Spark dialects.
 *
 * Example:
 *   SELECT * FROM t CROSS JOIN UNNEST(arr) AS u(col)
 *   becomes:
 *   SELECT * FROM t LATERAL VIEW EXPLODE(arr) u AS col
 */
export function unnestToExplode(
  expression: exp.Expression,
  unnestUsingArraysZip = true,
): exp.Expression {
  if (!(expression instanceof exp.Select)) {
    return expression
  }

  // Helper: convert multiple UNNEST expressions to ARRAYS_ZIP if needed
  function unnestZipExprs(
    u: exp.Unnest,
    unnestExprs: exp.Expression[],
    hasMultiExpr: boolean,
  ): exp.Expression[] {
    if (hasMultiExpr) {
      if (!unnestUsingArraysZip) {
        throw new UnsupportedError(
          "Cannot transpile UNNEST with multiple input arrays",
        )
      }

      // Use INLINE(ARRAYS_ZIP(...)) for multiple expressions
      const zipExprs: exp.Expression[] = [
        new exp.Anonymous({ this: "ARRAYS_ZIP", expressions: unnestExprs }),
      ]
      u.set("expressions", zipExprs)
      return zipExprs
    }
    return unnestExprs
  }

  // Helper: determine UDTF type (Explode, Posexplode, or Inline)
  function udtfType(
    u: exp.Unnest,
    hasMultiExpr: boolean,
  ): typeof exp.Explode | typeof exp.Posexplode | typeof exp.Inline {
    if (u.args.offset) {
      return exp.Posexplode
    }
    return hasMultiExpr ? exp.Inline : exp.Explode
  }

  // Handle FROM UNNEST case
  const from = expression.args.from_ as exp.From | undefined
  if (from && from.args.this instanceof exp.Unnest) {
    const unnest = from.args.this
    const alias = unnest.args.alias as exp.TableAlias | undefined
    const exprs = unnest.expressions
    const hasMultiExpr = exprs.length > 1
    const [first] = unnestZipExprs(unnest, exprs, hasMultiExpr)

    const aliasColumns = alias?.args.columns
    const columns: exp.Expression[] = Array.isArray(aliasColumns)
      ? [...aliasColumns]
      : []
    const offset = unnest.args.offset
    if (offset) {
      const offsetId =
        offset instanceof exp.Identifier ? offset : exp.toIdentifier("pos")
      columns.unshift(offsetId)
    }

    // Replace UNNEST with Table(EXPLODE/POSEXPLODE/INLINE)
    const udtfClass = udtfType(unnest, hasMultiExpr)
    const udtfInstance = new udtfClass({ this: first })
    const table = new exp.Table({
      this: udtfInstance,
      alias: alias
        ? new exp.TableAlias({
            this: alias.args.this as exp.Identifier,
            columns,
          })
        : undefined,
    })
    unnest.replace(table)
  }

  // Handle CROSS JOIN UNNEST case
  const joins = expression.args.joins as exp.Join[] | undefined
  if (joins) {
    const toRemove: exp.Join[] = []

    for (const join of joins) {
      const joinExpr = join.args.this as exp.Expression

      const isLateral = joinExpr instanceof exp.Lateral
      const unnest = isLateral ? (joinExpr as exp.Lateral).args.this : joinExpr

      if (unnest instanceof exp.Unnest) {
        const alias = isLateral
          ? (joinExpr.args.alias as exp.TableAlias | undefined)
          : (unnest.args.alias as exp.TableAlias | undefined)

        const exprs = unnest.expressions
        const hasMultiExpr = exprs.length > 1
        const zippedExprs = unnestZipExprs(unnest, exprs, hasMultiExpr)

        toRemove.push(join)

        const aliasColumns = alias?.args.columns
        let aliasCols: exp.Expression[] = Array.isArray(aliasColumns)
          ? [...aliasColumns]
          : []

        // Spark LATERAL VIEW EXPLODE requires 1 or 2 aliases
        if (!hasMultiExpr && aliasCols.length !== 1 && aliasCols.length !== 2) {
          throw new UnsupportedError(
            "CROSS JOIN UNNEST to LATERAL VIEW EXPLODE transformation requires explicit column aliases",
          )
        }

        const offset = unnest.args.offset
        if (offset) {
          const offsetId =
            offset instanceof exp.Identifier ? offset : exp.toIdentifier("pos")
          // Deduplicate by removing duplicates (Python behavior)
          const seen = new Set<string>()
          const dedupedCols: exp.Expression[] = []
          for (const col of [offsetId, ...aliasCols]) {
            const colName =
              col instanceof exp.Identifier ? col.name : String(col)
            if (!seen.has(colName)) {
              seen.add(colName)
              dedupedCols.push(col)
            }
          }
          aliasCols = dedupedCols
        }

        // Create LATERAL VIEW for each expression
        for (const e of zippedExprs) {
          const udtfClass = udtfType(unnest, hasMultiExpr)
          const udtfInstance = new udtfClass({ this: e })

          const lateral = new exp.Lateral({
            this: udtfInstance,
            view: true,
            alias: new exp.TableAlias({
              this: alias?.args.this as exp.Identifier,
              columns: aliasCols,
            }),
          })

          // Append to laterals array
          const existingLaterals = expression.args.laterals as
            | exp.Lateral[]
            | undefined
          if (existingLaterals) {
            existingLaterals.push(lateral)
          } else {
            expression.set("laterals", [lateral])
          }
        }
      }
    }

    // Remove processed joins
    if (toRemove.length > 0) {
      const newJoins = joins.filter((j) => !toRemove.includes(j))
      if (newJoins.length === 0) {
        expression.set("joins", null)
      } else {
        expression.set("joins", newJoins)
      }
    }
  }

  return expression
}

/**
 * Convert SELECT DISTINCT ON statements to a subquery with a window function.
 *
 * This is useful for dialects that don't support SELECT DISTINCT ON but support window functions.
 *
 * Example:
 *   SELECT DISTINCT ON (a) b, c FROM t ORDER BY a, d
 *   becomes:
 *   SELECT b, c FROM (
 *     SELECT b, c, ROW_NUMBER() OVER (PARTITION BY a ORDER BY a, d) AS _row_number
 *     FROM t
 *   ) AS _t
 *   WHERE _row_number = 1
 */
export function eliminateDistinctOn(
  expression: exp.Expression,
): exp.Expression {
  if (!(expression instanceof exp.Select)) {
    return expression
  }

  const distinctArg = expression.args["distinct"]
  if (!(distinctArg instanceof exp.Expression)) {
    return expression
  }

  const onArg = distinctArg.args["on"]
  if (!(onArg instanceof exp.Tuple)) {
    return expression
  }

  const rowNumberWindowAlias = findNewName(
    new Set(expression.namedSelects),
    "_row_number",
  )

  const distinctCols = onArg.expressions

  // Remove the DISTINCT ON clause
  distinctArg.pop()

  // Build ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
  const window = new exp.Window({
    this: new exp.RowNumber({}),
    partition_by: distinctCols,
  })

  // Get or build ORDER BY clause
  const orderArg = expression.args["order"]
  if (orderArg instanceof exp.Expression) {
    window.set("order", orderArg.pop())
  } else {
    window.set(
      "order",
      new exp.Order({
        expressions: distinctCols.map((c) => c.copy()),
      }),
    )
  }

  // Add the window function as a column to the select
  const windowAlias = exp.alias_(window, rowNumberWindowAlias)
  const currentExprs = expression.args["expressions"] as exp.Expression[]
  expression.args["expressions"] = [...currentExprs, windowAlias]

  // Add aliases to all projections (except the window function we just added)
  const newSelects: exp.Expression[] = []
  const takenNames = new Set([rowNumberWindowAlias])

  // Use currentExprs (before we added window) for the loop
  for (let i = 0; i < currentExprs.length; i++) {
    let selectExpr = currentExprs[i]
    if (!selectExpr) continue

    if (selectExpr.isStar) {
      newSelects.push(new exp.Star({}))
      break
    }

    if (!(selectExpr instanceof exp.Alias)) {
      const aliasName = findNewName(takenNames, selectExpr.outputName || "_col")
      const quoted =
        selectExpr instanceof exp.Column ? selectExpr.args["quoted"] : undefined
      selectExpr = selectExpr.replace(
        exp.alias_(selectExpr, aliasName),
      ) as exp.Expression
      if (quoted !== undefined && selectExpr instanceof exp.Alias) {
        const aliasId = selectExpr.args["alias"]
        if (aliasId instanceof exp.Identifier) {
          aliasId.set("quoted", quoted)
        }
      }
    }

    takenNames.add(selectExpr.outputName)
    const aliasNode = (selectExpr as exp.Alias).args["alias"]
    if (aliasNode instanceof exp.Expression) {
      newSelects.push(aliasNode)
    }
  }

  // Wrap in subquery and filter
  return exp
    .select(...newSelects)
    .from(expression.subquery("_t"))
    .where(
      new exp.EQ({
        this: exp.column(rowNumberWindowAlias),
        expression: exp.Literal.number(1),
      }),
    )
}

/**
 * Strips precision from parameterized types in non-DDL contexts.
 * Used by BigQuery, Presto.
 */
export function removePrecisionParameterizedTypes(
  expression: exp.Expression,
): exp.Expression {
  for (const node of expression.findAll(exp.DataType)) {
    const dataType = node as exp.DataType
    const expressions = dataType.expressions
    if (expressions && expressions.length > 0) {
      dataType.set(
        "expressions",
        expressions.filter((e) => !(e instanceof exp.DataTypeSize)),
      )
    }
  }
  return expression
}

/**
 * Removes table qualifications from UNNEST columns.
 * E.g., `t.col` inside UNNEST scope → `col`.
 * Used by BigQuery, Redshift.
 */
export function unqualifyUnnest(expression: exp.Expression): exp.Expression {
  if (!(expression instanceof exp.Select)) {
    return expression
  }

  const unnestAliases = new Set<string>()
  for (const unnest of findAllInScope(expression, [exp.Unnest])) {
    const parent = unnest.parent
    if (parent instanceof exp.From || parent instanceof exp.Join) {
      // Get alias from TableAlias (unnest.args.alias.this)
      const aliasArg = unnest.args.alias
      if (aliasArg instanceof exp.TableAlias) {
        const aliasId = aliasArg.args.this
        if (aliasId instanceof exp.Identifier) {
          const aliasName = aliasId.args.this as string
          if (aliasName) {
            unnestAliases.add(aliasName)
          }
        }
      }
    }
  }

  if (unnestAliases.size > 0) {
    for (const column of expression.findAll(exp.Column)) {
      const col = column as exp.Column
      // Python: leftmost_part = column.parts[0]
      // parts = [catalog, db, table, this] (in that order, skipping undefined)
      // Find the leftmost part (first non-undefined qualifier)
      for (const partKey of ["catalog", "db", "table"] as const) {
        const part = col.args[partKey]
        if (part instanceof exp.Identifier) {
          const name = part.args.this as string
          // Python: if leftmost_part.arg_key != "this" and leftmost_part.this in unnest_aliases
          // arg_key != "this" is guaranteed since we're checking catalog/db/table
          if (unnestAliases.has(name)) {
            part.pop()
          }
          // Only check the leftmost part
          break
        }
      }
    }
  }

  return expression
}

/**
 * Adds explicit column names to recursive CTEs if they're missing.
 * Used by Presto.
 */
export function addRecursiveCteColumnNames(
  expression: exp.Expression,
): exp.Expression {
  if (!(expression instanceof exp.With) || !expression.args.recursive) {
    return expression
  }

  let counter = 0
  const nextName = () => `_c_${counter++}`

  const ctes = expression.expressions
  for (const cte of ctes) {
    if (!(cte instanceof exp.CTE)) continue

    const aliasArg = cte.args.alias
    if (!(aliasArg instanceof exp.TableAlias)) continue

    const columns = aliasArg.args.columns
    if (Array.isArray(columns) && columns.length > 0) continue

    let query = cte.args.this as exp.Expression
    if (
      query instanceof exp.Union ||
      query instanceof exp.Intersect ||
      query instanceof exp.Except
    ) {
      query = query.args.this as exp.Expression
    }

    if (!(query instanceof exp.Select)) continue

    const selects = query.selects
    const newColumns = selects.map((s) =>
      exp.toIdentifier(s.aliasOrName || nextName()),
    )
    aliasArg.set("columns", newColumns)
  }

  return expression
}

/**
 * Converts `ANY(subquery)` to `EXISTS(subquery)` with lambda.
 * E.g., `5 > ANY(tbl.col)` → `EXISTS(tbl.col, x -> x < 5)`.
 * Used by Hive, Spark, Databricks.
 */
export function anyToExists(expression: exp.Expression): exp.Expression {
  if (!(expression instanceof exp.Select)) {
    return expression
  }

  for (const anyExpr of expression.findAll(exp.Any)) {
    const thisArg = anyExpr.args.this as exp.Expression | undefined
    if (!thisArg) continue

    if (thisArg instanceof exp.Query) continue

    const parent = anyExpr.parent
    if (parent instanceof exp.Like || parent instanceof exp.ILike) continue

    if (parent instanceof exp.Binary) {
      const lambdaArg = exp.toIdentifier("x")
      anyExpr.replace(lambdaArg)
      const lambdaExpr = new exp.Lambda({
        this: parent.copy(),
        expressions: [lambdaArg],
      })
      parent.replace(
        new exp.Exists({
          this: thisArg.unnest(),
          expression: lambdaExpr,
        }),
      )
    }
  }

  return expression
}

export function moveCTEsToTopLevel<E extends exp.Expression>(expression: E): E {
  let topLevelWith = expression.args.with_ as exp.With | undefined

  for (const innerWith of expression.findAll(exp.With)) {
    if (innerWith.parent === expression) {
      continue
    }

    if (!topLevelWith) {
      topLevelWith = innerWith.pop() as exp.With
      expression.set("with_", topLevelWith)
    } else {
      if (innerWith.args.recursive) {
        topLevelWith.set("recursive", true)
      }

      const parentCTE = innerWith.findAncestor(exp.CTE)
      innerWith.pop()

      if (parentCTE) {
        const i = topLevelWith.expressions!.indexOf(parentCTE as exp.CTE)
        topLevelWith.expressions!.splice(i, 0, ...innerWith.expressions!)
        topLevelWith.set("expressions", topLevelWith.expressions)
      } else {
        topLevelWith.set("expressions", [
          ...topLevelWith.expressions!,
          ...innerWith.expressions!,
        ])
      }
    }
  }

  return expression
}

const NUMERIC_TYPES = [
  "TINYINT",
  "SMALLINT",
  "INT",
  "BIGINT",
  "DECIMAL",
  "FLOAT",
  "DOUBLE",
]

export function ensureBools(expression: exp.Expression): exp.Expression {
  const replaceFunc = (node: exp.Expression | undefined): void => {
    if (!(node instanceof exp.Expression)) return

    const isNumber = node instanceof exp.Literal && node.isNumber
    const isNumericType =
      !(node instanceof exp.SubqueryPredicate) &&
      node.type instanceof exp.DataType &&
      typeof node.type.args.this === "string" &&
      NUMERIC_TYPES.includes(node.type.args.this)
    const isUntypedColumn = node instanceof exp.Column && !node.type

    if (isNumber || isNumericType || isUntypedColumn) {
      const parent = node.parent
      const argKey = node.argKey
      const index = node.index

      const neq = new exp.NEQ({ this: node, expression: exp.Literal.number(0) })

      if (parent && argKey) {
        if (index !== undefined) {
          ;(parent.args[argKey] as exp.Expression[])[index] = neq
        } else {
          parent.args[argKey] = neq
        }
        parent.setParentForValue(argKey, neq, index)
      }
    }
  }

  for (const node of expression.walk()) {
    if (node instanceof exp.Connector) {
      replaceFunc(node.left)
      replaceFunc(node.right)
    } else if (node instanceof exp.Not) {
      replaceFunc(node.args.this as exp.Expression | undefined)
    } else if (
      node instanceof exp.If &&
      !(node.parent instanceof exp.Case && (node.parent as exp.Case).args.this)
    ) {
      replaceFunc(node.args.this as exp.Expression | undefined)
    } else if (node instanceof exp.Where || node instanceof exp.Having) {
      replaceFunc(node.args.this as exp.Expression | undefined)
    }
  }

  return expression
}

export function regexpReplaceGlobalModifier(
  expression: exp.RegexpReplace,
): exp.Expression | undefined {
  const modifiers = expression.args.modifiers as exp.Expression | undefined
  const singleReplace = expression.args.single_replace
  const occurrence = expression.args.occurrence as exp.Expression | undefined

  if (
    !singleReplace &&
    (!occurrence ||
      (occurrence instanceof exp.Literal &&
        occurrence.is_int &&
        Number(occurrence.value) === 0))
  ) {
    if (
      !modifiers ||
      (modifiers instanceof exp.Literal && modifiers.is_string)
    ) {
      const value = modifiers ? String((modifiers as exp.Literal).value) : ""
      return exp.Literal.string(value + "g")
    }
  }

  return modifiers
}

export function regexpReplaceSql(gen: Generator, e: exp.Expression): string {
  const expr = e as exp.RegexpReplace
  return gen.funcCall(
    "REGEXP_REPLACE",
    [
      expr.args.this as exp.Expression,
      expr.args.expression as exp.Expression,
      expr.args.replacement as exp.Expression,
    ].filter((x): x is exp.Expression => x != null),
  )
}

export function addWithinGroupForPercentiles(
  expression: exp.Expression,
): exp.Expression {
  if (
    (expression instanceof exp.PercentileCont ||
      expression instanceof exp.PercentileDisc) &&
    !(expression.parent instanceof exp.WithinGroup) &&
    expression.args.expression
  ) {
    const column = (expression.args.this as exp.Expression).pop()
    expression.set("this", (expression.args.expression as exp.Expression).pop())
    const order = new exp.Order({
      expressions: [new exp.Ordered({ this: column })],
    })
    return new exp.WithinGroup({ this: expression, expression: order })
  }
  return expression
}

export function removeWithinGroupForPercentiles(
  expression: exp.Expression,
): exp.Expression {
  if (
    expression instanceof exp.WithinGroup &&
    (expression.args.this instanceof exp.PercentileCont ||
      expression.args.this instanceof exp.PercentileDisc) &&
    expression.args.expression instanceof exp.Order
  ) {
    const quantile = (expression.args.this as exp.Expression).args
      .this as exp.Expression
    const ordered = expression.find(exp.Ordered)
    if (ordered) {
      const inputValue = (ordered as exp.Ordered).args.this as exp.Expression
      return expression.replace(
        new exp.ApproxQuantile({ this: inputValue, quantile }),
      )
    }
  }
  return expression
}

export function epochCastToTs(expression: exp.Expression): exp.Expression {
  if (
    (expression instanceof exp.Cast || expression instanceof exp.TryCast) &&
    expression.name.toLowerCase() === "epoch" &&
    expression.args.to instanceof exp.DataType &&
    (exp.DataType.TEMPORAL_TYPES as Set<string>).has(
      expression.args.to.text("this"),
    )
  ) {
    ;(expression.args.this as exp.Expression).replace(
      exp.Literal.string("1970-01-01 00:00:00"),
    )
  }
  return expression
}

export function unqualifyColumns(expression: exp.Expression): exp.Expression {
  for (const column of expression.findAll(exp.Column)) {
    for (const partKey of ["catalog", "db", "table"] as const) {
      const part = column.args[partKey]
      if (part instanceof exp.Expression) {
        part.pop()
      }
    }
  }
  return expression
}

export function removeUniqueConstraints(
  expression: exp.Expression,
): exp.Expression {
  for (const constraint of expression.findAll(exp.UniqueColumnConstraint)) {
    if (constraint.parent) {
      constraint.parent.pop()
    }
  }
  return expression
}

export function ctasWithTmpTablesToCreateTmpView(
  expression: exp.Expression,
  tmpStorageProvider: (e: exp.Expression) => exp.Expression = (e) => e,
): exp.Expression {
  if (!(expression instanceof exp.Create)) return expression

  const properties = expression.args.properties as exp.Expression | undefined
  const propsExprs = properties?.expressions ?? []
  const temporary = (propsExprs as exp.Expression[]).some(
    (prop) => prop instanceof exp.TemporaryProperty,
  )

  if (expression.args.kind === "TABLE" && temporary) {
    if (expression.args.expression) {
      return new exp.Create({
        kind: "TEMPORARY VIEW",
        this: expression.args.this as exp.Expression,
        expression: expression.args.expression as exp.Expression,
      })
    }
    return tmpStorageProvider(expression)
  }

  return expression
}

export function structKvToAlias(expression: exp.Expression): exp.Expression {
  if (expression instanceof exp.Struct) {
    expression.set(
      "expressions",
      (expression.expressions ?? []).map((e) =>
        e instanceof exp.PropertyEQ
          ? exp.alias_(
              e.args.expression as exp.Expression,
              (e.args.this as exp.Expression).name,
            )
          : e,
      ),
    )
  }
  return expression
}

export function moveSchemaColumnsToPartitionedBy(
  expression: exp.Expression,
): exp.Expression {
  if (!(expression instanceof exp.Create)) return expression
  const hasSchema = expression.args.this instanceof exp.Schema
  const isPartitionable =
    expression.args.kind === "TABLE" || expression.args.kind === "VIEW"

  if (hasSchema && isPartitionable) {
    const prop = expression.find(exp.PartitionedByProperty)
    if (prop && prop.args.this && !(prop.args.this instanceof exp.Schema)) {
      const schema = expression.args.this as exp.Schema
      const propThis = prop.args.this as exp.Expression
      const columns = new Set(
        (propThis.expressions ?? []).map((v: exp.Expression) =>
          v.name.toUpperCase(),
        ),
      )
      const partitions = (schema.expressions ?? []).filter(
        (col: exp.Expression) => columns.has(col.name.toUpperCase()),
      )
      schema.set(
        "expressions",
        (schema.expressions ?? []).filter(
          (e: exp.Expression) => !partitions.includes(e),
        ),
      )
      prop.replace(
        new exp.PartitionedByProperty({
          this: new exp.Schema({ expressions: partitions }),
        }),
      )
      expression.set("this", schema)
    }
  }

  return expression
}

export function movePartitionedByToSchemaColumns(
  expression: exp.Expression,
): exp.Expression {
  if (!(expression instanceof exp.Create)) return expression
  const prop = expression.find(exp.PartitionedByProperty)
  if (
    prop &&
    prop.args.this &&
    prop.args.this instanceof exp.Schema &&
    (prop.args.this as exp.Schema).expressions.every(
      (e: exp.Expression) => e instanceof exp.ColumnDef && e.args.kind,
    )
  ) {
    const propThis = prop.args.this as exp.Schema
    const propExpressions = propThis.expressions as exp.Expression[]
    const newPropThis = new exp.Tuple({
      expressions: propExpressions.map((e: exp.Expression) =>
        exp.toIdentifier(e.name),
      ),
    })
    const schema = expression.args.this as exp.Schema
    const existing = schema.args.expressions as exp.Expression[] | undefined
    schema.set("expressions", [...(existing ?? []), ...propExpressions])
    prop.set("this", newPropThis)
  }

  return expression
}

export function eliminateFullOuterJoin(
  expression: exp.Expression,
): exp.Expression {
  if (!(expression instanceof exp.Select)) return expression

  const joins = (expression.args.joins as exp.Join[] | undefined) ?? []
  const fullOuterJoins: [number, exp.Join][] = []
  for (let i = 0; i < joins.length; i++) {
    const join = joins[i]!
    if (join.text("side").toUpperCase() === "FULL") {
      fullOuterJoins.push([i, join])
    }
  }

  if (fullOuterJoins.length === 1) {
    const expressionCopy = expression.copy() as exp.Select
    expression.set("limit", undefined)
    const [index, fullOuterJoin] = fullOuterJoins[0]!

    const fromExpr = expression.args.from_ as exp.From
    const tables = [fromExpr.aliasOrName, fullOuterJoin.aliasOrName]
    const joinConditions =
      (fullOuterJoin.args.on as exp.Expression | undefined) ??
      exp.and_(
        ...(
          (fullOuterJoin.args.using as exp.Expression[] | undefined) ?? []
        ).map((col: exp.Expression) =>
          exp.column(col.name, tables[0]).eq(exp.column(col.name, tables[1])),
        ),
      )!

    fullOuterJoin.set("side", "left")
    const antiJoinClause = exp
      .select("1")
      .from(fromExpr.sql())
      .where(joinConditions.sql())

    const copyJoins = expressionCopy.args.joins as exp.Join[]
    copyJoins[index]!.set("side", "right")
    const existsNot = new exp.Exists({ this: antiJoinClause }).not_()
    expressionCopy.where(existsNot.sql())
    expressionCopy.set("with_", undefined)
    expression.set("order", undefined)

    return new exp.Union({
      this: expression,
      expression: expressionCopy,
      distinct: false,
    })
  }

  return expression
}

export function inheritStructFieldNames(
  expression: exp.Expression,
): exp.Expression {
  if (
    expression instanceof exp.Array &&
    expression.args.struct_name_inheritance
  ) {
    const exprs = expression.expressions ?? []
    const firstItem = exprs[0]
    if (
      firstItem instanceof exp.Struct &&
      firstItem.expressions.every(
        (f: exp.Expression) => f instanceof exp.PropertyEQ,
      )
    ) {
      const fieldNames = firstItem.expressions.map(
        (f: exp.Expression) => f.args.this as exp.Expression,
      )

      for (let i = 1; i < exprs.length; i++) {
        const struct = exprs[i]
        if (
          !(struct instanceof exp.Struct) ||
          struct.expressions.length !== fieldNames.length
        ) {
          continue
        }

        const newExpressions: exp.Expression[] = []
        for (let j = 0; j < struct.expressions.length; j++) {
          const expr = struct.expressions[j]!
          if (!(expr instanceof exp.PropertyEQ)) {
            const propertyEq = new exp.PropertyEQ({
              this: new exp.Identifier({ this: fieldNames[j]!.copy() }),
              expression: expr,
            })
            newExpressions.push(propertyEq)
          } else {
            newExpressions.push(expr)
          }
        }
        struct.set("expressions", newExpressions)
      }
    }
  }

  return expression
}

export function timestrtotime_sql(gen: Generator, e: exp.Expression): string {
  const expression = e as exp.TimeStrToTime
  const dataType = exp.DataType.build(
    expression.args.zone
      ? exp.DataType.Type.TIMESTAMPTZ
      : exp.DataType.Type.TIMESTAMP,
  )
  return gen.sql(exp.cast(expression.args.this as exp.Expression, dataType))
}

export function datestrtodate_sql(gen: Generator, e: exp.Expression): string {
  return gen.sql(
    exp.cast(
      (e as exp.DateStrToDate).args.this as exp.Expression,
      exp.DataType.Type.DATE,
    ),
  )
}

export function unitToStr(
  expression: exp.Expression,
  defaultUnit = "DAY",
): string {
  const unit = (expression as exp.Func).args.unit
  if (typeof unit === "string") return unit.toUpperCase()
  if (unit instanceof exp.Expression)
    return String(unit.args.this ?? defaultUnit).toUpperCase()
  return defaultUnit
}

const UNABBREVIATED_UNIT_NAME: Record<string, string> = {
  D: "DAY",
  H: "HOUR",
  M: "MINUTE",
  MS: "MILLISECOND",
  NS: "NANOSECOND",
  Q: "QUARTER",
  S: "SECOND",
  US: "MICROSECOND",
  W: "WEEK",
  Y: "YEAR",
}

function normalizeUnitName(name: string): string {
  const upper = name.toUpperCase()
  return UNABBREVIATED_UNIT_NAME[upper] ?? upper
}

export function unitToVar(
  expression: exp.Expression,
  defaultUnit = "DAY",
): exp.Expression | undefined {
  const unit = (expression as exp.Func).args.unit
  if (unit instanceof exp.Placeholder) {
    return unit
  }
  if (unit instanceof exp.WeekStart) {
    return unit
  }
  if (unit instanceof exp.Var) {
    return new exp.Var({ this: normalizeUnitName(unit.name) })
  }
  if (unit instanceof exp.Column) {
    if (!unit.args.table) {
      return new exp.Var({ this: normalizeUnitName(unit.name) })
    }
    return unit
  }
  if (unit instanceof exp.Literal) {
    return new exp.Var({ this: normalizeUnitName(unit.name) })
  }
  const value =
    unit instanceof exp.Expression ? String(unit.args.this ?? "") : undefined
  const result = value || defaultUnit
  return result ? new exp.Var({ this: normalizeUnitName(result) }) : undefined
}

export function tsOrDsAddCast(expression: exp.TsOrDsAdd): exp.TsOrDsAdd {
  let thisNode = (expression.args.this as exp.Expression).copy()

  const returnTypeArg = expression.args.return_type
  const returnType =
    returnTypeArg instanceof exp.DataType
      ? returnTypeArg
      : exp.DataType.build("DATE")

  if (returnType.isType(exp.DataType.Type.DATE)) {
    thisNode = exp.cast(thisNode, exp.DataType.Type.TIMESTAMP)
  }
  ;(expression.args.this as exp.Expression).replace(
    exp.cast(thisNode, returnType),
  )
  return expression
}

export function dateDeltaSql(name: string, cast = false): Transform {
  return (gen: Generator, e: exp.Expression): string => {
    let expression = e
    if (cast && expression instanceof exp.TsOrDsAdd) {
      expression = tsOrDsAddCast(expression)
    }
    const unit = unitToVar(expression)
    const args: exp.Expression[] = []
    if (unit) args.push(unit)
    args.push(expression.args.expression as exp.Expression)
    args.push(expression.args.this as exp.Expression)
    return gen.funcCall(name, args)
  }
}

export function timestamptrunc_sql(
  func = "DATE_TRUNC",
  zone = false,
): Transform {
  return (gen: Generator, e: exp.Expression): string => {
    const expr = e as exp.TimestampTrunc
    const unit = unitToStr(expr)
    const args: exp.Expression[] = [
      exp.Literal.string(unit),
      expr.args.this as exp.Expression,
    ]
    if (zone && expr.args.zone) {
      args.push(expr.args.zone as exp.Expression)
    }
    return gen.funcCall(func, args)
  }
}

export function no_paren_current_date_sql(
  _gen: Generator,
  _e: exp.Expression,
): string {
  return "CURRENT_DATE"
}

export function no_timestamp_sql(gen: Generator, e: exp.Expression): string {
  const expression = e as exp.Timestamp
  const zone = expression.args.zone as exp.Expression | undefined
  if (zone) {
    return gen.funcCall("TIMESTAMP", [
      expression.args.this as exp.Expression,
      zone,
    ])
  }
  return gen.sql(
    exp.cast(
      expression.args.this as exp.Expression,
      exp.DataType.Type.TIMESTAMP,
    ),
  )
}

export function no_time_sql(gen: Generator, e: exp.Expression): string {
  const expression = e as exp.Time
  const zone = expression.args.zone as exp.Expression | undefined
  if (zone) {
    return gen.funcCall("TIME", [expression.args.this as exp.Expression, zone])
  }
  return gen.sql(
    exp.cast(expression.args.this as exp.Expression, exp.DataType.Type.TIME),
  )
}

export function no_datetime_sql(gen: Generator, e: exp.Expression): string {
  const expression = e as exp.Datetime
  const zone = expression.args.zone as exp.Expression | undefined
  if (zone) {
    return gen.funcCall("DATETIME", [
      expression.args.this as exp.Expression,
      zone,
    ])
  }
  return gen.sql(
    exp.cast(
      expression.args.this as exp.Expression,
      exp.DataType.Type.DATETIME,
    ),
  )
}

export function left_to_substring_sql(
  gen: Generator,
  e: exp.Expression,
): string {
  const expression = e as exp.Left
  return gen.sql(
    new exp.Substring({
      this: expression.args.this as exp.Expression,
      start: exp.Literal.number(1),
      length: expression.args.expression as exp.Expression,
    }),
  )
}

export function right_to_substring_sql(
  gen: Generator,
  e: exp.Expression,
): string {
  const expression = e as exp.Left
  return gen.sql(
    new exp.Substring({
      this: expression.args.this as exp.Expression,
      start: new exp.Sub({
        this: new exp.Length({
          this: expression.args.this as exp.Expression,
        }),
        expression: new exp.Paren({
          this: new exp.Sub({
            this: expression.args.expression as exp.Expression,
            expression: exp.Literal.number(1),
          }),
        }),
      }),
    }),
  )
}

export function no_ilike_sql(gen: Generator, e: exp.Expression): string {
  const expression = e as exp.ILike
  return gen.sql(
    new exp.Like({
      this: new exp.Lower({ this: expression.args.this as exp.Expression }),
      expression: new exp.Lower({
        this: expression.args.expression as exp.Expression,
      }),
    }),
  )
}

export function no_tablesample_sql(gen: Generator, _e: exp.Expression): string {
  gen.unsupported("TABLESAMPLE")
  return ""
}

export function no_pivot_sql(gen: Generator, _e: exp.Expression): string {
  gen.unsupported("PIVOT")
  return ""
}

export function concat_to_dpipe_sql(gen: Generator, e: exp.Expression): string {
  const expression = e as exp.Concat
  const exprs = expression.expressions ?? []
  if (exprs.length === 1) {
    return gen.sql(exprs[0]!)
  }
  return gen.sql(
    exprs.reduce(
      (a: exp.Expression, b: exp.Expression) =>
        new exp.DPipe({ this: a, expression: b }),
    ),
  )
}

export function any_value_to_max_sql(
  gen: Generator,
  e: exp.Expression,
): string {
  return gen.funcCall("MAX", [(e as exp.AnyValue).args.this as exp.Expression])
}

export function unnestGenerateSeries(
  expression: exp.Expression,
): exp.Expression {
  const thisExpr = expression.args.this as exp.Expression | undefined
  if (
    expression instanceof exp.Table &&
    thisExpr instanceof exp.GenerateSeries
  ) {
    const unnest = new exp.Unnest({ expressions: [thisExpr] })
    if (expression.args.alias) {
      return exp.alias_(unnest, "_u")
    }
    return unnest
  }

  return expression
}

export function renameFunc(name: string): Transform {
  return (gen: Generator, e: exp.Expression) => {
    const expr = e as exp.Func
    const args: exp.Expression[] = []
    for (const [, v] of Object.entries(expr.args)) {
      if (v instanceof exp.Expression) {
        args.push(v)
      } else if (Array.isArray(v)) {
        for (const item of v) {
          if (item instanceof exp.Expression) {
            args.push(item)
          }
        }
      }
    }
    return gen.funcCall(name, args)
  }
}

export function noTryCastSql(gen: Generator, e: exp.Expression): string {
  const tryCast = e as exp.TryCast
  const cast = new exp.Cast({
    this: tryCast.this,
    to: tryCast.args.to,
    format: tryCast.args.format,
    safe: tryCast.args.safe,
  })
  return gen.sql(cast)
}

export { noTryCastSql as no_trycast_sql }

export function strPositionSql(
  gen: Generator,
  e: exp.Expression,
  opts: {
    funcName?: string
    supportsPosition?: boolean
    supportsOccurrence?: boolean
  } = {},
): string {
  const {
    funcName = "STRPOS",
    supportsPosition = false,
    supportsOccurrence = false,
  } = opts
  const expr = e as exp.StrPosition
  let string = expr.args.this as exp.Expression
  const substr = expr.args.substr as exp.Expression
  const position = expr.args.position as exp.Expression | undefined
  const occurrence = expr.args.occurrence as exp.Expression | undefined

  let posArg = position
  if (supportsOccurrence && occurrence && supportsPosition && !position) {
    posArg = exp.Literal.number(1)
  }

  const transpilePosition = posArg && !supportsPosition
  if (transpilePosition) {
    string = new exp.Substring({ this: string, start: posArg })
  }

  const funcArgs =
    funcName === "LOCATE" || funcName === "CHARINDEX"
      ? [substr, string]
      : [string, substr]
  if (supportsPosition && posArg) funcArgs.push(posArg)
  if (supportsOccurrence && occurrence) funcArgs.push(occurrence)

  let result = gen.funcCall(funcName, funcArgs)
  if (transpilePosition) {
    result = `${result} + ${gen.sql(posArg as exp.Expression)} - 1`
  }
  return result
}

export function maxOrGreatest(gen: Generator, e: exp.Expression): string {
  const expr = e as exp.Max
  const name = expr.expressions.length > 0 ? "GREATEST" : "MAX"
  return renameFunc(name)(gen, e)
}

export function minOrLeast(gen: Generator, e: exp.Expression): string {
  const expr = e as exp.Min
  const name = expr.expressions.length > 0 ? "LEAST" : "MIN"
  return renameFunc(name)(gen, e)
}

export function ifSql(
  name = "IF",
  falseValue?: exp.Expression | string,
): Transform {
  return (gen: Generator, e: exp.Expression) => {
    const expr = e as exp.If
    const args: exp.Expression[] = [
      expr.args.this as exp.Expression,
      expr.args.true as exp.Expression,
    ]
    const falseExpr =
      (expr.args.false as exp.Expression | undefined) ??
      (falseValue
        ? typeof falseValue === "string"
          ? exp.Literal.string(falseValue)
          : falseValue
        : undefined)
    if (falseExpr) args.push(falseExpr)
    return gen.funcCall(name, args)
  }
}

export function varMapSql(
  gen: Generator,
  e: exp.Expression,
  mapFuncName = "MAP",
): string {
  const keys = e.args.keys as exp.Array | undefined
  const values = e.args.values as exp.Array | undefined
  if (!(keys instanceof exp.Array) || !(values instanceof exp.Array)) {
    return gen.funcCall(mapFuncName, [
      keys as exp.Expression,
      values as exp.Expression,
    ])
  }
  const args: exp.Expression[] = []
  const keyExprs = keys.expressions
  const valExprs = values.expressions
  for (let i = 0; i < keyExprs.length; i++) {
    args.push(keyExprs[i] as exp.Expression)
    args.push(valExprs[i] as exp.Expression)
  }
  return gen.funcCall(mapFuncName, args)
}

export function inlineArraySql(gen: Generator, e: exp.Expression): string {
  return `[${gen.expressions(e.expressions)}]`
}

export function arrowJsonExtractSql(gen: Generator, e: exp.Expression): string {
  const op = e instanceof exp.JSONExtract ? "->" : "->>"
  return gen.binary_sql(e as exp.Binary, op)
}

export function approxCountDistinctSql(
  gen: Generator,
  e: exp.Expression,
): string {
  return gen.funcCall("APPROX_COUNT_DISTINCT", [
    (e as exp.ApproxDistinct).args.this as exp.Expression,
  ])
}

export function noRecursiveCteSql(gen: Generator, e: exp.Expression): string {
  const w = e as exp.With
  if (w.args.recursive) {
    w.set("recursive", false)
  }
  return gen.sql(w)
}

export function argMaxOrMinNoCount(name: string): Transform {
  return (gen: Generator, e: exp.Expression) => {
    return gen.funcCall(name, [
      e.args.this as exp.Expression,
      e.args.expression as exp.Expression,
    ])
  }
}

export function dateAddIntervalSql(dataType: string, kind: string): Transform {
  return (gen: Generator, e: exp.Expression) => {
    const thisSql = gen.sql(e, "this")
    const unit = unitToVar(e)
    const interval = new exp.Interval({
      this: e.args.expression as exp.Expression,
      unit: unit ?? undefined,
    })
    return `${dataType}_${kind}(${thisSql}, ${gen.sql(interval)})`
  }
}

export function countIfToSum(gen: Generator, e: exp.Expression): string {
  const expr = e as exp.CountIf
  const cond = expr.args.this as exp.Expression
  return gen.funcCall("SUM", [
    new exp.If({
      this: cond,
      true: exp.Literal.number(1),
      false: exp.Literal.number(0),
    }),
  ])
}

export function timestampdiffSql(gen: Generator, e: exp.Expression): string {
  return gen.funcCall("TIMESTAMPDIFF", [
    unitToVar(e) ?? new exp.Var({ this: "DAY" }),
    e.args.expression as exp.Expression,
    e.args.this as exp.Expression,
  ])
}
