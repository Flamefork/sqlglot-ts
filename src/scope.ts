/**
 * Scope utility functions for walking AST while respecting scope boundaries.
 *
 * Scope boundaries are:
 * - CTEs
 * - Derived tables (Subquery with alias or containing unwrapped queries)
 * - UDTFs with Query args
 * - Unwrapped queries (Select, SetOperation)
 */

import { Expression } from "./expression-base.js"
import * as exp from "./expressions.js"

/**
 * Checks if a node is an unwrapped query (Select or SetOperation).
 * These are queries that aren't wrapped in a Subquery.
 */
function isUnwrappedQuery(node: Expression): boolean {
  return node instanceof exp.Select || node instanceof exp.SetOperation
}

/**
 * Checks if a node is a derived table.
 * A derived table is a Subquery with an alias or containing unwrapped queries.
 */
function isDerivedTable(node: Expression): boolean {
  if (!(node instanceof exp.Subquery)) {
    return false
  }

  if (node.args.alias) {
    return true
  }

  const thisArg = node.args.this
  return thisArg instanceof Expression && isUnwrappedQuery(thisArg)
}

/**
 * Walks an expression tree but stops at scope boundaries.
 *
 * Scope boundaries are:
 * - CTEs
 * - Derived tables (subqueries in FROM/JOIN)
 * - UDTFs with Query arguments
 * - Unwrapped queries (Select, SetOperation) not wrapped in Subquery
 *
 * For Subquery and UDTF nodes, also yields their joins, laterals, and pivots.
 *
 * @param expression - Starting expression node
 * @param bfs - Use breadth-first search (default true)
 * @yields Expression nodes within the current scope
 */
export function* walkInScope(
  expression: Expression,
  bfs = true,
): Generator<Expression> {
  let crossedScopeBoundary = false

  for (const node of expression.walk(bfs, () => crossedScopeBoundary)) {
    crossedScopeBoundary = false
    yield node

    // Don't check scope boundaries for the root expression
    if (node === expression) {
      continue
    }

    const parent = node.parent

    // Check if we've crossed a scope boundary
    if (
      // CTE is a scope boundary
      node instanceof exp.CTE ||
      // Derived tables in FROM/JOIN are scope boundaries
      ((parent instanceof exp.From || parent instanceof exp.Join) &&
        isDerivedTable(node)) ||
      // UDTF with Query argument is a scope boundary
      (parent instanceof exp.UDTF && node instanceof exp.Query) ||
      // Unwrapped queries are scope boundaries
      isUnwrappedQuery(node)
    ) {
      crossedScopeBoundary = true

      // For Subquery and UDTF, still yield their joins, laterals, and pivots
      if (node instanceof exp.Subquery || node instanceof exp.UDTF) {
        for (const key of ["joins", "laterals", "pivots"] as const) {
          const arr = node.args[key]
          if (Array.isArray(arr)) {
            for (const item of arr) {
              if (item instanceof Expression) {
                yield* walkInScope(item, bfs)
              }
            }
          }
        }
      }
    }
  }
}

/**
 * Finds all nodes of the given types within the current scope.
 *
 * @param expression - Starting expression node
 * @param expressionTypes - Array of expression class constructors to match
 * @param bfs - Use breadth-first search (default true)
 * @yields Expression nodes matching the types
 */
export function* findAllInScope<T extends Expression>(
  expression: Expression,
  expressionTypes: (new (args?: any) => T)[],
  bfs = true,
): Generator<T> {
  for (const node of walkInScope(expression, bfs)) {
    if (expressionTypes.some((t) => node instanceof t)) {
      yield node as T
    }
  }
}

/**
 * Finds the first node of the given types within the current scope.
 *
 * @param expression - Starting expression node
 * @param expressionTypes - Array of expression class constructors to match
 * @param bfs - Use breadth-first search (default true)
 * @returns First matching expression or undefined
 */
export function findInScope<T extends Expression>(
  expression: Expression,
  expressionTypes: (new (args?: any) => T)[],
  bfs = true,
): T | undefined {
  for (const node of findAllInScope(expression, expressionTypes, bfs)) {
    return node
  }
  return undefined
}
