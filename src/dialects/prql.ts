/**
 * PRQL dialect
 */

import { Dialect } from "../dialect.js"

export class PRQLDialect extends Dialect {
  static override readonly name = "prql"
}

// Register dialect
Dialect.register(PRQLDialect)
