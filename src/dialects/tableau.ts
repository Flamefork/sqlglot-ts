/**
 * Tableau dialect
 */

import { Dialect } from "../dialect.js"

export class TableauDialect extends Dialect {
  static override readonly name = "tableau"
}

// Register dialect
Dialect.register(TableauDialect)
