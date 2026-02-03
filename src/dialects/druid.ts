/**
 * Apache Druid dialect
 */

import { Dialect } from "../dialect.js"

export class DruidDialect extends Dialect {
  static override readonly name = "druid"
}

// Register dialect
Dialect.register(DruidDialect)
