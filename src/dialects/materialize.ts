/**
 * Materialize dialect (extends Postgres)
 */

import { Dialect } from "../dialect.js"
import { PostgresGenerator } from "./postgres.js"

export class MaterializeDialect extends Dialect {
  static override readonly name = "materialize"
  protected static override GeneratorClass: typeof PostgresGenerator =
    PostgresGenerator
}

// Register dialect
Dialect.register(MaterializeDialect)
