/**
 * RisingWave dialect (extends Postgres)
 */

import { Dialect } from "../dialect.js"
import { PostgresGenerator } from "./postgres.js"

export class RisingWaveDialect extends Dialect {
  static override readonly name = "risingwave"
  protected static override GeneratorClass = PostgresGenerator
}

// Register dialect
Dialect.register(RisingWaveDialect)
