/**
 * AWS Athena dialect (extends Presto)
 */

import { Dialect } from "../dialect.js"
import { PrestoGenerator, PrestoParser } from "./presto.js"

export class AthenaDialect extends Dialect {
  static override readonly name = "athena"
  static override NULL_ORDERING:
    | "nulls_are_small"
    | "nulls_are_large"
    | "nulls_are_last" = "nulls_are_last"
  static override INDEX_OFFSET = 1
  protected static override ParserClass = PrestoParser
  protected static override GeneratorClass = PrestoGenerator
}

// Register dialect
Dialect.register(AthenaDialect)
