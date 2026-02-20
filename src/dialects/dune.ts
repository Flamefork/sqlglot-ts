/**
 * Dune Analytics dialect (extends Trino/Presto)
 */

import { Dialect } from "../dialect.js"
import { PrestoGenerator, PrestoParser } from "./presto.js"

export class DuneDialect extends Dialect {
  static override readonly name = "dune"
  static override NULL_ORDERING:
    | "nulls_are_small"
    | "nulls_are_large"
    | "nulls_are_last" = "nulls_are_last"
  static override INDEX_OFFSET = 1
  protected static override ParserClass: typeof PrestoParser = PrestoParser
  protected static override GeneratorClass: typeof PrestoGenerator =
    PrestoGenerator
}

// Register dialect
Dialect.register(DuneDialect)
