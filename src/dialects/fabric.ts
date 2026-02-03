/**
 * Microsoft Fabric dialect (extends T-SQL)
 */

import { Dialect } from "../dialect.js"
import { TSQLGenerator, TSQLParser } from "./tsql.js"

export class FabricDialect extends Dialect {
  static override readonly name = "fabric"
  static override CONCAT_COALESCE = true
  protected static override ParserClass = TSQLParser
  protected static override GeneratorClass = TSQLGenerator
}

// Register dialect
Dialect.register(FabricDialect)
