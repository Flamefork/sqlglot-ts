/**
 * AWS Athena dialect (extends Presto)
 */

import {
  Dialect,
  buildEscapedSequences,
  buildUnescapedSequences,
} from "../dialect.js"
import { PrestoGenerator, PrestoParser } from "./presto.js"

export class AthenaGenerator extends PrestoGenerator {
  static override STRINGS_SUPPORT_ESCAPED_SEQUENCES = true
  static override ESCAPED_SEQUENCES: Record<string, string> =
    buildEscapedSequences(buildUnescapedSequences())
  static override STRING_ESCAPES: string[] = ["'", "\\"]
}

export class AthenaDialect extends Dialect {
  static override readonly name = "athena"
  static override NULL_ORDERING:
    | "nulls_are_small"
    | "nulls_are_large"
    | "nulls_are_last" = "nulls_are_last"
  static override INDEX_OFFSET = 1
  static override STRING_ESCAPES: string[] = ["'", "\\"]
  static override UNESCAPED_SEQUENCES: Record<string, string> =
    buildUnescapedSequences()
  static override ESCAPED_SEQUENCES: Record<string, string> =
    buildEscapedSequences(AthenaDialect.UNESCAPED_SEQUENCES)
  static override STRINGS_SUPPORT_ESCAPED_SEQUENCES = true
  protected static override ParserClass: typeof PrestoParser = PrestoParser
  protected static override GeneratorClass: typeof AthenaGenerator =
    AthenaGenerator
}

// Register dialect
Dialect.register(AthenaDialect)
