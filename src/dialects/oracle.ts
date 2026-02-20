/**
 * Oracle SQL dialect
 */

import { Dialect } from "../dialect.js"
import type { ExpressionClass } from "../expression-base.js"
import * as exp from "../expressions.js"
import { Generator } from "../generator.js"
import { Parser } from "../parser.js"
import { TokenType, Tokenizer } from "../tokens.js"
import { eliminateQualify, preprocess } from "../transforms.js"

type Transform = (generator: Generator, expression: exp.Expression) => string

export class OracleParser extends Parser {}

export class OracleGenerator extends Generator {
  static override NULL_ORDERING:
    | "nulls_are_small"
    | "nulls_are_large"
    | "nulls_are_last" = "nulls_are_large"
  static override TYPE_MAPPING: Map<string, string> = new Map([
    ...Generator.TYPE_MAPPING,
    ["TINYINT", "SMALLINT"],
    ["SMALLINT", "SMALLINT"],
    ["INT", "INT"],
    ["BIGINT", "INT"],
    ["DECIMAL", "NUMBER"],
    ["DOUBLE", "DOUBLE PRECISION"],
    ["VARCHAR", "VARCHAR2"],
    ["NVARCHAR", "NVARCHAR2"],
    ["NCHAR", "NCHAR"],
    ["TEXT", "CLOB"],
    ["TIMETZ", "TIME"],
    ["TIMESTAMPNTZ", "TIMESTAMP"],
    ["TIMESTAMPTZ", "TIMESTAMP"],
    ["BINARY", "BLOB"],
    ["VARBINARY", "BLOB"],
    ["ROWVERSION", "BLOB"],
  ])

  static override TRANSFORMS: Map<ExpressionClass, Transform> = new Map<
    ExpressionClass,
    Transform
  >([...Generator.TRANSFORMS, [exp.Select, preprocess([eliminateQualify])]])

  // Oracle uses double quotes for identifier quoting
  protected override quoteIdentifier(name: string): string {
    return `"${name.replace(/"/g, '""')}"`
  }
}

export class OracleDialect extends Dialect {
  static override readonly name = "oracle"
  static override NULL_ORDERING:
    | "nulls_are_small"
    | "nulls_are_large"
    | "nulls_are_last" = "nulls_are_large"
  protected static override ParserClass: typeof OracleParser = OracleParser
  protected static override GeneratorClass: typeof OracleGenerator =
    OracleGenerator

  override createTokenizer(): Tokenizer {
    return new Tokenizer({
      ...this.options.tokenizer,
      keywords: new Map([
        ...(this.options.tokenizer?.keywords ?? []),
        ["MATCH_RECOGNIZE", TokenType.MATCH_RECOGNIZE],
        ["COLUMNS", TokenType.COLUMN],
      ]),
    })
  }
}

Dialect.register(OracleDialect)
