/**
 * Exasol dialect
 */

import { Dialect } from "../dialect.js"
import type { ExpressionClass } from "../expression-base.js"
import * as exp from "../expressions.js"
import { Generator } from "../generator.js"
import { FunctionBuilder, Parser } from "../parser.js"
import { renameFunc } from "../transforms.js"

type Transform = (generator: Generator, expression: exp.Expression) => string

function sha2Sql(gen: Generator, e: exp.Expression): string {
  const expr = e as exp.SHA2
  const length = String(
    (expr.args.length as exp.Expression | undefined)?.args?.this ?? "",
  )
  const funcName = length === "256" ? "HASH_SHA256" : "HASH_SHA512"
  return gen.funcCall(funcName, [expr.args.this as exp.Expression])
}

export class ExasolParser extends Parser {
  static override FUNCTIONS: Map<string, FunctionBuilder> = new Map([
    ...Parser.FUNCTIONS,
    [
      "BIT_AND",
      (args: exp.Expression[]) =>
        new exp.BitwiseAnd({ this: args[0], expression: args[1] }),
    ],
    [
      "BIT_OR",
      (args: exp.Expression[]) =>
        new exp.BitwiseOr({ this: args[0], expression: args[1] }),
    ],
    [
      "BIT_XOR",
      (args: exp.Expression[]) =>
        new exp.BitwiseXor({ this: args[0], expression: args[1] }),
    ],
    [
      "BIT_NOT",
      (args: exp.Expression[]) => new exp.BitwiseNot({ this: args[0] }),
    ],
    [
      "BIT_LSHIFT",
      (args: exp.Expression[]) =>
        new exp.BitwiseLeftShift({ this: args[0], expression: args[1] }),
    ],
    [
      "BIT_RSHIFT",
      (args: exp.Expression[]) =>
        new exp.BitwiseRightShift({ this: args[0], expression: args[1] }),
    ],
    ["EVERY", (args: exp.Expression[]) => new exp.All({ this: args[0] })],
    ["HASH_SHA", (args: exp.Expression[]) => new exp.SHA({ this: args[0] })],
    ["HASH_SHA1", (args: exp.Expression[]) => new exp.SHA({ this: args[0] })],
    ["HASH_MD5", (args: exp.Expression[]) => new exp.MD5({ this: args[0] })],
    [
      "HASH_SHA256",
      (args: exp.Expression[]) =>
        new exp.SHA2({ this: args[0], length: exp.Literal.number(256) }),
    ],
    [
      "HASH_SHA512",
      (args: exp.Expression[]) =>
        new exp.SHA2({ this: args[0], length: exp.Literal.number(512) }),
    ],
    [
      "VAR_POP",
      (args: exp.Expression[]) => new exp.VariancePop({ this: args[0] }),
    ],
    [
      "NULLIFZERO",
      (args: exp.Expression[]) =>
        new exp.If({
          this: new exp.EQ({
            this: args[0],
            expression: exp.Literal.number(0),
          }),
          true: new exp.Null({}),
          false: args[0],
        }),
    ],
    [
      "ZEROIFNULL",
      (args: exp.Expression[]) =>
        new exp.If({
          this: new exp.Is({ this: args[0], expression: new exp.Null({}) }),
          true: exp.Literal.number(0),
          false: args[0],
        }),
    ],
  ])
}

export class ExasolGenerator extends Generator {
  static override TRANSFORMS: Map<ExpressionClass, Transform> = new Map<
    ExpressionClass,
    Transform
  >([
    ...Generator.TRANSFORMS,
    [exp.All, renameFunc("EVERY")],
    [exp.BitwiseAnd, renameFunc("BIT_AND")],
    [exp.BitwiseOr, renameFunc("BIT_OR")],
    [exp.BitwiseNot, renameFunc("BIT_NOT")],
    [exp.BitwiseLeftShift, renameFunc("BIT_LSHIFT")],
    [exp.BitwiseRightShift, renameFunc("BIT_RSHIFT")],
    [exp.BitwiseXor, renameFunc("BIT_XOR")],
    [exp.VariancePop, renameFunc("VAR_POP")],
    [exp.SHA, renameFunc("HASH_SHA")],
    [exp.SHA2, sha2Sql],
    [exp.MD5, renameFunc("HASH_MD5")],
    [exp.Mod, renameFunc("MOD")],
  ])

  protected override if_sql(expression: exp.If): string {
    const thisExpr = this.sql(expression, "this")
    const trueExpr = this.sql(expression, "true")
    const falseExpr = this.sql(expression, "false")
    return `IF ${thisExpr} THEN ${trueExpr} ELSE ${falseExpr} ENDIF`
  }
}

export class ExasolDialect extends Dialect {
  static override readonly name = "exasol"
  protected static override ParserClass: typeof ExasolParser = ExasolParser
  protected static override GeneratorClass: typeof ExasolGenerator =
    ExasolGenerator
}

// Register dialect
Dialect.register(ExasolDialect)
