import { Dialect } from "../dialect.js"
import type { ExpressionClass } from "../expression-base.js"
import * as exp from "../expressions.js"
import { Generator } from "../generator.js"
import { Parser } from "../parser.js"
import {
  eliminateQualify,
  eliminateSemiAndAntiJoins,
  preprocess,
} from "../transforms.js"

type Transform = (generator: Generator, expression: exp.Expression) => string

export class SQLiteParser extends Parser {
  static override ADD_JOIN_ON_TRUE = true
}

export class SQLiteGenerator extends Generator {
  static override HEX_START: string | null = "x'"
  static override HEX_END: string | null = "'"

  static override FEATURES = {
    ...Generator.FEATURES,
    SAFE_DIVISION: true,
    TYPED_DIVISION: true,
  }

  static override TYPE_MAPPING: Map<string, string> = new Map([
    ...Generator.TYPE_MAPPING,
    ["BOOLEAN", "INTEGER"],
    ["TINYINT", "INTEGER"],
    ["SMALLINT", "INTEGER"],
    ["INT", "INTEGER"],
    ["MEDIUMINT", "INTEGER"],
    ["BIGINT", "INTEGER"],
    ["FLOAT", "REAL"],
    ["DOUBLE", "REAL"],
    ["DECIMAL", "REAL"],
    ["CHAR", "TEXT"],
    ["NCHAR", "TEXT"],
    ["VARCHAR", "TEXT"],
    ["NVARCHAR", "TEXT"],
    ["BINARY", "BLOB"],
    ["VARBINARY", "BLOB"],
  ])

  static override TRANSFORMS: Map<ExpressionClass, Transform> = new Map<
    ExpressionClass,
    Transform
  >([
    ...Generator.TRANSFORMS,
    [exp.Select, preprocess([eliminateQualify, eliminateSemiAndAntiJoins])],
    [
      exp.TimeStrToTime,
      (gen: Generator, e: exp.Expression) =>
        gen.sql((e as exp.TimeStrToTime).args.this as exp.Expression),
    ],
    [
      exp.TryCast,
      (gen: Generator, e: exp.Expression) => {
        const expr = e as exp.TryCast
        const thisExpr = gen.sql(expr.args.this as exp.Expression)
        const to = gen.sql(expr.args.to as exp.Expression)
        return `CAST(${thisExpr} AS ${to})`
      },
    ],
  ])

  protected override ignorenulls_sql(expression: exp.IgnoreNulls): string {
    this.unsupported("SQLite does not support IGNORE NULLS.")
    return this.sql(expression.args.this as exp.Expression)
  }

  protected override respectnulls_sql(expression: exp.RespectNulls): string {
    return this.sql(expression.args.this as exp.Expression)
  }

  protected dateadd_sql(expression: exp.DateAdd): string {
    const modifier = expression.args.expression as exp.Expression
    const modifierSql =
      modifier instanceof exp.Literal && modifier.args.is_string
        ? modifier.name
        : this.sql(modifier)
    const unitArg = expression.args.unit
    const unitName =
      unitArg instanceof exp.Expression
        ? String(unitArg.args.this ?? "")
        : typeof unitArg === "string"
          ? unitArg
          : ""
    const modifierWithUnit = unitName
      ? `'${modifierSql} ${unitName}'`
      : `'${modifierSql}'`
    return `DATE(${this.sql(expression.args.this as exp.Expression)}, ${modifierWithUnit})`
  }

  protected datediff_sql(expression: exp.DateDiff): string {
    const unitArg = expression.args.unit
    const unit =
      unitArg instanceof exp.Expression
        ? String(unitArg.args.this ?? "DAY").toUpperCase()
        : typeof unitArg === "string"
          ? unitArg.toUpperCase()
          : "DAY"

    const thisExpr = this.sql(expression.args.this as exp.Expression)
    const exprExpr = this.sql(expression.args.expression as exp.Expression)
    let sql = `(JULIANDAY(${thisExpr}) - JULIANDAY(${exprExpr}))`

    if (unit === "MONTH") {
      sql = `${sql} / 30.0`
    } else if (unit === "YEAR") {
      sql = `${sql} / 365.0`
    } else if (unit === "HOUR") {
      sql = `${sql} * 24.0`
    } else if (unit === "MINUTE") {
      sql = `${sql} * 1440.0`
    } else if (unit === "SECOND") {
      sql = `${sql} * 86400.0`
    } else if (unit === "MILLISECOND") {
      sql = `${sql} * 86400000.0`
    } else if (unit === "MICROSECOND") {
      sql = `${sql} * 86400000000.0`
    } else if (unit === "NANOSECOND") {
      sql = `${sql} * 8640000000000.0`
    } else if (unit !== "DAY") {
      this.unsupported(`DATEDIFF unsupported for '${unit}'.`)
    }

    return `CAST(${sql} AS INTEGER)`
  }

  protected generateseries_sql(expression: exp.GenerateSeries): string {
    const parent = expression.parent
    const aliasParent = parent?.parent
    const alias =
      aliasParent instanceof exp.Alias
        ? (aliasParent.args.alias as exp.TableAlias | undefined)
        : parent instanceof exp.Table
          ? (parent.args.alias as exp.TableAlias | undefined)
          : undefined

    if (alias instanceof exp.TableAlias) {
      const columns = alias.args.columns as exp.Identifier[] | undefined
      if (columns && columns.length > 0) {
        const columnAlias = columns[0]!
        alias.set("columns", undefined)
        const selectExpr = new exp.Select({
          expressions: [
            new exp.Alias({
              this: new exp.Column({
                this: new exp.Identifier({ this: "value" }),
              }),
              alias: columnAlias,
            }),
          ],
          from_: new exp.From({ this: expression }),
        })
        return this.sql(new exp.Subquery({ this: selectExpr }))
      }
    }

    return this.function_fallback_sql(expression)
  }
}

export class SQLiteDialect extends Dialect {
  static override readonly name = "sqlite"
  static override TYPED_DIVISION = true
  static override SAFE_DIVISION = true
  protected static override ParserClass = SQLiteParser
  protected static override GeneratorClass = SQLiteGenerator
}

Dialect.register(SQLiteDialect)
