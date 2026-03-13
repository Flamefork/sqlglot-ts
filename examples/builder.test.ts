import { describe, test } from "node:test"
import { ok, strictEqual } from "node:assert/strict"
import {
  select,
  from_,
  condition,
  and_,
  or_,
  not_,
  case_,
  union,
  intersect,
  except_,
  delete_,
  update,
  values,
  alias_,
  insert,
  merge,
  Expression,
  parseOne,
} from "sqlglot-ts"

import * as exp from "sqlglot-ts/expressions"
import "sqlglot-ts/dialects"

function sql(expr: Expression, dialect?: string): string {
  return expr.sql(dialect ? { dialect } : undefined)
}

describe("builder", () => {
  // --- Select basics ---
  test("select().from_()", () => {
    strictEqual(sql(select("a", "b").from_("t")), "SELECT a, b FROM t")
  })

  test("from_().select() — reverse order", () => {
    strictEqual(sql(from_("t").select("a", "b")), "SELECT a, b FROM t")
  })

  test(".select() appends columns by default", () => {
    strictEqual(sql(select("a").from_("t").select("b")), "SELECT a, b FROM t")
  })

  test(".select({ append: false }) replaces columns", () => {
    strictEqual(
      sql(select("a").from_("t").select("b", { append: false })),
      "SELECT b FROM t",
    )
  })

  test(".from_() replaces previous FROM", () => {
    strictEqual(
      sql(select("x").from_("tbl").from_("tbl2")),
      "SELECT x FROM tbl2",
    )
  })

  // --- Where ---
  test("where clause", () => {
    strictEqual(
      sql(select("x").from_("t").where("x > 0")),
      "SELECT x FROM t WHERE x > 0",
    )
  })

  test("multiple .where() calls are ANDed", () => {
    strictEqual(
      sql(select("x").from_("t").where("x > 0").where("x < 9")),
      "SELECT x FROM t WHERE x > 0 AND x < 9",
    )
  })

  test("where with multiple args ANDs them", () => {
    strictEqual(
      sql(select("x").from_("t").where("x > 0", "x < 9")),
      "SELECT x FROM t WHERE x > 0 AND x < 9",
    )
  })

  test("where({ append: false }) replaces", () => {
    strictEqual(
      sql(
        select("x").from_("t").where("x > 0").where("x < 9", { append: false }),
      ),
      "SELECT x FROM t WHERE x < 9",
    )
  })

  test("where(null) — null filtering", () => {
    strictEqual(
      sql(select("x").from_("t").where(null).where("false", "")),
      "SELECT x FROM t WHERE FALSE",
    )
  })

  // --- Limit / Offset ---
  test("limit and offset", () => {
    strictEqual(
      sql(select("x").from_("t").limit(10).offset(5)),
      "SELECT x FROM t LIMIT 10 OFFSET 5",
    )
  })

  // --- Order By ---
  test("orderBy", () => {
    strictEqual(
      sql(select("x").from_("t").orderBy("x DESC")),
      "SELECT x FROM t ORDER BY x DESC",
    )
  })

  test("orderBy appends", () => {
    strictEqual(
      sql(
        select("x", "y", "z", "a").from_("t").orderBy("x, y", "z").orderBy("a"),
      ),
      "SELECT x, y, z, a FROM t ORDER BY x, y, z, a",
    )
  })

  test("orderBy on UNION", () => {
    strictEqual(
      sql(
        parseOne("SELECT * FROM x UNION SELECT * FROM y")
          .assertIs(exp.Union)
          .orderBy("y"),
      ),
      "SELECT * FROM x UNION SELECT * FROM y ORDER BY y",
    )
  })

  // --- Group By ---
  test("groupBy", () => {
    strictEqual(
      sql(select("x", "y").from_("t").groupBy("x")),
      "SELECT x, y FROM t GROUP BY x",
    )
  })

  test("groupBy with multi-column string", () => {
    strictEqual(
      sql(select("x", "y").from_("t").groupBy("x, y")),
      "SELECT x, y FROM t GROUP BY x, y",
    )
  })

  test("groupBy appends", () => {
    strictEqual(
      sql(
        select("x", "y", "z", "a").from_("t").groupBy("x, y", "z").groupBy("a"),
      ),
      "SELECT x, y, z, a FROM t GROUP BY x, y, z, a",
    )
  })

  test("groupBy() with no args is a no-op", () => {
    strictEqual(sql(select("x").from_("t").groupBy()), "SELECT x FROM t")
  })

  test("groupBy({ append: false }) replaces", () => {
    strictEqual(
      sql(
        select("x", "y")
          .from_("t")
          .groupBy("x")
          .groupBy("y", { append: false }),
      ),
      "SELECT x, y FROM t GROUP BY y",
    )
  })

  // --- Having ---
  test("having", () => {
    strictEqual(
      sql(
        select("x", "COUNT(y)").from_("t").groupBy("x").having("COUNT(y) > 0"),
      ),
      "SELECT x, COUNT(y) FROM t GROUP BY x HAVING COUNT(y) > 0",
    )
  })

  // --- Sort By / Cluster By ---
  test("sortBy", () => {
    strictEqual(
      sql(select("x").from_("t").sortBy("x DESC")),
      "SELECT x FROM t SORT BY x DESC",
    )
  })

  test("clusterBy", () => {
    strictEqual(
      sql(select("x").from_("t").clusterBy("y")),
      "SELECT x FROM t CLUSTER BY y",
    )
  })

  // --- Distinct ---
  test("distinct(true)", () => {
    strictEqual(
      sql(select("x").distinct({ distinct: true }).from_("t")),
      "SELECT DISTINCT x FROM t",
    )
  })

  test("distinct(false)", () => {
    strictEqual(
      sql(select("x").distinct({ distinct: false }).from_("t")),
      "SELECT x FROM t",
    )
  })

  test("distinct ON", () => {
    strictEqual(
      sql(select("x").distinct("a", "b").from_("t")),
      "SELECT DISTINCT ON (a, b) x FROM t",
    )
  })

  // --- Join ---
  test("join with ON string", () => {
    strictEqual(
      sql(select("x").from_("t1").join("t2", { on: "t1.y = t2.y" })),
      "SELECT x FROM t1 JOIN t2 ON t1.y = t2.y",
    )
  })

  test("join with ON array", () => {
    strictEqual(
      sql(
        select("x")
          .from_("t1")
          .join("t2", { on: ["t1.y = t2.y", "a = b"] }),
      ),
      "SELECT x FROM t1 JOIN t2 ON t1.y = t2.y AND a = b",
    )
  })

  test("join with joinType", () => {
    strictEqual(
      sql(select("x").from_("t1").join("t2", { joinType: "left outer" })),
      "SELECT x FROM t1 LEFT OUTER JOIN t2",
    )
  })

  test("join with joinAlias", () => {
    strictEqual(
      sql(
        select("x")
          .from_("t1")
          .join(new exp.Table({ this: "t2" }), {
            joinType: "left outer",
            joinAlias: "foo",
          }),
      ),
      "SELECT x FROM t1 LEFT OUTER JOIN t2 AS foo",
    )
  })

  test("join with subquery", () => {
    strictEqual(
      sql(
        select("x")
          .from_("t1")
          .join(select("y").from_("t2").subquery(), { joinType: "left outer" }),
      ),
      "SELECT x FROM t1 LEFT OUTER JOIN (SELECT y FROM t2)",
    )
  })

  test("join with subquery alias", () => {
    strictEqual(
      sql(
        select("x")
          .from_("t1")
          .join(select("y").from_("t2").subquery("aliased"), {
            joinType: "left outer",
          }),
      ),
      "SELECT x FROM t1 LEFT OUTER JOIN (SELECT y FROM t2) AS aliased",
    )
  })

  test("join with USING", () => {
    strictEqual(
      sql(
        select("x", "y", "z")
          .from_("t1")
          .join("t2", { using: ["id", "name"] }),
      ),
      "SELECT x, y, z FROM t1 JOIN t2 USING (id, name)",
    )
  })

  test("join with USING single column", () => {
    strictEqual(
      sql(select("x").from_("foo").join("bla", { using: "bob" })),
      "SELECT x FROM foo JOIN bla USING (bob)",
    )
  })

  test("join parsed SQL", () => {
    strictEqual(
      sql(select("x").from_("t1").join("x", { on: "a=b", joinType: "left" })),
      "SELECT x FROM t1 LEFT JOIN x ON a = b",
    )
  })

  // --- Join .on() / .using() ---
  test("Join.on() chaining", () => {
    strictEqual(
      sql(
        parseOne("JOIN x", { into: exp.Join })
          .assertIs(exp.Join)
          .on("y = 1", "z = 1"),
      ),
      "JOIN x ON y = 1 AND z = 1",
    )
  })

  test("Join.using() chaining", () => {
    strictEqual(
      sql(
        parseOne("JOIN x", { into: exp.Join })
          .assertIs(exp.Join)
          .using("bar", "bob"),
      ),
      "JOIN x USING (bar, bob)",
    )
  })

  // --- Lateral ---
  test("lateral", () => {
    strictEqual(
      sql(select("x").lateral("OUTER explode(y) tbl2 AS z").from_("t")),
      "SELECT x FROM t LATERAL VIEW OUTER EXPLODE(y) tbl2 AS z",
    )
  })

  // --- Window ---
  test("window definitions", () => {
    strictEqual(
      sql(
        select("AVG(a) OVER b")
          .from_("table")
          .window("b AS (PARTITION BY c ORDER BY d)"),
      ),
      "SELECT AVG(a) OVER b FROM table WINDOW b AS (PARTITION BY c ORDER BY d)",
    )
  })

  test("multiple window definitions", () => {
    strictEqual(
      sql(
        select("AVG(a) OVER b", "MIN(c) OVER d")
          .from_("table")
          .window("b AS (PARTITION BY e ORDER BY f)")
          .window("d AS (PARTITION BY g ORDER BY h)"),
      ),
      "SELECT AVG(a) OVER b, MIN(c) OVER d FROM table WINDOW b AS (PARTITION BY e ORDER BY f), d AS (PARTITION BY g ORDER BY h)",
    )
  })

  // --- Qualify ---
  test("qualify", () => {
    strictEqual(
      sql(
        select("*")
          .from_("table")
          .qualify("row_number() OVER (PARTITION BY a ORDER BY b) = 1"),
      ),
      "SELECT * FROM table QUALIFY ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) = 1",
    )
  })

  // --- Lock / Hint ---
  test("lock FOR UPDATE", () => {
    strictEqual(
      sql(select("x").from_("t").where("x > 0").lock(), "mysql"),
      "SELECT x FROM t WHERE x > 0 FOR UPDATE",
    )
  })

  test("lock FOR SHARE", () => {
    strictEqual(
      sql(select("x").from_("t").where("x > 0").lock(false), "postgres"),
      "SELECT x FROM t WHERE x > 0 FOR SHARE",
    )
  })

  test("hints (Spark)", () => {
    strictEqual(
      sql(select("x").from_("t").hint("coalesce(3)", "broadcast(x)"), "spark"),
      "SELECT /*+ COALESCE(3), BROADCAST(x) */ x FROM t",
    )
  })

  // --- Subquery ---
  test("subquery without alias", () => {
    strictEqual(sql(select("x").from_("t").subquery()), "(SELECT x FROM t)")
  })

  test("subquery with alias", () => {
    strictEqual(
      sql(select("x").from_("t").subquery("y")),
      "(SELECT x FROM t) AS y",
    )
  })

  test("from subquery", () => {
    strictEqual(
      sql(select("x").from_(select("x").from_("t").subquery())),
      "SELECT x FROM (SELECT x FROM t)",
    )
  })

  // --- WITH (CTE) ---
  test("with_ basic", () => {
    strictEqual(
      sql(select("x").from_("t").with_("t", "SELECT x FROM t2")),
      "WITH t AS (SELECT x FROM t2) SELECT x FROM t",
    )
  })

  test("with_ materialized", () => {
    strictEqual(
      sql(
        select("x")
          .from_("t")
          .with_("t", "SELECT x FROM t2", { materialized: true }),
      ),
      "WITH t AS MATERIALIZED (SELECT x FROM t2) SELECT x FROM t",
    )
  })

  test("with_ not materialized", () => {
    strictEqual(
      sql(
        select("x")
          .from_("t")
          .with_("t", "SELECT x FROM t2", { materialized: false }),
      ),
      "WITH t AS NOT MATERIALIZED (SELECT x FROM t2) SELECT x FROM t",
    )
  })

  test("with_ recursive", () => {
    strictEqual(
      sql(
        select("x")
          .from_("t")
          .with_("t", "SELECT x FROM t2", { recursive: true }),
      ),
      "WITH RECURSIVE t AS (SELECT x FROM t2) SELECT x FROM t",
    )
  })

  test("with_ column list", () => {
    strictEqual(
      sql(
        select("x").from_("t").with_("t (x, y)", select("x", "y").from_("t2")),
      ),
      "WITH t(x, y) AS (SELECT x, y FROM t2) SELECT x FROM t",
    )
  })

  test("multiple CTEs", () => {
    strictEqual(
      sql(
        select("x")
          .from_("t")
          .with_("t", select("x").from_("t2"))
          .with_("t2", select("x").from_("t3")),
      ),
      "WITH t AS (SELECT x FROM t2), t2 AS (SELECT x FROM t3) SELECT x FROM t",
    )
  })

  test("with_ scalar (ClickHouse)", () => {
    strictEqual(
      sql(
        select("x")
          .with_("var1", select("x").from_("t2"), { scalar: true })
          .from_("t")
          .where("x > var1"),
        "clickhouse",
      ),
      "WITH (SELECT x FROM t2) AS var1 SELECT x FROM t WHERE x > var1",
    )
  })

  // --- CTAS ---
  test("ctas", () => {
    strictEqual(
      sql(parseOne("SELECT * FROM y").assertIs(exp.Select).ctas("x")),
      "CREATE TABLE x AS SELECT * FROM y",
    )
  })

  // --- Condition operators ---
  test("condition eq / neq / is_", () => {
    const x = condition("x")
    strictEqual(sql(x.eq(1)), "x = 1")
    strictEqual(sql(x.neq(1)), "x <> 1")
    strictEqual(sql(x.is_(new exp.Null({}))), "x IS NULL")
  })

  test("condition like / ilike / rlike", () => {
    const x = condition("x")
    strictEqual(sql(x.like("y")), "x LIKE 'y'")
    strictEqual(sql(x.ilike("y")), "x ILIKE 'y'")
    strictEqual(sql(x.rlike("y")), "REGEXP_LIKE(x, 'y')")
  })

  test("condition isin", () => {
    strictEqual(sql(condition("x").isin(1, "2")), "x IN (1, '2')")
  })

  test("condition isin with query", () => {
    strictEqual(
      sql(condition("x").isin({ query: "select 1" })),
      "x IN (SELECT 1)",
    )
  })

  test("condition isin with unnest", () => {
    strictEqual(
      sql(condition("x").isin({ unnest: "x" })),
      "x IN (SELECT UNNEST(x))",
    )
  })

  test("condition between", () => {
    strictEqual(sql(condition("x").between(1, 2)), "x BETWEEN 1 AND 2")
  })

  test("condition as_", () => {
    strictEqual(sql(condition("x").as_("y")), "x AS y")
  })

  test("condition asc / desc", () => {
    strictEqual(sql(exp.column("x").asc()), "x")
    strictEqual(sql(exp.column("x").asc(false)), "x NULLS LAST")
    strictEqual(sql(exp.column("x").desc()), "x DESC")
    strictEqual(sql(exp.column("x").desc(true)), "x DESC NULLS FIRST")
  })

  test("Ordered follows Python shape", () => {
    const ordered = parseOne("SELECT x FROM t ORDER BY x DESC NULLS FIRST")
      .find(exp.Ordered)
      ?.assertIs(exp.Ordered)

    ok(ordered)
    strictEqual(ordered.args.desc, true)
    strictEqual(ordered.args.nulls_first, true)
    strictEqual(typeof ordered.desc, "function")
    strictEqual("nullsFirst" in ordered, false)
  })

  // --- and_ / or_ / not_ ---
  test("and_", () => {
    strictEqual(sql(and_("x=1", "y=1")!), "x = 1 AND y = 1")
  })

  test("and_ with three args", () => {
    strictEqual(sql(and_("x=1", "y=1", "z=1")!), "x = 1 AND y = 1 AND z = 1")
  })

  test("and_ nested", () => {
    strictEqual(
      sql(and_("x=1", and_("y=1", "z=1"))!),
      "x = 1 AND (y = 1 AND z = 1)",
    )
  })

  test("or_ combined with and_", () => {
    strictEqual(
      sql(or_(and_("x=1", "y=1"), "z=1")!),
      "(x = 1 AND y = 1) OR z = 1",
    )
  })

  test("not_", () => {
    strictEqual(sql(not_("x=1")), "NOT x = 1")
  })

  test("condition.and_().or_() chaining", () => {
    strictEqual(
      sql((condition("x=1").and_("y=1") as exp.Condition).or_("z=1")),
      "(x = 1 AND y = 1) OR z = 1",
    )
  })

  test("condition.not_() chaining", () => {
    strictEqual(
      sql((condition("x=1").and_("y=1") as exp.Condition).not_()),
      "NOT (x = 1 AND y = 1)",
    )
  })

  test("where with condition expression", () => {
    strictEqual(
      sql(select("*").from_("x").where(condition("y=1").and_("z=1"))),
      "SELECT * FROM x WHERE y = 1 AND z = 1",
    )
  })

  // --- Case ---
  test("case when else", () => {
    strictEqual(
      sql(case_().when("x = 1", "x").else_("bar")),
      "CASE WHEN x = 1 THEN x ELSE bar END",
    )
  })

  test("case with expression", () => {
    strictEqual(
      sql(case_("x").when("1", "x").else_("bar")),
      "CASE x WHEN 1 THEN x ELSE bar END",
    )
  })

  // --- Union / Intersect / Except ---
  test("union", () => {
    strictEqual(
      sql(union("SELECT * FROM foo", "SELECT * FROM bar")),
      "SELECT * FROM foo UNION SELECT * FROM bar",
    )
  })

  test("union ALL", () => {
    strictEqual(
      sql(
        parseOne("SELECT * FROM foo")
          .assertIs(exp.Select)
          .union("SELECT * FROM bar", { distinct: false }),
      ),
      "SELECT * FROM foo UNION ALL SELECT * FROM bar",
    )
  })

  test("union multi", () => {
    strictEqual(
      sql(union("SELECT 1", "SELECT 2", "SELECT 3", "SELECT 4")),
      "SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4",
    )
  })

  test("intersect", () => {
    strictEqual(
      sql(intersect("SELECT * FROM foo", "SELECT * FROM bar")),
      "SELECT * FROM foo INTERSECT SELECT * FROM bar",
    )
  })

  test("except_", () => {
    strictEqual(
      sql(except_("SELECT * FROM foo", "SELECT * FROM bar")),
      "SELECT * FROM foo EXCEPT SELECT * FROM bar",
    )
  })

  test("Query.union()", () => {
    strictEqual(
      sql(
        parseOne("SELECT * FROM foo")
          .assertIs(exp.Select)
          .union("SELECT * FROM bar"),
      ),
      "SELECT * FROM foo UNION SELECT * FROM bar",
    )
  })

  test("Query.intersect()", () => {
    strictEqual(
      sql(
        parseOne("SELECT * FROM foo")
          .assertIs(exp.Select)
          .intersect("SELECT * FROM bar"),
      ),
      "SELECT * FROM foo INTERSECT SELECT * FROM bar",
    )
  })

  test("Query.except_()", () => {
    strictEqual(
      sql(
        parseOne("SELECT * FROM foo")
          .assertIs(exp.Select)
          .except_("SELECT * FROM bar"),
      ),
      "SELECT * FROM foo EXCEPT SELECT * FROM bar",
    )
  })

  test("SetOperation.select()", () => {
    strictEqual(
      sql(parseOne("SELECT 1 UNION SELECT 2").assertIs(exp.Union).select("3")),
      "SELECT 1, 3 UNION SELECT 2, 3",
    )
  })

  // --- Delete ---
  test("delete_ with where", () => {
    strictEqual(
      sql(delete_("y", { where: "x > 1" })),
      "DELETE FROM y WHERE x > 1",
    )
  })

  test("delete_ chaining .where()", () => {
    strictEqual(
      sql(delete_("tbl").where("x = 1")),
      "DELETE FROM tbl WHERE x = 1",
    )
  })

  test("delete_ with returning", () => {
    strictEqual(
      sql(
        delete_("tbl").where("x = 1").returning("*", { dialect: "postgres" }),
        "postgres",
      ),
      "DELETE FROM tbl WHERE x = 1 RETURNING *",
    )
  })

  test("delete_ where AND chaining", () => {
    strictEqual(
      sql(delete_("tbl", { where: "x = 1" }).where("y = 2")),
      "DELETE FROM tbl WHERE x = 1 AND y = 2",
    )
  })

  test("delete_ where with or_ condition", () => {
    strictEqual(
      sql(
        delete_("tbl", { where: "x = 1" }).where(
          condition("y = 2").or_("z = 3"),
        ),
      ),
      "DELETE FROM tbl WHERE x = 1 AND (y = 2 OR z = 3)",
    )
  })

  // --- Update ---
  test("update with properties", () => {
    strictEqual(
      sql(update("tbl", { x: 1 }, { where: "y > 0" })),
      "UPDATE tbl SET x = 1 WHERE y > 0",
    )
  })

  test("update with from_", () => {
    strictEqual(
      sql(update("tbl", { x: 1 }, { from_: "tbl2" })),
      "UPDATE tbl SET x = 1 FROM tbl2",
    )
  })

  test("update chaining .set_().where()", () => {
    strictEqual(
      sql(update("my_table").set_("x = 1").where("y = 2")),
      "UPDATE my_table SET x = 1 WHERE y = 2",
    )
  })

  test("update .set_() appends", () => {
    strictEqual(
      sql(update("my_table").set_("a = 1").set_("b = 2")),
      "UPDATE my_table SET a = 1, b = 2",
    )
  })

  test("update with with_", () => {
    strictEqual(
      sql(
        update("my_table")
          .set_("x = 1")
          .where("my_table.id = baz.id")
          .from_("baz")
          .with_("baz", "SELECT id FROM foo"),
      ),
      "WITH baz AS (SELECT id FROM foo) UPDATE my_table SET x = 1 FROM baz WHERE my_table.id = baz.id",
    )
  })

  // --- Insert ---
  test("insert", () => {
    strictEqual(
      sql(insert("SELECT * FROM t2", "t")),
      "INSERT INTO t SELECT * FROM t2",
    )
  })

  test("insert with returning", () => {
    strictEqual(
      sql(insert("SELECT * FROM t2", "t", { returning: "*" })),
      "INSERT INTO t SELECT * FROM t2 RETURNING *",
    )
  })

  test("insert overwrite", () => {
    strictEqual(
      sql(insert("SELECT * FROM t2", "t", { overwrite: true })),
      "INSERT OVERWRITE TABLE t SELECT * FROM t2",
    )
  })

  test("insert with columns", () => {
    strictEqual(
      sql(insert("VALUES (1, 2), (3, 4)", "t", { columns: ["a", "b"] })),
      "INSERT INTO t (a, b) VALUES (1, 2), (3, 4)",
    )
  })

  test("insert with CTE", () => {
    strictEqual(
      sql(insert("SELECT * FROM cte", "t").with_("cte", "SELECT x FROM tbl")),
      "WITH cte AS (SELECT x FROM tbl) INSERT INTO t SELECT * FROM cte",
    )
  })

  // --- Values ---
  test("values", () => {
    strictEqual(sql(values([["1", 2]])), "VALUES ('1', 2)")
  })

  test("values with alias", () => {
    strictEqual(sql(values([["1", 2]], "alias")), "(VALUES ('1', 2)) AS alias")
  })

  test("values with alias and columns", () => {
    strictEqual(
      sql(
        values(
          [
            ["1", 2, null],
            ["2", 3, null],
          ],
          "alias",
          ["c1", "c2", "c3"],
        ),
      ),
      "(VALUES ('1', 2, NULL), ('2', 3, NULL)) AS alias(c1, c2, c3)",
    )
  })

  test("merge", () => {
    strictEqual(
      sql(
        merge(
          "WHEN MATCHED THEN UPDATE SET col1 = source_table.col1",
          "WHEN NOT MATCHED THEN INSERT (col1) VALUES (source_table.col1)",
          {
            into: "my_table",
            using: "source_table",
            on: "my_table.id = source_table.id",
          },
        ),
      ),
      "MERGE INTO my_table USING source_table ON my_table.id = source_table.id WHEN MATCHED THEN UPDATE SET col1 = source_table.col1 WHEN NOT MATCHED THEN INSERT (col1) VALUES (source_table.col1)",
    )
  })

  // --- Alias ---
  test("alias_", () => {
    strictEqual(
      sql(alias_(parseOne("LAG(x) OVER (PARTITION BY y)"), "a")),
      "LAG(x) OVER (PARTITION BY y) AS a",
    )
  })

  // --- Subquery function ---
  test("exp.subquery()", () => {
    strictEqual(
      sql(exp.subquery("select x from tbl", "foo").select("x").where("x > 0")),
      "SELECT x FROM (SELECT x FROM tbl) AS foo WHERE x > 0",
    )
  })

  // --- BuilderOptions: copy, dialect ---
  test("select with { copy: false } mutates in-place", () => {
    const s = select("a").from_("t")
    const s2 = s.select("b", { copy: false })
    strictEqual(s, s2)
    strictEqual(sql(s), "SELECT a, b FROM t")
  })

  test("groupBy with { copy: false } mutates in-place", () => {
    const s = select("x").from_("t")
    const s2 = s.groupBy("x", { copy: false })
    strictEqual(s, s2)
    strictEqual(sql(s), "SELECT x FROM t GROUP BY x")
  })

  test("where with { copy: false } mutates in-place", () => {
    const s = select("x").from_("t")
    const s2 = s.where("x > 0", { copy: false })
    strictEqual(s, s2)
    strictEqual(sql(s), "SELECT x FROM t WHERE x > 0")
  })

  // --- assertIs ---
  test("assertIs for type narrowing", () => {
    strictEqual(
      sql(parseOne("SELECT a FROM tbl").assertIs(exp.Select).select("b")),
      "SELECT a, b FROM tbl",
    )
  })

  // --- func ---
  test("exp.func()", () => {
    const a = parseOne("x")
    const b = parseOne("1")
    strictEqual(sql(exp.func("COALESCE", a, b)), "COALESCE(x, 1)")
  })

  // --- Query chaining on parsed expressions ---
  test("union limit offset", () => {
    strictEqual(
      sql(
        parseOne("SELECT 1 UNION SELECT 2")
          .assertIs(exp.Union)
          .limit(5)
          .offset(2),
      ),
      "SELECT 1 UNION SELECT 2 LIMIT 5 OFFSET 2",
    )
  })
})
