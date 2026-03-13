/**
 * Re-export all dialects for tree-shakeable imports
 */

export { PostgresDialect } from "./postgres.js"
export { MySQLDialect } from "./mysql.js"
export { BigQueryDialect } from "./bigquery.js"
export { SnowflakeDialect } from "./snowflake.js"
export { DuckDBDialect } from "./duckdb.js"
export { PrestoDialect } from "./presto.js"
export { TrinoDialect } from "./trino.js"
export { SparkDialect, Spark2Dialect } from "./spark.js"
export { HiveDialect } from "./hive.js"
export { ClickHouseDialect } from "./clickhouse.js"
export { TSQLDialect } from "./tsql.js"
export { DatabricksDialect } from "./databricks.js"
export { RedshiftDialect } from "./redshift.js"
export { OracleDialect } from "./oracle.js"
export { StarRocksDialect } from "./starrocks.js"
export { SQLiteDialect } from "./sqlite.js"
export { DorisDialect } from "./doris.js"
export { DrillDialect } from "./drill.js"
export { TeradataDialect } from "./teradata.js"
export { SingleStoreDialect } from "./singlestore.js"
export { DremioDialect } from "./dremio.js"
export { FabricDialect } from "./fabric.js"
export { AthenaDialect } from "./athena.js"
export { ExasolDialect } from "./exasol.js"
export { RisingWaveDialect } from "./risingwave.js"
export { MaterializeDialect } from "./materialize.js"
export { TableauDialect } from "./tableau.js"
export { DruidDialect } from "./druid.js"
export { DuneDialect } from "./dune.js"
export { SolrDialect } from "./solr.js"
export { PRQLDialect } from "./prql.js"

// Convenience singletons — use the registered instances from Dialect.get()
// These match Python SQLGlot's API: `from sqlglot.dialects import DuckDB`
import { Dialect } from "../dialect.js"

function get(name: string): Dialect {
  return Dialect.get(name)
}

export const Athena: Dialect = get("athena")
export const BigQuery: Dialect = get("bigquery")
export const ClickHouse: Dialect = get("clickhouse")
export const Databricks: Dialect = get("databricks")
export const Doris: Dialect = get("doris")
export const Dremio: Dialect = get("dremio")
export const Drill: Dialect = get("drill")
export const Druid: Dialect = get("druid")
export const DuckDB: Dialect = get("duckdb")
export const Dune: Dialect = get("dune")
export const Exasol: Dialect = get("exasol")
export const Fabric: Dialect = get("fabric")
export const Hive: Dialect = get("hive")
export const Materialize: Dialect = get("materialize")
export const MySQL: Dialect = get("mysql")
export const Oracle: Dialect = get("oracle")
export const Postgres: Dialect = get("postgres")
export const Presto: Dialect = get("presto")
export const PRQL: Dialect = get("prql")
export const Redshift: Dialect = get("redshift")
export const RisingWave: Dialect = get("risingwave")
export const SingleStore: Dialect = get("singlestore")
export const Snowflake: Dialect = get("snowflake")
export const Solr: Dialect = get("solr")
export const Spark: Dialect = get("spark")
export const Spark2: Dialect = get("spark2")
export const SQLite: Dialect = get("sqlite")
export const StarRocks: Dialect = get("starrocks")
export const Tableau: Dialect = get("tableau")
export const Teradata: Dialect = get("teradata")
export const Trino: Dialect = get("trino")
export const TSQL: Dialect = get("tsql")
