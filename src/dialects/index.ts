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

// Also export as named objects for convenience
import { AthenaDialect } from "./athena.js"
import { BigQueryDialect } from "./bigquery.js"
import { ClickHouseDialect } from "./clickhouse.js"
import { DatabricksDialect } from "./databricks.js"
import { DorisDialect } from "./doris.js"
import { DremioDialect } from "./dremio.js"
import { DrillDialect } from "./drill.js"
import { DruidDialect } from "./druid.js"
import { DuckDBDialect } from "./duckdb.js"
import { DuneDialect } from "./dune.js"
import { ExasolDialect } from "./exasol.js"
import { FabricDialect } from "./fabric.js"
import { HiveDialect } from "./hive.js"
import { MaterializeDialect } from "./materialize.js"
import { MySQLDialect } from "./mysql.js"
import { OracleDialect } from "./oracle.js"
import { PostgresDialect } from "./postgres.js"
import { PrestoDialect } from "./presto.js"
import { PRQLDialect } from "./prql.js"
import { RedshiftDialect } from "./redshift.js"
import { RisingWaveDialect } from "./risingwave.js"
import { SingleStoreDialect } from "./singlestore.js"
import { SnowflakeDialect } from "./snowflake.js"
import { SolrDialect } from "./solr.js"
import { Spark2Dialect, SparkDialect } from "./spark.js"
import { SQLiteDialect } from "./sqlite.js"
import { StarRocksDialect } from "./starrocks.js"
import { TableauDialect } from "./tableau.js"
import { TeradataDialect } from "./teradata.js"
import { TrinoDialect } from "./trino.js"
import { TSQLDialect } from "./tsql.js"

export const Athena: AthenaDialect = new AthenaDialect()
export const BigQuery: BigQueryDialect = new BigQueryDialect()
export const ClickHouse: ClickHouseDialect = new ClickHouseDialect()
export const Databricks: DatabricksDialect = new DatabricksDialect()
export const Doris: DorisDialect = new DorisDialect()
export const Dremio: DremioDialect = new DremioDialect()
export const Drill: DrillDialect = new DrillDialect()
export const Druid: DruidDialect = new DruidDialect()
export const DuckDB: DuckDBDialect = new DuckDBDialect()
export const Dune: DuneDialect = new DuneDialect()
export const Exasol: ExasolDialect = new ExasolDialect()
export const Fabric: FabricDialect = new FabricDialect()
export const Hive: HiveDialect = new HiveDialect()
export const Materialize: MaterializeDialect = new MaterializeDialect()
export const MySQL: MySQLDialect = new MySQLDialect()
export const Oracle: OracleDialect = new OracleDialect()
export const Postgres: PostgresDialect = new PostgresDialect()
export const Presto: PrestoDialect = new PrestoDialect()
export const PRQL: PRQLDialect = new PRQLDialect()
export const Redshift: RedshiftDialect = new RedshiftDialect()
export const RisingWave: RisingWaveDialect = new RisingWaveDialect()
export const SingleStore: SingleStoreDialect = new SingleStoreDialect()
export const Snowflake: SnowflakeDialect = new SnowflakeDialect()
export const Solr: SolrDialect = new SolrDialect()
export const Spark: SparkDialect = new SparkDialect()
export const Spark2: Spark2Dialect = new Spark2Dialect()
export const SQLite: SQLiteDialect = new SQLiteDialect()
export const StarRocks: StarRocksDialect = new StarRocksDialect()
export const Tableau: TableauDialect = new TableauDialect()
export const Teradata: TeradataDialect = new TeradataDialect()
export const Trino: TrinoDialect = new TrinoDialect()
export const TSQL: TSQLDialect = new TSQLDialect()
