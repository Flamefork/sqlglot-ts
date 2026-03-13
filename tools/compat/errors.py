class ParseError(Exception):
    pass


class UnsupportedError(Exception):
    pass


class TokenError(Exception):
    pass


class ErrorLevel:
    IGNORE = "IGNORE"
    WARN = "WARN"
    RAISE = "RAISE"
    IMMEDIATE = "IMMEDIATE"


class Expression:
    pass


class Dialects:
    POSTGRES = "postgres"
    MYSQL = "mysql"
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"
    DUCKDB = "duckdb"
    HIVE = "hive"
    SPARK = "spark"
    PRESTO = "presto"
    TRINO = "trino"
    SQLITE = "sqlite"
    ORACLE = "oracle"
    TSQL = "tsql"
    DATABRICKS = "databricks"
    REDSHIFT = "redshift"
    CLICKHOUSE = "clickhouse"
    DRILL = "drill"
    TERADATA = "teradata"
    STARROCKS = "starrocks"
    DORIS = "doris"
    ATHENA = "athena"
