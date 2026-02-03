#!/usr/bin/env python3
"""
Minimal fake sqlglot module backed by TypeScript via subprocess bridge.
Only provides parse_one, parse, transpile - no expression builders or optimizer.
"""

import datetime
import json
import select
import subprocess
import sys
import types
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).parent.parent


import logging

_sqlglot_logger = logging.getLogger("sqlglot")


class TSBridge:
    _instance: "TSBridge | None" = None

    def __init__(self):
        self.proc = subprocess.Popen(
            ["node", "tools/ts_bridge.js"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=PROJECT_ROOT,
        )

    @classmethod
    def get(cls) -> "TSBridge":
        if cls._instance is None:
            cls._instance = TSBridge()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        if cls._instance is not None:
            cls._instance.close()
            cls._instance = None

    def call(self, method: str, timeout: float = 30.0, **kwargs: Any) -> dict:
        cmd = {"method": method, **kwargs}
        assert self.proc.stdin is not None
        assert self.proc.stdout is not None
        self.proc.stdin.write(json.dumps(cmd) + "\n")
        self.proc.stdin.flush()

        ready, _, _ = select.select([self.proc.stdout], [], [], timeout)
        if not ready:
            self.proc.kill()
            raise TimeoutError(f"Bridge call '{method}' timed out after {timeout}s")

        line = self.proc.stdout.readline()
        if not line:
            assert self.proc.stderr is not None
            stderr = self.proc.stderr.read()
            raise RuntimeError(f"Bridge died: {stderr}")
        return json.loads(line)

    def close(self):
        if self.proc.stdin:
            self.proc.stdin.close()
        self.proc.wait()


def _emit_bridge_logs(result: dict) -> None:
    for msg in result.get("logs", []):
        if msg.startswith("WARNING:sqlglot:"):
            _sqlglot_logger.warning(msg[len("WARNING:sqlglot:"):])
        else:
            _sqlglot_logger.info(msg)


def deserialize(data: dict) -> Any:
    t = data.get("type")
    if t == "null":
        return None
    if t == "string":
        return data["value"]
    if t == "number":
        return data["value"]
    if t == "boolean":
        return data["value"]
    if t == "array":
        return [deserialize(v) for v in data["value"]]
    if t == "expr":
        return ExpressionProxy(data["id"], data["key"])
    if t == "object":
        return {k: deserialize(v) for k, v in data["value"].items()}
    if t == "method":
        return None
    if t == "unknown":
        return data.get("value")
    raise ValueError(f"Unknown deserialize type: {t}")


def serialize_arg(arg: Any) -> Any:
    if isinstance(arg, ExpressionProxy):
        return {"__expr_id__": arg._id}
    if isinstance(arg, list):
        return [serialize_arg(a) for a in arg]
    if isinstance(arg, dict):
        return {k: serialize_arg(v) for k, v in arg.items()}
    return arg


class ExpressionProxyMeta(type):
    """Metaclass that allows isinstance checks and expression construction."""
    def __instancecheck__(cls, instance: Any) -> bool:
        if isinstance(instance, ExpressionProxy):
            return instance.key.lower() == cls.__name__.lower()
        return super().__instancecheck__(instance)

    def __call__(cls, **kwargs: Any) -> "ExpressionProxy":
        bridge = TSBridge.get()
        serialized = {k: serialize_arg(v) for k, v in kwargs.items()}
        result = bridge.call("createExpression", className=cls.__name__, args=serialized)
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])


class ExpressionProxy:
    _all_ids: list[int] = []

    def __init__(self, expr_id: int, key: str):
        object.__setattr__(self, "_id", expr_id)
        object.__setattr__(self, "_key", key)
        ExpressionProxy._all_ids.append(expr_id)

    @classmethod
    def release_all(cls) -> None:
        if cls._all_ids:
            bridge = TSBridge.get()
            bridge.call("release", ids=cls._all_ids)
            cls._all_ids = []

    @property
    def key(self) -> str:
        return object.__getattribute__(self, "_key")

    def sql(
        self,
        dialect: str | None = None,
        pretty: bool = False,
        identify: bool = False,
        unsupported_level: str | None = None,
        **kwargs: Any,
    ) -> str:
        bridge = TSBridge.get()
        result = bridge.call(
            "sql",
            id=self._id,
            dialect=dialect or "",
            pretty=pretty,
            identify=identify,
            unsupportedLevel=unsupported_level or "",
        )
        if not result["ok"]:
            error_msg = result.get("error", "Unknown error")
            if result.get("errorType") == "UnsupportedError":
                raise UnsupportedError(error_msg)
            raise ValueError(error_msg)
        _emit_bridge_logs(result)
        return result["sql"]

    def text(self, name: str) -> str:
        bridge = TSBridge.get()
        result = bridge.call("text", id=self._id, name=name)
        if not result["ok"]:
            raise AttributeError(result["error"])
        return result["value"]

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)

        bridge = TSBridge.get()
        result = bridge.call("getattr", id=self._id, name=name)
        if not result["ok"]:
            raise AttributeError(result["error"])

        value = result["value"]
        if value.get("type") == "method":
            def method_proxy(*args: Any, **kwargs: Any) -> Any:
                serialized_args = [serialize_arg(a) for a in args]
                serialized_kwargs = {k: serialize_arg(v) for k, v in kwargs.items()}
                res = bridge.call("call", id=self._id, name=name,
                                  args=serialized_args, kwargs=serialized_kwargs)
                if not res["ok"]:
                    raise ValueError(res["error"])
                return deserialize(res["value"])
            return method_proxy

        return deserialize(value)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ExpressionProxy):
            return self._id == other._id
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._id)

    def __repr__(self) -> str:
        return f"<Expr:{self._key}#{self._id}>"

    def assert_is(self, expr_type: type) -> "ExpressionProxy":
        bridge = TSBridge.get()
        result = bridge.call("assertIs", id=self._id, expectedKey=expr_type.__name__)
        if not result["ok"]:
            raise AssertionError(result["error"])
        return self

    def find(self, expr_type: type) -> "ExpressionProxy | None":
        bridge = TSBridge.get()
        result = bridge.call("find", id=self._id, exprType=expr_type.__name__)
        if not result["ok"]:
            raise ValueError(result["error"])
        return deserialize(result["value"])

    def find_all(self, expr_type: type):
        bridge = TSBridge.get()
        result = bridge.call("findAll", id=self._id, exprType=expr_type.__name__)
        if not result["ok"]:
            raise ValueError(result["error"])
        return [ExpressionProxy(v["id"], v["key"]) for v in result["values"]]


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


def parse_one(sql: str, read: str | None = None, into: type | None = None, **kwargs: Any) -> ExpressionProxy:
    bridge = TSBridge.get()
    dialect = str(read) if read else ""

    into_name = getattr(into, "__name__", None) if into else None

    if into_name == "Command":
        try:
            result = bridge.call("parseOne", sql=sql, dialect=dialect)
            if not result["ok"]:
                raise ParseError(result["error"])
            _emit_bridge_logs(result)
            return ExpressionProxy(result["id"], result["key"])
        except ParseError:
            cmd_result = bridge.call("createExpression", className="Command", args={"this": sql})
            if not cmd_result["ok"]:
                raise ParseError(cmd_result.get("error", "Failed to create Command"))
            return ExpressionProxy(cmd_result["id"], cmd_result["key"])

    result = bridge.call("parseOne", sql=sql, dialect=dialect)
    if not result["ok"]:
        raise ParseError(result["error"])
    _emit_bridge_logs(result)
    proxy = ExpressionProxy(result["id"], result["key"])

    if into_name and into_name != "Command":
        found = proxy.find(into)
        if found is not None:
            return found
    return proxy


def parse(sql: str, read: str | None = None, **kwargs: Any) -> list[ExpressionProxy]:
    bridge = TSBridge.get()
    dialect = str(read) if read else ""
    result = bridge.call("parse", sql=sql, dialect=dialect)
    if not result["ok"]:
        raise ParseError(result["error"])
    return [ExpressionProxy(id, key) for id, key in zip(result["ids"], result["keys"])]


def transpile(sql: str, read: str | None = None, write: str | None = None, **kwargs: Any) -> list[str]:
    bridge = TSBridge.get()
    result = bridge.call(
        "transpile",
        sql=sql,
        readDialect=str(read) if read else "",
        writeDialect=str(write) if write else "",
    )
    if not result["ok"]:
        raise ParseError(result["error"])
    return result["sql"]


# Minimal Expression base class for isinstance checks
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


def register_fake_sqlglot():
    """Register minimal fake sqlglot module in sys.modules."""
    import logging

    sqlglot_mod = types.ModuleType("sqlglot")
    sqlglot_mod.parse = parse  # type: ignore
    sqlglot_mod.parse_one = parse_one  # type: ignore
    sqlglot_mod.transpile = transpile  # type: ignore
    sqlglot_mod.Expression = Expression  # type: ignore
    sqlglot_mod.ParseError = ParseError  # type: ignore
    sqlglot_mod.TokenError = TokenError  # type: ignore
    sqlglot_mod.UnsupportedError = UnsupportedError  # type: ignore
    sqlglot_mod.ErrorLevel = ErrorLevel  # type: ignore
    sqlglot_mod.Dialects = Dialects  # type: ignore
    sys.modules["sqlglot"] = sqlglot_mod

    # Helper function for DataType.build
    def _datatype_build(dtype: str | Any, dialect: str | None = None, udt: bool = False, copy: bool = True, **kwargs: Any) -> ExpressionProxy:
        """Build a DataType from a string or existing DataType."""
        if isinstance(dtype, ExpressionProxy) and dtype.key.lower() == "datatype":
            return dtype
        # Parse as datatype
        result = parse_one(f"CAST(x AS {dtype})", read=dialect)
        dt = result.find(sqlglot_exp.DataType)  # type: ignore
        if dt is None:
            raise ValueError(f"Could not parse datatype from: {dtype}")
        return dt

    # Helper function for Literal.string
    def _literal_string(value: str, **kwargs: Any) -> ExpressionProxy:
        bridge = TSBridge.get()
        result = bridge.call("createExpression", className="Literal", args={"this": value, "is_string": True})
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    # Helper function for Literal.number
    def _literal_number(value: Any, **kwargs: Any) -> ExpressionProxy:
        bridge = TSBridge.get()
        result = bridge.call("createExpression", className="Literal", args={"this": str(value), "is_string": False})
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    # Simple enum-like class for DataType.Type values
    class DataTypeEnum:
        """Minimal enum-like class for DataType.Type values."""
        def __init__(self, value: str):
            self.value = value
            self.name = value

        def __str__(self) -> str:
            return self.value

        def __repr__(self) -> str:
            return f"Type.{self.name}"

        def __eq__(self, other: Any) -> bool:
            if isinstance(other, DataTypeEnum):
                return self.value == other.value
            return str(self) == str(other)

        def __hash__(self) -> int:
            return hash(self.value)

    # Minimal exp module with dynamic expression class creation
    # This allows tests to use exp.Install, exp.Show, etc. for type checks
    class ExpModule(types.ModuleType):
        _expr_classes: dict[str, type] = {}

        def __getattr__(self, name: str) -> Any:
            if name.startswith("_"):
                raise AttributeError(name)
            # Return cached class or create new one
            if name not in self._expr_classes:
                # Create a new Expression subclass with metaclass for isinstance checks
                cls = ExpressionProxyMeta(name, (Expression,), {"__name__": name})

                # Add static methods and attributes for special classes
                if name == "DataType":
                    cls.build = staticmethod(_datatype_build)  # type: ignore
                    # Add Type enum with all data type constants
                    type_enum = types.SimpleNamespace()
                    for type_name in [
                        "AGGREGATEFUNCTION", "ARRAY", "BIGDECIMAL", "BIGINT", "BIGNUM",
                        "BIGSERIAL", "BINARY", "BIT", "BLOB", "BOOLEAN", "BPCHAR", "CHAR",
                        "DATE", "DATE32", "DATEMULTIRANGE", "DATERANGE", "DATETIME",
                        "DATETIME2", "DATETIME64", "DECIMAL", "DECIMAL32", "DECIMAL64",
                        "DECIMAL128", "DECIMAL256", "DECFLOAT", "DOUBLE", "DYNAMIC",
                        "ENUM", "ENUM8", "ENUM16", "FILE", "FIXEDSTRING", "FLOAT",
                        "GEOGRAPHY", "GEOGRAPHYPOINT", "GEOMETRY", "HLLSKETCH", "HSTORE",
                        "IMAGE", "INET", "INT", "INT128", "INT256", "INT4MULTIRANGE",
                        "INT4RANGE", "INT8MULTIRANGE", "INT8RANGE", "INTERVAL",
                        "IPADDRESS", "IPPREFIX", "IPV4", "IPV6", "JSON", "JSONB", "LIST",
                        "LINESTRING", "LONGBLOB", "LONGTEXT", "LOWCARDINALITY", "MAP",
                        "MEDIUMBLOB", "MEDIUMINT", "MEDIUMTEXT", "MONEY",
                        "MULTILINESTRING", "MULTIPOLYGON", "NAME", "NCHAR", "NESTED",
                        "NOTHING", "NULL", "NUMERIC", "NUMMULTIRANGE", "NUMRANGE",
                        "NVARCHAR", "OBJECT", "POINT", "POLYGON", "RANGE", "REAL",
                        "RING", "ROWVERSION", "SERIAL", "SET", "SIMPLEAGGREGATEFUNCTION",
                        "SMALLDATETIME", "SMALLINT", "SMALLMONEY", "SMALLSERIAL",
                        "STRUCT", "SUPER", "TEXT", "TIME", "TIMETZ", "TIME_NS",
                        "TIMESTAMP", "TIMESTAMPNTZ", "TIMESTAMPLTZ", "TIMESTAMPTZ",
                        "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS",
                        "TINYBLOB", "TINYINT", "TINYTEXT",
                        "TSMULTIRANGE", "TSRANGE", "TSTZMULTIRANGE", "TSTZRANGE",
                        "UBIGINT", "UDECIMAL", "UDOUBLE", "UINT", "UINT128", "UINT256",
                        "UMEDIUMINT", "UNION", "UNIQUEIDENTIFIER", "UNKNOWN",
                        "USERDEFINED", "USMALLINT", "UTINYINT", "UUID",
                        "VARBINARY", "VARCHAR", "VARIANT", "XML"
                    ]:
                        setattr(type_enum, type_name, DataTypeEnum(type_name))
                    cls.Type = type_enum  # type: ignore
                elif name == "Literal":
                    cls.string = staticmethod(_literal_string)  # type: ignore
                    cls.number = staticmethod(_literal_number)  # type: ignore

                self._expr_classes[name] = cls
            return self._expr_classes[name]

    def _to_identifier(name: str | None, quoted: bool | None = None, copy: bool = True) -> ExpressionProxy | None:
        if name is None:
            return None
        bridge = TSBridge.get()
        args: dict[str, Any] = {"this": name}
        if quoted is not None:
            args["quoted"] = quoted
        result = bridge.call("createExpression", className="Identifier", args=args)
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    class SelectBuilder:
        def __init__(self, *expressions: Any):
            self._select_exprs = [self._to_str(e) for e in expressions]
            self._from: str | None = None
            self._limit: str | None = None
            self._where: str | None = None
            self._offset: str | None = None

        @staticmethod
        def _to_str(e: Any) -> str:
            if isinstance(e, ExpressionProxy):
                return e.sql()
            return str(e)

        def select(self, *expressions: Any, **kwargs: Any) -> "SelectBuilder":
            self._select_exprs.extend(self._to_str(e) for e in expressions)
            return self

        def from_(self, table: Any, **kwargs: Any) -> "SelectBuilder":
            self._from = self._to_str(table)
            return self

        def limit(self, expr: Any, **kwargs: Any) -> "SelectBuilder":
            self._limit = self._to_str(expr)
            return self

        def where(self, expr: Any, **kwargs: Any) -> "SelectBuilder":
            self._where = self._to_str(expr)
            return self

        def offset(self, expr: Any, **kwargs: Any) -> "SelectBuilder":
            self._offset = self._to_str(expr)
            return self

        def subquery(self, **kwargs: Any) -> ExpressionProxy:
            return parse_one(f"({self._build_sql()})")

        def _build_sql(self) -> str:
            parts = [f"SELECT {', '.join(self._select_exprs)}"]
            if self._from:
                parts.append(f"FROM {self._from}")
            if self._where:
                parts.append(f"WHERE {self._where}")
            if self._limit:
                parts.append(f"LIMIT {self._limit}")
            if self._offset:
                parts.append(f"OFFSET {self._offset}")
            return " ".join(parts)

        def sql(self, dialect: str | None = None, **kwargs: Any) -> str:
            sql = self._build_sql()
            return parse_one(sql, read=dialect).sql(dialect=dialect, **kwargs)

    def _select(*expressions: Any, **kwargs: Any) -> SelectBuilder:
        return SelectBuilder(*expressions)

    def _maybe_parse(sql_or_expression: Any, into: Any = None, dialect: Any = None, prefix: str | None = None, copy: bool = False, **kwargs: Any) -> Any:
        if isinstance(sql_or_expression, ExpressionProxy):
            return sql_or_expression
        sql_str = str(sql_or_expression)
        if into is not None and getattr(into, "__name__", "") == "DataType":
            wrapper = parse_one(f"CAST(x AS {sql_str})", read=dialect)
            return wrapper.find(into)
        return parse_one(sql_str, read=dialect)

    def _to_table(sql_path: str | Any, dialect: str | None = None, copy: bool = True, **kwargs: Any) -> ExpressionProxy:
        """Create a table expression from a [catalog].[schema].[table] sql path."""
        if isinstance(sql_path, ExpressionProxy) and sql_path.key.lower() == "table":
            return sql_path
        # Parse as table reference
        result = parse_one(f"SELECT * FROM {sql_path}", read=dialect)
        table = result.find(sqlglot_exp.Table)  # type: ignore
        if table is None:
            raise ValueError(f"Could not parse table from: {sql_path}")
        return table

    def _to_column(sql_path: str | Any, quoted: bool | None = None, dialect: str | None = None, copy: bool = True, **kwargs: Any) -> ExpressionProxy:
        """Create a column expression from a [table].[column] sql path."""
        if isinstance(sql_path, ExpressionProxy) and sql_path.key.lower() == "column":
            return sql_path
        # Parse as column reference
        result = parse_one(f"SELECT {sql_path}", read=dialect)
        # Find the first Column expression
        col = result.find(sqlglot_exp.Column)  # type: ignore
        if col is None:
            raise ValueError(f"Could not parse column from: {sql_path}")
        return col

    def _column(col: str, table: str | None = None, db: str | None = None, catalog: str | None = None, quoted: bool | None = None, **kwargs: Any) -> ExpressionProxy:
        """Build a Column expression."""
        parts = []
        if catalog:
            parts.append(catalog)
        if db:
            parts.append(db)
        if table:
            parts.append(table)
        parts.append(col)
        return _to_column(".".join(parts), quoted=quoted)

    def _table(table: str, db: str | None = None, catalog: str | None = None, quoted: bool | None = None, alias: str | None = None, **kwargs: Any) -> ExpressionProxy:
        """Build a Table expression."""
        parts = []
        if catalog:
            parts.append(catalog)
        if db:
            parts.append(db)
        parts.append(table)
        path = ".".join(parts)
        if alias:
            path = f"{path} AS {alias}"
        return _to_table(path)

    def _cast(expression: str | Any, to: str, **kwargs: Any) -> ExpressionProxy:
        """Cast an expression to a data type."""
        if isinstance(expression, ExpressionProxy):
            expr_sql = expression.sql()
        else:
            expr_sql = str(expression)
        result = parse_one(f"CAST({expr_sql} AS {to})")
        return result

    def _merge(*when_exprs: Any, into: str, using: str, on: str, returning: str | None = None, **kwargs: Any) -> ExpressionProxy:
        """Build a MERGE statement."""
        when_clauses = " ".join(str(w) for w in when_exprs)
        sql = f"MERGE INTO {into} USING {using} ON {on} {when_clauses}"
        if returning:
            sql += f" RETURNING {returning}"
        return parse_one(sql)

    def _func(name: str, *args: Any, **kwargs: Any) -> ExpressionProxy:
        """Build a function call."""
        arg_strs = []
        for arg in args:
            if isinstance(arg, ExpressionProxy):
                arg_strs.append(arg.sql())
            else:
                arg_strs.append(str(arg))
        sql = f"{name}({', '.join(arg_strs)})"
        return parse_one(sql)

    def _convert(value: Any, copy: bool = False, **kwargs: Any) -> Any:
        if isinstance(value, ExpressionProxy):
            return value
        if isinstance(value, str):
            return _literal_string(value)
        if isinstance(value, bool):
            bridge = TSBridge.get()
            result = bridge.call("createExpression", className="Boolean", args={"this": value})
            if not result["ok"]:
                raise ValueError(result["error"])
            return ExpressionProxy(result["id"], result["key"])
        if value is None:
            bridge = TSBridge.get()
            result = bridge.call("createExpression", className="Null", args={})
            if not result["ok"]:
                raise ValueError(result["error"])
            return ExpressionProxy(result["id"], result["key"])
        if isinstance(value, (int, float)):
            return _literal_number(value)
        if isinstance(value, datetime.datetime):
            datetime_str = value.isoformat(sep=" ")
            literal = _literal_string(datetime_str)
            bridge = TSBridge.get()
            args: dict[str, Any] = {"this": {"__expr_id__": literal._id}}
            if value.tzinfo:
                tz_literal = _literal_string(str(value.tzinfo))
                args["zone"] = {"__expr_id__": tz_literal._id}
            result = bridge.call("createExpression", className="TimeStrToTime", args=args)
            if not result["ok"]:
                raise ValueError(result["error"])
            return ExpressionProxy(result["id"], result["key"])
        if isinstance(value, datetime.date):
            date_str = value.strftime("%Y-%m-%d")
            literal = _literal_string(date_str)
            bridge = TSBridge.get()
            result = bridge.call("createExpression", className="DateStrToDate",
                                 args={"this": {"__expr_id__": literal._id}})
            if not result["ok"]:
                raise ValueError(result["error"])
            return ExpressionProxy(result["id"], result["key"])
        return value

    sqlglot_exp = ExpModule("sqlglot.expressions")
    sqlglot_exp.Expression = Expression  # type: ignore
    sqlglot_exp.convert = _convert  # type: ignore
    sqlglot_exp.to_identifier = _to_identifier  # type: ignore
    sqlglot_exp.maybe_parse = _maybe_parse  # type: ignore
    sqlglot_exp.select = _select  # type: ignore
    sqlglot_exp.to_table = _to_table  # type: ignore
    sqlglot_exp.to_column = _to_column  # type: ignore
    sqlglot_exp.column = _column  # type: ignore
    sqlglot_exp.table_ = _table  # type: ignore
    sqlglot_exp.cast = _cast  # type: ignore
    sqlglot_exp.merge = _merge  # type: ignore
    sqlglot_exp.func = _func  # type: ignore
    sys.modules["sqlglot.expressions"] = sqlglot_exp
    sqlglot_mod.exp = sqlglot_exp  # type: ignore

    # Logger modules needed by test imports
    # helper.logger uses "sqlglot" (not "sqlglot.helper") to match real sqlglot
    for submod in ["helper", "generator", "parser"]:
        mod = types.ModuleType(f"sqlglot.{submod}")
        # Both helper and generator use logging.getLogger("sqlglot") in real SQLGlot
        logger_name = "sqlglot" if submod in ("helper", "generator") else f"sqlglot.{submod}"
        mod.logger = logging.getLogger(logger_name)  # type: ignore
        sys.modules[f"sqlglot.{submod}"] = mod

    # Optimizer stubs - raise NotImplementedError when called
    def _not_implemented(*args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("Optimizer functions are not implemented in sqlglot-ts")

    sqlglot_optimizer = types.ModuleType("sqlglot.optimizer")
    sqlglot_optimizer.__path__ = []  # type: ignore
    sys.modules["sqlglot.optimizer"] = sqlglot_optimizer

    def _annotate_types(expression: ExpressionProxy, **kwargs: Any) -> ExpressionProxy:
        bridge = TSBridge.get()
        result = bridge.call("annotateTypes", id=expression._id)
        if not result["ok"]:
            raise ValueError(result["error"])
        return expression

    optimizer_annotate = types.ModuleType("sqlglot.optimizer.annotate_types")
    optimizer_annotate.annotate_types = _annotate_types  # type: ignore
    sys.modules["sqlglot.optimizer.annotate_types"] = optimizer_annotate

    optimizer_qualify = types.ModuleType("sqlglot.optimizer.qualify")
    optimizer_qualify.qualify = _not_implemented  # type: ignore
    sys.modules["sqlglot.optimizer.qualify"] = optimizer_qualify

    optimizer_normalize = types.ModuleType("sqlglot.optimizer.normalize_identifiers")
    optimizer_normalize.normalize_identifiers = _not_implemented  # type: ignore
    sys.modules["sqlglot.optimizer.normalize_identifiers"] = optimizer_normalize

    optimizer_qualify_columns = types.ModuleType("sqlglot.optimizer.qualify_columns")
    optimizer_qualify_columns.quote_identifiers = _not_implemented  # type: ignore
    sys.modules["sqlglot.optimizer.qualify_columns"] = optimizer_qualify_columns

    # traverse_scope function directly on optimizer module
    sqlglot_optimizer.traverse_scope = _not_implemented  # type: ignore

    # Dialects submodule
    sqlglot_dialects = types.ModuleType("sqlglot.dialects")
    sqlglot_dialects.__path__ = []  # type: ignore
    sys.modules["sqlglot.dialects"] = sqlglot_dialects
    sqlglot_mod.dialects = sqlglot_dialects  # type: ignore

    # Create empty dialect classes for imports
    dialect_names = [
        "BigQuery", "ClickHouse", "Databricks", "Doris", "Drill", "Dremio",
        "DuckDB", "Hive", "MySQL", "Oracle", "Postgres", "Presto", "Prql",
        "Redshift", "Snowflake", "Spark", "Spark2", "SQLite", "StarRocks",
        "Tableau", "Teradata", "Trino", "TSQL", "Athena", "Dune", "Exasol",
        "Fabric", "Materialize", "RisingWave", "SingleStore", "Solr"
    ]
    for name in dialect_names:
        # Create generator class with dialect-specific attributes
        generator_attrs: dict[str, Any] = {}
        if name == "DuckDB":
            # Use expression proxy classes so tuple concatenation and `in` checks work
            generator_attrs["IGNORE_RESPECT_NULLS_WINDOW_FUNCTIONS"] = tuple(
                sqlglot_exp.__getattr__(n) for n in ("FirstValue", "LastValue", "NthValue", "Lead", "Lag")
            )
        if name == "MySQL":
            # MySQL generator has CHAR_CAST_MAPPING and SIGNED_CAST_MAPPING attributes
            # Keys must be DataTypeEnum instances (exp.DataType.Type values)
            DataType = sqlglot_exp.__getattr__("DataType")  # Get DataType class with Type enum
            generator_attrs["CHAR_CAST_MAPPING"] = {
                DataType.Type.LONGTEXT: "CHAR",
                DataType.Type.LONGBLOB: "CHAR",
                DataType.Type.MEDIUMBLOB: "CHAR",
                DataType.Type.MEDIUMTEXT: "CHAR",
                DataType.Type.TEXT: "CHAR",
                DataType.Type.TINYBLOB: "CHAR",
                DataType.Type.TINYTEXT: "CHAR",
                DataType.Type.VARCHAR: "CHAR",
            }
            generator_attrs["SIGNED_CAST_MAPPING"] = {
                DataType.Type.BIGINT: "SIGNED",
                DataType.Type.BOOLEAN: "SIGNED",
                DataType.Type.INT: "SIGNED",
                DataType.Type.SMALLINT: "SIGNED",
                DataType.Type.TINYINT: "SIGNED",
                DataType.Type.MEDIUMINT: "SIGNED",
            }
        generator_class = type(f"{name}Generator", (), generator_attrs)
        dialect_class = type(name, (), {"Tokenizer": type(f"{name}Tokenizer", (), {}), "Parser": type(f"{name}Parser", (), {}), "Generator": generator_class})
        setattr(sqlglot_dialects, name, dialect_class)

        # Create individual dialect modules like sqlglot.dialects.mysql
        dialect_module_name = name.lower()
        dialect_mod = types.ModuleType(f"sqlglot.dialects.{dialect_module_name}")
        setattr(dialect_mod, name, dialect_class)
        dialect_mod.logger = logging.getLogger("sqlglot")  # type: ignore
        sys.modules[f"sqlglot.dialects.{dialect_module_name}"] = dialect_mod

    # sqlglot.dialects.dialect module with Dialects class
    dialect_dialect = types.ModuleType("sqlglot.dialects.dialect")
    dialect_dialect.Dialects = Dialects  # type: ignore
    sys.modules["sqlglot.dialects.dialect"] = dialect_dialect

    # Errors module
    sqlglot_errors = types.ModuleType("sqlglot.errors")
    sqlglot_errors.ParseError = ParseError  # type: ignore
    sqlglot_errors.UnsupportedError = UnsupportedError  # type: ignore
    sqlglot_errors.TokenError = TokenError  # type: ignore
    sqlglot_errors.ErrorLevel = ErrorLevel  # type: ignore
    sys.modules["sqlglot.errors"] = sqlglot_errors
