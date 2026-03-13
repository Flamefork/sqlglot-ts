# pyright: reportAttributeAccessIssue=false
import datetime
import logging
import sys
import types
from pathlib import Path
from typing import Any
from typing import ClassVar

from compat.api import parse
from compat.api import parse_one
from compat.api import transpile
from compat.bridge import TSBridge
from compat.errors import Dialects
from compat.errors import ErrorLevel
from compat.errors import Expression
from compat.errors import ParseError
from compat.errors import TokenError
from compat.errors import UnsupportedError
from compat.proxy import ExpressionProxy
from compat.proxy import ExpressionProxyMeta
from compat.proxy import deserialize
from compat.proxy import serialize_arg
from compat.proxy import set_convert_handler
from compat.proxy import set_create_datatype_handler
from compat.proxy import set_parse_one_handler
from compat.tokens import Token
from compat.tokens import Tokenizer
from compat.tokens import TokenType


class DataTypeEnum:
    def __init__(self, value: str):
        self.value = value
        self.name = value

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return f"Type.{self.name}"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DataTypeEnum):
            return self.value == other.value
        return str(self) == str(other)

    def __hash__(self) -> int:
        return hash(self.value)


class FuncClass(Expression):
    arg_types: ClassVar[dict[str, bool]] = {}
    is_var_len_args: ClassVar[bool] = False

    @classmethod
    def default_parser_mappings(cls) -> dict[str, Any]:
        return {}


class ExpModule(types.ModuleType):
    _expr_classes: ClassVar[dict[str, type]] = {}

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        if name == "Func":
            return FuncClass
        if name not in self._expr_classes:
            cls = ExpressionProxyMeta(name, (Expression,), {"__name__": name})

            if name == "DataType":
                cls.build = staticmethod(_datatype_build)
                type_enum = types.SimpleNamespace()
                for type_name in _DATATYPE_NAMES:
                    setattr(type_enum, type_name, DataTypeEnum(type_name))
                cls.Type = type_enum
            elif name == "Literal":
                cls.string = staticmethod(_literal_string)
                cls.number = staticmethod(_literal_number)

            self._expr_classes[name] = cls
        return self._expr_classes[name]


_DATATYPE_NAMES = [
    "AGGREGATEFUNCTION",
    "ARRAY",
    "BIGDECIMAL",
    "BIGINT",
    "BIGNUM",
    "BIGSERIAL",
    "BINARY",
    "BIT",
    "BLOB",
    "BOOLEAN",
    "BPCHAR",
    "CHAR",
    "DATE",
    "DATE32",
    "DATEMULTIRANGE",
    "DATERANGE",
    "DATETIME",
    "DATETIME2",
    "DATETIME64",
    "DECIMAL",
    "DECIMAL32",
    "DECIMAL64",
    "DECIMAL128",
    "DECIMAL256",
    "DECFLOAT",
    "DOUBLE",
    "DYNAMIC",
    "ENUM",
    "ENUM8",
    "ENUM16",
    "FILE",
    "FIXEDSTRING",
    "FLOAT",
    "GEOGRAPHY",
    "GEOGRAPHYPOINT",
    "GEOMETRY",
    "HLLSKETCH",
    "HSTORE",
    "IMAGE",
    "INET",
    "INT",
    "INT128",
    "INT256",
    "INT4MULTIRANGE",
    "INT4RANGE",
    "INT8MULTIRANGE",
    "INT8RANGE",
    "INTERVAL",
    "IPADDRESS",
    "IPPREFIX",
    "IPV4",
    "IPV6",
    "JSON",
    "JSONB",
    "LIST",
    "LINESTRING",
    "LONGBLOB",
    "LONGTEXT",
    "LOWCARDINALITY",
    "MAP",
    "MEDIUMBLOB",
    "MEDIUMINT",
    "MEDIUMTEXT",
    "MONEY",
    "MULTILINESTRING",
    "MULTIPOLYGON",
    "NAME",
    "NCHAR",
    "NESTED",
    "NOTHING",
    "NULL",
    "NUMERIC",
    "NUMMULTIRANGE",
    "NUMRANGE",
    "NVARCHAR",
    "OBJECT",
    "POINT",
    "POLYGON",
    "RANGE",
    "REAL",
    "RING",
    "ROWVERSION",
    "SERIAL",
    "SET",
    "SIMPLEAGGREGATEFUNCTION",
    "SMALLDATETIME",
    "SMALLINT",
    "SMALLMONEY",
    "SMALLSERIAL",
    "STRUCT",
    "SUPER",
    "TEXT",
    "TIME",
    "TIMETZ",
    "TIME_NS",
    "TIMESTAMP",
    "TIMESTAMPNTZ",
    "TIMESTAMPLTZ",
    "TIMESTAMPTZ",
    "TIMESTAMP_S",
    "TIMESTAMP_MS",
    "TIMESTAMP_NS",
    "TINYBLOB",
    "TINYINT",
    "TINYTEXT",
    "TSMULTIRANGE",
    "TSRANGE",
    "TSTZMULTIRANGE",
    "TSTZRANGE",
    "UBIGINT",
    "UDECIMAL",
    "UDOUBLE",
    "UINT",
    "UINT128",
    "UINT256",
    "UMEDIUMINT",
    "UNION",
    "UNIQUEIDENTIFIER",
    "UNKNOWN",
    "USERDEFINED",
    "USMALLINT",
    "UTINYINT",
    "UUID",
    "VARBINARY",
    "VARCHAR",
    "VARIANT",
    "XML",
]

_DIALECT_NAMES = [
    "BigQuery",
    "ClickHouse",
    "Databricks",
    "Doris",
    "Drill",
    "Dremio",
    "DuckDB",
    "Hive",
    "MySQL",
    "Oracle",
    "Postgres",
    "Presto",
    "Prql",
    "Redshift",
    "Snowflake",
    "Spark",
    "Spark2",
    "SQLite",
    "StarRocks",
    "Tableau",
    "Teradata",
    "Trino",
    "TSQL",
    "Athena",
    "Dune",
    "Exasol",
    "Fabric",
    "Materialize",
    "RisingWave",
    "SingleStore",
    "Solr",
]


def _datatype_build(
    dtype: str | Any,
    dialect: str | None = None,
    *,
    _udt: bool = False,
    _copy: bool = True,
    **_kwargs: Any,
) -> ExpressionProxy:
    if isinstance(dtype, ExpressionProxy) and dtype.key.lower() == "datatype":
        return dtype
    result = parse_one(f"CAST(x AS {dtype})", read=dialect)
    dt = result.find(sys.modules["sqlglot.expressions"].DataType)
    if dt is None:
        msg = f"Could not parse datatype from: {dtype}"
        raise ValueError(msg)
    return dt


def _literal_string(value: str, **_kwargs: Any) -> ExpressionProxy:
    bridge = TSBridge.get()
    result = bridge.call(
        "createExpression",
        className="Literal",
        args={"this": value, "is_string": True},
    )
    if not result["ok"]:
        raise ValueError(result["error"])
    return ExpressionProxy(result["id"], result["key"])


def _literal_number(value: Any, **_kwargs: Any) -> ExpressionProxy:
    bridge = TSBridge.get()
    result = bridge.call(
        "createExpression",
        className="Literal",
        args={"this": str(value), "is_string": False},
    )
    if not result["ok"]:
        raise ValueError(result["error"])
    return ExpressionProxy(result["id"], result["key"])


def _to_identifier(
    name: str | None,
    quoted: bool | None = None,  # noqa: FBT001
    *,
    _copy: bool = True,
    **_kwargs: Any,
) -> ExpressionProxy | None:
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


def _call_ts_function(name: str, *args: Any, **kwargs: Any) -> Any:
    bridge = TSBridge.get()
    serialized_args = [serialize_arg(arg) for arg in args]
    serialized_kwargs = {key: serialize_arg(value) for key, value in kwargs.items()}
    result = bridge.call(
        "callFunction", name=name, args=serialized_args, kwargs=serialized_kwargs
    )
    if not result["ok"]:
        raise ValueError(result["error"])
    return deserialize(result["value"])


def _select(*expressions: Any, **_kwargs: Any) -> ExpressionProxy:
    return _call_ts_function("select", *expressions)


def _from_(expression: Any, **_kwargs: Any) -> ExpressionProxy:
    return _call_ts_function("from_", expression)


def _maybe_parse(
    sql_or_expression: Any,
    into: Any = None,
    dialect: Any = None,
    _prefix: str | None = None,
    *,
    _copy: bool = False,
    **_kwargs: Any,
) -> Any:
    if isinstance(sql_or_expression, ExpressionProxy):
        return sql_or_expression
    sql_str = str(sql_or_expression)
    if into is not None and getattr(into, "__name__", "") == "DataType":
        wrapper = parse_one(f"CAST(x AS {sql_str})", read=dialect)
        return wrapper.find(into)
    return parse_one(sql_str, read=dialect)


def _to_table(
    sql_path: str | Any,
    dialect: str | None = None,
    *,
    _copy: bool = True,
    **_kwargs: Any,
) -> ExpressionProxy:
    if isinstance(sql_path, ExpressionProxy) and sql_path.key.lower() == "table":
        return sql_path
    result = parse_one(f"SELECT * FROM {sql_path}", read=dialect)  # noqa: S608
    table = result.find(sys.modules["sqlglot.expressions"].Table)
    if table is None:
        msg = f"Could not parse table from: {sql_path}"
        raise ValueError(msg)
    return table


def _to_column(
    sql_path: str | Any,
    _quoted: bool | None = None,  # noqa: FBT001
    dialect: str | None = None,
    *,
    _copy: bool = True,
    **_kwargs: Any,
) -> ExpressionProxy:
    if isinstance(sql_path, ExpressionProxy) and sql_path.key.lower() == "column":
        return sql_path
    result = parse_one(f"SELECT {sql_path}", read=dialect)
    col = result.find(sys.modules["sqlglot.expressions"].Column)
    if col is None:
        msg = f"Could not parse column from: {sql_path}"
        raise ValueError(msg)
    return col


def _column(
    col: str,
    table: str | None = None,
    db: str | None = None,
    catalog: str | None = None,
    quoted: bool | None = None,  # noqa: FBT001
    **_kwargs: Any,
) -> ExpressionProxy:
    parts = []
    if catalog:
        parts.append(catalog)
    if db:
        parts.append(db)
    if table:
        parts.append(table)
    parts.append(col)
    return _to_column(".".join(parts), quoted=quoted)


def _table(
    table: str,
    db: str | None = None,
    catalog: str | None = None,
    _quoted: bool | None = None,  # noqa: FBT001
    alias: str | None = None,
    **_kwargs: Any,
) -> ExpressionProxy:
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


def _cast(expression: str | Any, to: str, **_kwargs: Any) -> ExpressionProxy:
    if isinstance(expression, str):
        parsed = parse_one(expression)
        if parsed.key == "cast":
            existing_sql = parsed.sql()
            expected_suffix = f" AS {to.upper()})"
            if existing_sql.upper().endswith(expected_suffix):
                return parsed
        expr_sql = parsed.sql()
    elif isinstance(expression, ExpressionProxy):
        if expression.key == "cast":
            existing_sql = expression.sql()
            expected_suffix = f" AS {to.upper()})"
            if existing_sql.upper().endswith(expected_suffix):
                return expression
        expr_sql = expression.sql()
    else:
        expr_sql = str(expression)
    return parse_one(f"CAST({expr_sql} AS {to})")


def _merge(
    *when_exprs: Any,
    into: str,
    using: str,
    on: str,
    returning: str | None = None,
    **_kwargs: Any,
) -> ExpressionProxy:
    opts: dict[str, Any] = {"into": into, "using": using, "on": on}
    if returning is not None:
        opts["returning"] = returning
    return _call_ts_function("merge", *when_exprs, opts)


def _func(name: str, *args: Any, **_kwargs: Any) -> ExpressionProxy:
    arg_strs = []
    for arg in args:
        if isinstance(arg, ExpressionProxy):
            arg_strs.append(arg.sql())
        else:
            arg_strs.append(str(arg))
    return parse_one(f"{name}({', '.join(arg_strs)})")


def _create_expression(class_name: str, args: dict[str, Any]) -> ExpressionProxy:
    bridge = TSBridge.get()
    result = bridge.call("createExpression", className=class_name, args=args)
    if not result["ok"]:
        raise ValueError(result["error"])
    return ExpressionProxy(result["id"], result["key"])


def _convert_to_array(items: list) -> ExpressionProxy:
    converted = [serialize_arg(_convert(item)) for item in items]
    return _create_expression("Anonymous", {"this": "ARRAY", "expressions": converted})


def _convert_dict(value: dict) -> ExpressionProxy:
    keys_proxy = _convert_to_array(list(value.keys()))
    vals_proxy = _convert_to_array(list(value.values()))
    return _create_expression(
        "Map",
        {"keys": serialize_arg(keys_proxy), "values": serialize_arg(vals_proxy)},
    )


def _convert_tuple(value: tuple) -> ExpressionProxy:
    converted = [serialize_arg(_convert(item)) for item in value]
    return _create_expression("Tuple", {"expressions": converted})


def _convert_datetime(value: datetime.datetime) -> ExpressionProxy:
    literal = _literal_string(value.isoformat(sep=" "))
    args: dict[str, Any] = {"this": {"__expr_id__": literal.expr_id}}
    if value.tzinfo:
        tz_literal = _literal_string(str(value.tzinfo))
        args["zone"] = {"__expr_id__": tz_literal.expr_id}
    return _create_expression("TimeStrToTime", args)


def _convert_date(value: datetime.date) -> ExpressionProxy:
    literal = _literal_string(value.strftime("%Y-%m-%d"))
    return _create_expression(
        "DateStrToDate",
        {"this": {"__expr_id__": literal.expr_id}},
    )


_MISSING = object()


def _convert_scalar(value: Any) -> Any:
    if isinstance(value, ExpressionProxy):
        return value
    if isinstance(value, str):
        return _literal_string(value)
    if isinstance(value, bool):
        return _create_expression("Boolean", {"this": value})
    if value is None:
        return _create_expression("Null", {})
    if isinstance(value, (int, float)):
        return _literal_number(value)
    return _MISSING


def _convert_collection(value: Any) -> Any:
    if isinstance(value, list):
        return _convert_to_array(value)
    if isinstance(value, dict):
        return _convert_dict(value)
    if isinstance(value, tuple):
        return _convert_tuple(value)
    if isinstance(value, datetime.datetime):
        return _convert_datetime(value)
    if isinstance(value, datetime.date):
        return _convert_date(value)
    return _MISSING


def _convert(value: Any, *, _copy: bool = False, **_kwargs: Any) -> Any:
    scalar = _convert_scalar(value)
    if scalar is not _MISSING:
        return scalar
    collection = _convert_collection(value)
    if collection is not _MISSING:
        return collection
    return value


def _condition(
    expression: Any,
    dialect: str | None = None,
    *,
    _copy: bool = True,
    **_kwargs: Any,
) -> ExpressionProxy:
    return _call_ts_function("condition", expression, dialect=dialect, copy=_copy)


def _wrap_connector(expr: Any) -> Any:
    if isinstance(expr, ExpressionProxy) and expr.key in {"and", "or", "xor"}:
        bridge = TSBridge.get()
        result = bridge.call(
            "createExpression",
            className="Paren",
            args={"this": serialize_arg(expr)},
        )
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])
    return expr


def _combine(
    expressions: Any,
    operator_name: str,
    dialect: str | None = None,
    *,
    copy: bool = True,
    wrap: bool = True,
    **_kwargs: Any,
) -> ExpressionProxy:
    conditions = [
        _condition(expression, dialect=dialect, _copy=copy)
        for expression in expressions
        if expression is not None
    ]
    this = conditions[0]
    rest = conditions[1:]
    if rest and wrap:
        this = _wrap_connector(this)
    for expression in rest:
        bridge = TSBridge.get()
        wrapped = _wrap_connector(expression) if wrap else expression
        result = bridge.call(
            "createExpression",
            className=operator_name,
            args={
                "this": serialize_arg(this),
                "expression": serialize_arg(wrapped),
            },
        )
        if not result["ok"]:
            raise ValueError(result["error"])
        this = ExpressionProxy(result["id"], result["key"])
    return this


def _and(
    *expressions: Any,
    dialect: str | None = None,
    copy: bool = True,
    wrap: bool = True,
    **_kwargs: Any,
) -> ExpressionProxy:
    return _combine(expressions, "And", dialect=dialect, copy=copy, wrap=wrap)


def _or(
    *expressions: Any,
    dialect: str | None = None,
    copy: bool = True,
    wrap: bool = True,
    **_kwargs: Any,
) -> ExpressionProxy:
    return _combine(expressions, "Or", dialect=dialect, copy=copy, wrap=wrap)


def _not(
    expression: Any,
    dialect: str | None = None,
    *,
    _copy: bool = True,
    **_kwargs: Any,
) -> ExpressionProxy:
    parsed = (
        _condition(expression, dialect=dialect)
        if isinstance(expression, str)
        else expression
    )
    wrapped = _wrap_connector(parsed)
    bridge = TSBridge.get()
    result = bridge.call(
        "createExpression", className="Not", args={"this": serialize_arg(wrapped)}
    )
    if not result["ok"]:
        raise ValueError(result["error"])
    return ExpressionProxy(result["id"], result["key"])


def _alias(expression: Any, alias: str | Any, **_kwargs: Any) -> ExpressionProxy:
    if isinstance(expression, str):
        expression = parse_one(expression)
    alias_str = alias if isinstance(alias, str) else alias.sql()
    return _call_ts_function("alias_", expression, alias_str)


def _case(expression: Any = None, **_kwargs: Any) -> ExpressionProxy:
    if expression is not None:
        return _call_ts_function("case_", expression)
    return _call_ts_function("case_")


def _union(
    *expressions: Any,
    _distinct: bool = True,
    **_kwargs: Any,
) -> ExpressionProxy:
    return _call_ts_function(
        "union",
        *[
            parse_one(str(expression)) if isinstance(expression, str) else expression
            for expression in expressions
        ],
    )


def _intersect(*expressions: Any, **_kwargs: Any) -> ExpressionProxy:
    return _call_ts_function(
        "intersect",
        *[
            parse_one(str(expression)) if isinstance(expression, str) else expression
            for expression in expressions
        ],
    )


def _except(*expressions: Any, **_kwargs: Any) -> ExpressionProxy:
    return _call_ts_function(
        "except_",
        *[
            parse_one(str(expression)) if isinstance(expression, str) else expression
            for expression in expressions
        ],
    )


def _update(
    table: str | Any,
    properties: dict | None = None,
    where: str | Any = None,
    from_: str | Any = None,
    with_: dict | None = None,
    **_kwargs: Any,
) -> ExpressionProxy:
    opts: dict[str, Any] = {}
    if where is not None:
        opts["where"] = where
    if from_ is not None:
        opts["from_"] = from_
    if with_ is not None:
        opts["with_"] = with_
    return _call_ts_function("update", table, properties, **opts)


def _values(
    values_list: list,
    alias: str | None = None,
    columns: list | None = None,
    **_kwargs: Any,
) -> ExpressionProxy:
    return _call_ts_function("values", values_list, alias, columns)


def _delete(
    table: str | Any,
    where: str | Any = None,
    returning: str | None = None,
    dialect: str | None = None,
    **_kwargs: Any,
) -> ExpressionProxy:
    opts: dict[str, Any] = {}
    if where is not None:
        opts["where"] = where
    if returning is not None:
        opts["returning"] = returning
    if dialect is not None:
        opts["dialect"] = dialect
    return _call_ts_function("delete_", table, **opts)


def _insert(
    expression: str | Any,
    into: str | Any,
    columns: list | None = None,
    *,
    overwrite: bool = False,
    returning: str | None = None,
    **_kwargs: Any,
) -> ExpressionProxy:
    opts: dict[str, Any] = {}
    if columns is not None:
        opts["columns"] = columns
    if overwrite:
        opts["overwrite"] = overwrite
    if returning is not None:
        opts["returning"] = returning
    return _call_ts_function("insert", expression, into, **opts)


def _rename_column(
    table: str,
    old_name: str,
    new_name: str,
    *,
    if_exists: bool = False,
    **_kwargs: Any,
) -> ExpressionProxy:
    return _call_ts_function(
        "renameColumn", table, old_name, new_name, if_exists or None
    )


def _subquery(
    expression: str | Any, alias: str | None = None, **_kwargs: Any
) -> ExpressionProxy:
    return _call_ts_function("subquery", expression, alias)


def _not_implemented(*_args: Any, **_kwargs: Any) -> Any:
    msg = "Optimizer functions are not implemented in sqlglot-ts"
    raise NotImplementedError(msg)


def _annotate_types(expression: ExpressionProxy, **_kwargs: Any) -> ExpressionProxy:
    bridge = TSBridge.get()
    result = bridge.call("annotateTypes", id=expression.expr_id)
    if not result["ok"]:
        raise ValueError(result["error"])
    return expression


class ParserProxy:
    FUNCTIONS: ClassVar[dict[str, Any]] = {}

    def parse(self, tokens: list, **_kwargs: Any) -> list[ExpressionProxy]:
        sql = " ".join(t.text for t in tokens)
        return [parse_one(sql)]


def _register_core_module(sqlglot_mod: types.ModuleType) -> None:
    sqlglot_mod.parse = parse
    sqlglot_mod.parse_one = parse_one
    sqlglot_mod.transpile = transpile
    sqlglot_mod.Expression = Expression
    sqlglot_mod.Parser = ParserProxy
    sqlglot_mod.ParseError = ParseError
    sqlglot_mod.TokenError = TokenError
    sqlglot_mod.UnsupportedError = UnsupportedError
    sqlglot_mod.ErrorLevel = ErrorLevel
    sqlglot_mod.Dialects = Dialects
    sys.modules["sqlglot"] = sqlglot_mod


def _register_expressions_module(
    sqlglot_mod: types.ModuleType,
) -> ExpModule:
    sqlglot_exp = ExpModule("sqlglot.expressions")
    sqlglot_exp.Expression = Expression
    sqlglot_exp.convert = _convert
    sqlglot_exp.to_identifier = _to_identifier
    sqlglot_exp.maybe_parse = _maybe_parse
    sqlglot_exp.select = _select
    sqlglot_exp.from_ = _from_
    sqlglot_exp.to_table = _to_table
    sqlglot_exp.to_column = _to_column
    sqlglot_exp.column = _column
    sqlglot_exp.table_ = _table
    sqlglot_exp.cast = _cast
    sqlglot_exp.merge = _merge
    sqlglot_exp.func = _func
    sqlglot_exp.update = _update
    sqlglot_exp.values = _values
    sqlglot_exp.delete = _delete
    sqlglot_exp.insert = _insert
    sqlglot_exp.rename_column = _rename_column
    sqlglot_exp.subquery = _subquery
    sqlglot_exp.and_ = _and
    sqlglot_exp.or_ = _or
    sqlglot_exp.not_ = _not
    sqlglot_exp.condition = _condition
    sqlglot_exp.union = _union
    sqlglot_exp.intersect = _intersect
    sqlglot_exp.except_ = _except
    sys.modules["sqlglot.expressions"] = sqlglot_exp
    sqlglot_mod.exp = sqlglot_exp

    sqlglot_mod.select = _select
    sqlglot_mod.from_ = _from_
    sqlglot_mod.condition = _condition
    sqlglot_mod.and_ = _and
    sqlglot_mod.or_ = _or
    sqlglot_mod.not_ = _not
    sqlglot_mod.alias = _alias
    sqlglot_mod.case = _case
    sqlglot_mod.union = _union
    sqlglot_mod.intersect = _intersect
    sqlglot_mod.except_ = _except

    from compat.time_shim import merge_ranges  # noqa: PLC0415
    from compat.time_shim import name_sequence  # noqa: PLC0415
    from compat.time_shim import tsort  # noqa: PLC0415

    helper_mod = types.ModuleType("sqlglot.helper")
    helper_mod.logger = logging.getLogger("sqlglot")
    helper_mod.merge_ranges = merge_ranges
    helper_mod.name_sequence = name_sequence
    helper_mod.tsort = tsort
    sys.modules["sqlglot.helper"] = helper_mod

    generator_mod = types.ModuleType("sqlglot.generator")
    generator_mod.logger = logging.getLogger("sqlglot")
    sys.modules["sqlglot.generator"] = generator_mod

    parser_mod = types.ModuleType("sqlglot.parser")
    parser_mod.logger = logging.getLogger("sqlglot.parser")
    parser_mod.Parser = ParserProxy
    sys.modules["sqlglot.parser"] = parser_mod

    return sqlglot_exp


def _register_optimizer_modules() -> None:
    sqlglot_optimizer = types.ModuleType("sqlglot.optimizer")
    sqlglot_optimizer.__path__ = []
    sys.modules["sqlglot.optimizer"] = sqlglot_optimizer

    optimizer_annotate = types.ModuleType("sqlglot.optimizer.annotate_types")
    optimizer_annotate.annotate_types = _annotate_types
    sys.modules["sqlglot.optimizer.annotate_types"] = optimizer_annotate

    optimizer_qualify = types.ModuleType("sqlglot.optimizer.qualify")
    optimizer_qualify.qualify = _not_implemented
    sys.modules["sqlglot.optimizer.qualify"] = optimizer_qualify

    optimizer_normalize = types.ModuleType("sqlglot.optimizer.normalize_identifiers")
    optimizer_normalize.normalize_identifiers = _not_implemented
    sys.modules["sqlglot.optimizer.normalize_identifiers"] = optimizer_normalize

    optimizer_qualify_columns = types.ModuleType("sqlglot.optimizer.qualify_columns")
    optimizer_qualify_columns.quote_identifiers = _not_implemented
    sys.modules["sqlglot.optimizer.qualify_columns"] = optimizer_qualify_columns

    sqlglot_optimizer.traverse_scope = _not_implemented


def _register_dialect_modules(
    sqlglot_mod: types.ModuleType, sqlglot_exp: ExpModule
) -> None:
    sqlglot_dialects = types.ModuleType("sqlglot.dialects")
    sqlglot_dialects.__path__ = []
    sys.modules["sqlglot.dialects"] = sqlglot_dialects
    sqlglot_mod.dialects = sqlglot_dialects

    for name in _DIALECT_NAMES:
        generator_attrs: dict[str, Any] = {}
        if name == "DuckDB":
            generator_attrs["IGNORE_RESPECT_NULLS_WINDOW_FUNCTIONS"] = tuple(
                getattr(sqlglot_exp, expr_name)
                for expr_name in ("FirstValue", "LastValue", "NthValue", "Lead", "Lag")
            )
        if name == "MySQL":
            data_type = sqlglot_exp.DataType
            generator_attrs["CHAR_CAST_MAPPING"] = {
                data_type.Type.LONGTEXT: "CHAR",
                data_type.Type.LONGBLOB: "CHAR",
                data_type.Type.MEDIUMBLOB: "CHAR",
                data_type.Type.MEDIUMTEXT: "CHAR",
                data_type.Type.TEXT: "CHAR",
                data_type.Type.TINYBLOB: "CHAR",
                data_type.Type.TINYTEXT: "CHAR",
                data_type.Type.VARCHAR: "CHAR",
            }
            generator_attrs["SIGNED_CAST_MAPPING"] = {
                data_type.Type.BIGINT: "SIGNED",
                data_type.Type.BOOLEAN: "SIGNED",
                data_type.Type.INT: "SIGNED",
                data_type.Type.SMALLINT: "SIGNED",
                data_type.Type.TINYINT: "SIGNED",
                data_type.Type.MEDIUMINT: "SIGNED",
            }
        generator_class = type(f"{name}Generator", (), generator_attrs)
        dialect_name_lower = name.lower()
        tokenizer_class = type(
            f"{name}Tokenizer",
            (Tokenizer,),
            {
                "__init__": lambda self, _d=dialect_name_lower: Tokenizer.__init__(
                    self, _d
                )
            },
        )
        dialect_class = type(
            name,
            (),
            {
                "Tokenizer": tokenizer_class,
                "Parser": type(f"{name}Parser", (), {}),
                "Generator": generator_class,
            },
        )
        setattr(sqlglot_dialects, name, dialect_class)

        dialect_module_name = name.lower()
        dialect_mod = types.ModuleType(f"sqlglot.dialects.{dialect_module_name}")
        setattr(dialect_mod, name, dialect_class)
        dialect_mod.logger = logging.getLogger("sqlglot")
        sys.modules[f"sqlglot.dialects.{dialect_module_name}"] = dialect_mod

    dialect_dialect = types.ModuleType("sqlglot.dialects.dialect")
    dialect_dialect.Dialects = Dialects
    sys.modules["sqlglot.dialects.dialect"] = dialect_dialect


def _register_tokens_module() -> None:
    sqlglot_tokens = types.ModuleType("sqlglot.tokens")
    sqlglot_tokens.Tokenizer = Tokenizer
    sqlglot_tokens.TokenType = TokenType
    sqlglot_tokens.Token = Token
    sys.modules["sqlglot.tokens"] = sqlglot_tokens


def _register_error_module() -> None:
    from compat.time_shim import ANSI_RESET  # noqa: PLC0415
    from compat.time_shim import ANSI_UNDERLINE  # noqa: PLC0415
    from compat.time_shim import highlight_sql  # noqa: PLC0415

    sqlglot_errors = types.ModuleType("sqlglot.errors")
    sqlglot_errors.ParseError = ParseError
    sqlglot_errors.UnsupportedError = UnsupportedError
    sqlglot_errors.TokenError = TokenError
    sqlglot_errors.ErrorLevel = ErrorLevel
    sqlglot_errors.highlight_sql = highlight_sql
    sqlglot_errors.ANSI_UNDERLINE = ANSI_UNDERLINE
    sqlglot_errors.ANSI_RESET = ANSI_RESET
    sys.modules["sqlglot.errors"] = sqlglot_errors


def _register_time_module() -> None:
    from compat.time_shim import format_time  # noqa: PLC0415
    from compat.time_shim import subsecond_precision  # noqa: PLC0415

    sqlglot_time = types.ModuleType("sqlglot.time")
    sqlglot_time.format_time = format_time
    sqlglot_time.subsecond_precision = subsecond_precision
    sys.modules["sqlglot.time"] = sqlglot_time


def _register_test_modules() -> None:
    from compat.validator import Validator  # noqa: PLC0415

    tests_mod = types.ModuleType("tests")
    tests_mod.__path__ = []
    sys.modules["tests"] = tests_mod

    tests_dialects = types.ModuleType("tests.dialects")
    tests_dialects.__path__ = []
    sys.modules["tests.dialects"] = tests_dialects

    tests_test_dialect = types.ModuleType("tests.dialects.test_dialect")
    tests_test_dialect.Validator = Validator
    sys.modules["tests.dialects.test_dialect"] = tests_test_dialect

    tests_helpers = types.ModuleType("tests.helpers")
    tests_helpers.assert_logger_contains = _assert_logger_contains
    tests_helpers.load_sql_fixtures = _load_sql_fixtures
    tests_helpers.load_sql_fixture_pairs = _load_sql_fixture_pairs
    tests_helpers.FIXTURES_DIR = _FIXTURES_DIR
    sys.modules["tests.helpers"] = tests_helpers


def _assert_logger_contains(message: str, logger: Any, level: str = "error") -> None:
    output = "\n".join(
        str(args[0][0]) for args in getattr(logger, level).call_args_list
    )
    if message not in output:
        msg = f"Expected '{message}' not in {output}"
        raise AssertionError(msg)


def _filter_comments(s: str) -> str:
    return "\n".join(
        line for line in s.splitlines() if line and not line.startswith("--")
    )


def _extract_meta(sql: str) -> tuple[str, dict[str, str]]:
    meta: dict[str, str] = {}
    sql_lines = sql.split("\n")
    i = 0
    while i < len(sql_lines) and sql_lines[i].startswith("#"):
        key, val = sql_lines[i].split(":", maxsplit=1)
        meta[key.lstrip("#").strip()] = val.strip()
        i += 1
    sql = "\n".join(sql_lines[i:])
    return sql, meta


_FIXTURES_DIR = (
    Path(__file__).resolve().parent.parent.parent / "sqlglot" / "tests" / "fixtures"
)


def _load_sql_fixtures(filename: str):
    text = (_FIXTURES_DIR / filename).read_text(encoding="utf-8")
    yield from _filter_comments(text).splitlines()


def _load_sql_fixture_pairs(filename: str):
    text = (_FIXTURES_DIR / filename).read_text(encoding="utf-8")
    statements = _filter_comments(text).split(";")
    size = len(statements)
    for i in range(0, size, 2):
        if i + 1 < size:
            sql = statements[i].strip()
            sql, meta = _extract_meta(sql)
            expected = statements[i + 1].strip()
            yield meta, sql, expected


def register_fake_sqlglot() -> None:
    sqlglot_mod = types.ModuleType("sqlglot")
    _register_core_module(sqlglot_mod)
    set_convert_handler(_convert)
    sqlglot_exp = _register_expressions_module(sqlglot_mod)
    _register_optimizer_modules()
    _register_dialect_modules(sqlglot_mod, sqlglot_exp)
    _register_tokens_module()
    _register_error_module()
    _register_time_module()
    _register_test_modules()

    from compat.api import create_datatype  # noqa: PLC0415
    from compat.api import parse_one  # noqa: PLC0415

    set_parse_one_handler(parse_one)
    set_create_datatype_handler(create_datatype)
