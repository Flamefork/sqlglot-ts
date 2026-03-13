from typing import Any

from compat.bridge import TSBridge
from compat.bridge import emit_bridge_logs
from compat.errors import ParseError
from compat.proxy import ExpressionProxy


def create_datatype(type_name: str) -> ExpressionProxy:
    bridge = TSBridge.get()
    result = bridge.call(
        "createExpression",
        className="DataType",
        args={"this": type_name, "nested": False},
    )
    if not result["ok"]:
        raise ValueError(result["error"])
    return ExpressionProxy(result["id"], result["key"])


def parse_one(
    sql: str,
    read: str | None = None,
    into: type | None = None,
    **_kwargs: Any,
) -> ExpressionProxy:
    bridge = TSBridge.get()
    dialect = str(read) if read else ""
    into_name = getattr(into, "__name__", None) if into else None

    if into_name == "Command":
        result = bridge.call("parseOne", sql=sql, dialect=dialect)
        if result["ok"]:
            emit_bridge_logs(result)
            return ExpressionProxy(result["id"], result["key"])
        cmd_result = bridge.call(
            "createExpression",
            className="Command",
            args={"this": sql},
        )
        if not cmd_result["ok"]:
            msg = cmd_result.get("error", "Failed to create Command")
            raise ParseError(msg)
        return ExpressionProxy(cmd_result["id"], cmd_result["key"])

    call_kwargs: dict[str, Any] = {"sql": sql, "dialect": dialect}
    if into_name:
        call_kwargs["into"] = into_name

    result = bridge.call("parseOne", **call_kwargs)
    if not result["ok"]:
        raise ParseError(result["error"])
    emit_bridge_logs(result)
    proxy = ExpressionProxy(result["id"], result["key"])

    if into is not None and into_name and not isinstance(proxy, into):
        found = proxy.find(into)
        if found is not None:
            return found

    return proxy


def parse(sql: str, read: str | None = None, **_kwargs: Any) -> list[ExpressionProxy]:
    bridge = TSBridge.get()
    dialect = str(read) if read else ""
    result = bridge.call("parse", sql=sql, dialect=dialect)
    if not result["ok"]:
        raise ParseError(result["error"])
    return list(map(ExpressionProxy, result["ids"], result["keys"]))


def transpile(
    sql: str,
    read: str | None = None,
    write: str | None = None,
    **_kwargs: Any,
) -> list[str]:
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
