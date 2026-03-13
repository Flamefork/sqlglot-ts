import re
from collections.abc import Callable
from typing import Any
from typing import ClassVar

from compat.bridge import TSBridge
from compat.bridge import emit_bridge_logs
from compat.errors import Expression
from compat.errors import UnsupportedError

_convert_handler: Callable[[Any], Any] | None = None
_parse_one_handler: Callable[..., Any] | None = None
_create_datatype_handler: Callable[[str], Any] | None = None


def set_convert_handler(handler: Callable[[Any], Any]) -> None:
    global _convert_handler  # noqa: PLW0603
    _convert_handler = handler


def set_parse_one_handler(handler: Callable[..., Any]) -> None:
    global _parse_one_handler  # noqa: PLW0603
    _parse_one_handler = handler


def set_create_datatype_handler(handler: Callable[[str], Any]) -> None:
    global _create_datatype_handler  # noqa: PLW0603
    _create_datatype_handler = handler


def deserialize(data: dict) -> Any:
    data_type = data.get("type")
    if data_type == "null":
        return None
    if data_type == "string":
        return data["value"]
    if data_type == "number":
        return data["value"]
    if data_type == "boolean":
        return data["value"]
    if data_type == "array":
        return [deserialize(value) for value in data["value"]]
    if data_type == "expr":
        return ExpressionProxy(data["id"], data["key"])
    if data_type == "object":
        return {key: deserialize(value) for key, value in data["value"].items()}
    if data_type == "method":
        return None
    if data_type == "unknown":
        return data.get("value")
    msg = f"Unknown deserialize type: {data_type}"
    raise ValueError(msg)


def serialize_arg(arg: Any) -> Any:
    if isinstance(arg, ExpressionProxy):
        return {"__expr_id__": arg.expr_id}
    if isinstance(arg, list):
        return [serialize_arg(value) for value in arg]
    if isinstance(arg, dict):
        return {key: serialize_arg(value) for key, value in arg.items()}
    return arg


def _parse_one(sql: str, read: str | None = None) -> "ExpressionProxy":
    if _parse_one_handler is None:
        msg = "_parse_one handler not initialized"
        raise RuntimeError(msg)
    return _parse_one_handler(sql, read=read)


def _create_datatype(type_name: str) -> "ExpressionProxy":
    if _create_datatype_handler is None:
        msg = "_create_datatype handler not initialized"
        raise RuntimeError(msg)
    return _create_datatype_handler(type_name)


def _convert_value(value: Any) -> "ExpressionProxy":
    if isinstance(value, ExpressionProxy):
        return value
    if _convert_handler is None:
        msg = "_convert handler not initialized"
        raise RuntimeError(msg)
    return _convert_handler(value)


def _handle_lock(
    bridge: TSBridge, self_id: int, name: str, args: tuple, kwargs: dict
) -> Any:
    update = kwargs.get("update", args[0] if args else True)
    copy = kwargs.get("copy", True)
    call_args = (update, copy)
    serialized_args = [serialize_arg(arg) for arg in call_args]
    call_result = bridge.call(
        "call",
        id=self_id,
        name=name,
        args=serialized_args,
    )
    if not call_result["ok"]:
        raise ValueError(call_result["error"])
    return deserialize(call_result["value"])


def _handle_distinct(
    bridge: TSBridge,
    self_proxy: "ExpressionProxy",
    self_id: int,
    args: tuple,
    kwargs: dict,
) -> Any:
    if "distinct" in kwargs:
        call_args = (kwargs["distinct"],)
        serialized_args = [serialize_arg(arg) for arg in call_args]
        call_result = bridge.call(
            "call",
            id=self_id,
            name="distinct",
            args=serialized_args,
        )
        if not call_result["ok"]:
            raise ValueError(call_result["error"])
        return deserialize(call_result["value"])
    if args and isinstance(args[0], str):
        parsed_on = [_parse_one(str(arg)) for arg in args]
        tuple_result = bridge.call(
            "createExpression",
            className="Tuple",
            args={"expressions": [serialize_arg(parsed) for parsed in parsed_on]},
        )
        if not tuple_result["ok"]:
            raise ValueError(tuple_result["error"])
        tuple_proxy = ExpressionProxy(tuple_result["id"], tuple_result["key"])
        distinct_result = bridge.call(
            "createExpression",
            className="Distinct",
            args={"on": serialize_arg(tuple_proxy)},
        )
        if not distinct_result["ok"]:
            raise ValueError(distinct_result["error"])
        distinct_proxy = ExpressionProxy(distinct_result["id"], distinct_result["key"])
        this_copy = self_proxy.copy()
        set_result = bridge.call(
            "call",
            id=this_copy.expr_id,
            name="set",
            args=["distinct", serialize_arg(distinct_proxy)],
        )
        if not set_result["ok"]:
            raise ValueError(set_result["error"])
        return this_copy
    return None


def _handle_returning(
    bridge: TSBridge,
    self_proxy: "ExpressionProxy",
    self_id: int,
    args: tuple,
    kwargs: dict,
) -> Any:
    dialect = kwargs.get("dialect")
    copy = kwargs.get("copy", True)
    expr_str = args[0] if args else "*"
    if isinstance(expr_str, str):
        parsed_exprs = _parse_one(f"SELECT {expr_str}", read=dialect)
        select_exprs_result = bridge.call(
            "getattr",
            id=parsed_exprs.expr_id,
            name="expressions",
        )
        if select_exprs_result["ok"]:
            exprs = deserialize(select_exprs_result["value"])
            if isinstance(exprs, list) and exprs:
                returning_result = bridge.call(
                    "createExpression",
                    className="Returning",
                    args={"expressions": [serialize_arg(expr) for expr in exprs]},
                )
                if returning_result["ok"]:
                    returning_proxy = ExpressionProxy(
                        returning_result["id"], returning_result["key"]
                    )
                    this_copy = self_proxy.copy() if copy else self_proxy
                    set_result = bridge.call(
                        "call",
                        id=this_copy.expr_id,
                        name="set",
                        args=[
                            "returning",
                            serialize_arg(returning_proxy),
                        ],
                    )
                    if set_result["ok"]:
                        return this_copy
    call_kwargs = {key: value for key, value in kwargs.items() if key == "copy"}
    serialized_args = [serialize_arg(arg) for arg in args]
    serialized_kwargs = {
        key: serialize_arg(value) for key, value in call_kwargs.items()
    }
    call_result = bridge.call(
        "call",
        id=self_id,
        name="returning",
        args=serialized_args,
        kwargs=serialized_kwargs,
    )
    if not call_result["ok"]:
        raise ValueError(call_result["error"])
    return deserialize(call_result["value"])


def _handle_group_by(
    bridge: TSBridge, self_id: int, name: str, args: tuple, kwargs: dict
) -> Any:
    real_args: list[Any] = []
    with_val = None
    for arg in args:
        if isinstance(arg, str):
            match = re.search(r"\s+with\s+(cube|rollup)\s*$", arg, re.IGNORECASE)
            if match:
                with_val = match.group(1).upper()
                real_args.append(arg[: match.start()])
            else:
                real_args.append(arg)
        elif isinstance(arg, dict) and "with_" in arg:
            with_val = arg["with_"].upper()
        else:
            real_args.append(arg)
    serialized_args = [serialize_arg(arg) for arg in real_args]
    serialized_kwargs = {key: serialize_arg(value) for key, value in kwargs.items()}
    group_result = bridge.call(
        "call",
        id=self_id,
        name=name,
        args=serialized_args,
        kwargs=serialized_kwargs,
    )
    if not group_result["ok"]:
        raise ValueError(group_result["error"])
    result_proxy = deserialize(group_result["value"])
    if with_val and isinstance(result_proxy, ExpressionProxy):
        group_type = type("Group", (Expression,), {"__name__": "Group"})
        group_node = result_proxy.find(group_type)
        if group_node is not None:
            class_name = "Cube" if with_val == "CUBE" else "Rollup"
            node_result = bridge.call(
                "createExpression",
                className=class_name,
                args={"expressions": []},
            )
            if node_result["ok"]:
                node_proxy = ExpressionProxy(node_result["id"], node_result["key"])
                key = "cube" if with_val == "CUBE" else "rollup"
                bridge.call(
                    "call",
                    id=group_node.expr_id,
                    name="set",
                    args=[key, [serialize_arg(node_proxy)]],
                )
    return result_proxy


def _handle_ctas(bridge: TSBridge, self_id: int, args: tuple, kwargs: dict) -> Any:
    props_dict = kwargs.pop("properties")
    table_arg = args[0] if args else ""
    ctas_kwargs = {key: value for key, value in kwargs.items() if key != "properties"}
    ctas_result = bridge.call(
        "call",
        id=self_id,
        name="ctas",
        args=[serialize_arg(table_arg)],
        kwargs={key: serialize_arg(value) for key, value in ctas_kwargs.items()},
    )
    if not ctas_result["ok"]:
        raise ValueError(ctas_result["error"])
    create_proxy = deserialize(ctas_result["value"])
    prop_exprs: list[Any] = []
    name_to_property = {"FORMAT": "FileFormatProperty"}
    for key, value in props_dict.items():
        prop_class = name_to_property.get(key.upper())
        if prop_class:
            prop_result = bridge.call(
                "createExpression",
                className=prop_class,
                args={"this": value},
            )
            if prop_result["ok"]:
                prop_exprs.append(
                    serialize_arg(
                        ExpressionProxy(prop_result["id"], prop_result["key"])
                    )
                )
        else:
            key_lit = bridge.call(
                "createExpression",
                className="Literal",
                args={"this": key, "is_string": True},
            )
            val_lit = bridge.call(
                "createExpression",
                className="Literal",
                args={"this": str(value), "is_string": True},
            )
            if key_lit["ok"] and val_lit["ok"]:
                prop_result = bridge.call(
                    "createExpression",
                    className="Property",
                    args={
                        "this": serialize_arg(
                            ExpressionProxy(key_lit["id"], key_lit["key"])
                        ),
                        "value": serialize_arg(
                            ExpressionProxy(val_lit["id"], val_lit["key"])
                        ),
                    },
                )
                if prop_result["ok"]:
                    prop_exprs.append(
                        serialize_arg(
                            ExpressionProxy(prop_result["id"], prop_result["key"])
                        )
                    )
    if prop_exprs:
        props_result = bridge.call(
            "createExpression",
            className="Properties",
            args={"expressions": prop_exprs},
        )
        if props_result["ok"]:
            props_proxy = ExpressionProxy(props_result["id"], props_result["key"])
            bridge.call(
                "call",
                id=create_proxy.expr_id,
                name="set",
                args=["properties", serialize_arg(props_proxy)],
            )
    return create_proxy


def _handle_generic_method_call(
    bridge: TSBridge, self_id: int, name: str, args: tuple, kwargs: dict
) -> Any:
    serialized_args = [serialize_arg(arg) for arg in args]
    serialized_kwargs = {key: serialize_arg(value) for key, value in kwargs.items()}
    call_result = bridge.call(
        "call",
        id=self_id,
        name=name,
        args=serialized_args,
        kwargs=serialized_kwargs,
    )
    if not call_result["ok"]:
        raise ValueError(call_result["error"])
    return deserialize(call_result["value"])


def _dispatch_method_call(
    bridge: TSBridge,
    self_proxy: "ExpressionProxy",
    self_id: int,
    name: str,
    args: tuple,
    kwargs: dict,
) -> Any:
    if name == "lock":
        return _handle_lock(bridge, self_id, name, args, kwargs)
    if name == "distinct":
        result = _handle_distinct(bridge, self_proxy, self_id, args, kwargs)
        if result is not None:
            return result
        return _handle_generic_method_call(bridge, self_id, name, args, kwargs)
    if name == "returning":
        return _handle_returning(bridge, self_proxy, self_id, args, kwargs)
    if name == "group_by":
        return _handle_group_by(bridge, self_id, name, args, kwargs)
    if name == "ctas" and "properties" in kwargs:
        return _handle_ctas(bridge, self_id, args, kwargs)
    return _handle_generic_method_call(bridge, self_id, name, args, kwargs)


class ExpressionProxyMeta(type):
    def __instancecheck__(cls, instance: Any) -> bool:
        if isinstance(instance, ExpressionProxy):
            return instance.key.lower() == cls.__name__.lower()
        return super().__instancecheck__(instance)

    def __call__(cls, **kwargs: Any) -> "ExpressionProxy":
        bridge = TSBridge.get()
        serialized = {key: serialize_arg(value) for key, value in kwargs.items()}
        result = bridge.call(
            "createExpression", className=cls.__name__, args=serialized
        )
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])


class ExpressionProxy:  # noqa: PLR0904
    _all_ids: ClassVar[list[int]] = []
    _BINARY_KEYS = frozenset({
        "add",
        "sub",
        "mul",
        "div",
        "mod",
        "dpipe",
        "and",
        "or",
        "bitwiseand",
        "bitwiseor",
        "bitwisexor",
        "eq",
        "neq",
        "gt",
        "gte",
        "lt",
        "lte",
        "like",
        "ilike",
        "is",
        "regexplike",
        "similarto",
        "nullsafeeq",
        "nullsafeneq",
    })

    def __init__(self, expr_id: int, key: str):
        object.__setattr__(self, "expr_id", expr_id)
        object.__setattr__(self, "expr_key", key)
        ExpressionProxy._all_ids.append(expr_id)

    @classmethod
    def retained_count(cls) -> int:
        return len(cls._all_ids)

    @classmethod
    def release_all(cls) -> None:
        if cls._all_ids:
            bridge = TSBridge.get()
            bridge.call("release", ids=cls._all_ids)
            cls._all_ids = []

    @property
    def key(self) -> str:
        return self.expr_key

    def sql(
        self,
        dialect: str | None = None,
        *,
        pretty: bool = False,
        identify: bool | str = False,
        unsupported_level: str | None = None,
        **_kwargs: Any,
    ) -> str:
        bridge = TSBridge.get()
        result = bridge.call(
            "sql",
            id=self.expr_id,
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
        emit_bridge_logs(result)
        return result["sql"]

    def text(self, name: str) -> str:
        bridge = TSBridge.get()
        result = bridge.call("text", id=self.expr_id, name=name)
        if not result["ok"]:
            raise AttributeError(result["error"])
        return result["value"]

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)

        bridge = TSBridge.get()
        result = bridge.call("getattr", id=self.expr_id, name=name)
        if not result["ok"]:
            raise AttributeError(result["error"])

        value = result["value"]
        if value.get("type") == "method":
            self_proxy = self
            self_id = self.expr_id

            def method_proxy(*args: Any, **kwargs: Any) -> Any:
                return _dispatch_method_call(
                    bridge, self_proxy, self_id, name, args, kwargs
                )

            return method_proxy

        return deserialize(value)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ExpressionProxy):
            if self.expr_id == other.expr_id:
                return True
            bridge = TSBridge.get()
            result = bridge.call("equals", id=self.expr_id, otherId=other.expr_id)
            return result.get("ok", False) and result.get("value", False)
        return NotImplemented

    def __hash__(self) -> int:
        bridge = TSBridge.get()
        result = bridge.call("hashCode", id=self.expr_id)
        if result.get("ok"):
            return result["value"]
        return hash(self.expr_id)

    def __repr__(self) -> str:
        return f"<Expr:{self.key}#{self.expr_id}>"

    def __str__(self) -> str:
        return self.sql()

    def assert_is(self, expr_type: type) -> "ExpressionProxy":
        bridge = TSBridge.get()
        result = bridge.call(
            "assertIs", id=self.expr_id, expectedKey=expr_type.__name__
        )
        if not result["ok"]:
            raise AssertionError(result["error"])
        return self

    def find(self, expr_type: type) -> "ExpressionProxy | None":
        bridge = TSBridge.get()
        result = bridge.call("find", id=self.expr_id, exprType=expr_type.__name__)
        if not result["ok"]:
            raise ValueError(result["error"])
        return deserialize(result["value"])

    def find_all(self, expr_type: type):
        bridge = TSBridge.get()
        result = bridge.call("findAll", id=self.expr_id, exprType=expr_type.__name__)
        if not result["ok"]:
            raise ValueError(result["error"])
        return [
            ExpressionProxy(value["id"], value["key"]) for value in result["values"]
        ]

    def copy(self) -> "ExpressionProxy":
        bridge = TSBridge.get()
        result = bridge.call("copy", id=self.expr_id)
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    def _create_binop(
        self, class_name: str, other: Any, *, reverse: bool = False
    ) -> "ExpressionProxy":
        bridge = TSBridge.get()
        other_proxy = _convert_value(other)
        this_copy = self.copy()
        target_key = class_name.lower()
        if this_copy.key != target_key and (
            not hasattr(other_proxy, "key") or other_proxy.key != target_key
        ):
            if this_copy.key in self._BINARY_KEYS:
                this_copy = this_copy.wrap_paren()
            if hasattr(other_proxy, "key") and other_proxy.key in self._BINARY_KEYS:
                other_proxy = other_proxy.wrap_paren()
        this_arg = serialize_arg(other_proxy if reverse else this_copy)
        expr_arg = serialize_arg(this_copy if reverse else other_proxy)
        result = bridge.call(
            "createExpression",
            className=class_name,
            args={"this": this_arg, "expression": expr_arg},
        )
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    def wrap_paren(self) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy()
        result = bridge.call(
            "createExpression",
            className="Paren",
            args={"this": serialize_arg(this_copy)},
        )
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    def __add__(self, other: Any) -> "ExpressionProxy":
        return self._create_binop("Add", other)

    def __radd__(self, other: Any) -> "ExpressionProxy":
        return self._create_binop("Add", other, reverse=True)

    def __sub__(self, other: Any) -> "ExpressionProxy":
        return self._create_binop("Sub", other)

    def __rsub__(self, other: Any) -> "ExpressionProxy":
        return self._create_binop("Sub", other, reverse=True)

    def __mul__(self, other: Any) -> "ExpressionProxy":
        return self._create_binop("Mul", other)

    def __rmul__(self, other: Any) -> "ExpressionProxy":
        return self._create_binop("Mul", other, reverse=True)

    def __truediv__(self, other: Any) -> "ExpressionProxy":
        return self._create_binop("Div", other)

    def __rtruediv__(self, other: Any) -> "ExpressionProxy":
        return self._create_binop("Div", other, reverse=True)

    def __floordiv__(self, other: Any) -> "ExpressionProxy":
        bridge = TSBridge.get()
        div = self._create_binop("Div", other)
        cast_result = bridge.call(
            "createExpression",
            className="Cast",
            args={
                "this": serialize_arg(div),
                "to": serialize_arg(_create_datatype("INT")),
            },
        )
        if not cast_result["ok"]:
            raise ValueError(cast_result["error"])
        return ExpressionProxy(cast_result["id"], cast_result["key"])

    def __rfloordiv__(self, other: Any) -> "ExpressionProxy":
        bridge = TSBridge.get()
        div = self._create_binop("Div", other, reverse=True)
        cast_result = bridge.call(
            "createExpression",
            className="Cast",
            args={
                "this": serialize_arg(div),
                "to": serialize_arg(_create_datatype("INT")),
            },
        )
        if not cast_result["ok"]:
            raise ValueError(cast_result["error"])
        return ExpressionProxy(cast_result["id"], cast_result["key"])

    def __mod__(self, other: Any) -> "ExpressionProxy":
        return self._create_binop("Mod", other)

    def __rmod__(self, other: Any) -> "ExpressionProxy":
        return self._create_binop("Mod", other, reverse=True)

    def __pow__(self, other: Any) -> "ExpressionProxy":
        bridge = TSBridge.get()
        other_proxy = _convert_value(other)
        this_copy = self.copy()
        result = bridge.call(
            "callFunction",
            name="func",
            args=["POWER", serialize_arg(this_copy), serialize_arg(other_proxy)],
        )
        if not result["ok"]:
            raise ValueError(result["error"])
        return deserialize(result["value"])

    def __rpow__(self, other: Any) -> "ExpressionProxy":
        bridge = TSBridge.get()
        other_proxy = _convert_value(other)
        this_copy = self.copy()
        result = bridge.call(
            "callFunction",
            name="func",
            args=["POWER", serialize_arg(other_proxy), serialize_arg(this_copy)],
        )
        if not result["ok"]:
            raise ValueError(result["error"])
        return deserialize(result["value"])

    def __and__(self, other: Any) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy()
        other_val = _convert_value(other)
        result = bridge.call(
            "createExpression",
            className="And",
            args={
                "this": serialize_arg(this_copy),
                "expression": serialize_arg(other_val),
            },
        )
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    def __rand__(self, other: Any) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy()
        other_val = _convert_value(other)
        result = bridge.call(
            "createExpression",
            className="And",
            args={
                "this": serialize_arg(other_val),
                "expression": serialize_arg(this_copy),
            },
        )
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    def __or__(self, other: Any) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy()
        other_val = _convert_value(other)
        result = bridge.call(
            "createExpression",
            className="Or",
            args={
                "this": serialize_arg(this_copy),
                "expression": serialize_arg(other_val),
            },
        )
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    def __ror__(self, other: Any) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy()
        other_val = _convert_value(other)
        result = bridge.call(
            "createExpression",
            className="Or",
            args={
                "this": serialize_arg(other_val),
                "expression": serialize_arg(this_copy),
            },
        )
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    def __lt__(self, other: Any) -> "ExpressionProxy":
        return self._create_binop("LT", other)

    def __le__(self, other: Any) -> "ExpressionProxy":
        return self._create_binop("LTE", other)

    def __gt__(self, other: Any) -> "ExpressionProxy":
        return self._create_binop("GT", other)

    def __ge__(self, other: Any) -> "ExpressionProxy":
        return self._create_binop("GTE", other)

    def __neg__(self) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy()
        result = bridge.call(
            "createExpression", className="Neg", args={"this": serialize_arg(this_copy)}
        )
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    def __invert__(self) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy()
        result = bridge.call(
            "createExpression", className="Not", args={"this": serialize_arg(this_copy)}
        )
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    def __iter__(self):
        bridge = TSBridge.get()
        result = bridge.call("hasArgType", id=self.expr_id, name="expressions")
        if result.get("ok") and result.get("value"):
            attr_result = bridge.call("getattr", id=self.expr_id, name="expressions")
            if attr_result["ok"]:
                items = deserialize(attr_result["value"])
                if isinstance(items, list):
                    return iter(items)
        msg = f"'{self.key}' object is not iterable"
        raise TypeError(msg)

    def __getitem__(self, other: Any) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy()
        if isinstance(other, tuple):
            items = [serialize_arg(_convert_value(value)) for value in other]
        else:
            items = [serialize_arg(_convert_value(other))]
        result = bridge.call(
            "createExpression",
            className="Bracket",
            args={"this": serialize_arg(this_copy), "expressions": items},
        )
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    def isin(
        self,
        *expressions: Any,
        query: Any = None,
        unnest: Any = None,
        copy: bool = True,
        **_opts: Any,
    ) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy() if copy else self
        args: dict[str, Any] = {"this": serialize_arg(this_copy)}
        if expressions:
            args["expressions"] = [
                serialize_arg(_convert_value(expression)) for expression in expressions
            ]
        if query is not None:
            subquery = _parse_one(str(query)) if isinstance(query, str) else query
            sub_result = bridge.call("call", id=subquery.expr_id, name="subquery")
            if sub_result["ok"]:
                subquery = deserialize(sub_result["value"])
            args["query"] = serialize_arg(subquery)
        if unnest is not None:
            unnest_list = unnest if isinstance(unnest, list) else [unnest]
            unnest_exprs = [
                serialize_arg(
                    _parse_one(str(value)) if isinstance(value, str) else value
                )
                for value in unnest_list
            ]
            unnest_result = bridge.call(
                "createExpression",
                className="Unnest",
                args={"expressions": unnest_exprs},
            )
            if unnest_result["ok"]:
                args["unnest"] = serialize_arg(
                    ExpressionProxy(unnest_result["id"], unnest_result["key"])
                )
        result = bridge.call("createExpression", className="In", args=args)
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    def between(
        self, low: Any, high: Any, *, copy: bool = True, **_opts: Any
    ) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy() if copy else self
        result = bridge.call(
            "createExpression",
            className="Between",
            args={
                "this": serialize_arg(this_copy),
                "low": serialize_arg(_convert_value(low)),
                "high": serialize_arg(_convert_value(high)),
            },
        )
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    def is_(self, other: Any) -> "ExpressionProxy":
        return self._create_binop("Is", other)

    def like(self, other: Any) -> "ExpressionProxy":
        return self._create_binop("Like", other)

    def ilike(self, other: Any) -> "ExpressionProxy":
        return self._create_binop("ILike", other)

    def eq(self, other: Any) -> "ExpressionProxy":
        return self._create_binop("EQ", other)

    def neq(self, other: Any) -> "ExpressionProxy":
        return self._create_binop("NEQ", other)

    def rlike(self, other: Any) -> "ExpressionProxy":
        return self._create_binop("RegexpLike", other)

    def as_(self, alias: str, **_kwargs: Any) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy()
        alias_result = bridge.call(
            "createExpression", className="Identifier", args={"this": alias}
        )
        if not alias_result["ok"]:
            raise ValueError(alias_result["error"])
        alias_id = ExpressionProxy(alias_result["id"], alias_result["key"])
        result = bridge.call(
            "createExpression",
            className="Alias",
            args={"this": serialize_arg(this_copy), "alias": serialize_arg(alias_id)},
        )
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    def desc(self, *, nulls_first: bool = False) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy()
        args: dict[str, Any] = {"this": serialize_arg(this_copy), "desc": True}
        if nulls_first:
            args["nulls_first"] = True
        result = bridge.call("createExpression", className="Ordered", args=args)
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    def asc(self, *, nulls_first: bool = True) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy()
        args: dict[str, Any] = {"this": serialize_arg(this_copy)}
        if nulls_first:
            args["nulls_first"] = True
        result = bridge.call("createExpression", className="Ordered", args=args)
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    def on(
        self,
        *expressions: Any,
        append: bool = True,  # noqa: ARG002
        copy: bool = True,
        **_kwargs: Any,
    ) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy() if copy else self
        parsed = [
            _parse_one(str(expression)) if isinstance(expression, str) else expression
            for expression in expressions
            if expression is not None
        ]
        if not parsed:
            return this_copy
        node = parsed[0]
        for expression in parsed[1:]:
            and_result = bridge.call(
                "createExpression",
                className="And",
                args={
                    "this": serialize_arg(node),
                    "expression": serialize_arg(expression),
                },
            )
            if not and_result["ok"]:
                raise ValueError(and_result["error"])
            node = ExpressionProxy(and_result["id"], and_result["key"])
        set_result = bridge.call(
            "call",
            id=this_copy.expr_id,
            name="set",
            args=["on", serialize_arg(node)],
        )
        if not set_result["ok"]:
            raise ValueError(set_result["error"])
        return this_copy

    def using(
        self,
        *expressions: Any,
        append: bool = True,  # noqa: ARG002
        copy: bool = True,
        **_kwargs: Any,
    ) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy() if copy else self
        parsed = [
            _parse_one(str(expression)) if isinstance(expression, str) else expression
            for expression in expressions
            if expression is not None
        ]
        set_result = bridge.call(
            "call",
            id=this_copy.expr_id,
            name="set",
            args=["using", [serialize_arg(parsed_expr) for parsed_expr in parsed]],
        )
        if not set_result["ok"]:
            raise ValueError(set_result["error"])
        return this_copy
