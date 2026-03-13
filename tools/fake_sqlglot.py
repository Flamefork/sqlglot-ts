#!/usr/bin/env python3
"""
Minimal fake sqlglot module backed by TypeScript via subprocess bridge.
Only provides parse_one, parse, transpile - no expression builders or optimizer.
"""

import datetime
import json
import re
import select
import subprocess
import sys
import types
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]


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
                call_args = args
                call_kwargs = kwargs

                if name == "lock":
                    # lock(update=False) → TS lock(update, copy) positional
                    update = kwargs.get("update", args[0] if args else True)
                    copy = kwargs.get("copy", True)
                    call_args = (update, copy)
                    call_kwargs = {}
                elif name == "distinct":
                    # distinct(distinct=True/False) → pass boolean; distinct("a", "b") → Distinct ON
                    if "distinct" in kwargs:
                        call_args = (kwargs["distinct"],)
                        call_kwargs = {}
                    elif args and isinstance(args[0], str):
                        # distinct("a", "b") → create Distinct(on=Tuple([a, b]))
                        parsed_on = [parse_one(str(a)) for a in args]
                        tuple_result = bridge.call("createExpression", className="Tuple",
                                                   args={"expressions": [serialize_arg(p) for p in parsed_on]})
                        if not tuple_result["ok"]:
                            raise ValueError(tuple_result["error"])
                        tuple_proxy = ExpressionProxy(tuple_result["id"], tuple_result["key"])
                        distinct_result = bridge.call("createExpression", className="Distinct",
                                                      args={"on": serialize_arg(tuple_proxy)})
                        if not distinct_result["ok"]:
                            raise ValueError(distinct_result["error"])
                        distinct_proxy = ExpressionProxy(distinct_result["id"], distinct_result["key"])
                        # Set distinct on a copy of self
                        this_copy = self.copy()
                        res = bridge.call("call", id=this_copy._id, name="set",
                                          args=["distinct", serialize_arg(distinct_proxy)])
                        if not res["ok"]:
                            raise ValueError(res["error"])
                        return this_copy
                elif name == "returning":
                    # returning("*", dialect="postgres") → build Returning node manually
                    # The TS _applyBuilder with into=Returning double-matches the RETURNING token
                    dialect = kwargs.get("dialect")
                    copy = kwargs.get("copy", True)
                    expr_str = args[0] if args else "*"
                    if isinstance(expr_str, str):
                        parsed_exprs = parse_one(f"SELECT {expr_str}", read=dialect)
                        # Extract the select expressions
                        select_exprs_result = bridge.call("getattr", id=parsed_exprs._id, name="expressions")
                        if select_exprs_result["ok"]:
                            exprs = deserialize(select_exprs_result["value"])
                            if isinstance(exprs, list) and exprs:
                                returning_result = bridge.call("createExpression", className="Returning",
                                                               args={"expressions": [serialize_arg(e) for e in exprs]})
                                if returning_result["ok"]:
                                    returning_proxy = ExpressionProxy(returning_result["id"], returning_result["key"])
                                    this_copy = self.copy() if copy else self
                                    res = bridge.call("call", id=this_copy._id, name="set",
                                                      args=["returning", serialize_arg(returning_proxy)])
                                    if res["ok"]:
                                        return this_copy
                    # Fallback: try normal dispatch without dialect kwarg
                    call_kwargs = {k: v for k, v in kwargs.items() if k in ("copy",)}
                elif name == "group_by":
                    # "x with cube" → strip "with cube/rollup" suffix, handle separately
                    real_args = []
                    with_val = None
                    for a in args:
                        if isinstance(a, str):
                            m = re.search(r'\s+with\s+(cube|rollup)\s*$', a, re.IGNORECASE)
                            if m:
                                with_val = m.group(1).upper()
                                real_args.append(a[:m.start()])
                            else:
                                real_args.append(a)
                        elif isinstance(a, dict) and "with_" in a:
                            with_val = a["with_"].upper()
                        else:
                            real_args.append(a)
                    serialized_args = [serialize_arg(a) for a in real_args]
                    serialized_kwargs = {k: serialize_arg(v) for k, v in call_kwargs.items()}
                    res = bridge.call("call", id=self._id, name=name,
                                      args=serialized_args, kwargs=serialized_kwargs)
                    if not res["ok"]:
                        raise ValueError(res["error"])
                    result_proxy = deserialize(res["value"])
                    if with_val and isinstance(result_proxy, ExpressionProxy):
                        _GroupType = type("Group", (Expression,), {"__name__": "Group"})
                        group_node = result_proxy.find(_GroupType)
                        if group_node is not None:
                            class_name = "Cube" if with_val == "CUBE" else "Rollup"
                            node_result = bridge.call("createExpression", className=class_name,
                                                      args={"expressions": []})
                            if node_result["ok"]:
                                node_proxy = ExpressionProxy(node_result["id"], node_result["key"])
                                key = "cube" if with_val == "CUBE" else "rollup"
                                bridge.call("call", id=group_node._id, name="set",
                                            args=[key, [serialize_arg(node_proxy)]])
                    return result_proxy

                if name == "ctas" and "properties" in kwargs:
                    props_dict = kwargs.pop("properties")
                    # Call TS ctas without properties
                    table_arg = args[0] if args else ""
                    ctas_kwargs = {k: v for k, v in kwargs.items() if k != "properties"}
                    res = bridge.call("call", id=self._id, name="ctas",
                                      args=[serialize_arg(table_arg)],
                                      kwargs={k: serialize_arg(v) for k, v in ctas_kwargs.items()})
                    if not res["ok"]:
                        raise ValueError(res["error"])
                    create_proxy = deserialize(res["value"])
                    # Build Properties node with sub-properties
                    prop_exprs = []
                    NAME_TO_PROPERTY = {"FORMAT": "FileFormatProperty"}
                    for k, v in props_dict.items():
                        prop_class = NAME_TO_PROPERTY.get(k.upper())
                        if prop_class:
                            # Known property: FileFormatProperty(this=v)
                            r = bridge.call("createExpression", className=prop_class,
                                            args={"this": v})
                            if r["ok"]:
                                prop_exprs.append(serialize_arg(ExpressionProxy(r["id"], r["key"])))
                        else:
                            # Generic Property(this=Literal.string(k), value=Literal.string(v))
                            key_lit = bridge.call("createExpression", className="Literal",
                                                  args={"this": k, "is_string": True})
                            val_lit = bridge.call("createExpression", className="Literal",
                                                  args={"this": str(v), "is_string": True})
                            if key_lit["ok"] and val_lit["ok"]:
                                prop_r = bridge.call("createExpression", className="Property",
                                                     args={"this": serialize_arg(ExpressionProxy(key_lit["id"], key_lit["key"])),
                                                           "value": serialize_arg(ExpressionProxy(val_lit["id"], val_lit["key"]))})
                                if prop_r["ok"]:
                                    prop_exprs.append(serialize_arg(ExpressionProxy(prop_r["id"], prop_r["key"])))
                    if prop_exprs:
                        props_r = bridge.call("createExpression", className="Properties",
                                              args={"expressions": prop_exprs})
                        if props_r["ok"]:
                            props_proxy = ExpressionProxy(props_r["id"], props_r["key"])
                            bridge.call("call", id=create_proxy._id, name="set",
                                        args=["properties", serialize_arg(props_proxy)])
                    return create_proxy

                serialized_args = [serialize_arg(a) for a in call_args]
                serialized_kwargs = {k: serialize_arg(v) for k, v in call_kwargs.items()}
                res = bridge.call("call", id=self._id, name=name,
                                  args=serialized_args, kwargs=serialized_kwargs)
                if not res["ok"]:
                    raise ValueError(res["error"])
                return deserialize(res["value"])
            return method_proxy

        return deserialize(value)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ExpressionProxy):
            if self._id == other._id:
                return True
            bridge = TSBridge.get()
            result = bridge.call("equals", id=self._id, otherId=other._id)
            return result.get("ok", False) and result.get("value", False)
        return NotImplemented

    def __hash__(self) -> int:
        bridge = TSBridge.get()
        result = bridge.call("hashCode", id=self._id)
        if result.get("ok"):
            return result["value"]
        return hash(self._id)

    def __repr__(self) -> str:
        return f"<Expr:{self._key}#{self._id}>"

    def __str__(self) -> str:
        return self.sql()

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

    def copy(self) -> "ExpressionProxy":
        bridge = TSBridge.get()
        result = bridge.call("copy", id=self._id)
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    # Operator overloading — mirrors Python SQLGlot's Condition operators
    _BINARY_KEYS = frozenset({
        "add", "sub", "mul", "div", "mod", "dpipe", "and", "or",
        "bitwiseand", "bitwiseor", "bitwisexor",
        "eq", "neq", "gt", "gte", "lt", "lte",
        "like", "ilike", "is", "regexplike", "similarto",
        "nullsafeeq", "nullsafeneq",
    })

    def _create_binop(self, class_name: str, other: Any, reverse: bool = False) -> "ExpressionProxy":
        bridge = TSBridge.get()
        other_proxy = _convert_value(other)
        this_copy = self.copy()
        target_key = class_name.lower()
        if this_copy._key != target_key and (not hasattr(other_proxy, '_key') or other_proxy._key != target_key):
            if this_copy._key in self._BINARY_KEYS:
                this_copy = this_copy._wrap_paren()
            if hasattr(other_proxy, '_key') and other_proxy._key in self._BINARY_KEYS:
                other_proxy = other_proxy._wrap_paren()
        this_arg = serialize_arg(other_proxy if reverse else this_copy)
        expr_arg = serialize_arg(this_copy if reverse else other_proxy)
        result = bridge.call("createExpression", className=class_name,
                             args={"this": this_arg, "expression": expr_arg})
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    def _create_unary(self, class_name: str) -> "ExpressionProxy":
        bridge = TSBridge.get()
        paren = self._wrap_paren()
        result = bridge.call("createExpression", className=class_name,
                             args={"this": serialize_arg(paren)})
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    def _wrap_paren(self) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy()
        result = bridge.call("createExpression", className="Paren",
                             args={"this": serialize_arg(this_copy)})
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
        cast_result = bridge.call("createExpression", className="Cast",
                                  args={"this": serialize_arg(div),
                                        "to": serialize_arg(_create_datatype("INT"))})
        if not cast_result["ok"]:
            raise ValueError(cast_result["error"])
        return ExpressionProxy(cast_result["id"], cast_result["key"])
    def __rfloordiv__(self, other: Any) -> "ExpressionProxy":
        bridge = TSBridge.get()
        div = self._create_binop("Div", other, reverse=True)
        cast_result = bridge.call("createExpression", className="Cast",
                                  args={"this": serialize_arg(div),
                                        "to": serialize_arg(_create_datatype("INT"))})
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
        result = bridge.call("callFunction", name="func", args=[
            "POWER", serialize_arg(this_copy), serialize_arg(other_proxy)
        ])
        if not result["ok"]:
            raise ValueError(result["error"])
        return deserialize(result["value"])
    def __rpow__(self, other: Any) -> "ExpressionProxy":
        bridge = TSBridge.get()
        other_proxy = _convert_value(other)
        this_copy = self.copy()
        result = bridge.call("callFunction", name="func", args=[
            "POWER", serialize_arg(other_proxy), serialize_arg(this_copy)
        ])
        if not result["ok"]:
            raise ValueError(result["error"])
        return deserialize(result["value"])
    def __and__(self, other: Any) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy()
        other_val = _convert_value(other)
        result = bridge.call("createExpression", className="And",
                             args={"this": serialize_arg(this_copy), "expression": serialize_arg(other_val)})
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])
    def __rand__(self, other: Any) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy()
        other_val = _convert_value(other)
        result = bridge.call("createExpression", className="And",
                             args={"this": serialize_arg(other_val), "expression": serialize_arg(this_copy)})
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])
    def __or__(self, other: Any) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy()
        other_val = _convert_value(other)
        result = bridge.call("createExpression", className="Or",
                             args={"this": serialize_arg(this_copy), "expression": serialize_arg(other_val)})
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])
    def __ror__(self, other: Any) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy()
        other_val = _convert_value(other)
        result = bridge.call("createExpression", className="Or",
                             args={"this": serialize_arg(other_val), "expression": serialize_arg(this_copy)})
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
        result = bridge.call("createExpression", className="Neg",
                             args={"this": serialize_arg(this_copy)})
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])
    def __invert__(self) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy()
        result = bridge.call("createExpression", className="Not",
                             args={"this": serialize_arg(this_copy)})
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])
    def __iter__(self):
        bridge = TSBridge.get()
        result = bridge.call("hasArgType", id=self._id, name="expressions")
        if result.get("ok") and result.get("value"):
            attr_result = bridge.call("getattr", id=self._id, name="expressions")
            if attr_result["ok"]:
                items = deserialize(attr_result["value"])
                if isinstance(items, list):
                    return iter(items)
        raise TypeError(f"'{self._key}' object is not iterable")

    def __getitem__(self, other: Any) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy()
        if isinstance(other, tuple):
            items = [serialize_arg(_convert_value(v)) for v in other]
        else:
            items = [serialize_arg(_convert_value(other))]
        result = bridge.call("createExpression", className="Bracket",
                             args={"this": serialize_arg(this_copy), "expressions": items})
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    def isin(self, *expressions: Any, query: Any = None, unnest: Any = None,
             copy: bool = True, **opts: Any) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy() if copy else self
        args: dict[str, Any] = {"this": serialize_arg(this_copy)}
        if expressions:
            args["expressions"] = [serialize_arg(_convert_value(e)) for e in expressions]
        if query is not None:
            subquery = parse_one(str(query)) if isinstance(query, str) else query
            sub_result = bridge.call("call", id=subquery._id, name="subquery")
            if sub_result["ok"]:
                subquery = deserialize(sub_result["value"])
            args["query"] = serialize_arg(subquery)
        if unnest is not None:
            unnest_list = unnest if isinstance(unnest, list) else [unnest]
            unnest_exprs = [serialize_arg(parse_one(str(e)) if isinstance(e, str) else e) for e in unnest_list]
            unnest_result = bridge.call("createExpression", className="Unnest",
                                        args={"expressions": unnest_exprs})
            if unnest_result["ok"]:
                args["unnest"] = serialize_arg(ExpressionProxy(unnest_result["id"], unnest_result["key"]))
        result = bridge.call("createExpression", className="In", args=args)
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    def between(self, low: Any, high: Any, copy: bool = True, **opts: Any) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy() if copy else self
        result = bridge.call("createExpression", className="Between",
                             args={"this": serialize_arg(this_copy),
                                   "low": serialize_arg(_convert_value(low)),
                                   "high": serialize_arg(_convert_value(high))})
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

    def as_(self, alias: str, **kwargs: Any) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy()
        alias_id_result = bridge.call("createExpression", className="Identifier",
                                      args={"this": alias})
        if not alias_id_result["ok"]:
            raise ValueError(alias_id_result["error"])
        alias_id = ExpressionProxy(alias_id_result["id"], alias_id_result["key"])
        result = bridge.call("createExpression", className="Alias",
                             args={"this": serialize_arg(this_copy),
                                   "alias": serialize_arg(alias_id)})
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    def desc(self, nulls_first: bool = False) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy()
        args: dict[str, Any] = {"this": serialize_arg(this_copy), "desc": True}
        if nulls_first:
            args["nulls_first"] = True
        result = bridge.call("createExpression", className="Ordered", args=args)
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    def asc(self, nulls_first: bool = True) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy()
        args: dict[str, Any] = {"this": serialize_arg(this_copy)}
        if nulls_first:
            args["nulls_first"] = True
        result = bridge.call("createExpression", className="Ordered", args=args)
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    def on(self, *expressions: Any, append: bool = True, copy: bool = True, **kwargs: Any) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy() if copy else self
        parsed = [parse_one(str(e)) if isinstance(e, str) else e for e in expressions if e is not None]
        if not parsed:
            return this_copy
        node = parsed[0]
        for expr in parsed[1:]:
            and_result = bridge.call("createExpression", className="And",
                                     args={"this": serialize_arg(node), "expression": serialize_arg(expr)})
            if not and_result["ok"]:
                raise ValueError(and_result["error"])
            node = ExpressionProxy(and_result["id"], and_result["key"])
        res = bridge.call("call", id=this_copy._id, name="set", args=["on", serialize_arg(node)])
        if not res["ok"]:
            raise ValueError(res["error"])
        return this_copy

    def using(self, *expressions: Any, append: bool = True, copy: bool = True, **kwargs: Any) -> "ExpressionProxy":
        bridge = TSBridge.get()
        this_copy = self.copy() if copy else self
        parsed = [parse_one(str(e)) if isinstance(e, str) else e for e in expressions if e is not None]
        res = bridge.call("call", id=this_copy._id, name="set",
                          args=["using", [serialize_arg(p) for p in parsed]])
        if not res["ok"]:
            raise ValueError(res["error"])
        return this_copy


def _convert_value(value: Any) -> "ExpressionProxy":
    """Convert a Python value to an ExpressionProxy (lazy import of _convert)."""
    if isinstance(value, ExpressionProxy):
        return value
    return _do_convert(value)


def _do_convert(value: Any) -> "ExpressionProxy":
    """Placeholder — replaced in register_fake_sqlglot with the real _convert."""
    raise RuntimeError("_do_convert not initialized")


def _create_datatype(type_name: str) -> "ExpressionProxy":
    """Create a DataType expression for the given type name."""
    bridge = TSBridge.get()
    result = bridge.call("createExpression", className="DataType",
                         args={"this": type_name, "nested": False})
    if not result["ok"]:
        raise ValueError(result["error"])
    return ExpressionProxy(result["id"], result["key"])


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

    call_kwargs: dict[str, Any] = {"sql": sql, "dialect": dialect}
    if into_name:
        call_kwargs["into"] = into_name
    result = bridge.call("parseOne", **call_kwargs)
    if not result["ok"]:
        raise ParseError(result["error"])
    _emit_bridge_logs(result)
    proxy = ExpressionProxy(result["id"], result["key"])

    if into_name and not isinstance(proxy, into):
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

    def _call_ts_function(name: str, *args: Any, **kwargs: Any) -> Any:
        bridge = TSBridge.get()
        serialized_args = [serialize_arg(a) for a in args]
        serialized_kwargs = {k: serialize_arg(v) for k, v in kwargs.items()}
        result = bridge.call("callFunction", name=name,
                             args=serialized_args, kwargs=serialized_kwargs)
        if not result["ok"]:
            raise ValueError(result["error"])
        return deserialize(result["value"])

    def _select(*expressions: Any, **kwargs: Any) -> ExpressionProxy:
        return _call_ts_function("select", *expressions)

    def _from_(expression: Any, **kwargs: Any) -> ExpressionProxy:
        return _call_ts_function("from_", expression)

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
        if isinstance(expression, str):
            parsed = parse_one(expression)
            if parsed._key == "cast":
                existing_sql = parsed.sql()
                expected_suffix = f" AS {to.upper()})"
                if existing_sql.upper().endswith(expected_suffix):
                    return parsed
            expr_sql = parsed.sql()
        elif isinstance(expression, ExpressionProxy):
            if expression._key == "cast":
                existing_sql = expression.sql()
                expected_suffix = f" AS {to.upper()})"
                if existing_sql.upper().endswith(expected_suffix):
                    return expression
            expr_sql = expression.sql()
        else:
            expr_sql = str(expression)
        result = parse_one(f"CAST({expr_sql} AS {to})")
        return result

    def _merge(*when_exprs: Any, into: str, using: str, on: str, returning: str | None = None, **kwargs: Any) -> ExpressionProxy:
        opts: dict[str, Any] = {"into": into, "using": using, "on": on}
        if returning is not None:
            opts["returning"] = returning
        return _call_ts_function("merge", *when_exprs, opts)

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
        if isinstance(value, list):
            return _convert_to_array(value)
        if isinstance(value, dict):
            bridge = TSBridge.get()
            keys_proxy = _convert_to_array(list(value.keys()))
            vals_proxy = _convert_to_array(list(value.values()))
            result = bridge.call("createExpression", className="Map",
                                 args={"keys": serialize_arg(keys_proxy),
                                       "values": serialize_arg(vals_proxy)})
            if not result["ok"]:
                raise ValueError(result["error"])
            return ExpressionProxy(result["id"], result["key"])
        if isinstance(value, tuple):
            bridge = TSBridge.get()
            converted = [serialize_arg(_convert(v)) for v in value]
            result = bridge.call("createExpression", className="Tuple",
                                 args={"expressions": converted})
            if not result["ok"]:
                raise ValueError(result["error"])
            return ExpressionProxy(result["id"], result["key"])
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

    # Wire up _do_convert for operator support
    global _do_convert
    _do_convert = _convert

    def _convert_to_array(items: list) -> ExpressionProxy:
        """Convert a Python list to an Array expression using function call syntax."""
        bridge = TSBridge.get()
        converted = [serialize_arg(_convert(v)) for v in items]
        # Use Anonymous("ARRAY", ...) to get ARRAY() function syntax instead of ARRAY[] brackets
        result = bridge.call("createExpression", className="Anonymous",
                             args={"this": "ARRAY", "expressions": converted})
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    # Top-level builder functions — match Python sqlglot pattern
    def _condition(expression: Any, dialect: str | None = None, copy: bool = True, **kwargs: Any) -> ExpressionProxy:
        return _call_ts_function("condition", expression, dialect=dialect, copy=copy)

    def _wrap_connector(expr: Any) -> Any:
        """Wrap expression in Paren if it's a Connector (And/Or/Xor)."""
        if isinstance(expr, ExpressionProxy) and expr._key in ("and", "or", "xor"):
            bridge = TSBridge.get()
            result = bridge.call("createExpression", className="Paren", args={"this": serialize_arg(expr)})
            if not result["ok"]:
                raise ValueError(result["error"])
            return ExpressionProxy(result["id"], result["key"])
        return expr

    def _combine(expressions: Any, operator_name: str, dialect: str | None = None, copy: bool = True, wrap: bool = True, **kwargs: Any) -> ExpressionProxy:
        conditions = [_condition(e, dialect=dialect) for e in expressions if e is not None]
        this = conditions[0]
        rest = conditions[1:]
        if rest and wrap:
            this = _wrap_connector(this)
        for expr in rest:
            bridge = TSBridge.get()
            wrapped = _wrap_connector(expr) if wrap else expr
            result = bridge.call("createExpression", className=operator_name,
                                 args={"this": serialize_arg(this), "expression": serialize_arg(wrapped)})
            if not result["ok"]:
                raise ValueError(result["error"])
            this = ExpressionProxy(result["id"], result["key"])
        return this

    def _and(*expressions: Any, dialect: str | None = None, copy: bool = True, wrap: bool = True, **kwargs: Any) -> ExpressionProxy:
        return _combine(expressions, "And", dialect=dialect, copy=copy, wrap=wrap)

    def _or(*expressions: Any, dialect: str | None = None, copy: bool = True, wrap: bool = True, **kwargs: Any) -> ExpressionProxy:
        return _combine(expressions, "Or", dialect=dialect, copy=copy, wrap=wrap)

    def _not(expression: Any, dialect: str | None = None, copy: bool = True, **kwargs: Any) -> ExpressionProxy:
        parsed = _condition(expression, dialect=dialect) if isinstance(expression, str) else expression
        wrapped = _wrap_connector(parsed)
        bridge = TSBridge.get()
        result = bridge.call("createExpression", className="Not", args={"this": serialize_arg(wrapped)})
        if not result["ok"]:
            raise ValueError(result["error"])
        return ExpressionProxy(result["id"], result["key"])

    def _alias(expression: Any, alias: str | Any, **kwargs: Any) -> ExpressionProxy:
        if isinstance(expression, str):
            expression = parse_one(expression)
        alias_str = alias if isinstance(alias, str) else alias.sql()
        return _call_ts_function("alias_", expression, alias_str)

    def _case(expression: Any = None, **kwargs: Any) -> ExpressionProxy:
        if expression is not None:
            return _call_ts_function("case_", expression)
        return _call_ts_function("case_")

    def _union(*expressions: Any, distinct: bool = True, **kwargs: Any) -> ExpressionProxy:
        parsed = [parse_one(str(e)) if isinstance(e, str) else e for e in expressions]
        return _call_ts_function("union", *parsed)

    def _intersect(*expressions: Any, **kwargs: Any) -> ExpressionProxy:
        parsed = [parse_one(str(e)) if isinstance(e, str) else e for e in expressions]
        return _call_ts_function("intersect", *parsed)

    def _except(*expressions: Any, **kwargs: Any) -> ExpressionProxy:
        parsed = [parse_one(str(e)) if isinstance(e, str) else e for e in expressions]
        return _call_ts_function("except_", *parsed)

    # exp-level helper functions
    def _update(table: str | Any, properties: dict | None = None, where: str | Any = None,
                from_: str | Any = None, with_: dict | None = None, **kwargs: Any) -> ExpressionProxy:
        opts: dict[str, Any] = {}
        if where is not None:
            opts["where"] = where
        if from_ is not None:
            opts["from_"] = from_
        if with_ is not None:
            opts["with_"] = with_
        return _call_ts_function("update", table, properties, **opts)

    def _values(values_list: list, alias: str | None = None, columns: list | None = None, **kwargs: Any) -> ExpressionProxy:
        return _call_ts_function("values", values_list, alias, columns)

    def _delete(table: str | Any, where: str | Any = None, returning: str | None = None,
                dialect: str | None = None, **kwargs: Any) -> ExpressionProxy:
        opts: dict[str, Any] = {}
        if where is not None:
            opts["where"] = where
        if returning is not None:
            opts["returning"] = returning
        if dialect is not None:
            opts["dialect"] = dialect
        return _call_ts_function("delete_", table, **opts)

    def _insert(expression: str | Any, into: str | Any, columns: list | None = None,
                overwrite: bool = False, returning: str | None = None, **kwargs: Any) -> ExpressionProxy:
        opts: dict[str, Any] = {}
        if columns is not None:
            opts["columns"] = columns
        if overwrite:
            opts["overwrite"] = overwrite
        if returning is not None:
            opts["returning"] = returning
        return _call_ts_function("insert", expression, into, **opts)

    def _rename_column(table: str, old_name: str, new_name: str, if_exists: bool = False, **kwargs: Any) -> ExpressionProxy:
        return _call_ts_function("renameColumn", table, old_name, new_name, if_exists or None)

    def _subquery(expression: str | Any, alias: str | None = None, **kwargs: Any) -> ExpressionProxy:
        return _call_ts_function("subquery", expression, alias)

    sqlglot_exp = ExpModule("sqlglot.expressions")
    sqlglot_exp.Expression = Expression  # type: ignore
    sqlglot_exp.convert = _convert  # type: ignore
    sqlglot_exp.to_identifier = _to_identifier  # type: ignore
    sqlglot_exp.maybe_parse = _maybe_parse  # type: ignore
    sqlglot_exp.select = _select  # type: ignore
    sqlglot_exp.from_ = _from_  # type: ignore
    sqlglot_exp.to_table = _to_table  # type: ignore
    sqlglot_exp.to_column = _to_column  # type: ignore
    sqlglot_exp.column = _column  # type: ignore
    sqlglot_exp.table_ = _table  # type: ignore
    sqlglot_exp.cast = _cast  # type: ignore
    sqlglot_exp.merge = _merge  # type: ignore
    sqlglot_exp.func = _func  # type: ignore
    sqlglot_exp.update = _update  # type: ignore
    sqlglot_exp.values = _values  # type: ignore
    sqlglot_exp.delete = _delete  # type: ignore
    sqlglot_exp.insert = _insert  # type: ignore
    sqlglot_exp.rename_column = _rename_column  # type: ignore
    sqlglot_exp.subquery = _subquery  # type: ignore
    sqlglot_exp.and_ = _and  # type: ignore
    sqlglot_exp.or_ = _or  # type: ignore
    sqlglot_exp.not_ = _not  # type: ignore
    sqlglot_exp.condition = _condition  # type: ignore
    sqlglot_exp.union = _union  # type: ignore
    sqlglot_exp.intersect = _intersect  # type: ignore
    sqlglot_exp.except_ = _except  # type: ignore
    sys.modules["sqlglot.expressions"] = sqlglot_exp
    sqlglot_mod.exp = sqlglot_exp  # type: ignore

    # Register top-level functions on sqlglot module
    sqlglot_mod.select = _select  # type: ignore
    sqlglot_mod.from_ = _from_  # type: ignore
    sqlglot_mod.condition = _condition  # type: ignore
    sqlglot_mod.and_ = _and  # type: ignore
    sqlglot_mod.or_ = _or  # type: ignore
    sqlglot_mod.not_ = _not  # type: ignore
    sqlglot_mod.alias = _alias  # type: ignore
    sqlglot_mod.case = _case  # type: ignore
    sqlglot_mod.union = _union  # type: ignore
    sqlglot_mod.intersect = _intersect  # type: ignore
    sqlglot_mod.except_ = _except  # type: ignore

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
