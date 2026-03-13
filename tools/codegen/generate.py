"""
Generate TypeScript expression classes from Python SQLGlot.
Run: just generate
"""

import ast
import logging
import textwrap
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass
class PropertyInfo:
    name: str
    pattern: str  # "text", "bool", "expr", "const_true", "expressions_alias"
    arg_key: str = ""  # for text/bool/expr: the args key
    override: bool = False


@dataclass
class MethodInfo:
    name: str
    pattern: str
    arg: str = ""
    into: str = ""
    prefix: str = ""
    append: bool = False
    binop_type: str = ""
    return_self: bool = True  # True = `: this`, False = use class name


@dataclass
class ClassInfo:
    name: str
    parents: list[str]
    arg_types: dict[str, bool] | None
    is_var_len_args: bool
    sql_names: list[str] | None
    properties: list[PropertyInfo] | None = None
    methods: list[MethodInfo] | None = None


def parse_dict_literal(node: ast.Dict) -> dict[str, bool]:
    result = {}
    for key, value in zip(node.keys, node.values, strict=False):
        if isinstance(key, ast.Constant) and isinstance(key.value, str):
            if isinstance(value, ast.Constant):
                result[key.value] = bool(value.value)
            elif isinstance(value, ast.Name):
                result[key.value] = value.id == "True"
    return result


def parse_list_literal(node: ast.List) -> list[str]:
    return [
        elt.value
        for elt in node.elts
        if isinstance(elt, ast.Constant) and isinstance(elt.value, str)
    ]


def resolve_spread(
    node: ast.Dict, shared_dicts: dict[str, dict[str, bool]]
) -> dict[str, bool]:
    result: dict[str, bool] = {}
    for key, value in zip(node.keys, node.values, strict=False):
        if key is None:
            if isinstance(value, ast.Name) and value.id in shared_dicts:
                result.update(shared_dicts[value.id])
            elif (
                isinstance(value, ast.Call)
                and isinstance(value.func, ast.Attribute)
                and value.func.attr == "copy"
                and isinstance(value.func.value, ast.Name)
            ):
                dict_name = value.func.value.id
                if dict_name in shared_dicts:
                    result.update(shared_dicts[dict_name])
        elif isinstance(key, ast.Constant) and isinstance(key.value, str):
            if isinstance(value, ast.Constant):
                result[key.value] = bool(value.value)
            elif isinstance(value, ast.Name):
                result[key.value] = value.id == "True"
    return result


def extract_module_dicts(tree: ast.Module) -> dict[str, dict[str, bool]]:
    dicts: dict[str, dict[str, bool]] = {}
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if (
                    isinstance(target, ast.Name)
                    and target.id.isupper()
                    and isinstance(node.value, ast.Dict)
                ):
                    dicts[target.id] = parse_dict_literal(node.value)
    return dicts


def _get_return_value(func: ast.FunctionDef) -> ast.expr | None:
    for stmt in func.body:
        if isinstance(stmt, ast.Return) and stmt.value:
            return stmt.value
    return None


def _is_self_method_call(node: ast.expr, method: str) -> ast.Call | None:
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
        return None
    if not isinstance(node.func.value, ast.Name) or node.func.value.id != "self":
        return None
    if node.func.attr != method:
        return None
    return node


def _is_self_text_call(node: ast.expr) -> str | None:
    call = _is_self_method_call(node, "text")
    if call is None or len(call.args) != 1:
        return None
    arg = call.args[0]
    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
        return arg.value
    return None


def _is_self_text_upper_call(node: ast.expr) -> str | None:
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "upper"
        and len(node.args) == 0
    ):
        return _is_self_text_call(node.func.value)
    return None


def _is_self_args_attr(node: ast.expr) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
        and node.attr == "args"
    )


def _is_self_args_access(node: ast.expr) -> str | None:
    if isinstance(node, ast.Subscript):
        if isinstance(node.slice, ast.Constant) and _is_self_args_attr(node.value):
            return str(node.slice.value)
        return None
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
        return None
    if node.func.attr != "get" or len(node.args) < 1:
        return None
    if _is_self_args_attr(node.func.value) and isinstance(node.args[0], ast.Constant):
        return str(node.args[0].value)
    return None


def _is_bool_args(node: ast.expr) -> str | None:
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
        return None
    if node.func.id != "bool" or len(node.args) != 1:
        return None
    return _is_self_args_access(node.args[0])


def _extract_apply_kwargs(call: ast.Call) -> dict[str, str]:
    result: dict[str, str] = {}
    for kw in call.keywords:
        if kw.arg is None:
            continue
        if isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
            result[kw.arg] = kw.value.value
        elif isinstance(kw.value, ast.Name):
            result[kw.arg] = kw.value.id
        elif isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, bool):
            result[kw.arg] = str(kw.value.value)
    return result


def _is_apply_call(node: ast.expr) -> tuple[str, dict[str, str]] | None:
    if not isinstance(node, ast.Call):
        return None
    func_name = None
    if isinstance(node.func, ast.Name):
        func_name = node.func.id
    if func_name not in {
        "_apply_builder",
        "_apply_list_builder",
        "_apply_child_list_builder",
        "_apply_conjunction_builder",
    }:
        return None
    return func_name, _extract_apply_kwargs(node)


def _is_binop_call(node: ast.expr) -> str | None:
    call = _is_self_method_call(node, "_binop")
    if call is None or len(call.args) < 1:
        return None
    if isinstance(call.args[0], ast.Name):
        return call.args[0].id
    return None


def _snake_to_camel(name: str) -> str:
    trail = (
        "_"
        if name.endswith("_")
        and not name.startswith("_")
        and not name[:-1].endswith("_")
        else ""
    )
    base = name.rstrip("_")
    parts = base.split("_")
    if not parts:
        return name
    result = parts[0]
    for p in parts[1:]:
        if p:
            result += p[0].upper() + p[1:]
    return result + trail


_APPLY_PATTERN_MAP = {
    "_apply_builder": "apply_builder",
    "_apply_list_builder": "apply_list_builder",
    "_apply_child_list_builder": "apply_child_list_builder",
    "_apply_conjunction_builder": "apply_conjunction_builder",
}

_OVERRIDE_PROPERTIES = {"name", "is_star"}


def _extract_property(func: ast.FunctionDef, class_name: str) -> PropertyInfo | None:
    ret = _get_return_value(func)
    if ret is None:
        return None

    py_name = func.name
    ts_name = _snake_to_camel(py_name)
    needs_override = py_name in _OVERRIDE_PROPERTIES and class_name != "Expression"

    text_key = _is_self_text_call(ret)
    if text_key is not None:
        if py_name == "name" and text_key == "this":
            return None
        return PropertyInfo(
            name=ts_name,
            pattern="text",
            arg_key=text_key,
            override=needs_override,
        )

    text_upper_key = _is_self_text_upper_call(ret)
    if text_upper_key is not None:
        return PropertyInfo(
            name=ts_name,
            pattern="text_upper",
            arg_key=text_upper_key,
            override=needs_override,
        )

    bool_key = _is_bool_args(ret)
    if bool_key is not None:
        return PropertyInfo(
            name=ts_name,
            pattern="bool",
            arg_key=bool_key,
            override=needs_override,
        )

    if isinstance(ret, ast.Constant) and ret.value is True:
        return PropertyInfo(name=ts_name, pattern="const_true", override=needs_override)

    if (
        isinstance(ret, ast.Attribute)
        and isinstance(ret.value, ast.Name)
        and ret.value.id == "self"
        and ret.attr == "expressions"
    ):
        return PropertyInfo(
            name=ts_name,
            pattern="expressions_alias",
            override=needs_override,
        )

    return None


def _extract_method(func: ast.FunctionDef) -> MethodInfo | None:
    py_name = func.name
    if py_name.startswith("_") or py_name in {"__init__", "__repr__", "__str__"}:
        return None

    ret = _get_return_value(func)
    if ret is None:
        return None

    apply = _is_apply_call(ret)
    if apply is not None:
        func_name, kwargs = apply
        return MethodInfo(
            name=_snake_to_camel(py_name),
            pattern=_APPLY_PATTERN_MAP[func_name],
            arg=kwargs.get("arg", ""),
            into=kwargs.get("into", ""),
            prefix=kwargs.get("prefix", ""),
            append=kwargs.get("append", "") in {"True", "append"},
        )

    binop_type = _is_binop_call(ret)
    if binop_type is not None:
        return MethodInfo(
            name=_snake_to_camel(py_name),
            pattern="binop",
            binop_type=binop_type,
        )

    return None


def extract_methods(
    class_node: ast.ClassDef, class_name: str
) -> tuple[list[PropertyInfo], list[MethodInfo]]:
    properties: list[PropertyInfo] = []
    methods: list[MethodInfo] = []

    for item in class_node.body:
        if not isinstance(item, ast.FunctionDef):
            continue

        is_property = any(
            isinstance(d, ast.Name) and d.id == "property" for d in item.decorator_list
        )

        if is_property:
            prop = _extract_property(item, class_name)
            if prop is not None:
                properties.append(prop)
        else:
            method = _extract_method(item)
            if method is not None:
                methods.append(method)

    return properties, methods


@dataclass
class _ClassAttrs:
    arg_types: dict[str, bool] | None = None
    is_var_len_args: bool = False
    sql_names: list[str] | None = None


def _extract_arg_types(
    value: ast.expr, shared_dicts: dict[str, dict[str, bool]]
) -> dict[str, bool] | None:
    if isinstance(value, ast.Dict):
        has_spread = any(k is None for k in value.keys)
        if has_spread:
            return resolve_spread(value, shared_dicts)
        return parse_dict_literal(value)
    if (
        isinstance(value, ast.Call)
        and isinstance(value.func, ast.Attribute)
        and value.func.attr == "copy"
        and isinstance(value.func.value, ast.Name)
        and value.func.value.id in shared_dicts
    ):
        return shared_dicts[value.func.value.id].copy()
    return None


def _parse_bool_value(value: ast.expr) -> bool:
    if isinstance(value, ast.Constant):
        return bool(value.value)
    if isinstance(value, ast.Name):
        return value.id == "True"
    return False


def _extract_class_attrs(
    node: ast.ClassDef, shared_dicts: dict[str, dict[str, bool]]
) -> _ClassAttrs:
    attrs = _ClassAttrs()
    for item in node.body:
        if not isinstance(item, ast.Assign):
            continue
        for target in item.targets:
            if not isinstance(target, ast.Name):
                continue
            match target.id:
                case "arg_types":
                    result = _extract_arg_types(item.value, shared_dicts)
                    if result is not None:
                        attrs.arg_types = result
                case "is_var_len_args":
                    attrs.is_var_len_args = _parse_bool_value(item.value)
                case "_sql_names" if isinstance(item.value, ast.List):
                    attrs.sql_names = parse_list_literal(item.value)
                case _:
                    pass
    return attrs


def extract_classes(
    tree: ast.Module, shared_dicts: dict[str, dict[str, bool]]
) -> list[ClassInfo]:
    classes: list[ClassInfo] = []

    for node in ast.iter_child_nodes(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        if node.name.startswith("_") or node.name in {"ExpOrStr", "IntoType"}:
            continue

        parents = [
            base.id if isinstance(base, ast.Name) else base.attr
            for base in node.bases
            if isinstance(base, (ast.Name, ast.Attribute))
        ]
        if not parents and node.name != "Expression":
            continue

        attrs = _extract_class_attrs(node, shared_dicts)
        props, meths = extract_methods(node, node.name)

        classes.append(
            ClassInfo(
                name=node.name,
                parents=parents,
                arg_types=attrs.arg_types,
                is_var_len_args=attrs.is_var_len_args,
                sql_names=attrs.sql_names,
                properties=props or None,
                methods=meths or None,
            )
        )

    return classes


def extract_datatype_types(tree: ast.Module) -> list[str]:
    for node in ast.iter_child_nodes(tree):
        if not isinstance(node, ast.ClassDef) or node.name != "DataType":
            continue
        for item in node.body:
            if isinstance(item, ast.ClassDef) and item.name == "Type":
                types: list[str] = [
                    t.id
                    for stmt in item.body
                    if isinstance(stmt, ast.Assign)
                    for t in stmt.targets
                    if isinstance(t, ast.Name)
                ]
                return types
    return []


def extract_datatype_sets(tree: ast.Module) -> dict[str, list[str]]:
    sets: dict[str, list[str]] = {}
    for node in ast.iter_child_nodes(tree):
        if not isinstance(node, ast.ClassDef) or node.name != "DataType":
            continue
        for item in node.body:
            if not isinstance(item, ast.Assign):
                continue
            for target in item.targets:
                if not isinstance(target, ast.Name) or not target.id.endswith("_TYPES"):
                    continue
                if isinstance(item.value, ast.Set):
                    members = _extract_set_members(item.value, sets)
                    if members:
                        sets[target.id] = members
    return sets


def _extract_set_members(node: ast.Set, known_sets: dict[str, list[str]]) -> list[str]:
    members: list[str] = []
    for elt in node.elts:
        if (
            isinstance(elt, ast.Attribute)
            and isinstance(elt.value, ast.Name)
            and elt.value.id == "Type"
        ):
            members.append(elt.attr)
        elif (
            isinstance(elt, ast.Starred)
            and isinstance(elt.value, ast.Name)
            and elt.value.id in known_sets
        ):
            members.extend(known_sets[elt.value.id])
    return members


def topological_sort(classes: list[ClassInfo]) -> list[ClassInfo]:
    class_map = {c.name: c for c in classes}
    output: list[ClassInfo] = []
    output_names: set[str] = set()
    in_progress: set[str] = set()

    def visit(name: str) -> None:
        if name in output_names or name in in_progress or name not in class_map:
            return
        in_progress.add(name)
        cls = class_map[name]
        for parent in cls.parents:
            visit(parent)
        in_progress.remove(name)
        output.append(cls)
        output_names.add(name)

    for cls in classes:
        visit(cls.name)

    return output


def code_lines(code: str) -> list[str]:
    return textwrap.dedent(code).strip("\n").split("\n")


# Methods provided manually through explicit generated class members below.
# Format: "ClassName.methodName" — auto-generation skips these names.
MANUAL_METHODS: set[str] = {
    # Complex builder methods
    "Query.subquery",
    "Query.with_",
    "Query.union",
    "Query.intersect",
    "Query.except_",
    "Query.select",  # abstract in Python, raises NotImplementedError
    "Select.from_",
    "Select.select",
    "Select.join",
    "Select.distinct",
    "Select.ctas",
    "Select.lock",
    "Select.hint",
    "Insert.with_",
    "Delete.delete",
    # Update.set_ conflicts with Expression.set signature
    "Update.set_",
    # Expression methods live in expression-base.ts
    "Expression.and_",
    "Expression.or_",
    "Expression.not_",
    "Expression.asc",
    "Expression.desc",
    "Expression.div",
    "Expression.between",
    "Expression.isin",
    "Expression.alias",
    # Case builder methods
    "Case.when",
    "Case.else_",
}

CUSTOM_CLASS_MEMBERS: dict[str, list[str]] = {
    "Query": code_lines("""
        get ctes(): Expression[] {
          const with_ = this.args.with_;
          return with_ instanceof Expression ? with_.expressions : [];
        }
        get selects(): Expression[] {
          throw new Error("Query objects must implement `selects`");
        }
        get namedSelects(): string[] {
          throw new Error("Query objects must implement `namedSelects`");
        }
        select(...args: (string | Expression | BuilderOptions)[]): this {
          void args;
          throw new Error("Query objects must implement `select`");
        }
        subquery(alias?: string): Subquery {
          const subquery = new Subquery({ this: this });
          if (alias) {
            subquery.set(
              "alias",
              new TableAlias({ this: new Identifier({ this: alias }) }),
            );
          }
          return subquery;
        }
        with_(
          alias: string | Expression,
          as_: string | Expression,
          options?: {
            recursive?: boolean;
            materialized?: boolean;
            append?: boolean;
            copy?: boolean;
            dialect?: string;
            scalar?: boolean;
          },
        ): this {
          return applyCteBuilder(alias, as_, this, options);
        }
        union(
          ...args: (string | Expression | { distinct?: boolean })[]
        ): Union {
          const { expressions, distinct } = extractSetOperationArgs(args);
          return applySetOperation(
            [this, ...expressions], Union, { distinct },
          ) as Union;
        }
        intersect(
          ...args: (string | Expression | { distinct?: boolean })[]
        ): Intersect {
          const { expressions, distinct } = extractSetOperationArgs(args);
          return applySetOperation(
            [this, ...expressions], Intersect, { distinct },
          ) as Intersect;
        }
        except_(
          ...args: (string | Expression | { distinct?: boolean })[]
        ): Except {
          const { expressions, distinct } = extractSetOperationArgs(args);
          return applySetOperation(
            [this, ...expressions], Except, { distinct },
          ) as Except;
        }
    """),
    "Column": code_lines("""
        override get isStar(): boolean {
          const thisValue = this.args.this;
          return thisValue instanceof Expression && thisValue.key === "star";
        }
    """),
    "Delete": code_lines("""
        delete_(
          table: string | Expression,
          options?: { copy?: boolean; dialect?: string },
        ): this {
          return _applyBuilder(table, this, "this", {
            copy: options?.copy ?? true,
            into: Table,
            dialect: options?.dialect,
          }) as this;
        }
    """),
    "Literal": code_lines("""
        get isString(): boolean {
          return !!this.args.is_string;
        }
        get isNumber(): boolean {
          return !this.args.is_string;
        }
        get value(): string | number {
          const value = this.args.this;
          if (typeof value === "string") {
            return this.args.is_string ? value : Number.parseFloat(value);
          }
          if (typeof value === "number") {
            return value;
          }
          return "";
        }
        static string(value: string): Literal {
          return new Literal({ this: value, is_string: true });
        }
        static number(value: number | string): Literal {
          return new Literal({ this: `${value}`, is_string: false });
        }
    """),
    "Join": code_lines("""
        get aliasOrName(): string {
          const thisExpression = this.args.this;
          return thisExpression instanceof Expression ? thisExpression.aliasOrName : "";
        }
        get isSemiOrAntiJoin(): boolean {
          return this.kind === "SEMI" || this.kind === "ANTI";
        }
        on(...expressions: (string | Expression | null | undefined)[]): this {
          const join = _applyConjunctionBuilder(expressions, this, "on", {
            append: true,
            copy: true,
          }) as this;
          if (join.text("kind") === "CROSS") {
            join.set("kind", undefined);
          }
          return join;
        }
        using(...expressions: (string | Expression | null | undefined)[]): this {
          const filtered = expressions.filter(
            (expression): expression is string | Expression =>
              expression !== null && expression !== undefined,
          );
          const join = _applyListBuilder(filtered, this, "using", {
            append: true,
            copy: true,
          }) as this;
          if (join.text("kind") === "CROSS") {
            join.set("kind", undefined);
          }
          return join;
        }
    """),
    "Boolean": code_lines("""
        get value(): boolean {
          return !!this.args.this;
        }
        static true_(): Boolean {
          return new Boolean({ this: true });
        }
        static false_(): Boolean {
          return new Boolean({ this: false });
        }
    """),
    "SetOperation": code_lines("""
        override select(...args: (string | Expression | BuilderOptions)[]): this {
          const { expressions, options } = _extractBuilderArgs(args);
          const instance = (options.copy === false ? this : this.copy()) as this;
          const left = instance.args.this;
          const right = instance.args.expression;
          if (!(left instanceof Expression) || !(right instanceof Expression)) {
            return instance;
          }
          const leftSelect = left.unnest();
          const rightSelect = right.unnest();
          _applyListBuilder(expressions, leftSelect, "expressions", {
            copy: false,
            append: options.append ?? true,
            dialect: options.dialect,
          });
          _applyListBuilder(expressions, rightSelect, "expressions", {
            copy: false,
            append: options.append ?? true,
            dialect: options.dialect,
          });
          return instance;
        }
        override get namedSelects(): string[] {
          let expression: Query = this;
          while (expression instanceof SetOperation) {
            const left = expression.args.this;
            if (!(left instanceof Expression)) {
              return [];
            }
            const unnested = left.unnest();
            if (!(unnested instanceof Query)) {
              return [];
            }
            expression = unnested;
          }
          return expression.namedSelects;
        }
        override get isStar(): boolean {
          const left = this.args.this;
          const right = this.args.expression;
          return left instanceof Expression && right instanceof Expression
            ? left.isStar || right.isStar
            : false;
        }
        override get selects(): Expression[] {
          let expression: Query = this;
          while (expression instanceof SetOperation) {
            const left = expression.args.this;
            if (!(left instanceof Expression)) {
              return [];
            }
            const unnested = left.unnest();
            if (!(unnested instanceof Query)) {
              return [];
            }
            expression = unnested;
          }
          return expression.selects;
        }
        get left(): Query | undefined {
          const value = this.args.this;
          return value instanceof Query ? value : undefined;
        }
        get right(): Query | undefined {
          const value = this.args.expression;
          return value instanceof Query ? value : undefined;
        }
    """),
    "Update": code_lines("""
        set_(...expressions: (string | Expression)[]): this {
          return _applyListBuilder(expressions, this, "expressions", {
            append: true,
            copy: true,
          }) as this;
        }
        with_(
          alias: string | Expression,
          as_: string | Expression,
          options?: {
            recursive?: boolean;
            materialized?: boolean;
            append?: boolean;
            copy?: boolean;
            dialect?: string;
          },
        ): this {
          return applyCteBuilder(alias, as_, this, options);
        }
    """),
    "Select": code_lines("""
        override get namedSelects(): string[] {
          return this.expressions.map((expression) => expression.outputName);
        }
        override get isStar(): boolean {
          return this.expressions.some((expression) => expression.isStar);
        }
        get where_(): Expression | undefined {
          const where = this.args.where;
          return where instanceof Expression ? where : undefined;
        }
        from_(expression: string | Expression, options?: BuilderOptions): this {
          return _applyBuilder(expression, this, "from_", {
            copy: options?.copy ?? true,
            into: From,
            prefix: "FROM",
            dialect: options?.dialect,
          }) as this;
        }
        override select(...args: (string | Expression | BuilderOptions)[]): this {
          const { expressions, options } = _extractBuilderArgs(args);
          return _applyListBuilder(expressions, this, "expressions", {
            copy: options.copy ?? true,
            append: options.append ?? true,
            dialect: options.dialect,
          }) as this;
        }
        join(
          expression: string | Expression,
          options?: {
            on?: string | Expression | (string | Expression)[];
            using?: string | (string | Expression)[];
            joinType?: string;
            joinAlias?: string | Expression;
            append?: boolean;
            copy?: boolean;
            dialect?: string;
          },
        ): this {
          const copy = options?.copy ?? true;
          const dialect = options?.dialect;
          let join: Expression;
          try {
            join = maybeParse(expression, { into: Join, prefix: "JOIN", dialect });
          } catch {
            join = maybeParse(expression, { dialect });
          }
          if (!(join instanceof Join)) {
            join = new Join({ this: join });
          }
          if (options?.joinType) {
            const parts = options.joinType.toUpperCase().split(/\\s+/);
            for (const part of parts) {
              if (part === "LEFT" || part === "RIGHT" || part === "FULL") {
                join.set("side", part);
              } else if (
                part === "INNER" ||
                part === "OUTER" ||
                part === "CROSS" ||
                part === "SEMI" ||
                part === "ANTI"
              ) {
                join.set("kind", part);
              } else if (part === "NATURAL") {
                join.set("method", part);
              }
            }
          }
          if (options?.on) {
            const onValues = globalThis.Array.isArray(options.on)
              ? options.on : [options.on];
            join = _applyConjunctionBuilder(onValues, join, "on", {
              append: true,
              copy: false,
              dialect,
            });
          }
          if (options?.using) {
            const usingValues = globalThis.Array.isArray(options.using)
              ? options.using : [options.using];
            const usingExpressions = usingValues.map((value) =>
              typeof value === "string" ? new Identifier({ this: value }) : value,
            );
            join = _applyListBuilder(usingExpressions, join, "using", {
              append: true,
              copy: false,
            });
          }
          if (options?.joinAlias) {
            const joinValue = join.args.this;
            const alias = typeof options.joinAlias === "string"
              ? new Identifier({ this: options.joinAlias })
              : options.joinAlias;
            if (joinValue instanceof Expression && alias instanceof Expression) {
              joinValue.set("alias", new TableAlias({ this: alias }));
            }
          }
          const joinValue = join.args.this;
          if (joinValue instanceof Select) {
            joinValue.replace(joinValue.subquery());
          }
          return _applyListBuilder([join], this, "joins", {
            copy,
            append: options?.append ?? true,
          }) as this;
        }
        distinct(...args: (string | Expression | { distinct?: boolean })[]): this {
          const instance = this.copy() as this;
          let distinctValue = true;
          const onValues: (string | Expression)[] = [];
          for (const arg of args) {
            if (
              typeof arg === "object" &&
              arg !== null &&
              !(arg instanceof Expression) &&
              "distinct" in arg
            ) {
              distinctValue = arg.distinct ?? true;
            } else {
              onValues.push(arg as string | Expression);
            }
          }
          const on = onValues.length > 0
            ? new Tuple({ expressions: onValues.map((value) => maybeParse(value)) })
            : undefined;
          instance.set("distinct", distinctValue ? new Distinct({ on }) : undefined);
          return instance;
        }
        ctas(
          table: string | Expression,
          options?: { dialect?: string; copy?: boolean },
        ): Create {
          const instance = options?.copy !== false ? this.copy() : this;
          const tableExpression = maybeParse(table, {
            into: Table,
            dialect: options?.dialect,
          });
          return new Create({
            this: tableExpression,
            kind: "TABLE",
            expression: instance,
          });
        }
        lock(update = true, copy = true): this {
          const instance = copy ? (this.copy() as this) : this;
          instance.set("locks", [new Lock({ update })]);
          return instance;
        }
        hint(...hints: (string | Expression)[]): this {
          const instance = this.copy() as this;
          instance.set("hint", new Hint({
            expressions: hints.map((hint) => maybeParse(hint)),
          }));
          return instance;
        }
    """),
    "Star": code_lines("""
        override get isStar(): boolean {
          return true;
        }
    """),
    "Func": code_lines("""
        static readonly sqlNames: readonly string[] | undefined = undefined;
        override get name(): string {
          const ctor = this.constructor as typeof Func
            & { readonly sqlNames?: readonly string[] };
          const first = ctor.sqlNames?.[0];
          if (first) {
            return first;
          }
          return camelToSnakeCase(ctor.className);
        }
    """),
    "Anonymous": code_lines("""
        override get name(): string {
          const thisValue = this.args.this;
          if (typeof thisValue === "string") {
            return thisValue;
          }
          return thisValue instanceof Expression ? thisValue.name : "";
        }
    """),
    "Case": code_lines("""
        when(
          condition: string | Expression,
          then: string | Expression,
          copy = true,
        ): this {
          const instance = copy ? (this.copy() as this) : this;
          instance.append("ifs", new If({
            this: maybeParse(condition),
            true: maybeParse(then),
          }));
          return instance;
        }
        else_(condition: string | Expression, copy = true): this {
          const instance = copy ? (this.copy() as this) : this;
          instance.set("default", maybeParse(condition));
          return instance;
        }
    """),
    "Binary": code_lines("""
        get left(): Expression | undefined {
          const value = this.args.this;
          return value instanceof Expression ? value : undefined;
        }
        get right(): Expression | undefined {
          const value = this.args.expression;
          return value instanceof Expression ? value : undefined;
        }
    """),
    "Insert": code_lines("""
        returning(
          expression: string | Expression,
          options?: { copy?: boolean; dialect?: string },
        ): this {
          return _applyBuilder(expression, this, "returning", {
            copy: options?.copy ?? true,
            into: Returning,
            prefix: "RETURNING",
            dialect: options?.dialect,
          }) as this;
        }
        with_(
          alias: string | Expression,
          as_: string | Expression,
          options?: {
            recursive?: boolean;
            materialized?: boolean;
            append?: boolean;
            copy?: boolean;
            dialect?: string;
          },
        ): this {
          return applyCteBuilder(alias, as_, this, options);
        }
    """),
}

CUSTOM_HELPERS = code_lines("""
    type SetOperationBuilderArg = string | Expression | { distinct?: boolean };
    type CteBuilderOptions = {
      recursive?: boolean;
      materialized?: boolean;
      append?: boolean;
      copy?: boolean;
      dialect?: string;
      scalar?: boolean;
    };

    function extractSetOperationArgs(
      args: SetOperationBuilderArg[],
    ): { expressions: (string | Expression)[]; distinct: boolean } {
      let distinct = true;
      const expressions: (string | Expression)[] = [];
      for (const arg of args) {
        if (
          typeof arg === "object" &&
          arg !== null &&
          !(arg instanceof Expression) &&
          "distinct" in arg
        ) {
          distinct = arg.distinct ?? true;
        } else {
          expressions.push(arg as string | Expression);
        }
      }
      return { expressions, distinct };
    }

    function applySetOperation(
      expressions: (string | Expression)[],
      setConstructor: new (args?: Args) => Expression,
      options?: {
        distinct?: boolean;
        dialect?: string;
        copy?: boolean;
      },
    ): Expression {
      const parsed = expressions.map((expression) =>
        maybeParse(expression, {
          dialect: options?.dialect,
          copy: options?.copy,
        }),
      );
      let result = parsed[0];
      if (!result) {
        throw new Error("Set operations require at least one expression");
      }
      for (const expression of parsed.slice(1)) {
        result = new setConstructor({
          this: result,
          expression,
          distinct: options?.distinct ?? true,
        });
      }
      return result;
    }

    function applyCteBuilder<T extends Query | Update | Insert>(
      alias: string | Expression,
      as_: string | Expression,
      instance: T,
      options?: CteBuilderOptions,
    ): T {
      const aliasExpression = maybeParse(alias, {
        dialect: options?.dialect,
        into: TableAlias,
      });
      let asExpression = maybeParse(as_, {
        dialect: options?.dialect,
        copy: options?.copy,
      });
      if (options?.scalar && !(asExpression instanceof Subquery)) {
        asExpression = new Subquery({ this: asExpression });
      }
      const cte = new CTE({
        this: asExpression,
        alias: aliasExpression,
        materialized: options?.materialized,
        scalar: options?.scalar,
      });
      return _applyChildListBuilder([cte], instance, "with_", {
        append: options?.append ?? true,
        copy: options?.copy ?? true,
        into: With,
        properties: options?.recursive ? { recursive: options.recursive } : {},
      }) as T;
    }
""")


def _all_ancestors(
    name: str,
    class_map: dict[str, list[str]],
    visited: set[str] | None = None,
) -> set[str]:
    if visited is None:
        visited = set()
    if name in visited or name not in class_map:
        return set()
    visited.add(name)
    result: set[str] = set()
    for parent in class_map[name]:
        result.add(parent)
        result.update(_all_ancestors(parent, class_map, visited))
    return result


def _ts_ancestors(
    name: str,
    class_map: dict[str, list[str]],
    visited: set[str] | None = None,
) -> set[str]:
    if visited is None:
        visited = set()
    if name in visited or name not in class_map:
        return set()
    visited.add(name)
    parents = class_map.get(name, [])
    if not parents:
        return set()
    primary = parents[0]
    result = {primary}
    result.update(_ts_ancestors(primary, class_map, visited))
    return result


def compute_multi_inheritance_map(
    classes: list[ClassInfo],
) -> dict[str, list[str]]:
    class_map = {c.name: c.parents for c in classes}
    expr_classes = set(class_map.keys())
    extra: dict[str, list[str]] = {}

    for name in class_map:
        lost = _all_ancestors(name, class_map) - _ts_ancestors(name, class_map)
        for parent in lost:
            if parent in expr_classes:
                extra.setdefault(parent, []).append(name)

    return {k: sorted(v) for k, v in sorted(extra.items())}


def emit_property(prop: PropertyInfo) -> list[str]:
    ov = "override " if prop.override else ""
    k = prop.arg_key
    n = prop.name
    match prop.pattern:
        case "text":
            body = f"return this.text('{k}');"
            return [f"  {ov}get {n}(): string {{ {body} }}"]
        case "text_upper":
            body = f"return this.text('{k}').toUpperCase();"
            return [f"  {ov}get {n}(): string {{ {body} }}"]
        case "bool":
            body = f"return !!this.args['{k}'];"
            return [f"  {ov}get {n}(): boolean {{ {body} }}"]
        case "const_true":
            return [f"  {ov}get {n}(): boolean {{ return true; }}"]
        case "expressions_alias":
            body = "return this.expressions;"
            return [f"  {ov}get {n}(): Expression[] {{ {body} }}"]
        case "expr":
            return [
                f"  {ov}get {n}(): Expression | undefined {{",
                f"    const val = this.args['{k}'];",
                "    return val instanceof Expression ? val : undefined;",
                "  }",
            ]
        case _:
            return []


def _builder_sig(name: str, params: str) -> str:
    return f"  {name}({params}): this {{"


def _builder_return(func: str, args: str, opts: str) -> str:
    return f"    return {func}({args}, {{ {opts} }}) as this;"


def _emit_apply_builder(m: MethodInfo) -> list[str]:
    parts = [f"copy: options?.copy ?? true, into: {m.into}"]
    if m.prefix:
        parts.append(f"prefix: '{m.prefix}'")
    parts.append("dialect: options?.dialect")
    opts = ", ".join(parts)
    return [
        _builder_sig(
            m.name,
            "expression: string | Expression | number, options?: BuilderOptions",
        ),
        "    const expr = typeof expression === 'number'"
        + " ? `${expression}` : expression;",
        _builder_return("_applyBuilder", f"expr, this, '{m.arg}'", opts),
        "  }",
    ]


def _emit_list_style_builder(
    m: MethodInfo, func_name: str, append_opts: str
) -> list[str]:
    parts = []
    if m.into:
        parts.append(f"into: {m.into}")
    if m.prefix:
        parts.append(f"prefix: '{m.prefix}'")
    extra = ", ".join(parts)
    extra_s = f", {extra}" if extra else ""
    opts = f"{append_opts}{extra_s}, dialect: options.dialect"
    sig = _builder_sig(m.name, "...args: (string | Expression | BuilderOptions)[]")
    extract = "    const { expressions, options } = _extractBuilderArgs(args);"
    lines = [sig, extract]
    if m.name == "groupBy":
        lines.append(
            "    if (!expressions.length)"
            + " return (options.copy === false"
            + " ? this : this.copy()) as this;"
        )
    lines.extend([
        _builder_return(func_name, f"expressions, this, '{m.arg}'", opts),
        "  }",
    ])
    return lines


def _emit_conjunction_builder(m: MethodInfo, append_opts: str) -> list[str]:
    opts = f"{append_opts}, into: {m.into}, dialect: options.dialect"
    sig = _builder_sig(
        m.name,
        "...args: (string | Expression | null | undefined | BuilderOptions)[]",
    )
    extract = "    const { expressions, options } = _extractBuilderArgs(args);"
    return [
        sig,
        extract,
        _builder_return(
            "_applyConjunctionBuilder", f"expressions, this, '{m.arg}'", opts
        ),
        "  }",
    ]


def emit_method(method: MethodInfo) -> list[str]:
    m = method
    common_opts = "copy: options.copy ?? true"
    append_opts = f"{common_opts}, append: options.append ?? true"

    match m.pattern:
        case "apply_builder":
            return _emit_apply_builder(m)
        case "apply_child_list_builder":
            return _emit_list_style_builder(m, "_applyChildListBuilder", append_opts)
        case "apply_conjunction_builder":
            return _emit_conjunction_builder(m, append_opts)
        case "apply_list_builder":
            return _emit_list_style_builder(m, "_applyListBuilder", append_opts)
        case "binop":
            body = f"return this._binop({m.binop_type}, other) as {m.binop_type};"
            return [f"  {m.name}(other: unknown): {m.binop_type} {{ {body} }}"]
        case _:
            return []


_FUNC_LIKE_PARENTS = frozenset({"Func", "AggFunc", "SafeFunc"})


def _emit_class(cls: ClassInfo, lines: list[str]) -> None:
    parent = cls.parents[0] if cls.parents else "Expression"
    other_parents = cls.parents[1:] if len(cls.parents) > 1 else []
    has_func_parent = any(p in _FUNC_LIKE_PARENTS for p in other_parents)

    if other_parents:
        lines.append(f"// Also extends: {', '.join(other_parents)}")

    lines.append(f"export class {cls.name} extends {parent} {{")
    _emit_static_fields(cls, parent, lines)
    _emit_members(cls, lines)

    if has_func_parent and cls.name not in CUSTOM_CLASS_MEMBERS:
        _emit_func_name_getter(cls.name, lines)

    lines.extend(["}", ""])


def _emit_static_fields(cls: ClassInfo, parent: str, lines: list[str]) -> None:
    if cls.arg_types is not None and cls.arg_types:
        arg_types_str = ", ".join(
            f"'{k}': {str(v).lower()}" for k, v in cls.arg_types.items()
        )
        lines.append(
            "  static readonly argTypes: Record<string, boolean>"
            + f" = {{ {arg_types_str} }};"
        )
    elif cls.arg_types is not None:
        lines.append("  static readonly argTypes: Record<string, boolean> = {};")

    if cls.is_var_len_args:
        lines.append("  static readonly isVarLenArgs = true;")

    if cls.sql_names is not None and cls.sql_names:
        sql_names_str = ", ".join(f"'{n}'" for n in cls.sql_names)
        lines.append(
            f"  static readonly sqlNames: readonly string[] = [{sql_names_str}];"
        )

    key = cls.name.lower()
    override_kw = "override " if parent != "Expression" else ""
    lines.extend([
        f"  {override_kw}get key(): string {{ return '{key}'; }}",
        f"  static readonly className: string = '{cls.name}';",
    ])


def _emit_members(cls: ClassInfo, lines: list[str]) -> None:
    if cls.properties:
        for prop in cls.properties:
            if f"{cls.name}.{prop.name}" not in MANUAL_METHODS:
                lines.extend(emit_property(prop))
    if cls.methods:
        for method in cls.methods:
            if f"{cls.name}.{method.name}" not in MANUAL_METHODS:
                lines.extend(emit_method(method))

    custom_members = CUSTOM_CLASS_MEMBERS.get(cls.name)
    if custom_members:
        lines.extend(f"  {line}" for line in custom_members)


def _emit_func_name_getter(class_name: str, lines: list[str]) -> None:
    ctor_cast = f"typeof {class_name} & {{ readonly sqlNames?: readonly string[] }}"
    lines.extend([
        "  override get name(): string {",
        f"    const ctor = this.constructor as {ctor_cast};",
        "    const first = ctor.sqlNames?.[0];",
        "    if (first) return first;",
        "    return camelToSnakeCase(ctor.className);",
        "  }",
    ])


def _emit_datatype(
    dt_types: list[str] | None,
    dt_sets: dict[str, list[str]] | None,
    lines: list[str],
) -> None:
    if dt_types:
        lines.append("export const DATA_TYPE_TYPES = {")
        lines.extend(f"  {t}: '{t}'," for t in dt_types)
        lines.extend([
            "} as const;",
            "",
            "export type DataTypeType"
            + " = typeof DATA_TYPE_TYPES[keyof typeof DATA_TYPE_TYPES];",
            "",
        ])

    if dt_sets:
        lines.extend([
            "// eslint-disable-next-line @typescript-eslint/no-shadow",
            "const NativeSet = globalThis.Set;",
        ])
        for set_name, members in dt_sets.items():
            members_str = ", ".join(f"'{m}'" for m in members)
            lines.append(
                f"export const {set_name}: ReadonlySet<string>"
                + f" = new NativeSet([{members_str}]);"
            )
        lines.append("")


def _build_import_line(lines: list[str], import_line_index: int) -> None:
    body = "\n".join(lines[import_line_index + 1 :])
    imports = ["Expression", "type Args", "type ExpressionClass"]
    for name in [
        "camelToSnakeCase",
        "maybeParse",
        "_applyBuilder",
        "_applyListBuilder",
        "_applyChildListBuilder",
        "_applyConjunctionBuilder",
        "type BuilderOptions",
        "_extractBuilderArgs",
    ]:
        bare = name.replace("type ", "")
        if bare in body:
            imports.append(name)
    lines[import_line_index] = (
        f"import {{ {', '.join(imports)} }} from './expression-base.js';"
    )


def generate_typescript(
    classes: list[ClassInfo],
    dt_types: list[str] | None = None,
    dt_sets: dict[str, list[str]] | None = None,
) -> str:
    lines: list[str] = [
        "/**",
        " * AUTO-GENERATED - DO NOT EDIT",
        " * Generated from sqlglot/sqlglot/expressions.py",
        " * Run: just generate",
        " */",
        "",
    ]
    import_line_index = len(lines)
    lines.extend(["", ""])

    generated_names: list[str] = []
    for cls in classes:
        if cls.name == "Expression":
            continue
        generated_names.append(cls.name)
        _emit_class(cls, lines)

    lines.extend(CUSTOM_HELPERS)
    lines.extend([
        "",
        "type NamedExpressionClass = ExpressionClass & { readonly className: string };",
        "",
        "export const GENERATED_CLASSES: readonly NamedExpressionClass[] = [",
    ])
    lines.extend(f"  {name}," for name in generated_names)
    lines.extend(["];", ""])

    multi_map = compute_multi_inheritance_map(classes)
    lines.append(
        "export const MULTI_INHERITANCE_MAP: Record<string, readonly string[]> = {"
    )
    for parent_name, children in multi_map.items():
        children_str = ", ".join(f"'{c}'" for c in children)
        lines.append(f"  '{parent_name}': [{children_str}],")
    lines.extend(["};", ""])

    _emit_datatype(dt_types, dt_sets, lines)
    _build_import_line(lines, import_line_index)

    return "\n".join(lines)


def main() -> None:
    source = PROJECT_ROOT / "sqlglot/sqlglot/expressions.py"
    output = PROJECT_ROOT / "src/expressions.generated.ts"

    logger.info(f"Reading {source}")
    source_code = source.read_text()

    logger.info("Parsing Python AST...")
    tree = ast.parse(source_code)

    logger.info("Extracting module-level dicts...")
    shared_dicts = extract_module_dicts(tree)
    logger.info(f"  Found: {list(shared_dicts.keys())}")

    logger.info("Extracting class definitions...")
    classes = extract_classes(tree, shared_dicts)
    logger.info(f"  Found {len(classes)} classes")

    logger.info("Sorting classes topologically...")
    sorted_classes = topological_sort(classes)

    logger.info("Extracting DataType.Type enum...")
    dt_types = extract_datatype_types(tree)
    logger.info(f"  Found {len(dt_types)} types")

    logger.info("Extracting DataType type sets...")
    dt_sets = extract_datatype_sets(tree)
    logger.info(f"  Found {len(dt_sets)} sets: {list(dt_sets.keys())}")

    logger.info("Generating TypeScript...")
    typescript = generate_typescript(sorted_classes, dt_types, dt_sets)

    logger.info(f"Writing {output}")
    output.write_text(typescript)

    logger.info(f"Done! Generated {len(sorted_classes)} classes.")


if __name__ == "__main__":
    main()
