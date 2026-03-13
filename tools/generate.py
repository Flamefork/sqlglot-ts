#!/usr/bin/env python3
"""
Generate TypeScript expression classes from Python SQLGlot.
Run: just generate
"""

import ast
from dataclasses import dataclass
from pathlib import Path
import textwrap

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass
class PropertyInfo:
    name: str
    pattern: str  # "text", "bool", "expr", "const_true", "expressions_alias"
    arg_key: str = ""  # for text/bool/expr: the args key
    override: bool = False


@dataclass
class MethodInfo:
    name: str
    pattern: str  # "apply_builder", "apply_list_builder", "apply_child_list_builder", "apply_conjunction_builder", "binop"
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
    for key, value in zip(node.keys, node.values):
        if isinstance(key, ast.Constant) and isinstance(key.value, str):
            if isinstance(value, ast.Constant):
                result[key.value] = bool(value.value)
            elif isinstance(value, ast.Name):
                result[key.value] = value.id == "True"
    return result


def parse_list_literal(node: ast.List) -> list[str]:
    result = []
    for elt in node.elts:
        if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
            result.append(elt.value)
    return result


def resolve_spread(
    node: ast.Dict, shared_dicts: dict[str, dict[str, bool]]
) -> dict[str, bool]:
    result: dict[str, bool] = {}
    for key, value in zip(node.keys, node.values):
        if key is None:
            if isinstance(value, ast.Name) and value.id in shared_dicts:
                result.update(shared_dicts[value.id])
            elif (
                isinstance(value, ast.Call)
                and isinstance(value.func, ast.Attribute)
                and value.func.attr == "copy"
            ):
                if isinstance(value.func.value, ast.Name):
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
                if isinstance(target, ast.Name) and target.id.isupper():
                    if isinstance(node.value, ast.Dict):
                        dicts[target.id] = parse_dict_literal(node.value)
    return dicts


def _get_return_value(func: ast.FunctionDef) -> ast.expr | None:
    for stmt in func.body:
        if isinstance(stmt, ast.Return) and stmt.value:
            return stmt.value
    return None


def _is_self_text_call(node: ast.expr) -> str | None:
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "self"
        and node.func.attr == "text"
        and len(node.args) == 1
        and isinstance(node.args[0], ast.Constant)
        and isinstance(node.args[0].value, str)
    ):
        return node.args[0].value
    return None


def _is_self_text_upper_call(node: ast.expr) -> str | None:
    """Detect pattern: return self.text("key").upper()"""
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "upper"
        and len(node.args) == 0
    ):
        return _is_self_text_call(node.func.value)
    return None


def _is_bool_args(node: ast.expr) -> str | None:
    # bool(self.args.get("key")) or bool(self.args["key"])
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "bool":
        if len(node.args) == 1:
            inner = node.args[0]
            # self.args.get("key")
            if (
                isinstance(inner, ast.Call)
                and isinstance(inner.func, ast.Attribute)
                and inner.func.attr == "get"
                and isinstance(inner.func.value, ast.Attribute)
                and isinstance(inner.func.value.value, ast.Name)
                and inner.func.value.value.id == "self"
                and inner.func.value.attr == "args"
                and len(inner.args) >= 1
                and isinstance(inner.args[0], ast.Constant)
            ):
                return str(inner.args[0].value)
            # self.args["key"]
            if (
                isinstance(inner, ast.Subscript)
                and isinstance(inner.value, ast.Attribute)
                and isinstance(inner.value.value, ast.Name)
                and inner.value.value.id == "self"
                and inner.value.attr == "args"
                and isinstance(inner.slice, ast.Constant)
            ):
                return str(inner.slice.value)
    return None


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
    if func_name not in (
        "_apply_builder",
        "_apply_list_builder",
        "_apply_child_list_builder",
        "_apply_conjunction_builder",
    ):
        return None
    return func_name, _extract_apply_kwargs(node)


def _is_binop_call(node: ast.expr) -> str | None:
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "self"
        and node.func.attr == "_binop"
        and len(node.args) >= 1
        and isinstance(node.args[0], ast.Name)
    ):
        return node.args[0].id
    return None


def _snake_to_camel(name: str) -> str:
    trail = "_" if name.endswith("_") and not name.startswith("_") and not name[:-1].endswith("_") else ""
    base = name.rstrip("_")
    parts = base.split("_")
    if not parts:
        return name
    result = parts[0]
    for p in parts[1:]:
        if p:
            result += p[0].upper() + p[1:]
    return result + trail


def extract_methods(
    class_node: ast.ClassDef, class_name: str
) -> tuple[list[PropertyInfo], list[MethodInfo]]:
    properties: list[PropertyInfo] = []
    methods: list[MethodInfo] = []

    apply_pattern_map = {
        "_apply_builder": "apply_builder",
        "_apply_list_builder": "apply_list_builder",
        "_apply_child_list_builder": "apply_child_list_builder",
        "_apply_conjunction_builder": "apply_conjunction_builder",
    }

    for item in class_node.body:
        if not isinstance(item, ast.FunctionDef):
            continue

        is_property = any(
            isinstance(d, ast.Name) and d.id == "property" for d in item.decorator_list
        )

        if is_property:
            ret = _get_return_value(item)
            if ret is None:
                continue

            py_name = item.name
            ts_name = _snake_to_camel(py_name)
            needs_override = py_name in ("name", "is_star") and class_name not in ("Expression",)

            # Pattern: return self.text("key")
            text_key = _is_self_text_call(ret)
            if text_key is not None:
                # Skip if it's just the base Expression.name pattern (self.text("this"))
                # since expression-base.ts already provides this
                if py_name == "name" and text_key == "this":
                    continue
                properties.append(PropertyInfo(
                    name=ts_name, pattern="text", arg_key=text_key, override=needs_override
                ))
                continue

            # Pattern: return self.text("key").upper()
            text_upper_key = _is_self_text_upper_call(ret)
            if text_upper_key is not None:
                properties.append(PropertyInfo(
                    name=ts_name, pattern="text_upper", arg_key=text_upper_key, override=needs_override
                ))
                continue

            # Pattern: return bool(self.args.get("key"))
            bool_key = _is_bool_args(ret)
            if bool_key is not None:
                properties.append(PropertyInfo(
                    name=ts_name, pattern="bool", arg_key=bool_key, override=needs_override
                ))
                continue

            # Pattern: return True
            if isinstance(ret, ast.Constant) and ret.value is True:
                properties.append(PropertyInfo(
                    name=ts_name, pattern="const_true", override=needs_override
                ))
                continue

            # Pattern: return self.expressions (alias)
            if (
                isinstance(ret, ast.Attribute)
                and isinstance(ret.value, ast.Name)
                and ret.value.id == "self"
                and ret.attr == "expressions"
            ):
                properties.append(PropertyInfo(
                    name=ts_name, pattern="expressions_alias", override=needs_override
                ))
                continue

        else:
            # Regular method
            py_name = item.name
            if py_name.startswith("_") or py_name in ("__init__", "__repr__", "__str__"):
                continue

            ret = _get_return_value(item)
            if ret is None:
                continue

            # Pattern: return _apply_*(...)
            apply = _is_apply_call(ret)
            if apply is not None:
                func_name, kwargs = apply
                pattern = apply_pattern_map[func_name]
                arg = kwargs.get("arg", "")
                into = kwargs.get("into", "")
                prefix = kwargs.get("prefix", "")
                append_val = kwargs.get("append", "")

                ts_name = _snake_to_camel(py_name)

                methods.append(MethodInfo(
                    name=ts_name,
                    pattern=pattern,
                    arg=arg,
                    into=into,
                    prefix=prefix,
                    append=append_val == "True" or append_val == "append",
                ))
                continue

            # Pattern: return self._binop(Type, other)
            binop_type = _is_binop_call(ret)
            if binop_type is not None:
                ts_name = _snake_to_camel(py_name)
                methods.append(MethodInfo(
                    name=ts_name,
                    pattern="binop",
                    binop_type=binop_type,
                ))
                continue

    return properties, methods


def extract_classes(
    tree: ast.Module, shared_dicts: dict[str, dict[str, bool]]
) -> list[ClassInfo]:
    classes: list[ClassInfo] = []

    for node in ast.iter_child_nodes(tree):
        if not isinstance(node, ast.ClassDef):
            continue

        if node.name.startswith("_") or node.name in ("ExpOrStr", "IntoType"):
            continue

        parents = []
        for base in node.bases:
            if isinstance(base, ast.Name):
                parents.append(base.id)
            elif isinstance(base, ast.Attribute):
                parents.append(base.attr)

        if not parents:
            if node.name != "Expression":
                continue

        arg_types: dict[str, bool] | None = None
        is_var_len_args = False
        sql_names: list[str] | None = None

        for item in node.body:
            if isinstance(item, ast.Assign):
                for target in item.targets:
                    if isinstance(target, ast.Name):
                        if target.id == "arg_types":
                            if isinstance(item.value, ast.Dict):
                                has_spread = any(k is None for k in item.value.keys)
                                if has_spread:
                                    arg_types = resolve_spread(item.value, shared_dicts)
                                else:
                                    arg_types = parse_dict_literal(item.value)
                            elif (
                                isinstance(item.value, ast.Call)
                                and isinstance(item.value.func, ast.Attribute)
                                and item.value.func.attr == "copy"
                            ):
                                if isinstance(item.value.func.value, ast.Name):
                                    dict_name = item.value.func.value.id
                                    if dict_name in shared_dicts:
                                        arg_types = shared_dicts[dict_name].copy()
                        elif target.id == "is_var_len_args":
                            if isinstance(item.value, ast.Constant):
                                is_var_len_args = bool(item.value.value)
                            elif isinstance(item.value, ast.Name):
                                is_var_len_args = item.value.id == "True"
                        elif target.id == "_sql_names":
                            if isinstance(item.value, ast.List):
                                sql_names = parse_list_literal(item.value)

        props, meths = extract_methods(node, node.name)

        classes.append(
            ClassInfo(
                name=node.name,
                parents=parents,
                arg_types=arg_types,
                is_var_len_args=is_var_len_args,
                sql_names=sql_names,
                properties=props if props else None,
                methods=meths if meths else None,
            )
        )

    return classes


def extract_datatype_types(tree: ast.Module) -> list[str]:
    for node in ast.iter_child_nodes(tree):
        if not isinstance(node, ast.ClassDef) or node.name != "DataType":
            continue
        for item in node.body:
            if isinstance(item, ast.ClassDef) and item.name == "Type":
                types: list[str] = []
                for stmt in item.body:
                    if isinstance(stmt, ast.Assign):
                        for t in stmt.targets:
                            if isinstance(t, ast.Name):
                                types.append(t.id)
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
        if isinstance(elt, ast.Attribute) and isinstance(elt.value, ast.Name) and elt.value.id == "Type":
            members.append(elt.attr)
        elif isinstance(elt, ast.Starred) and isinstance(elt.value, ast.Name) and elt.value.id in known_sets:
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
    "Query.subquery", "Query.with_", "Query.union", "Query.intersect", "Query.except_",
    "Query.select",  # abstract in Python, raises NotImplementedError
    "Select.from_", "Select.select", "Select.join",
    "Select.distinct", "Select.ctas", "Select.lock", "Select.hint",
    "Insert.with_",
    "Delete.delete",
    # Update.set_ conflicts with Expression.set signature (manual impl in expressions.ts)
    "Update.set_",
    # Expression methods live in expression-base.ts
    "Expression.and_", "Expression.or_", "Expression.not_",
    "Expression.asc", "Expression.desc",
    "Expression.div", "Expression.between", "Expression.isin", "Expression.alias",
    # Case builder methods
    "Case.when", "Case.else_",
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
            subquery.set("alias", new TableAlias({ this: new Identifier({ this: alias }) }));
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
        union(...args: (string | Expression | { distinct?: boolean })[]): Union {
          const { expressions, distinct } = extractSetOperationArgs(args);
          return applySetOperation([this, ...expressions], Union, { distinct }) as Union;
        }
        intersect(...args: (string | Expression | { distinct?: boolean })[]): Intersect {
          const { expressions, distinct } = extractSetOperationArgs(args);
          return applySetOperation([this, ...expressions], Intersect, { distinct }) as Intersect;
        }
        except_(...args: (string | Expression | { distinct?: boolean })[]): Except {
          const { expressions, distinct } = extractSetOperationArgs(args);
          return applySetOperation([this, ...expressions], Except, { distinct }) as Except;
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
            const onValues = globalThis.Array.isArray(options.on) ? options.on : [options.on];
            join = _applyConjunctionBuilder(onValues, join, "on", {
              append: true,
              copy: false,
              dialect,
            });
          }
          if (options?.using) {
            const usingValues = globalThis.Array.isArray(options.using) ? options.using : [options.using];
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
          return new Create({ this: tableExpression, kind: "TABLE", expression: instance });
        }
        lock(update = true, copy = true): this {
          const instance = copy ? (this.copy() as this) : this;
          instance.set("locks", [new Lock({ update })]);
          return instance;
        }
        hint(...hints: (string | Expression)[]): this {
          const instance = this.copy() as this;
          instance.set("hint", new Hint({ expressions: hints.map((hint) => maybeParse(hint)) }));
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
          const ctor = this.constructor as typeof Func & { readonly sqlNames?: readonly string[] };
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
        when(condition: string | Expression, then: string | Expression, copy = true): this {
          const instance = copy ? (this.copy() as this) : this;
          instance.append("ifs", new If({ this: maybeParse(condition), true: maybeParse(then) }));
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


def compute_multi_inheritance_map(classes: list[ClassInfo]) -> dict[str, list[str]]:
    class_map = {c.name: c.parents for c in classes}

    def all_ancestors(name: str, visited: set[str] | None = None) -> set[str]:
        if visited is None:
            visited = set()
        if name in visited or name not in class_map:
            return set()
        visited.add(name)
        result: set[str] = set()
        for parent in class_map[name]:
            result.add(parent)
            result.update(all_ancestors(parent, visited))
        return result

    def ts_ancestors(name: str, visited: set[str] | None = None) -> set[str]:
        if visited is None:
            visited = set()
        if name in visited or name not in class_map:
            return set()
        visited.add(name)
        result: set[str] = set()
        parents = class_map.get(name, [])
        if parents:
            primary = parents[0]
            result.add(primary)
            result.update(ts_ancestors(primary, visited))
        return result

    extra: dict[str, list[str]] = {}
    expr_classes = set(class_map.keys())
    for name in class_map:
        lost = all_ancestors(name) - ts_ancestors(name)
        for parent in lost:
            if parent in expr_classes:
                extra.setdefault(parent, []).append(name)

    return {k: sorted(v) for k, v in sorted(extra.items())}


def emit_property(prop: PropertyInfo) -> list[str]:
    override = "override " if prop.override else ""
    match prop.pattern:
        case "text":
            return [f"  {override}get {prop.name}(): string {{ return this.text('{prop.arg_key}'); }}"]
        case "text_upper":
            return [f"  {override}get {prop.name}(): string {{ return this.text('{prop.arg_key}').toUpperCase(); }}"]
        case "bool":
            return [f"  {override}get {prop.name}(): boolean {{ return !!this.args['{prop.arg_key}']; }}"]
        case "const_true":
            return [f"  {override}get {prop.name}(): boolean {{ return true; }}"]
        case "expressions_alias":
            return [f"  {override}get {prop.name}(): Expression[] {{ return this.expressions; }}"]
        case "expr":
            return [
                f"  {override}get {prop.name}(): Expression | undefined {{",
                f"    const val = this.args['{prop.arg_key}'];",
                f"    return val instanceof Expression ? val : undefined;",
                f"  }}",
            ]
        case _:
            return []


def emit_method(method: MethodInfo) -> list[str]:
    match method.pattern:
        case "apply_builder":
            opts_parts = [f"copy: options?.copy ?? true, into: {method.into}"]
            if method.prefix:
                opts_parts.append(f"prefix: '{method.prefix}'")
            opts_parts.append("dialect: options?.dialect")
            opts = ", ".join(opts_parts)
            return [
                f"  {method.name}(expression: string | Expression | number, options?: BuilderOptions): this {{",
                f"    const expr = typeof expression === 'number' ? `${{expression}}` : expression;",
                f"    return _applyBuilder(expr, this, '{method.arg}', {{ {opts} }}) as this;",
                f"  }}",
            ]
        case "apply_child_list_builder":
            opts_parts = []
            if method.into:
                opts_parts.append(f"into: {method.into}")
            if method.prefix:
                opts_parts.append(f"prefix: '{method.prefix}'")
            opts = ", ".join(opts_parts)
            # group_by is the only method with empty-args guard in Python
            is_group_by = method.name == "groupBy"
            lines = [
                f"  {method.name}(...args: (string | Expression | BuilderOptions)[]): this {{",
                f"    const {{ expressions, options }} = _extractBuilderArgs(args);",
            ]
            if is_group_by:
                lines.append(f"    if (!expressions.length) return (options.copy === false ? this : this.copy()) as this;")
            lines.extend([
                f"    return _applyChildListBuilder(expressions, this, '{method.arg}', {{ copy: options.copy ?? true, append: options.append ?? true, {opts}, dialect: options.dialect }}) as this;",
                f"  }}",
            ])
            return lines
        case "apply_conjunction_builder":
            return [
                f"  {method.name}(...args: (string | Expression | null | undefined | BuilderOptions)[]): this {{",
                f"    const {{ expressions, options }} = _extractBuilderArgs(args);",
                f"    return _applyConjunctionBuilder(expressions, this, '{method.arg}', {{ copy: options.copy ?? true, into: {method.into}, append: options.append ?? true, dialect: options.dialect }}) as this;",
                f"  }}",
            ]
        case "apply_list_builder":
            opts_parts = []
            if method.into:
                opts_parts.append(f"into: {method.into}")
            if method.prefix:
                opts_parts.append(f"prefix: '{method.prefix}'")
            opts = ", ".join(opts_parts)
            comma = ", " if opts else ""
            return [
                f"  {method.name}(...args: (string | Expression | BuilderOptions)[]): this {{",
                f"    const {{ expressions, options }} = _extractBuilderArgs(args);",
                f"    return _applyListBuilder(expressions, this, '{method.arg}', {{ copy: options.copy ?? true, append: options.append ?? true{comma}{opts}, dialect: options.dialect }}) as this;",
                f"  }}",
            ]
        case "binop":
            return [
                f"  {method.name}(other: unknown): {method.binop_type} {{ return this._binop({method.binop_type}, other) as {method.binop_type}; }}",
            ]
        case _:
            return []


def generate_typescript(
    classes: list[ClassInfo],
    dt_types: list[str] | None = None,
    dt_sets: dict[str, list[str]] | None = None,
) -> str:
    lines: list[str] = []

    lines.append("/**")
    lines.append(" * AUTO-GENERATED - DO NOT EDIT")
    lines.append(" * Generated from sqlglot/sqlglot/expressions.py")
    lines.append(" * Run: just generate")
    lines.append(" */")
    lines.append("")
    # Placeholder for import line — will be filled after code generation
    import_line_index = len(lines)
    lines.append("")  # placeholder
    lines.append("")

    generated_names: list[str] = []

    for cls in classes:
        if cls.name == "Expression":
            continue

        generated_names.append(cls.name)
        parent = cls.parents[0] if cls.parents else "Expression"
        other_parents = cls.parents[1:] if len(cls.parents) > 1 else []

        func_like_parents = {"Func", "AggFunc", "SafeFunc"}
        has_func_parent = any(p in func_like_parents for p in other_parents)

        if other_parents:
            lines.append(f"// Also extends: {', '.join(other_parents)}")

        has_own_arg_types = cls.arg_types is not None
        has_var_len_args = cls.is_var_len_args
        has_sql_names = cls.sql_names is not None

        lines.append(f"export class {cls.name} extends {parent} {{")

        if has_own_arg_types and cls.arg_types:
            arg_types_str = ", ".join(
                f"'{k}': {str(v).lower()}" for k, v in cls.arg_types.items()
            )
            lines.append(f"  static readonly argTypes: Record<string, boolean> = {{ {arg_types_str} }};")
        elif has_own_arg_types:
            lines.append("  static readonly argTypes: Record<string, boolean> = {};")

        if has_var_len_args:
            lines.append("  static readonly isVarLenArgs = true;")

        if has_sql_names and cls.sql_names:
            sql_names_str = ", ".join(f"'{n}'" for n in cls.sql_names)
            lines.append(f"  static readonly sqlNames: readonly string[] = [{sql_names_str}];")

        key = cls.name.lower()
        needs_override = parent != "Expression"
        override_kw = "override " if needs_override else ""
        lines.append(f"  {override_kw}get key(): string {{ return '{key}'; }}")
        lines.append(f"  static readonly className: string = '{cls.name}';")

        # Emit auto-generated properties (skip those manually provided)
        if cls.properties:
            for prop in cls.properties:
                if f"{cls.name}.{prop.name}" not in MANUAL_METHODS:
                    lines.extend(emit_property(prop))

        # Emit auto-generated methods (skip those manually provided)
        if cls.methods:
            for method in cls.methods:
                if f"{cls.name}.{method.name}" not in MANUAL_METHODS:
                    lines.extend(emit_method(method))

        custom_members = CUSTOM_CLASS_MEMBERS.get(cls.name)
        if custom_members:
            lines.extend(f"  {line}" for line in custom_members)

        # Func-like classes get auto-generated name getter when they do not define one explicitly.
        if has_func_parent and cls.name not in CUSTOM_CLASS_MEMBERS:
            lines.append(f"  override get name(): string {{")
            lines.append(
                f"    const ctor = this.constructor as typeof {cls.name} & {{ readonly sqlNames?: readonly string[] }};"
            )
            lines.append(f"    const first = ctor.sqlNames?.[0];")
            lines.append(f"    if (first) return first;")
            lines.append(f"    return camelToSnakeCase(ctor.className);")
            lines.append(f"  }}")

        lines.append("}")
        lines.append("")

    lines.extend(CUSTOM_HELPERS)
    lines.append("")

    lines.append("type NamedExpressionClass = ExpressionClass & { readonly className: string };")
    lines.append("")
    lines.append("export const GENERATED_CLASSES: readonly NamedExpressionClass[] = [")
    for name in generated_names:
        lines.append(f"  {name},")
    lines.append("];")
    lines.append("")

    multi_map = compute_multi_inheritance_map(classes)
    lines.append("export const MULTI_INHERITANCE_MAP: Record<string, readonly string[]> = {")
    for parent, children in multi_map.items():
        children_str = ", ".join(f"'{c}'" for c in children)
        lines.append(f"  '{parent}': [{children_str}],")
    lines.append("};")
    lines.append("")

    # Generate DataType.Type enum and type sets
    if dt_types:
        lines.append("export const DATA_TYPE_TYPES = {")
        for t in dt_types:
            lines.append(f"  {t}: '{t}',")
        lines.append("} as const;")
        lines.append("")
        lines.append("export type DataTypeType = typeof DATA_TYPE_TYPES[keyof typeof DATA_TYPE_TYPES];")
        lines.append("")

    if dt_sets:
        lines.append("// eslint-disable-next-line @typescript-eslint/no-shadow")
        lines.append("const NativeSet = globalThis.Set;")
        for set_name, members in dt_sets.items():
            members_str = ", ".join(f"'{m}'" for m in members)
            lines.append(f"export const {set_name}: ReadonlySet<string> = new NativeSet([{members_str}]);")
        lines.append("")

    # Build import line based on actual usage in generated code
    body = "\n".join(lines[import_line_index + 1:])
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
    lines[import_line_index] = f"import {{ {', '.join(imports)} }} from './expression-base.js';"

    return "\n".join(lines)


def main() -> None:
    source = PROJECT_ROOT / "sqlglot/sqlglot/expressions.py"
    output = PROJECT_ROOT / "src/expressions.generated.ts"

    print(f"Reading {source}")
    source_code = source.read_text()

    print("Parsing Python AST...")
    tree = ast.parse(source_code)

    print("Extracting module-level dicts...")
    shared_dicts = extract_module_dicts(tree)
    print(f"  Found: {list(shared_dicts.keys())}")

    print("Extracting class definitions...")
    classes = extract_classes(tree, shared_dicts)
    print(f"  Found {len(classes)} classes")

    print("Sorting classes topologically...")
    sorted_classes = topological_sort(classes)

    print("Extracting DataType.Type enum...")
    dt_types = extract_datatype_types(tree)
    print(f"  Found {len(dt_types)} types")

    print("Extracting DataType type sets...")
    dt_sets = extract_datatype_sets(tree)
    print(f"  Found {len(dt_sets)} sets: {list(dt_sets.keys())}")

    print("Generating TypeScript...")
    typescript = generate_typescript(sorted_classes, dt_types, dt_sets)

    print(f"Writing {output}")
    output.write_text(typescript)

    print(f"Done! Generated {len(sorted_classes)} classes.")


if __name__ == "__main__":
    main()
