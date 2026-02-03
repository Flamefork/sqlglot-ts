#!/usr/bin/env python3
"""
Generate TypeScript expression classes from Python SQLGlot.
Run: npm run generate
"""

import ast
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent


@dataclass
class ClassInfo:
    name: str
    parents: list[str]
    arg_types: dict[str, bool] | None
    is_var_len_args: bool
    sql_names: list[str] | None


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

        classes.append(
            ClassInfo(
                name=node.name,
                parents=parents,
                arg_types=arg_types,
                is_var_len_args=is_var_len_args,
                sql_names=sql_names,
            )
        )

    return classes


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


ENHANCED_CLASSES: dict[str, list[str]] = {
    "Identifier": [
        "get name(): string { return this.text('this'); }",
        "get quoted(): boolean { return !!this.args['quoted']; }",
    ],
    "Literal": [
        "get isString(): boolean { return !!this.args['is_string']; }",
        "get isNumber(): boolean { return !this.isString; }",
        "get value(): string | number {",
        "  const val = this.args['this'];",
        "  if (typeof val === 'string') { return this.isNumber ? parseFloat(val) : val; }",
        "  if (typeof val === 'number') { return val; }",
        "  return '';",
        "}",
        "static string(val: string): Literal { return new Literal({ this: val, is_string: true }); }",
        "static number(val: number | string): Literal { return new Literal({ this: `${val}`, is_string: false }); }",
    ],
    "Var": [
        "get name(): string { return this.text('this'); }",
    ],
    "Boolean": [
        "get value(): boolean { return !!this.args['this']; }",
        "static true_(): Boolean { return new Boolean({ this: true }); }",
        "static false_(): Boolean { return new Boolean({ this: false }); }",
    ],
    "Column": [
        "get name(): string { return this.text('this'); }",
        "get table(): string { return this.text('table'); }",
        "get db(): string { return this.text('db'); }",
        "get catalog(): string { return this.text('catalog'); }",
        "override get isStar(): boolean { const thisVal = this.args.this; return thisVal instanceof Expression && thisVal.key === 'star'; }",
    ],
    "Star": [
        "override get isStar(): boolean { return true; }",
    ],
    "Table": [
        "get name(): string { return this.text('this'); }",
        "get db(): string { return this.text('db'); }",
        "get catalog(): string { return this.text('catalog'); }",
    ],
    "Query": [
        "subquery(alias?: string): Subquery {",
        "  const sub = new Subquery({ this: this });",
        "  if (alias) { sub.set('alias', new TableAlias({ this: new Identifier({ this: alias }) })); }",
        "  return sub;",
        "}",
        "limit(expression: string | Expression, options?: { copy?: boolean; dialect?: string }): this {",
        "  return _applyBuilder(expression, this, 'limit', { copy: options?.copy ?? true, into: Limit, prefix: 'LIMIT', dialect: options?.dialect }) as this;",
        "}",
        "offset(expression: string | Expression, options?: { copy?: boolean; dialect?: string }): this {",
        "  return _applyBuilder(expression, this, 'offset', { copy: options?.copy ?? true, into: Offset, prefix: 'OFFSET', dialect: options?.dialect }) as this;",
        "}",
        "orderBy(...expressions: (string | Expression)[]): this {",
        "  return _applyChildListBuilder(expressions, this, 'order', { copy: true, into: Order, prefix: 'ORDER BY' }) as this;",
        "}",
        "where(...expressions: (string | Expression)[]): this {",
        "  return _applyConjunctionBuilder(expressions, this, 'where', { copy: true, into: Where, append: true }) as this;",
        "}",
        "with_(alias: string | Expression, as_: string | Expression, options?: { recursive?: boolean; materialized?: boolean; append?: boolean; copy?: boolean; dialect?: string; scalar?: boolean }): this {",
        "  const aliasExpr = maybeParse(alias, { dialect: options?.dialect, into: TableAlias });",
        "  let asExpr = maybeParse(as_, { dialect: options?.dialect, copy: options?.copy });",
        "  if (options?.scalar && !(asExpr instanceof Subquery)) { asExpr = new Subquery({ this: asExpr }); }",
        "  const cte = new CTE({ this: asExpr, alias: aliasExpr, materialized: options?.materialized, scalar: options?.scalar });",
        "  return _applyChildListBuilder([cte], this, 'with_', { append: options?.append ?? true, copy: options?.copy ?? true, into: With, properties: options?.recursive ? { recursive: options.recursive } : {} }) as this;",
        "}",
        "union(...expressions: (string | Expression)[]): Union {",
        "  const parsed = expressions.map(e => maybeParse(e));",
        "  let result: Expression = this as Expression;",
        "  for (const expr of parsed) { result = new Union({ this: result, expression: expr, distinct: true }); }",
        "  return result as Union;",
        "}",
        "intersect(...expressions: (string | Expression)[]): Intersect {",
        "  const parsed = expressions.map(e => maybeParse(e));",
        "  let result: Expression = this as Expression;",
        "  for (const expr of parsed) { result = new Intersect({ this: result, expression: expr, distinct: true }); }",
        "  return result as Intersect;",
        "}",
        "except_(...expressions: (string | Expression)[]): Except {",
        "  const parsed = expressions.map(e => maybeParse(e));",
        "  let result: Expression = this as Expression;",
        "  for (const expr of parsed) { result = new Except({ this: result, expression: expr, distinct: true }); }",
        "  return result as Except;",
        "}",
    ],
    "Select": [
        "get selects(): Expression[] {",
        "  return this.expressions;",
        "}",
        "get namedSelects(): string[] {",
        "  return this.expressions.map(expr => expr.outputName);",
        "}",
        "get from_(): Expression | undefined {",
        "  const from = this.args['from_'];",
        "  return from instanceof Expression ? from : undefined;",
        "}",
        "get where_(): Expression | undefined {",
        "  const where = this.args['where'];",
        "  return where instanceof Expression ? where : undefined;",
        "}",
        "from(expression: string | Expression, copy = true): Select {",
        "  return _applyBuilder(expression, this, 'from_', { copy, into: From, prefix: 'FROM' }) as Select;",
        "}",
        "select(...expressions: (string | Expression)[]): Select {",
        "  return _applyListBuilder(expressions, this, 'expressions', { copy: true, append: true }) as Select;",
        "}",
        "join(expression: string | Expression, options?: { on?: string | Expression; using?: (string | Expression)[]; joinType?: string; copy?: boolean }): Select {",
        "  const copy = options?.copy ?? true;",
        "  let join: Expression;",
        "  try { join = maybeParse(expression, { into: Join, prefix: 'JOIN' }); } catch { join = maybeParse(expression); }",
        "  if (!(join instanceof Join)) { join = new Join({ this: join }); }",
        "  if (options?.joinType) {",
        "    const parts = options.joinType.toUpperCase().split(/\\s+/);",
        "    for (const p of parts) {",
        "      if (p === 'LEFT' || p === 'RIGHT' || p === 'FULL') join.set('side', p);",
        "      else if (p === 'INNER' || p === 'OUTER' || p === 'CROSS' || p === 'SEMI' || p === 'ANTI') join.set('kind', p);",
        "      else if (p === 'NATURAL') join.set('method', p);",
        "    }",
        "  }",
        "  if (options?.on) {",
        "    join.set('on', maybeParse(options.on));",
        "  }",
        "  if (options?.using) {",
        "    join.set('using', options.using.map(u => maybeParse(u)));",
        "  }",
        "  return _applyListBuilder([join], this, 'joins', { copy, append: true }) as Select;",
        "}",
        "groupBy(...expressions: (string | Expression)[]): Select {",
        "  return _applyChildListBuilder(expressions, this, 'group', { copy: true, into: Group, prefix: 'GROUP BY' }) as Select;",
        "}",
        "having(...expressions: (string | Expression)[]): Select {",
        "  return _applyConjunctionBuilder(expressions, this, 'having', { copy: true, into: Having, append: true }) as Select;",
        "}",
        "distinct(value = true): Select {",
        "  const inst = this.copy() as Select;",
        "  inst.set('distinct', value ? new Distinct({}) : undefined);",
        "  return inst;",
        "}",
        "qualify(...expressions: (string | Expression)[]): Select {",
        "  return _applyConjunctionBuilder(expressions, this, 'qualify', { copy: true, into: Qualify, append: true }) as Select;",
        "}",
        "sortBy(...expressions: (string | Expression)[]): Select {",
        "  return _applyChildListBuilder(expressions, this, 'sort', { copy: true, into: Sort, prefix: 'SORT BY' }) as Select;",
        "}",
        "clusterBy(...expressions: (string | Expression)[]): Select {",
        "  return _applyChildListBuilder(expressions, this, 'cluster', { copy: true, into: Cluster, prefix: 'CLUSTER BY' }) as Select;",
        "}",
        "lateral(...expressions: (string | Expression)[]): Select {",
        "  return _applyListBuilder(expressions, this, 'laterals', { copy: true, append: true, into: Lateral, prefix: 'LATERAL VIEW' }) as Select;",
        "}",
        "window_(...expressions: (string | Expression)[]): Select {",
        "  return _applyListBuilder(expressions, this, 'windows', { copy: true, append: true, into: Window, prefix: 'WINDOW' }) as Select;",
        "}",
        "ctas(table: string | Expression, options?: { dialect?: string; copy?: boolean }): Create {",
        "  const inst = options?.copy !== false ? this.copy() : this;",
        "  const tableExpr = maybeParse(table, { into: Table, dialect: options?.dialect });",
        "  return new Create({ this: tableExpr, kind: 'TABLE', expression: inst });",
        "}",
        "lock(update = true, copy = true): Select {",
        "  const inst = copy ? this.copy() as Select : this;",
        "  inst.set('locks', [new Lock({ update })]);",
        "  return inst;",
        "}",
        "hint(...hints: (string | Expression)[]): Select {",
        "  const inst = this.copy() as Select;",
        "  const parsed = hints.map(h => maybeParse(h));",
        "  inst.set('hint', new Hint({ expressions: parsed }));",
        "  return inst;",
        "}",
    ],
    "Subquery": [
        "limit(expression: string | Expression, copy = true): this {",
        "  return _applyBuilder(expression, this, 'limit', { copy, into: Limit, prefix: 'LIMIT' }) as this;",
        "}",
        "offset(expression: string | Expression, copy = true): this {",
        "  return _applyBuilder(expression, this, 'offset', { copy, into: Offset, prefix: 'OFFSET' }) as this;",
        "}",
        "orderBy(...expressions: (string | Expression)[]): this {",
        "  return _applyChildListBuilder(expressions, this, 'order', { copy: true, into: Order, prefix: 'ORDER BY' }) as this;",
        "}",
        "where(...expressions: (string | Expression)[]): this {",
        "  return _applyConjunctionBuilder(expressions, this, 'where', { copy: true, into: Where, append: true }) as this;",
        "}",
    ],
    "Condition": [
        "and_(...expressions: (string | Expression)[]): Expression {",
        "  const parsed = expressions.map(e => typeof e === 'string' ? Expression.parseImpl(e) : e);",
        "  let result: Expression = this;",
        "  for (const expr of parsed) { result = new And({ this: result, expression: expr }); }",
        "  return result;",
        "}",
        "or_(...expressions: (string | Expression)[]): Expression {",
        "  const parsed = expressions.map(e => typeof e === 'string' ? Expression.parseImpl(e) : e);",
        "  let result: Expression = this;",
        "  for (const expr of parsed) { result = new Or({ this: result, expression: expr }); }",
        "  return result;",
        "}",
        "not_(): Not { return new Not({ this: this }); }",
        "eq(other: unknown): EQ { return this._binop(EQ, other) as EQ; }",
        "neq(other: unknown): NEQ { return this._binop(NEQ, other) as NEQ; }",
        "is_(other: unknown): Is { return this._binop(Is, other) as Is; }",
        "like(other: unknown): Like { return this._binop(Like, other) as Like; }",
        "ilike(other: unknown): ILike { return this._binop(ILike, other) as ILike; }",
        "rlike(other: unknown): RegexpLike { return this._binop(RegexpLike, other) as RegexpLike; }",
        "asc(nullsFirst = true): Ordered { return new Ordered({ this: this, nulls_first: nullsFirst }); }",
        "desc(nullsFirst = false): Ordered { return new Ordered({ this: this, desc: true, nulls_first: nullsFirst }); }",
    ],
    "Binary": [
        "get left(): Expression | undefined {",
        "  const val = this.args['this'];",
        "  return val instanceof Expression ? val : undefined;",
        "}",
        "get right(): Expression | undefined {",
        "  const val = this.args['expression'];",
        "  return val instanceof Expression ? val : undefined;",
        "}",
    ],
    "Ordered": [
        "get desc(): boolean { return !!this.args['desc']; }",
        "get nullsFirst(): boolean | undefined {",
        "  const val = this.args['nulls_first'];",
        "  return typeof val === 'boolean' ? val : undefined;",
        "}",
    ],
    "Func": [
        "static readonly sqlNames: readonly string[] | undefined = undefined;",
        "get name(): string {",
        "  const ctor = this.constructor as typeof Func;",
        "  const first = ctor.sqlNames?.[0];",
        "  if (first) return first;",
        "  return camelToSnakeCase(ctor.className);",
        "}",
    ],
    "Anonymous": [
        "override get name(): string { return this.text('this'); }",
    ],
}


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


def generate_typescript(classes: list[ClassInfo]) -> str:
    lines: list[str] = []

    lines.append("/**")
    lines.append(" * AUTO-GENERATED - DO NOT EDIT")
    lines.append(" * Generated from sqlglot/sqlglot/expressions.py")
    lines.append(" * Run: npm run generate")
    lines.append(" */")
    lines.append("")
    lines.append("import { Expression, maybeParse, _applyBuilder, _applyListBuilder, _applyChildListBuilder, _applyConjunctionBuilder } from './expression-base.js';")
    lines.append("")
    lines.append("function camelToSnakeCase(s: string): string {")
    lines.append("  return s.replace(/([a-z])([A-Z])/g, '$1_$2').toUpperCase();")
    lines.append("}")
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
            lines.append(f"  static readonly sqlNames = [{sql_names_str}];")

        key = cls.name.lower()
        needs_override = parent != "Expression"
        override_kw = "override " if needs_override else ""
        lines.append(f"  {override_kw}get key(): string {{ return '{key}'; }}")
        lines.append(f"  static readonly className: string = '{cls.name}';")

        if cls.name in ENHANCED_CLASSES:
            for method_line in ENHANCED_CLASSES[cls.name]:
                lines.append(f"  {method_line}")

        if has_func_parent and cls.name not in ENHANCED_CLASSES:
            lines.append(f"  override get name(): string {{")
            lines.append(f"    const ctor = this.constructor as typeof {cls.name};")
            lines.append(f"    const first = (ctor as any).sqlNames?.[0];")
            lines.append(f"    if (first) return first;")
            lines.append(f"    return camelToSnakeCase(ctor.className);")
            lines.append(f"  }}")

        lines.append("}")
        lines.append("")

    lines.append("export const GENERATED_CLASSES = [")
    for name in generated_names:
        lines.append(f"  {name},")
    lines.append("] as const;")
    lines.append("")

    multi_map = compute_multi_inheritance_map(classes)
    lines.append("export const MULTI_INHERITANCE_MAP: Record<string, readonly string[]> = {")
    for parent, children in multi_map.items():
        children_str = ", ".join(f"'{c}'" for c in children)
        lines.append(f"  '{parent}': [{children_str}],")
    lines.append("};")
    lines.append("")

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

    print("Generating TypeScript...")
    typescript = generate_typescript(sorted_classes)

    print(f"Writing {output}")
    output.write_text(typescript)

    print(f"Done! Generated {len(sorted_classes)} classes.")


if __name__ == "__main__":
    main()
