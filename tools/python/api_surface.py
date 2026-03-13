#!/usr/bin/env python3
import inspect
import json
import subprocess
import sys
from pathlib import Path

TOOLS_DIR = Path(__file__).resolve().parent
PROJECT_DIR = TOOLS_DIR.parent.parent

CLASSES = [
    "Expression",
    "Query",
    "Select",
    "Insert",
    "Update",
    "Delete",
    "Merge",
    "Join",
    "Union",
    "Case",
    "Condition",
]

RESERVED = {"array", "delete", "false", "true", "null", "case", "var"}


def to_camel(name: str) -> str:
    trail = "_" if name.endswith("_") and not name.startswith("_") and not name[:-1].endswith("_") else ""
    base = name.rstrip("_")
    parts = base.split("_")
    result = parts[0] + "".join(p.capitalize() for p in parts[1:])
    return result + trail


def get_python_surface() -> dict[str, list[str]]:
    sys.path.insert(0, str(PROJECT_DIR / "sqlglot"))
    import sqlglot.expressions as exp

    result: dict[str, list[str]] = {}

    funcs = [
        name
        for name, obj in inspect.getmembers(exp, inspect.isfunction)
        if not name.startswith("_")
    ]
    result["top_level"] = sorted(funcs)

    for cls_name in CLASSES:
        cls = getattr(exp, cls_name)
        own = []
        for name in dir(cls):
            if name.startswith("_"):
                continue
            for klass in cls.__mro__:
                if name in klass.__dict__:
                    if klass == cls:
                        own.append(name)
                    break
        result[cls_name] = sorted(own)

    return result


def get_ts_surface() -> dict[str, set[str]]:
    js_code = """
import * as mod from './dist/index.mjs';
import * as expMod from './dist/expressions.mjs';

const result = {};

// Top-level functions
const topLevel = [];
for (const [name, val] of Object.entries(mod)) {
    if (typeof val === 'function' && name[0] === name[0].toLowerCase() && name[0] !== '_') {
        topLevel.push(name);
    }
}
result.top_level = topLevel.sort();

// Class methods
const classNames = %s;
for (const cls_name of classNames) {
    const cls = expMod[cls_name] || mod[cls_name];
    if (!cls) { result[cls_name] = []; continue; }
    const own = new Set();
    const proto = cls.prototype;
    if (!proto) { result[cls_name] = []; continue; }
    for (const name of Object.getOwnPropertyNames(proto)) {
        if (name.startsWith('_') || name === 'constructor') continue;
        own.add(name);
    }
    // Static properties on class itself
    for (const name of Object.getOwnPropertyNames(cls)) {
        if (name.startsWith('_') || ['length', 'name', 'prototype'].includes(name)) continue;
        own.add(name);
    }
    // Instance properties (set in constructor, not on prototype)
    try {
        const instance = new cls({});
        for (const name of Object.getOwnPropertyNames(instance)) {
            if (name.startsWith('_') || name === 'constructor') continue;
            own.add(name);
        }
    } catch (e) {}
    result[cls_name] = [...own].sort();
}

console.log(JSON.stringify(result));
""" % json.dumps(CLASSES)

    proc = subprocess.run(
        ["node", "--input-type=module", "-e", js_code],
        capture_output=True,
        text=True,
        cwd=PROJECT_DIR,
    )
    if proc.returncode != 0:
        print(f"ERROR: Node.js introspection failed:\n{proc.stderr}", file=sys.stderr)
        sys.exit(1)

    data = json.loads(proc.stdout)
    return {k: set(v) for k, v in data.items()}


def load_excludes() -> dict[str, dict[str, str]]:
    path = TOOLS_DIR / "api_surface_excludes.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def check_match(py_name: str, ts_names: set[str]) -> bool:
    if py_name in ts_names:
        return True
    camel = to_camel(py_name)
    if camel in ts_names:
        return True
    if py_name in RESERVED and (py_name + "_") in ts_names:
        return True
    return False


def main() -> None:
    py_surface = get_python_surface()
    ts_surface = get_ts_surface()
    excludes = load_excludes()

    total_covered = 0
    total_missing = 0
    total_excluded = 0

    print("API Surface Parity Report")
    print("=========================")
    print()

    all_missing: list[str] = []

    for section in ["top_level"] + CLASSES:
        py_names = py_surface.get(section, [])
        ts_names = ts_surface.get(section, set())
        section_excludes = excludes.get(section, {})

        covered = []
        missing = []
        excluded = []

        for name in py_names:
            if name in section_excludes:
                excluded.append(name)
            elif check_match(name, ts_names):
                covered.append(name)
            else:
                missing.append(name)

        # Cross-class check: if a method is "missing", look for it on other TS classes
        misplaced = []
        truly_missing = []
        if missing and section != "top_level":
            for name in missing:
                found_on = []
                for other_section, other_ts in ts_surface.items():
                    if other_section == section or other_section == "top_level":
                        continue
                    if check_match(name, other_ts):
                        found_on.append(other_section)
                if found_on:
                    misplaced.append((name, found_on))
                else:
                    truly_missing.append(name)
        else:
            truly_missing = missing

        label = "Top-level functions" if section == "top_level" else f"{section} methods"
        print(f"{label}:")
        print(f"  ✓ {len(covered)} covered")
        if missing:
            names_str = ", ".join(missing)
            print(f"  ✗ {len(missing)} missing: {names_str}")
            if misplaced:
                for name, found_on in misplaced:
                    print(f"    ↳ {name}: MISPLACED (found on {', '.join(found_on)})")
        else:
            print(f"  ✗ 0 missing")
        if excluded:
            print(f"  ○ {len(excluded)} excluded")
        print()

        total_covered += len(covered)
        total_missing += len(missing)
        total_excluded += len(excluded)
        all_missing.extend(f"{section}.{n}" for n in missing)

    print(f"Total: {total_covered} covered, {total_missing} missing, {total_excluded} excluded")

    if total_missing > 0:
        print(f"\nAPI surface mismatch: {total_missing} missing item(s)")
        if all_missing:
            print("Missing items:")
            for item in all_missing:
                print(f"  - {item}")
        sys.exit(1)


if __name__ == "__main__":
    main()
