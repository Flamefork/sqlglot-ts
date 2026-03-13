import inspect
import json
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

PROJECT_DIR = Path(__file__).resolve().parents[1].parent
CHECKS_DIR = PROJECT_DIR / "tools" / "checks"

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


def get_python_surface() -> dict[str, list[str]]:
    sys.path.insert(0, str(PROJECT_DIR / "sqlglot"))
    import sqlglot.expressions as exp  # noqa: PLC0415  # pyright: ignore[reportMissingImports]

    result: dict[str, list[str]] = {}

    funcs = [
        name
        for name, _ in inspect.getmembers(exp, inspect.isfunction)
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


def main() -> None:
    surface = get_python_surface()
    output_path = CHECKS_DIR / "api_surface_python.json"
    output_path.write_text(json.dumps(surface, indent=2) + "\n")
    logger.info("Wrote %s", output_path)


if __name__ == "__main__":
    main()
