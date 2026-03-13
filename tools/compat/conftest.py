# pyright: reportAttributeAccessIssue=false
import subprocess
import sys
import types
from pathlib import Path

import pytest

from compat import ExpressionProxy
from compat import TSBridge
from compat import register_fake_sqlglot
from compat.validator import Validator

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MAX_RETAINED_EXPRESSIONS = 1000


def pytest_configure(config: pytest.Config) -> None:  # noqa: ARG001
    build = subprocess.run(
        ["just", "build"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if build.returncode != 0:
        pytest.exit(f"TypeScript build failed:\n{build.stderr}", returncode=1)

    register_fake_sqlglot()

    tests_mod = types.ModuleType("tests")
    tests_mod.__path__ = []
    sys.modules["tests"] = tests_mod

    tests_dialects = types.ModuleType("tests.dialects")
    tests_dialects.__path__ = []
    sys.modules["tests.dialects"] = tests_dialects

    tests_test_dialect = types.ModuleType("tests.dialects.test_dialect")
    tests_test_dialect.Validator = Validator
    sys.modules["tests.dialects.test_dialect"] = tests_test_dialect

    TSBridge.get()


def pytest_unconfigure(config: pytest.Config) -> None:  # noqa: ARG001
    TSBridge.reset()


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_teardown(item: pytest.Item, nextitem: pytest.Item | None) -> None:  # noqa: ARG001
    if ExpressionProxy.retained_count() > MAX_RETAINED_EXPRESSIONS:
        ExpressionProxy.release_all()
