#!/usr/bin/env python3
"""
pytest configuration for sqlglot-ts tests.
Registers fake sqlglot module and manages TS bridge lifecycle.
"""

import subprocess
import sys
from pathlib import Path

# Add this file's directory to path so we can import fake_sqlglot
CONFTEST_DIR = Path(__file__).parent
sys.path.insert(0, str(CONFTEST_DIR))

import pytest

# Determine project root based on where conftest is located
# It might be in tools/ or in sqlglot/tests/dialects/
if CONFTEST_DIR.name == "tools":
    PROJECT_ROOT = CONFTEST_DIR.parent
else:
    # Running from sqlglot/tests/dialects/ -> go up to sqlglot-ts
    # dialects -> tests -> sqlglot -> sqlglot-ts (3 levels)
    PROJECT_ROOT = CONFTEST_DIR.parent.parent.parent
    # Add tools to path for ts_bridge and other imports
    sys.path.insert(0, str(PROJECT_ROOT / "tools"))


def pytest_configure(config):
    import types

    print("\n=== SQLGLOT-TS CONFTEST LOADED ===\n")

    print("Building TypeScript...")
    build = subprocess.run(
        ["npm", "run", "build"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    if build.returncode != 0:
        pytest.exit(f"TypeScript build failed:\n{build.stderr}", returncode=1)

    # Register fake sqlglot BEFORE test collection
    from fake_sqlglot import register_fake_sqlglot, TSBridge
    print("Registering fake sqlglot module...")
    register_fake_sqlglot()

    # Register tests.dialects.test_dialect module with Validator
    from validator import Validator

    tests_mod = types.ModuleType("tests")
    tests_mod.__path__ = []  # type: ignore - make it a package
    sys.modules["tests"] = tests_mod

    tests_dialects = types.ModuleType("tests.dialects")
    tests_dialects.__path__ = []  # type: ignore - make it a package
    sys.modules["tests.dialects"] = tests_dialects

    tests_test_dialect = types.ModuleType("tests.dialects.test_dialect")
    tests_test_dialect.Validator = Validator  # type: ignore
    sys.modules["tests.dialects.test_dialect"] = tests_test_dialect

    print("Starting TS bridge...")
    TSBridge.get()


def pytest_unconfigure(config):
    from fake_sqlglot import TSBridge
    TSBridge.reset()


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_teardown(item, nextitem):
    from fake_sqlglot import ExpressionProxy
    # Release expressions every 1000 to prevent memory growth
    if len(ExpressionProxy._all_ids) > 1000:
        ExpressionProxy.release_all()
