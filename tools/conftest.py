# pyright: reportAttributeAccessIssue=false
import pytest

from compat import ExpressionProxy
from compat import TSBridge
from compat import register_fake_sqlglot

MAX_RETAINED_EXPRESSIONS = 1000


def pytest_configure(config: pytest.Config) -> None:  # noqa: ARG001
    register_fake_sqlglot()
    TSBridge.get()


def pytest_unconfigure(config: pytest.Config) -> None:  # noqa: ARG001
    TSBridge.reset()


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_teardown(item: pytest.Item, nextitem: pytest.Item | None) -> None:  # noqa: ARG001
    if ExpressionProxy.retained_count() > MAX_RETAINED_EXPRESSIONS:
        ExpressionProxy.release_all()
