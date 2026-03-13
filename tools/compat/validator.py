import unittest
from typing import Any

import pytest

from compat import ErrorLevel
from compat import ExpressionProxy
from compat import UnsupportedError
from compat import parse_one


class Validator(unittest.TestCase):
    dialect: str | None = None
    maxDiff = None

    def parse_one(self, sql: str, **_kwargs: Any) -> ExpressionProxy:
        return parse_one(sql, read=self.dialect)

    def validate_identity(
        self,
        sql: str,
        write_sql: str | None = None,
        *,
        pretty: bool = False,
        check_command_warning: bool = False,
        identify: bool | str = False,
        **_kwargs: Any,
    ) -> ExpressionProxy:
        expr = self.parse_one(sql)
        if check_command_warning:
            assert expr.key.lower() == "command", (  # noqa: S101
                f"Expected Command expression for '{sql}', got {expr.key}"
            )
        expected = write_sql if write_sql is not None else sql
        actual = expr.sql(dialect=self.dialect, pretty=pretty, identify=identify)
        assert expected == actual, f"\n  expected: {expected!r}\n  actual:   {actual!r}"  # noqa: S101
        return expr

    def validate_all(
        self,
        sql: str,
        read: dict[str, str] | None = None,
        write: dict[str, str] | None = None,
        *,
        pretty: bool = False,
        identify: bool | str = False,
        **_kwargs: Any,
    ):
        expr = self.parse_one(sql)

        for write_dialect, expected_sql in (write or {}).items():
            with self.subTest(f"{sql} -> {write_dialect}"):
                if expected_sql is UnsupportedError:
                    with pytest.raises(UnsupportedError):
                        expr.sql(
                            dialect=write_dialect,
                            unsupported_level=ErrorLevel.RAISE,
                            pretty=pretty,
                            identify=identify,
                        )
                else:
                    actual = expr.sql(
                        dialect=write_dialect,
                        unsupported_level=ErrorLevel.IGNORE,
                        pretty=pretty,
                        identify=identify,
                    )
                    assert expected_sql == actual  # noqa: S101

        for read_dialect, read_sql in (read or {}).items():
            with self.subTest(f"{read_dialect} -> {sql}"):
                read_expr = parse_one(read_sql, read=read_dialect)
                actual = read_expr.sql(
                    dialect=self.dialect,
                    unsupported_level=ErrorLevel.IGNORE,
                    pretty=pretty,
                    identify=identify,
                )
                assert sql == actual  # noqa: S101
