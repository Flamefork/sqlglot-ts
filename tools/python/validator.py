#!/usr/bin/env python3
"""
Validator base class for dialect tests.
Mirrors the original sqlglot Validator but calls through to TS bridge.
Tests that use unsupported features will fail (not skip).
"""

import unittest
from typing import Any

from fake_sqlglot import (
    ExpressionProxy,
    ParseError,
    UnsupportedError,
    ErrorLevel,
    parse_one,
)


class Validator(unittest.TestCase):
    dialect: str | None = None
    maxDiff = None

    def parse_one(self, sql: str, **kwargs: Any) -> ExpressionProxy:
        return parse_one(sql, read=self.dialect, **kwargs)

    def validate_identity(
        self,
        sql: str,
        write_sql: str | None = None,
        pretty: bool = False,
        check_command_warning: bool = False,
        identify: bool = False,
        **kwargs: Any,
    ) -> ExpressionProxy:
        expr = self.parse_one(sql)
        expected = write_sql if write_sql is not None else sql
        actual = expr.sql(dialect=self.dialect, pretty=pretty, identify=identify)
        self.assertEqual(expected, actual)
        return expr

    def validate_all(
        self,
        sql: str,
        read: dict[str, str] | None = None,
        write: dict[str, str] | None = None,
        pretty: bool = False,
        identify: bool = False,
        **kwargs: Any,
    ):
        expr = self.parse_one(sql)

        for write_dialect, expected_sql in (write or {}).items():
            with self.subTest(f"{sql} -> {write_dialect}"):
                if expected_sql is UnsupportedError:
                    with self.assertRaises(UnsupportedError):
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
                    self.assertEqual(expected_sql, actual)

        for read_dialect, read_sql in (read or {}).items():
            with self.subTest(f"{read_dialect} -> {sql}"):
                read_expr = parse_one(read_sql, read=read_dialect)
                actual = read_expr.sql(
                    dialect=self.dialect,
                    unsupported_level=ErrorLevel.IGNORE,
                    pretty=pretty,
                    identify=identify,
                )
                self.assertEqual(sql, actual)
