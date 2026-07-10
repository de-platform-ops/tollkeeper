from __future__ import annotations

from typing import Any

import polars as pl

from write_audit_publish.checks.base import BaseCheck, CheckResult


class NullCheck(BaseCheck):
    """Fails if the specified column contains any null values.

    Args:
        column: Column name to check for nulls.
    """

    def __init__(self, column: str) -> None:
        self._column = column

    def run(self, version_ref: str, *, conn: Any | None = None) -> CheckResult:
        df = pl.scan_csv(version_ref).collect()
        null_count = df[self._column].null_count()
        return CheckResult(
            check_name=self.name,
            passed=null_count == 0,
            details=f"{null_count} nulls in '{self._column}'",
        )


class RowCountCheck(BaseCheck):
    """Fails if the row count is below the minimum threshold.

    Args:
        min_rows: Minimum number of rows required to pass.
    """

    def __init__(self, min_rows: int) -> None:
        self._min_rows = min_rows

    def run(self, version_ref: str, *, conn: Any | None = None) -> CheckResult:
        df = pl.scan_csv(version_ref).collect()
        count = len(df)
        return CheckResult(
            check_name=self.name,
            passed=count >= self._min_rows,
            details=f"{count} rows, minimum {self._min_rows}",
        )


class ExpressionCheck(BaseCheck):
    """Fails if any row does not satisfy a Polars expression.

    Args:
        name: Check name (used in reports).
        expr: A Polars expression that evaluates to boolean per row.

    Example::

        ExpressionCheck("positive_age", pl.col("age") > 0)
    """

    def __init__(self, name: str, expr: pl.Expr) -> None:
        self._name = name
        self._expr = expr

    @property
    def name(self) -> str:
        return self._name

    def run(self, version_ref: str, *, conn: Any | None = None) -> CheckResult:
        df = pl.scan_csv(version_ref).collect()
        violations = df.filter(~self._expr)
        return CheckResult(
            check_name=self.name,
            passed=len(violations) == 0,
            details=f"{len(violations)} rows violate '{self._name}'",
        )


class SqlCheck(BaseCheck):
    """Fails if any row does not satisfy a SQL WHERE condition.

    Uses Polars ``SQLContext`` to evaluate the condition. The staged data
    is registered as a table named ``data``.

    Args:
        name: Check name (used in reports).
        condition: SQL WHERE clause (e.g. ``"age > 0 AND name IS NOT NULL"``).

    Example::

        SqlCheck("valid_age", "age > 0 AND age < 150")
    """

    def __init__(self, name: str, condition: str) -> None:
        self._name = name
        self._condition = condition

    @property
    def name(self) -> str:
        return self._name

    def run(self, version_ref: str, *, conn: Any | None = None) -> CheckResult:
        df = pl.scan_csv(version_ref).collect()
        ctx = pl.SQLContext({"data": df})
        violations = ctx.execute(f"SELECT * FROM data WHERE NOT ({self._condition})").collect()
        return CheckResult(
            check_name=self.name,
            passed=len(violations) == 0,
            details=f"{len(violations)} rows violate '{self._name}'",
        )


class UniqueCheck(BaseCheck):
    """Fails if any combination of the given columns has duplicate rows.

    Args:
        columns: List of column names that should form a unique key.

    Example::

        UniqueCheck(["region", "date"])
    """

    def __init__(self, columns: list[str]) -> None:
        self._columns = columns

    def run(self, version_ref: str, *, conn: Any | None = None) -> CheckResult:
        df = pl.scan_csv(version_ref).collect()
        duplicates = df.group_by(self._columns).len().filter(pl.col("len") > 1)
        return CheckResult(
            check_name=self.name,
            passed=len(duplicates) == 0,
            details=f"{len(duplicates)} duplicate groups on {self._columns}",
        )
