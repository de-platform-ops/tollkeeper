from __future__ import annotations

import polars as pl

from write_audit_publish.checks.base import BaseCheck, CheckResult


class NullCheck(BaseCheck):
    def __init__(self, column: str) -> None:
        self._column = column

    def run(self, version_ref: str) -> CheckResult:
        df = pl.read_csv(version_ref)
        null_count = df[self._column].null_count()
        return CheckResult(
            check_name=self.name,
            passed=null_count == 0,
            details=f"{null_count} nulls in '{self._column}'",
        )


class RowCountCheck(BaseCheck):
    def __init__(self, min_rows: int) -> None:
        self._min_rows = min_rows

    def run(self, version_ref: str) -> CheckResult:
        df = pl.read_csv(version_ref)
        count = len(df)
        return CheckResult(
            check_name=self.name,
            passed=count >= self._min_rows,
            details=f"{count} rows, minimum {self._min_rows}",
        )


class ExpressionCheck(BaseCheck):
    def __init__(self, name: str, expr: pl.Expr) -> None:
        self._name = name
        self._expr = expr

    @property
    def name(self) -> str:
        return self._name

    def run(self, version_ref: str) -> CheckResult:
        df = pl.read_csv(version_ref)
        violations = df.filter(~self._expr)
        return CheckResult(
            check_name=self.name,
            passed=len(violations) == 0,
            details=f"{len(violations)} rows violate '{self._name}'",
        )


class SqlCheck(BaseCheck):
    def __init__(self, name: str, condition: str) -> None:
        self._name = name
        self._condition = condition

    @property
    def name(self) -> str:
        return self._name

    def run(self, version_ref: str) -> CheckResult:
        df = pl.read_csv(version_ref)
        ctx = pl.SQLContext({"data": df})
        violations = ctx.execute(f"SELECT * FROM data WHERE NOT ({self._condition})").collect()
        return CheckResult(
            check_name=self.name,
            passed=len(violations) == 0,
            details=f"{len(violations)} rows violate '{self._name}'",
        )


class UniqueCheck(BaseCheck):
    def __init__(self, columns: list[str]) -> None:
        self._columns = columns

    def run(self, version_ref: str) -> CheckResult:
        df = pl.read_csv(version_ref)
        duplicates = df.group_by(self._columns).len().filter(pl.col("len") > 1)
        return CheckResult(
            check_name=self.name,
            passed=len(duplicates) == 0,
            details=f"{len(duplicates)} duplicate groups on {self._columns}",
        )
