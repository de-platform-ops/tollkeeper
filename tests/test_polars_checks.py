from __future__ import annotations

from pathlib import Path

import polars as pl

from tollkeeper.checks.polars import ExpressionCheck, NullCheck, RowCountCheck, SqlCheck, UniqueCheck


class TestNullCheck:
    def test_passes_no_nulls(self, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("id,name\n1,alice\n2,bob\n")
        result = NullCheck("id").run(str(csv))
        assert result.passed

    def test_fails_with_nulls(self, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("id,name\n1,alice\n,bob\n")
        result = NullCheck("id").run(str(csv))
        assert not result.passed
        assert "1" in result.details

    def test_check_name(self) -> None:
        assert NullCheck("id").name == "NullCheck"


class TestRowCountCheck:
    def test_passes_enough_rows(self, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("id\n1\n2\n3\n")
        result = RowCountCheck(min_rows=2).run(str(csv))
        assert result.passed

    def test_fails_too_few_rows(self, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("id\n1\n")
        result = RowCountCheck(min_rows=5).run(str(csv))
        assert not result.passed
        assert "1" in result.details
        assert "5" in result.details

    def test_exact_threshold_passes(self, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("id\n1\n2\n")
        result = RowCountCheck(min_rows=2).run(str(csv))
        assert result.passed

    def test_check_name(self) -> None:
        assert RowCountCheck(min_rows=1).name == "RowCountCheck"


class TestExpressionCheck:
    def test_passes_all_satisfy(self, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("age,name\n25,alice\n30,bob\n")
        result = ExpressionCheck("positive_age", pl.col("age") > 0).run(str(csv))
        assert result.passed

    def test_fails_some_violate(self, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("age,name\n25,alice\n-1,bob\n")
        result = ExpressionCheck("positive_age", pl.col("age") > 0).run(str(csv))
        assert not result.passed
        assert "1" in result.details

    def test_compound_expression(self, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("age,score\n25,80\n30,90\n")
        expr = (pl.col("age") > 0) & (pl.col("score") >= 0)
        result = ExpressionCheck("valid_row", expr).run(str(csv))
        assert result.passed

    def test_custom_name(self) -> None:
        check = ExpressionCheck("my_check", pl.col("x") > 0)
        assert check.name == "my_check"


class TestSqlCheck:
    def test_passes_all_satisfy(self, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("age,name\n25,alice\n30,bob\n")
        result = SqlCheck("positive_age", "age > 0").run(str(csv))
        assert result.passed

    def test_fails_some_violate(self, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("age,name\n25,alice\n-1,bob\n")
        result = SqlCheck("positive_age", "age > 0").run(str(csv))
        assert not result.passed
        assert "1" in result.details

    def test_compound_condition(self, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("age,score\n25,80\n30,90\n")
        result = SqlCheck("valid_row", "age > 0 AND score >= 0").run(str(csv))
        assert result.passed

    def test_null_handling(self, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("id,name\n1,alice\n,bob\n")
        result = SqlCheck("id_not_null", "id IS NOT NULL").run(str(csv))
        assert not result.passed

    def test_custom_name(self) -> None:
        check = SqlCheck("my_sql_check", "x > 0")
        assert check.name == "my_sql_check"


class TestUniqueCheck:
    def test_passes_all_unique(self, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("id,name\n1,alice\n2,bob\n")
        result = UniqueCheck(["id"]).run(str(csv))
        assert result.passed

    def test_fails_with_duplicates(self, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("id,name\n1,alice\n1,bob\n")
        result = UniqueCheck(["id"]).run(str(csv))
        assert not result.passed
        assert "1" in result.details

    def test_composite_key(self, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("region,date,value\nUS,2024-01,10\nUS,2024-02,20\nEU,2024-01,30\n")
        result = UniqueCheck(["region", "date"]).run(str(csv))
        assert result.passed

    def test_composite_key_duplicates(self, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("region,date,value\nUS,2024-01,10\nUS,2024-01,20\n")
        result = UniqueCheck(["region", "date"]).run(str(csv))
        assert not result.passed
