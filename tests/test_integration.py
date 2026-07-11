from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from tollkeeper import Tollkeeper, AuditFailedError
from tollkeeper.backends.csv import CsvBackend
from tollkeeper.checks.polars import NullCheck, RowCountCheck


class TestCsvPolarsIntegration:
    def test_end_to_end_publish(self, tmp_path: Path) -> None:
        staging = tmp_path / "staging"
        publish = tmp_path / "publish"
        upstream = tmp_path / "upstream.csv"
        upstream.write_text("id,name,age\n1,alice,30\n2,bob,25\n")

        backend = CsvBackend(staging_dir=staging, publish_dir=publish)
        session = (
            Tollkeeper(backend)
            .table("customers")
            .write(lambda ref: shutil.copy(upstream, ref))
            .audit([NullCheck("id"), RowCountCheck(min_rows=1)])
            .publish()
        )

        assert session.report.passed
        final = publish / "customers.csv"
        assert final.exists()
        assert final.read_text() == upstream.read_text()
        assert not list(staging.iterdir())

    def test_audit_failure_no_publish(self, tmp_path: Path) -> None:
        staging = tmp_path / "staging"
        publish = tmp_path / "publish"
        upstream = tmp_path / "upstream.csv"
        upstream.write_text("id,name\n1,alice\n,bob\n")

        backend = CsvBackend(staging_dir=staging, publish_dir=publish)
        with pytest.raises(AuditFailedError, match="NullCheck"):
            Tollkeeper(backend).table("customers").write(lambda ref: shutil.copy(upstream, ref)).audit(
                [NullCheck("id")], on_failure="stop"
            )

        assert not (publish / "customers.csv").exists()

    def test_soft_failure_still_publishes(self, tmp_path: Path) -> None:
        staging = tmp_path / "staging"
        publish = tmp_path / "publish"
        upstream = tmp_path / "upstream.csv"
        upstream.write_text("id\n1\n")

        backend = CsvBackend(staging_dir=staging, publish_dir=publish)
        session = (
            Tollkeeper(backend)
            .table("small")
            .write(lambda ref: shutil.copy(upstream, ref))
            .audit([RowCountCheck(min_rows=100)], on_failure="continue")
            .publish()
        )

        assert not session.report.passed
        assert (publish / "small.csv").exists()
