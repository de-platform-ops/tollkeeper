from __future__ import annotations

import os
from pathlib import Path

from tollkeeper.backends.csv import CsvBackend


class TestCsvBackend:
    def test_create_version_returns_staging_path(self, tmp_path: Path) -> None:
        backend = CsvBackend(staging_dir=tmp_path / "staging", publish_dir=tmp_path / "publish")
        ref = backend.create_version("sales")
        path = Path(ref)
        assert path.parent == tmp_path / "staging"
        assert path.name.startswith("sales.tollkeeper-")
        assert path.name.endswith(".csv")

    def test_create_version_creates_staging_dir(self, tmp_path: Path) -> None:
        staging = tmp_path / "staging"
        backend = CsvBackend(staging_dir=staging, publish_dir=tmp_path / "publish")
        backend.create_version("t")
        assert staging.exists()

    def test_publish_copies_to_final_and_deletes_staging(self, tmp_path: Path) -> None:
        staging = tmp_path / "staging"
        publish = tmp_path / "publish"
        backend = CsvBackend(staging_dir=staging, publish_dir=publish)
        ref = backend.create_version("sales")
        Path(ref).write_text("id,name\n1,alice\n")

        backend.publish_version("sales", ref)

        final = publish / "sales.csv"
        assert final.read_text() == "id,name\n1,alice\n"
        assert not Path(ref).exists()

    def test_publish_creates_publish_dir(self, tmp_path: Path) -> None:
        publish = tmp_path / "publish"
        backend = CsvBackend(staging_dir=tmp_path / "staging", publish_dir=publish)
        ref = backend.create_version("t")
        Path(ref).write_text("a\n1\n")

        backend.publish_version("t", ref)
        assert publish.exists()

    def test_rollback_deletes_staging(self, tmp_path: Path) -> None:
        backend = CsvBackend(staging_dir=tmp_path / "staging", publish_dir=tmp_path / "publish")
        ref = backend.create_version("sales")
        Path(ref).write_text("id\n1\n")

        backend.rollback_version("sales", ref)
        assert not Path(ref).exists()

    def test_rollback_missing_file_is_noop(self, tmp_path: Path) -> None:
        backend = CsvBackend(staging_dir=tmp_path / "staging", publish_dir=tmp_path / "publish")
        backend.rollback_version("sales", str(tmp_path / "staging" / "nonexistent.csv"))

    def test_unique_refs_per_call(self, tmp_path: Path) -> None:
        backend = CsvBackend(staging_dir=tmp_path / "staging", publish_dir=tmp_path / "publish")
        refs = {backend.create_version("t") for _ in range(10)}
        assert len(refs) == 10


class TestCleanupStaging:
    def test_removes_old_files(self, tmp_path: Path) -> None:
        backend = CsvBackend(staging_dir=tmp_path / "staging", publish_dir=tmp_path / "publish")
        ref = backend.create_version("t")
        Path(ref).write_text("x\n")
        old_time = os.path.getmtime(ref) - 7200
        os.utime(ref, (old_time, old_time))

        removed = backend.cleanup_staging(max_age_seconds=3600)
        assert len(removed) == 1
        assert not Path(ref).exists()

    def test_keeps_recent_files(self, tmp_path: Path) -> None:
        backend = CsvBackend(staging_dir=tmp_path / "staging", publish_dir=tmp_path / "publish")
        ref = backend.create_version("t")
        Path(ref).write_text("x\n")

        removed = backend.cleanup_staging(max_age_seconds=3600)
        assert len(removed) == 0
        assert Path(ref).exists()

    def test_no_staging_dir_returns_empty(self, tmp_path: Path) -> None:
        backend = CsvBackend(staging_dir=tmp_path / "nonexistent", publish_dir=tmp_path / "publish")
        assert backend.cleanup_staging() == []

    def test_ignores_non_tollkeeper_files(self, tmp_path: Path) -> None:
        staging = tmp_path / "staging"
        staging.mkdir()
        (staging / "other.csv").write_text("x\n")
        old_time = (staging / "other.csv").stat().st_mtime - 7200
        os.utime(staging / "other.csv", (old_time, old_time))

        backend = CsvBackend(staging_dir=staging, publish_dir=tmp_path / "publish")
        removed = backend.cleanup_staging(max_age_seconds=3600)
        assert len(removed) == 0
        assert (staging / "other.csv").exists()
