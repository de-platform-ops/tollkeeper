from __future__ import annotations

from pathlib import Path

from write_audit_publish.backends.csv import CsvBackend


class TestCsvBackend:
    def test_create_version_returns_staging_path(self, tmp_path: Path) -> None:
        backend = CsvBackend(staging_dir=tmp_path / "staging", publish_dir=tmp_path / "publish")
        ref = backend.create_version("sales")
        path = Path(ref)
        assert path.parent == tmp_path / "staging"
        assert path.name.startswith("sales.wap-")
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
