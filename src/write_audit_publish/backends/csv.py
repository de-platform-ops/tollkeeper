from __future__ import annotations

import shutil
from pathlib import Path
from uuid import uuid4

from write_audit_publish.backends.base import Backend


class CsvBackend(Backend):
    """WAP backend for local CSV files.

    Stages data as a temporary CSV in ``staging_dir``, then copies to
    ``publish_dir`` on publish or deletes on rollback.

    Args:
        staging_dir: Directory for temporary staging files.
        publish_dir: Directory where final published CSVs land.

    Example::

        backend = CsvBackend(staging_dir=Path("/tmp/wap"), publish_dir=Path("/data/output"))
        WAP(backend).table("sales").write(lambda ref: shutil.copy(src, ref)).audit([...]).publish()
    """

    def __init__(self, staging_dir: Path, publish_dir: Path) -> None:
        self._staging_dir = staging_dir
        self._publish_dir = publish_dir

    def create_version(self, table: str) -> str:
        self._staging_dir.mkdir(parents=True, exist_ok=True)
        staging_path = self._staging_dir / f"{table}.wap-{uuid4().hex[:8]}.csv"
        return str(staging_path)

    def publish_version(self, table: str, version_ref: str) -> None:
        self._publish_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(version_ref, self._publish_dir / f"{table}.csv")
        Path(version_ref).unlink()

    def rollback_version(self, table: str, version_ref: str) -> None:
        Path(version_ref).unlink(missing_ok=True)
