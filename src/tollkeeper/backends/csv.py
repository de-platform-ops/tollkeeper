from __future__ import annotations

import shutil
import time
from pathlib import Path
from uuid import uuid4

from tollkeeper.backends.base import Backend


def _move(src: Path, dst: Path) -> None:
    """Atomic rename when possible, copy+delete as cross-device fallback."""
    try:
        src.replace(dst)
    except OSError:
        shutil.copy2(str(src), str(dst))
        src.unlink()


class CsvBackend(Backend):
    """Tollkeeper backend for local CSV files.

    Stages data as a temporary CSV in ``staging_dir``, then copies to
    ``publish_dir`` on publish or deletes on rollback.

    Args:
        staging_dir: Directory for temporary staging files.
        publish_dir: Directory where final published CSVs land.

    Example::

        backend = CsvBackend(staging_dir=Path("/tmp/tollkeeper"), publish_dir=Path("/data/output"))
        Tollkeeper(backend).table("sales").write(lambda ref: shutil.copy(src, ref)).audit([...]).publish()
    """

    def __init__(self, staging_dir: Path, publish_dir: Path) -> None:
        self._staging_dir = staging_dir
        self._publish_dir = publish_dir

    def create_version(self, table: str) -> str:
        self._staging_dir.mkdir(parents=True, exist_ok=True)
        staging_path = self._staging_dir / f"{table}.tollkeeper-{uuid4().hex[:8]}.csv"
        return str(staging_path)

    def publish_version(self, table: str, version_ref: str) -> None:
        self._publish_dir.mkdir(parents=True, exist_ok=True)
        _move(Path(version_ref), self._publish_dir / f"{table}.csv")

    def rollback_version(self, table: str, version_ref: str) -> None:
        Path(version_ref).unlink(missing_ok=True)

    def cleanup_staging(self, max_age_seconds: float = 3600) -> list[Path]:
        """Remove Tollkeeper staging files older than ``max_age_seconds``.

        Args:
            max_age_seconds: Maximum age in seconds before a staging file is considered orphaned.

        Returns:
            List of removed file paths.
        """
        if not self._staging_dir.exists():
            return []
        cutoff = time.time() - max_age_seconds
        removed: list[Path] = []
        for p in self._staging_dir.glob("*.tollkeeper-*.csv"):
            if p.stat().st_mtime < cutoff:
                p.unlink()
                removed.append(p)
        return removed
