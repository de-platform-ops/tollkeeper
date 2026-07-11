from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from tollkeeper.backends.base import Backend
    from tollkeeper.checks.base import BaseCheck, CheckResult
    from tollkeeper.signals.base import SignalStore


@dataclass
class CheckReport:
    """Aggregated results from an audit pass.

    Attributes:
        results: List of individual ``CheckResult`` objects.
    """

    results: list[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def failed(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed]


class AuditFailedError(Exception):
    """Raised when a hard DQ audit (``on_failure="stop"``) finds violations."""

    def __init__(self, table: str, version_ref: str, failed: list[CheckResult]) -> None:
        self.table = table
        self.version_ref = version_ref
        self.failed = failed
        names = ", ".join(r.check_name for r in failed)
        super().__init__(f"Audit failed for {table}@{version_ref}: {names}")


class TollkeeperSession:
    """A single write-audit-publish session for one table version.

    Created by ``Tollkeeper.table()``. Supports fluent chaining::

        Tollkeeper(backend).table("sales").write(fn).audit([checks]).publish()
    """

    def __init__(self, backend: Backend, table: str, version_ref: str, signal_store: SignalStore | None = None) -> None:
        self._backend = backend
        self._table = table
        self._version_ref = version_ref
        self._report = CheckReport()
        self._published = False
        self._rolled_back = False
        self._audited = False
        self._signal_store = signal_store

    def __del__(self) -> None:
        if not self._published and not self._rolled_back:
            import logging

            logging.getLogger(__name__).warning(
                "TollkeeperSession for %s was never published or rolled back — rolling back", self._table
            )
            try:
                self._backend.rollback_version(self._table, self._version_ref)
            except Exception:
                pass
            self._rolled_back = True

    def __enter__(self) -> TollkeeperSession:
        return self

    def __exit__(self, exc_type: type | None, exc_val: BaseException | None, exc_tb: object) -> None:
        if not self._published and not self._rolled_back:
            try:
                self._backend.rollback_version(self._table, self._version_ref)
            except Exception:
                pass
            self._rolled_back = True

    @property
    def ref(self) -> str:
        return self._version_ref

    @property
    def report(self) -> CheckReport:
        return self._report

    def write(self, fn: Callable[[str], None]) -> TollkeeperSession:
        """Execute the write function against the staged version.

        Args:
            fn: Callable that receives the version reference and writes data to it.

        Raises:
            Exception: Re-raises any exception from ``fn`` after rolling back the staged version.
        """
        try:
            fn(self._version_ref)
        except Exception:
            self._backend.rollback_version(self._table, self._version_ref)
            self._rolled_back = True
            raise
        return self

    def audit(
        self,
        checks: list[BaseCheck],
        *,
        on_failure: str = "stop",
        on_notify: Callable[[str, str, list[CheckResult]], None] | None = None,
        execution_ctx: dict | None = None,
        conn: Any | None = None,
    ) -> TollkeeperSession:
        if on_failure not in ("stop", "continue"):
            raise ValueError("on_failure must be 'stop' or 'continue'")

        if self._signal_store:
            self._signal_store.delete(self._table, execution_ctx)

        self._report.results.extend(check.run(self._version_ref, conn=conn) for check in checks)
        self._audited = True

        if self._report.failed:
            if on_notify:
                on_notify(self._table, self._version_ref, self._report.failed)
            if on_failure == "stop":
                try:
                    self._backend.rollback_version(self._table, self._version_ref)
                finally:
                    self._rolled_back = True
                    raise AuditFailedError(self._table, self._version_ref, self._report.failed)

        if self._signal_store and self._report.passed:
            from datetime import datetime

            from tollkeeper.signals.base import Signal

            self._signal_store.write(
                Signal(
                    table_name=self._table,
                    execution_ctx=execution_ctx or {},
                    status="passed",
                    execution_ts=datetime.now(),
                    check_summary=f"{len(self._report.results)} checks passed",
                )
            )

        return self

    def publish(self) -> TollkeeperSession:
        """Promote the staged version to production.

        Raises:
            RuntimeError: If the session was already rolled back.
            RuntimeError: If ``audit()`` has not been called.
        """
        if self._rolled_back:
            raise RuntimeError("Cannot publish a rolled-back session")
        if not self._audited:
            raise RuntimeError("Cannot publish without running audit first")
        if not self._published:
            self._backend.publish_version(self._table, self._version_ref)
            self._published = True
        return self

    def rollback(self) -> TollkeeperSession:
        """Discard the staged version without publishing.

        Raises:
            RuntimeError: If the session was already published.
        """
        if self._published:
            raise RuntimeError("Cannot rollback a published session")
        if not self._rolled_back:
            self._backend.rollback_version(self._table, self._version_ref)
            self._rolled_back = True
        return self


class Tollkeeper:
    """Entry point for the write-audit-publish pattern.

    Args:
        backend: Storage backend (e.g. ``CsvBackend``, ``IcebergBackend``).

    Example::

        Tollkeeper(CsvBackend(staging, publish)).table("sales").write(fn).audit([NullCheck("id")]).publish()
    """

    def __init__(self, backend: Backend, signal_store: SignalStore | None = None) -> None:
        self._backend = backend
        self._signal_store = signal_store

    def table(self, table: str) -> TollkeeperSession:
        version_ref = self._backend.create_version(table)
        return TollkeeperSession(self._backend, table, version_ref, self._signal_store)
