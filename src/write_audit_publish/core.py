from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from write_audit_publish.backends.base import Backend
    from write_audit_publish.checks.base import BaseCheck, CheckResult


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


class WAPSession:
    """A single write-audit-publish session for one table version.

    Created by ``WAP.table()``. Supports fluent chaining::

        WAP(backend).table("sales").write(fn).audit([checks]).publish()
    """

    def __init__(self, backend: Backend, table: str, version_ref: str) -> None:
        self._backend = backend
        self._table = table
        self._version_ref = version_ref
        self._report = CheckReport()
        self._published = False

    @property
    def ref(self) -> str:
        return self._version_ref

    @property
    def report(self) -> CheckReport:
        return self._report

    def write(self, fn: Callable[[str], None]) -> WAPSession:
        fn(self._version_ref)
        return self

    def audit(
        self,
        checks: list[BaseCheck],
        *,
        on_failure: str = "stop",
        on_notify: Callable[[str, str, list[CheckResult]], None] | None = None,
    ) -> WAPSession:
        if on_failure not in ("stop", "continue"):
            raise ValueError("on_failure must be 'stop' or 'continue'")
        self._report = CheckReport([check.run(self._version_ref) for check in checks])
        if self._report.failed:
            if on_notify:
                on_notify(self._table, self._version_ref, self._report.failed)
            if on_failure == "stop":
                self._backend.rollback_version(self._table, self._version_ref)
                raise AuditFailedError(self._table, self._version_ref, self._report.failed)
        return self

    def publish(self) -> WAPSession:
        if not self._published:
            self._backend.publish_version(self._table, self._version_ref)
            self._published = True
        return self

    def rollback(self) -> WAPSession:
        self._backend.rollback_version(self._table, self._version_ref)
        return self


class WAP:
    """Entry point for the write-audit-publish pattern.

    Args:
        backend: Storage backend (e.g. ``CsvBackend``, ``IcebergBackend``).

    Example::

        WAP(CsvBackend(staging, publish)).table("sales").write(fn).audit([NullCheck("id")]).publish()
    """

    def __init__(self, backend: Backend) -> None:
        self._backend = backend

    def table(self, table: str) -> WAPSession:
        version_ref = self._backend.create_version(table)
        return WAPSession(self._backend, table, version_ref)
