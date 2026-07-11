from __future__ import annotations

from typing import Any

from tollkeeper import BaseCheck, CheckResult
from tollkeeper.backends.base import Backend


class FakeBackend(Backend):
    def __init__(self) -> None:
        self.created: list[str] = []
        self.published: list[tuple[str, str]] = []
        self.rolled_back: list[tuple[str, str]] = []

    def create_version(self, table: str) -> str:
        self.created.append(table)
        return f"branch-{table}-001"

    def publish_version(self, table: str, version_ref: str) -> None:
        self.published.append((table, version_ref))

    def rollback_version(self, table: str, version_ref: str) -> None:
        self.rolled_back.append((table, version_ref))


class BrokenRollbackBackend(Backend):
    def create_version(self, table: str) -> str:
        return f"branch-{table}-001"

    def publish_version(self, table: str, version_ref: str) -> None:
        pass

    def rollback_version(self, table: str, version_ref: str) -> None:
        raise ConnectionError("catalog unavailable")


class PassingCheck(BaseCheck):
    def run(self, version_ref: str, *, conn: Any | None = None) -> CheckResult:
        return CheckResult(check_name=self.name, passed=True)


class FailingCheck(BaseCheck):
    def run(self, version_ref: str, *, conn: Any | None = None) -> CheckResult:
        return CheckResult(check_name=self.name, passed=False, details="row count is 0")
