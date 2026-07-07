from __future__ import annotations

import pytest

from write_audit_publish import WAP, AuditFailedError, BaseCheck, CheckResult
from write_audit_publish.backends.base import Backend


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


class PassingCheck(BaseCheck):
    def run(self, version_ref: str) -> CheckResult:
        return CheckResult(check_name=self.name, passed=True)


class FailingCheck(BaseCheck):
    def run(self, version_ref: str) -> CheckResult:
        return CheckResult(check_name=self.name, passed=False, details="row count is 0")


class TestWAPFluentAPI:
    def test_table_creates_version_and_returns_session(self) -> None:
        backend = FakeBackend()
        session = WAP(backend).table("my_table")
        assert session.ref == "branch-my_table-001"
        assert backend.created == ["my_table"]

    def test_write_passes_ref_to_callable(self) -> None:
        backend = FakeBackend()
        written_to: list[str] = []
        WAP(backend).table("t").write(lambda ref: written_to.append(ref)).publish()
        assert written_to == ["branch-t-001"]
        assert backend.published == [("t", "branch-t-001")]

    def test_audit_publish_chain(self) -> None:
        backend = FakeBackend()
        session = WAP(backend).table("my_table").audit([PassingCheck()]).publish()
        assert backend.published == [("my_table", "branch-my_table-001")]
        assert session.report.passed

    def test_hard_dq_failure_rolls_back(self) -> None:
        backend = FakeBackend()
        with pytest.raises(AuditFailedError, match="FailingCheck"):
            WAP(backend).table("my_table").audit([FailingCheck()], on_failure="stop")
        assert len(backend.published) == 0
        assert backend.rolled_back == [("my_table", "branch-my_table-001")]

    def test_soft_dq_allows_publish(self) -> None:
        backend = FakeBackend()
        session = WAP(backend).table("my_table").audit([FailingCheck()], on_failure="continue").publish()
        assert backend.published == [("my_table", "branch-my_table-001")]
        assert not session.report.passed

    def test_soft_dq_calls_notify(self) -> None:
        notified: list[tuple[str, str, list[CheckResult]]] = []

        def on_notify(table: str, ref: str, failed: list[CheckResult]) -> None:
            notified.append((table, ref, failed))

        backend = FakeBackend()
        WAP(backend).table("my_table").audit([FailingCheck()], on_failure="continue", on_notify=on_notify).publish()
        assert len(notified) == 1
        assert notified[0][0] == "my_table"
        assert notified[0][1] == "branch-my_table-001"

    def test_hard_dq_calls_notify_before_raising(self) -> None:
        notified: list[str] = []

        def on_notify(table: str, ref: str, failed: list[CheckResult]) -> None:
            notified.append(table)

        backend = FakeBackend()
        with pytest.raises(AuditFailedError):
            WAP(backend).table("t").audit([FailingCheck()], on_failure="stop", on_notify=on_notify)
        assert notified == ["t"]

    def test_explicit_rollback(self) -> None:
        backend = FakeBackend()
        WAP(backend).table("t").rollback()
        assert backend.rolled_back == [("t", "branch-t-001")]

    def test_publish_is_idempotent(self) -> None:
        backend = FakeBackend()
        session = WAP(backend).table("t")
        session.publish()
        session.publish()
        assert len(backend.published) == 1

    def test_invalid_on_failure_value(self) -> None:
        backend = FakeBackend()
        with pytest.raises(ValueError, match="on_failure must be"):
            WAP(backend).table("t").audit([PassingCheck()], on_failure="invalid")

    def test_report_exposes_failed_checks(self) -> None:
        backend = FakeBackend()
        with pytest.raises(AuditFailedError):
            WAP(backend).table("t").audit([PassingCheck(), FailingCheck()], on_failure="stop")
        # rollback happened
        assert backend.rolled_back == [("t", "branch-t-001")]
