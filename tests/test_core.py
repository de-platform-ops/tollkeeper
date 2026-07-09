from __future__ import annotations

import pytest

from write_audit_publish import WAP, AuditFailedError, CheckResult

from .conftest import BrokenRollbackBackend, FailingCheck, FakeBackend, PassingCheck


class TestWAPFluentAPI:
    def test_table_creates_version_and_returns_session(self) -> None:
        backend = FakeBackend()
        session = WAP(backend).table("my_table")
        assert session.ref == "branch-my_table-001"
        assert backend.created == ["my_table"]

    def test_write_passes_ref_to_callable(self) -> None:
        backend = FakeBackend()
        written_to: list[str] = []
        WAP(backend).table("t").write(lambda ref: written_to.append(ref)).audit([PassingCheck()]).publish()
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
        session = WAP(backend).table("t").audit([PassingCheck()])
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
        assert backend.rolled_back == [("t", "branch-t-001")]


class TestBug1RollbackMasksAuditError:
    def test_audit_failed_error_raised_even_when_rollback_fails(self) -> None:
        backend = BrokenRollbackBackend()
        with pytest.raises(AuditFailedError, match="FailingCheck"):
            WAP(backend).table("t").audit([FailingCheck()], on_failure="stop")


class TestBug2WriteOrphansVersion:
    def test_write_rolls_back_on_exception(self) -> None:
        backend = FakeBackend()
        with pytest.raises(ZeroDivisionError):
            WAP(backend).table("t").write(lambda ref: 1 / 0)
        assert backend.rolled_back == [("t", "branch-t-001")]

    def test_write_rollback_is_idempotent_with_explicit_rollback(self) -> None:
        backend = FakeBackend()
        session = WAP(backend).table("t")
        with pytest.raises(ZeroDivisionError):
            session.write(lambda ref: 1 / 0)
        session.rollback()
        assert len(backend.rolled_back) == 1


class TestBug3RollbackStateGuard:
    def test_cannot_rollback_after_publish(self) -> None:
        backend = FakeBackend()
        session = WAP(backend).table("t").audit([PassingCheck()])
        session.publish()
        with pytest.raises(RuntimeError, match="Cannot rollback a published session"):
            session.rollback()
        assert len(backend.rolled_back) == 0

    def test_cannot_publish_after_rollback(self) -> None:
        backend = FakeBackend()
        session = WAP(backend).table("t")
        session.rollback()
        with pytest.raises(RuntimeError, match="Cannot publish a rolled-back session"):
            session.publish()
        assert len(backend.published) == 0

    def test_rollback_is_idempotent(self) -> None:
        backend = FakeBackend()
        session = WAP(backend).table("t")
        session.rollback()
        session.rollback()
        assert len(backend.rolled_back) == 1


class TestBug4OrphanedSession:
    def test_del_rolls_back_uncommitted_session(self) -> None:
        backend = FakeBackend()
        session = WAP(backend).table("t")
        session.__del__()
        assert backend.rolled_back == [("t", "branch-t-001")]

    def test_del_noop_after_publish(self) -> None:
        backend = FakeBackend()
        session = WAP(backend).table("t").audit([PassingCheck()])
        session.publish()
        session.__del__()
        assert len(backend.rolled_back) == 0

    def test_del_noop_after_rollback(self) -> None:
        backend = FakeBackend()
        session = WAP(backend).table("t")
        session.rollback()
        session.__del__()
        assert len(backend.rolled_back) == 1


class TestBug5AuditOverwrites:
    def test_multiple_audits_accumulate_results(self) -> None:
        backend = FakeBackend()
        session = WAP(backend).table("t")
        session.audit([PassingCheck()], on_failure="continue")
        session.audit([FailingCheck()], on_failure="continue")
        assert len(session.report.results) == 2
        assert not session.report.passed

    def test_multiple_audits_all_passing(self) -> None:
        backend = FakeBackend()
        session = WAP(backend).table("t")
        session.audit([PassingCheck()], on_failure="continue")
        session.audit([PassingCheck()], on_failure="continue")
        assert len(session.report.results) == 2
        assert session.report.passed


class TestBug6PublishWithoutAudit:
    def test_publish_without_audit_raises(self) -> None:
        backend = FakeBackend()
        session = WAP(backend).table("t")
        session.write(lambda ref: None)
        with pytest.raises(RuntimeError, match="Cannot publish without running audit"):
            session.publish()
        assert len(backend.published) == 0

    def test_publish_after_audit_works(self) -> None:
        backend = FakeBackend()
        WAP(backend).table("t").audit([PassingCheck()]).publish()
        assert len(backend.published) == 1

    def test_publish_without_write_or_audit_raises(self) -> None:
        backend = FakeBackend()
        session = WAP(backend).table("t")
        with pytest.raises(RuntimeError, match="Cannot publish without running audit"):
            session.publish()


class TestContextManager:
    def test_context_manager_rolls_back_on_exit(self) -> None:
        backend = FakeBackend()
        with WAP(backend).table("t") as session:
            assert session.ref == "branch-t-001"
        assert backend.rolled_back == [("t", "branch-t-001")]

    def test_context_manager_noop_after_publish(self) -> None:
        backend = FakeBackend()
        with WAP(backend).table("t") as session:
            session.audit([PassingCheck()]).publish()
        assert len(backend.rolled_back) == 0
        assert len(backend.published) == 1

    def test_context_manager_noop_after_rollback(self) -> None:
        backend = FakeBackend()
        with WAP(backend).table("t") as session:
            session.rollback()
        assert len(backend.rolled_back) == 1

    def test_context_manager_rolls_back_on_exception(self) -> None:
        backend = FakeBackend()
        with pytest.raises(ValueError, match="boom"):
            with WAP(backend).table("t"):
                raise ValueError("boom")
        assert backend.rolled_back == [("t", "branch-t-001")]

    def test_context_manager_suppresses_nothing(self) -> None:
        backend = FakeBackend()
        with pytest.raises(ValueError):
            with WAP(backend).table("t"):
                raise ValueError("should propagate")

    def test_context_manager_with_audit_failure(self) -> None:
        backend = FakeBackend()
        with pytest.raises(AuditFailedError):
            with WAP(backend).table("t") as session:
                session.audit([FailingCheck()], on_failure="stop")
        assert len(backend.rolled_back) == 1
