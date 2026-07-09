from __future__ import annotations

import sqlite3

import pytest

from write_audit_publish import WAP, AuditFailedError
from write_audit_publish.signals import DbApiSignalStore, Signal, SqliteSignalStore

from .conftest import FailingCheck, FakeBackend, PassingCheck


def _make_sqlite_store(tmp_path, suffix=""):
    return SqliteSignalStore(str(tmp_path / f"signals{suffix}.db"))


def _make_dbapi_store(tmp_path, suffix=""):
    conn = sqlite3.connect(str(tmp_path / f"dbapi{suffix}.db"))
    return DbApiSignalStore(conn)


@pytest.fixture(params=["sqlite", "dbapi"])
def store(request, tmp_path):
    if request.param == "sqlite":
        return _make_sqlite_store(tmp_path)
    return _make_dbapi_store(tmp_path)


class TestDbApiParamstyle:
    def test_qmark_paramstyle(self, tmp_path) -> None:
        conn = sqlite3.connect(str(tmp_path / "qmark.db"))
        store = DbApiSignalStore(conn, paramstyle="qmark")
        store.write(Signal(table_name="t"))
        assert store.check("t") is not None

    def test_format_paramstyle_rejected_by_sqlite(self, tmp_path) -> None:
        conn = sqlite3.connect(str(tmp_path / "format.db"))
        store = DbApiSignalStore(conn, paramstyle="format")
        assert store._ph == "%s"
        assert store._p("SELECT * WHERE x = ?") == "SELECT * WHERE x = %s"

    def test_invalid_paramstyle_raises(self, tmp_path) -> None:
        conn = sqlite3.connect(str(tmp_path / "bad.db"))
        with pytest.raises(ValueError, match="paramstyle must be"):
            DbApiSignalStore(conn, paramstyle="pyformat")

    def test_p_replaces_all_placeholders(self) -> None:
        conn = sqlite3.connect(":memory:")
        store = DbApiSignalStore(conn, paramstyle="qmark")
        assert store._p("WHERE a = ? AND b = ?") == "WHERE a = ? AND b = ?"
        store2 = DbApiSignalStore(conn, paramstyle="format")
        assert store2._p("WHERE a = ? AND b = ?") == "WHERE a = %s AND b = %s"


class TestSignalCRUD:
    def test_write_and_check(self, store) -> None:
        store.write(Signal(table_name="my_table"))
        signal = store.check("my_table")
        assert signal is not None
        assert signal.table_name == "my_table"
        assert signal.status == "passed"

    def test_check_nonexistent_returns_none(self, store) -> None:
        assert store.check("unknown_table") is None

    def test_delete_removes_signal(self, store) -> None:
        store.write(Signal(table_name="my_table"))
        store.delete("my_table")
        assert store.check("my_table") is None

    def test_write_overwrites_existing(self, store) -> None:
        store.write(Signal(table_name="my_table", status="passed"))
        store.write(Signal(table_name="my_table", status="failed"))
        signal = store.check("my_table")
        assert signal is not None
        assert signal.status == "failed"

    def test_execution_ctx_isolation(self, store) -> None:
        store.write(Signal(table_name="my_table", execution_ctx={"ds": "2026-07-08"}))
        assert store.check("my_table", {"ds": "2026-07-09"}) is None
        signal = store.check("my_table", {"ds": "2026-07-08"})
        assert signal is not None
        assert signal.execution_ctx == {"ds": "2026-07-08"}


class TestSignalWait:
    def test_wait_returns_existing_signal(self, store) -> None:
        store.write(Signal(table_name="my_table"))
        signal = store.wait("my_table", timeout_s=5, poll_s=1)
        assert signal.table_name == "my_table"

    def test_wait_timeout_raises(self, store) -> None:
        with pytest.raises(TimeoutError):
            store.wait("missing_table", timeout_s=1, poll_s=0.2)


class TestCascadeDelete:
    def test_cascade_deletes_downstream(self, store) -> None:
        store.register_dep("a", "b", cascade_policy="cascade")
        store.register_dep("b", "c", cascade_policy="cascade")
        store.write(Signal(table_name="a"))
        store.write(Signal(table_name="b"))
        store.write(Signal(table_name="c"))
        store.delete("a")
        assert store.check("b") is None
        assert store.check("c") is None

    @pytest.mark.parametrize("make_store", [_make_sqlite_store, _make_dbapi_store])
    def test_notify_calls_callback(self, tmp_path, make_store) -> None:
        notified: list[tuple[str, str, dict]] = []

        def cb(upstream, downstream, ctx):
            notified.append((upstream, downstream, ctx))

        s = make_store(tmp_path, suffix="_notify")
        s._on_delete_callback = cb
        s.register_dep("a", "b", cascade_policy="notify")
        s.write(Signal(table_name="a"))
        s.delete("a")
        assert notified == [("a", "b", {})]

    def test_no_deps_no_cascade(self, store) -> None:
        store.write(Signal(table_name="solo"))
        store.delete("solo")
        assert store.check("solo") is None


class TestWAPSignalIntegration:
    @pytest.mark.parametrize("make_store", [_make_sqlite_store, _make_dbapi_store])
    def test_audit_pass_writes_signal(self, tmp_path, make_store) -> None:
        backend = FakeBackend()
        signal_store = make_store(tmp_path, suffix="_pass")
        WAP(backend, signal_store=signal_store).table("my_table").audit([PassingCheck()]).publish()
        assert signal_store.check("my_table") is not None

    @pytest.mark.parametrize("make_store", [_make_sqlite_store, _make_dbapi_store])
    def test_audit_fail_no_signal(self, tmp_path, make_store) -> None:
        backend = FakeBackend()
        signal_store = make_store(tmp_path, suffix="_fail")
        with pytest.raises(AuditFailedError):
            WAP(backend, signal_store=signal_store).table("my_table").audit([FailingCheck()], on_failure="stop")
        assert signal_store.check("my_table") is None

    @pytest.mark.parametrize("make_store", [_make_sqlite_store, _make_dbapi_store])
    def test_audit_deletes_old_signal_before_running(self, tmp_path, make_store) -> None:
        backend = FakeBackend()
        signal_store = make_store(tmp_path, suffix="_stale")
        signal_store.write(Signal(table_name="my_table", status="stale"))
        with pytest.raises(AuditFailedError):
            WAP(backend, signal_store=signal_store).table("my_table").audit([FailingCheck()], on_failure="stop")
        assert signal_store.check("my_table") is None

    def test_no_signal_store_backward_compat(self) -> None:
        backend = FakeBackend()
        session = WAP(backend).table("my_table").audit([PassingCheck()]).publish()
        assert session.report.passed
        assert backend.published == [("my_table", "branch-my_table-001")]
