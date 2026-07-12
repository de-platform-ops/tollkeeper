from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from tollkeeper.backends.sql_passthrough import SqlPassthroughBackend

if TYPE_CHECKING:
    from pytest import LogCaptureFixture


class TestSqlPassthroughBackend:
    def test_create_version_returns_table_name(self) -> None:
        backend = SqlPassthroughBackend()
        assert backend.create_version("orders") == "orders"

    def test_publish_version_is_noop(self) -> None:
        backend = SqlPassthroughBackend()
        backend.publish_version("orders", "orders")

    def test_rollback_logs_warning(self, caplog: LogCaptureFixture) -> None:
        backend = SqlPassthroughBackend()
        with caplog.at_level(logging.WARNING):
            backend.rollback_version("orders", "orders")
        assert "cannot undo writes" in caplog.text.lower()

    def test_works_with_tollkeeper_session(self) -> None:
        from tollkeeper.core import Tollkeeper

        backend = SqlPassthroughBackend()
        tk = Tollkeeper(backend)
        session = tk.table("sales")
        assert session.ref == "sales"
