from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Callable

from tollkeeper.signals.base import Signal, SignalStore


class DbApiSignalStore(SignalStore):
    """Signal store backed by any PEP 249 (DB-API 2.0) connection.

    Args:
        connection: A DB-API 2.0 connection (psycopg2, mysql-connector, sqlite3, etc.).
        paramstyle: Parameter marker style — "qmark" (?) or "format" (%s). Defaults to "qmark".
    """

    def __init__(
        self,
        connection: Any,
        *,
        paramstyle: str = "qmark",
        on_delete_callback: Callable[[str, str, dict], None] | None = None,
    ) -> None:
        super().__init__(on_delete_callback=on_delete_callback)
        self._conn = connection
        if paramstyle not in ("qmark", "format"):
            raise ValueError("paramstyle must be 'qmark' or 'format'")
        self._ph = "?" if paramstyle == "qmark" else "%s"
        self._init_schema()

    def _p(self, sql: str) -> str:
        return sql.replace("?", self._ph)

    def _init_schema(self) -> None:
        cur = self._conn.cursor()
        cur.execute(
            self._p("""
            CREATE TABLE IF NOT EXISTS tollkeeper_signals (
                table_name    VARCHAR(512) NOT NULL,
                execution_ctx VARCHAR(2048) NOT NULL DEFAULT '{}',
                status        VARCHAR(20) NOT NULL,
                execution_ts  VARCHAR(64) NOT NULL,
                updated_at    VARCHAR(64) NOT NULL,
                check_summary VARCHAR(4096) DEFAULT '',
                metadata      VARCHAR(4096) DEFAULT '{}',
                PRIMARY KEY (table_name, execution_ctx)
            )
        """)
        )
        cur.execute(
            self._p("""
            CREATE TABLE IF NOT EXISTS tollkeeper_signal_deps (
                upstream_table   VARCHAR(512) NOT NULL,
                upstream_ctx     VARCHAR(2048) NOT NULL DEFAULT '{}',
                downstream_table VARCHAR(512) NOT NULL,
                downstream_ctx   VARCHAR(2048) NOT NULL DEFAULT '{}',
                cascade_policy   VARCHAR(20) NOT NULL DEFAULT 'notify',
                PRIMARY KEY (upstream_table, upstream_ctx, downstream_table, downstream_ctx)
            )
        """)
        )
        self._conn.commit()

    @staticmethod
    def _ctx_key(ctx: dict | None) -> str:
        return json.dumps(ctx or {}, sort_keys=True)

    def write(self, signal: Signal) -> None:
        ctx_key = self._ctx_key(signal.execution_ctx)
        now = datetime.now().isoformat()
        cur = self._conn.cursor()
        cur.execute(
            self._p("DELETE FROM tollkeeper_signals WHERE table_name = ? AND execution_ctx = ?"),
            (signal.table_name, ctx_key),
        )
        cur.execute(
            self._p("""INSERT INTO tollkeeper_signals
                       (table_name, execution_ctx, status, execution_ts, updated_at, check_summary, metadata)
                       VALUES (?, ?, ?, ?, ?, ?, ?)"""),
            (
                signal.table_name,
                ctx_key,
                signal.status,
                signal.execution_ts.isoformat(),
                now,
                signal.check_summary,
                json.dumps(signal.metadata),
            ),
        )
        self._conn.commit()

    def delete(self, table: str, execution_ctx: dict | None = None) -> None:
        ctx_key = self._ctx_key(execution_ctx)
        cur = self._conn.cursor()
        cur.execute(
            self._p("DELETE FROM tollkeeper_signals WHERE table_name = ? AND execution_ctx = ?"), (table, ctx_key)
        )
        self._conn.commit()

        for ds_table, ds_ctx, policy in self.get_downstream(table, execution_ctx):
            if policy == "cascade":
                self.delete(ds_table, ds_ctx)
            elif policy == "notify" and self._on_delete_callback:
                self._on_delete_callback(table, ds_table, ds_ctx)

    def check(self, table: str, execution_ctx: dict | None = None) -> Signal | None:
        ctx_key = self._ctx_key(execution_ctx)
        cur = self._conn.cursor()
        cur.execute(
            self._p("SELECT * FROM tollkeeper_signals WHERE table_name = ? AND execution_ctx = ?"), (table, ctx_key)
        )
        row = cur.fetchone()
        if row is None:
            return None
        return Signal(
            table_name=row[0],
            execution_ctx=json.loads(row[1]),
            status=row[2],
            execution_ts=datetime.fromisoformat(row[3]),
            check_summary=row[5],
            metadata=json.loads(row[6]),
        )

    def register_dep(
        self,
        upstream_table: str,
        downstream_table: str,
        cascade_policy: str = "notify",
        upstream_ctx: dict | None = None,
        downstream_ctx: dict | None = None,
    ) -> None:
        if cascade_policy not in ("notify", "cascade"):
            raise ValueError("cascade_policy must be 'notify' or 'cascade'")
        cur = self._conn.cursor()
        cur.execute(
            self._p(
                "DELETE FROM tollkeeper_signal_deps WHERE upstream_table = ? AND upstream_ctx = ? AND downstream_table = ? AND downstream_ctx = ?"
            ),
            (upstream_table, self._ctx_key(upstream_ctx), downstream_table, self._ctx_key(downstream_ctx)),
        )
        cur.execute(
            self._p("""INSERT INTO tollkeeper_signal_deps
                       (upstream_table, upstream_ctx, downstream_table, downstream_ctx, cascade_policy)
                       VALUES (?, ?, ?, ?, ?)"""),
            (
                upstream_table,
                self._ctx_key(upstream_ctx),
                downstream_table,
                self._ctx_key(downstream_ctx),
                cascade_policy,
            ),
        )
        self._conn.commit()

    def get_downstream(self, table: str, execution_ctx: dict | None = None) -> list[tuple[str, dict, str]]:
        cur = self._conn.cursor()
        cur.execute(
            self._p(
                "SELECT downstream_table, downstream_ctx, cascade_policy FROM tollkeeper_signal_deps WHERE upstream_table = ? AND upstream_ctx = ?"
            ),
            (table, self._ctx_key(execution_ctx)),
        )
        return [(r[0], json.loads(r[1]), r[2]) for r in cur.fetchall()]
