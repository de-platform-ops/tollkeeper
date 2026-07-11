from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Callable

from write_audit_publish.signals.base import Signal, SignalStore


class SqliteSignalStore(SignalStore):
    """Signal store backed by a SQLite database."""

    def __init__(
        self,
        db_path: str | Path = ":memory:",
        *,
        on_delete_callback: Callable[[str, str, dict], None] | None = None,
    ) -> None:
        super().__init__(on_delete_callback=on_delete_callback)
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS wap_signals (
                table_name    TEXT NOT NULL,
                execution_ctx TEXT NOT NULL DEFAULT '{}',
                status        TEXT NOT NULL,
                execution_ts  TEXT NOT NULL,
                updated_at    TEXT NOT NULL,
                check_summary TEXT DEFAULT '',
                metadata      TEXT DEFAULT '{}',
                PRIMARY KEY (table_name, execution_ctx)
            );
            CREATE TABLE IF NOT EXISTS wap_signal_deps (
                upstream_table   TEXT NOT NULL,
                upstream_ctx     TEXT NOT NULL DEFAULT '{}',
                downstream_table TEXT NOT NULL,
                downstream_ctx   TEXT NOT NULL DEFAULT '{}',
                cascade_policy   TEXT NOT NULL DEFAULT 'notify',
                PRIMARY KEY (upstream_table, upstream_ctx, downstream_table, downstream_ctx)
            );
        """)

    @staticmethod
    def _ctx_key(ctx: dict | None) -> str:
        return json.dumps(ctx or {}, sort_keys=True)

    def write(self, signal: Signal) -> None:
        ctx_key = self._ctx_key(signal.execution_ctx)
        now = datetime.now().isoformat()
        self._conn.execute(
            """INSERT INTO wap_signals (table_name, execution_ctx, status, execution_ts, updated_at, check_summary, metadata)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(table_name, execution_ctx) DO UPDATE SET
                   status=excluded.status, execution_ts=excluded.execution_ts,
                   updated_at=excluded.updated_at, check_summary=excluded.check_summary,
                   metadata=excluded.metadata""",
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
        self._conn.execute(
            "DELETE FROM wap_signals WHERE table_name = ? AND execution_ctx = ?",
            (table, ctx_key),
        )
        self._conn.commit()

        for ds_table, ds_ctx, policy in self.get_downstream(table, execution_ctx):
            if policy == "cascade":
                self.delete(ds_table, ds_ctx)
            elif policy == "notify" and self._on_delete_callback:
                self._on_delete_callback(table, ds_table, ds_ctx)

    def check(self, table: str, execution_ctx: dict | None = None) -> Signal | None:
        ctx_key = self._ctx_key(execution_ctx)
        row = self._conn.execute(
            "SELECT * FROM wap_signals WHERE table_name = ? AND execution_ctx = ?",
            (table, ctx_key),
        ).fetchone()
        if row is None:
            return None
        return Signal(
            table_name=row["table_name"],
            execution_ctx=json.loads(row["execution_ctx"]),
            status=row["status"],
            execution_ts=datetime.fromisoformat(row["execution_ts"]),
            check_summary=row["check_summary"],
            metadata=json.loads(row["metadata"]),
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
        self._conn.execute(
            """INSERT OR REPLACE INTO wap_signal_deps
               (upstream_table, upstream_ctx, downstream_table, downstream_ctx, cascade_policy)
               VALUES (?, ?, ?, ?, ?)""",
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
        rows = self._conn.execute(
            "SELECT downstream_table, downstream_ctx, cascade_policy FROM wap_signal_deps WHERE upstream_table = ? AND upstream_ctx = ?",
            (table, self._ctx_key(execution_ctx)),
        ).fetchall()
        return [(r["downstream_table"], json.loads(r["downstream_ctx"]), r["cascade_policy"]) for r in rows]
