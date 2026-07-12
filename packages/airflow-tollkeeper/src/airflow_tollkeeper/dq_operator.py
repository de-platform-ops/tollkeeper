from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from airflow_tollkeeper.compat import BaseOperator

if TYPE_CHECKING:
    from tollkeeper.signals.base import SignalStore


@dataclass
class DqSqlCheck:
    """A SQL-based DQ check definition.

    The SQL should return violation rows. Zero rows returned means the check passes.
    Use ``{table}`` as a placeholder for the target table name.
    """

    name: str
    sql: str


class TollkeeperDqOperator(BaseOperator):
    """Runs a DQ validation SQL query and stores the result.

    Executes ``check_sql`` via the Airflow connection. If the query returns
    zero rows, the check passes. The result is written to the signal store's
    ``tollkeeper_dq_results`` table.
    """

    template_fields = ("check_sql", "execution_ctx")

    def __init__(
        self,
        *,
        check_name: str,
        check_sql: str,
        table: str,
        conn_id: str,
        signal_store: SignalStore,
        execution_ctx: dict | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.check_name = check_name
        self.check_sql = check_sql
        self.table = table
        self.conn_id = conn_id
        self.signal_store = signal_store
        self.execution_ctx = execution_ctx

    def execute(self, context: Any) -> bool:
        from airflow.hooks.base import BaseHook

        hook = BaseHook.get_hook(self.conn_id)
        records = hook.get_records(self.check_sql)
        violation_count = len(records)
        passed = violation_count == 0

        from tollkeeper.signals.base import DqResult

        self.signal_store.write_dq_result(
            DqResult(
                table_name=self.table,
                check_name=self.check_name,
                passed=passed,
                details=f"{violation_count} violations" if not passed else "0 violations",
                execution_ctx=self.execution_ctx or {},
                executed_at=datetime.now(),
            )
        )

        self.log.info(
            "DQ check '%s' on '%s': %s (%d violations)",
            self.check_name,
            self.table,
            "PASS" if passed else "FAIL",
            violation_count,
        )
        return passed
