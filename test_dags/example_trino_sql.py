"""Example: Trino SQL operator wrapped with Tollkeeper.

Demonstrates tollkeeper with Trino (formerly PrestoSQL). Uses explicit
source declaration since Trino queries often involve catalog.schema.table
naming that benefits from explicit lineage.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow_tollkeeper.compat import DAG

from airflow_tollkeeper import register_defaults, tollkeeper_task_group
from tollkeeper.backends.sql_passthrough import SqlPassthroughBackend
from tollkeeper.checks.base import BaseCheck, CheckResult
from tollkeeper.signals.sqlite import SqliteSignalStore

register_defaults()


class NotEmptyCheck(BaseCheck):
    """Verify the target table is not empty after the write."""

    def __init__(self, table: str) -> None:
        self._table = table

    def run(self, version_ref: str, *, conn=None) -> CheckResult:
        # Real: cursor.execute(f"SELECT COUNT(*) FROM {table}")
        return CheckResult(
            check_name=self.name,
            passed=True,
            details=f"Placeholder: would count rows in {self._table}",
        )


backend = SqlPassthroughBackend()
signal_store = SqliteSignalStore("/tmp/tollkeeper_signals.db")

with DAG(
    dag_id="example_trino_sql",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    trino_op = SQLExecuteQueryOperator(
        task_id="trino_aggregate_inner",
        conn_id="trino_default",
        sql=(
            "INSERT INTO hive.analytics.daily_revenue "
            "SELECT date, SUM(amount) as total "
            "FROM hive.raw.transactions "
            "WHERE date = DATE '{{ ds }}' "
            "GROUP BY date"
        ),
    )

    tg = tollkeeper_task_group(
        sql_operator=trino_op,
        table="hive.analytics.daily_revenue",
        backend=backend,
        checks=[NotEmptyCheck("hive.analytics.daily_revenue")],
        signal_store=signal_store,
        sources=["hive.raw.transactions"],
        engine="local",
        execution_ctx={"ds": "{{ ds }}"},
    )
