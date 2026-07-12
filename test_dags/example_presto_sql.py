"""Example: Presto SQL operator wrapped with Tollkeeper.

Demonstrates a multi-table dependency chain where two tollkeeper
task groups form a pipeline: stg_sales feeds fct_sales. The second
group's sensor automatically waits for the first group's signal.
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


class SchemaCheck(BaseCheck):
    """Verify expected columns exist in the target table."""

    def __init__(self, table: str, expected_columns: list[str]) -> None:
        self._table = table
        self._expected = expected_columns

    def run(self, version_ref: str, *, conn=None) -> CheckResult:
        # Real: DESCRIBE table, then check columns match
        return CheckResult(
            check_name=self.name,
            passed=True,
            details=f"Placeholder: would verify {self._expected} in {self._table}",
        )


backend = SqlPassthroughBackend()
signal_store = SqliteSignalStore("/tmp/tollkeeper_signals.db")

with DAG(
    dag_id="example_presto_multi_table",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    # Stage 1: raw_sales -> stg_sales
    stg_op = SQLExecuteQueryOperator(
        task_id="load_stg_sales_inner",
        conn_id="presto_default",
        sql=(
            "INSERT INTO hive.staging.stg_sales "
            "SELECT id, amount, region, sale_date "
            "FROM hive.raw.raw_sales "
            "WHERE sale_date = DATE '{{ ds }}'"
        ),
    )

    stg_tg = tollkeeper_task_group(
        sql_operator=stg_op,
        table="hive.staging.stg_sales",
        backend=backend,
        checks=[SchemaCheck("hive.staging.stg_sales", ["id", "amount", "region", "sale_date"])],
        signal_store=signal_store,
        sources=["hive.raw.raw_sales"],
        engine="local",
        execution_ctx={"ds": "{{ ds }}"},
    )

    # Stage 2: stg_sales -> fct_sales
    # The sensor inside this group waits for stg_sales signal from Stage 1
    fct_op = SQLExecuteQueryOperator(
        task_id="build_fct_sales_inner",
        conn_id="presto_default",
        sql=(
            "INSERT INTO hive.analytics.fct_sales "
            "SELECT region, sale_date, SUM(amount) as total, COUNT(*) as cnt "
            "FROM hive.staging.stg_sales "
            "GROUP BY region, sale_date"
        ),
    )

    fct_tg = tollkeeper_task_group(
        sql_operator=fct_op,
        table="hive.analytics.fct_sales",
        backend=backend,
        checks=[SchemaCheck("hive.analytics.fct_sales", ["region", "sale_date", "total", "cnt"])],
        signal_store=signal_store,
        sources=["hive.staging.stg_sales"],
        engine="local",
        execution_ctx={"ds": "{{ ds }}"},
    )

    stg_tg >> fct_tg
