"""Example: Snowflake SQL operator wrapped with Tollkeeper.

Shows how a Snowflake SQLExecuteQueryOperator gets upstream signal
gating and DQ checks via tollkeeper_task_group. Uses sqlglot to
auto-parse source/sink lineage from the SQL.
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


class FreshnessCheck(BaseCheck):
    """Verify the target table was updated within the expected window."""

    def __init__(self, table: str) -> None:
        self._table = table

    def run(self, version_ref: str, *, conn=None) -> CheckResult:
        # Real implementation: SELECT MAX(updated_at) FROM table
        # and compare against current time.
        return CheckResult(
            check_name=self.name,
            passed=True,
            details=f"Placeholder: would check freshness of {self._table}",
        )


backend = SqlPassthroughBackend()
signal_store = SqliteSignalStore("/tmp/tollkeeper_signals.db")

# ---- BEFORE tollkeeper ----
#
# load_orders = SQLExecuteQueryOperator(
#     task_id="load_orders",
#     conn_id="snowflake_default",
#     sql="INSERT INTO analytics.fct_orders SELECT ... FROM staging.raw_orders",
# )
#
# ---- AFTER tollkeeper ----

with DAG(
    dag_id="example_snowflake_sql",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    snowflake_op = SQLExecuteQueryOperator(
        task_id="load_orders_inner",
        conn_id="snowflake_default",
        sql="INSERT INTO analytics.fct_orders SELECT o.*, c.segment "
        "FROM staging.raw_orders o JOIN staging.dim_customers c ON o.customer_id = c.id",
    )

    # sqlglot parses the SQL and extracts:
    #   sources: ["staging.raw_orders", "staging.dim_customers"]
    #   sinks:   ["analytics.fct_orders"]
    tg = tollkeeper_task_group(
        sql_operator=snowflake_op,
        table="analytics.fct_orders",
        backend=backend,
        checks=[FreshnessCheck("analytics.fct_orders")],
        signal_store=signal_store,
        dialect="snowflake",
        engine="local",
    )
