"""Example: Trino SQL operator wrapped with Tollkeeper.

Uses explicit source declaration for Trino's catalog.schema.table naming.
DQ checks validate the aggregated output.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from airflow_tollkeeper import DqSqlCheck, register_defaults, tollkeeper_sql_task_group
from airflow_tollkeeper.compat import DAG
from tollkeeper.signals.sqlite import SqliteSignalStore

register_defaults()

signal_store = SqliteSignalStore("/tmp/tollkeeper_signals.db")

with DAG(
    dag_id="example_trino_sql",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    trino_op = SQLExecuteQueryOperator(
        task_id="aggregate_revenue",
        conn_id="trino_default",
        sql=(
            "INSERT INTO hive.analytics.daily_revenue "
            "SELECT date, SUM(amount) as total "
            "FROM hive.raw.transactions "
            "WHERE date = DATE '{{ ds }}' "
            "GROUP BY date"
        ),
    )

    tg = tollkeeper_sql_task_group(
        sql_operator=trino_op,
        table="hive.analytics.daily_revenue",
        conn_id="trino_default",
        signal_store=signal_store,
        sources=["hive.raw.transactions"],
        execution_ctx={"ds": "{{ ds }}"},
        dq_checks=[
            DqSqlCheck(
                name="no_negative_revenue",
                sql="SELECT * FROM {table} WHERE total < 0",
            ),
            DqSqlCheck(
                name="not_empty",
                sql="SELECT 1 WHERE (SELECT COUNT(*) FROM {table}) = 0",
            ),
        ],
    )
