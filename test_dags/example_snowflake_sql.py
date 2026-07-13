"""Example: Snowflake SQL operator wrapped with Tollkeeper.

Uses sqlglot to auto-parse source/sink lineage from the SQL. DQ checks
run as separate Airflow tasks against the Snowflake connection.
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
    dag_id="example_snowflake_sql",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    snowflake_op = SQLExecuteQueryOperator(
        task_id="load_orders",
        conn_id="snowflake_default",
        sql=(
            "INSERT INTO analytics.fct_orders "
            "SELECT o.*, c.segment "
            "FROM staging.raw_orders o "
            "JOIN staging.dim_customers c ON o.customer_id = c.id"
        ),
    )

    # sqlglot auto-parses sources from the SQL (snowflake dialect)
    tg = tollkeeper_sql_task_group(
        sql_operator=snowflake_op,
        table="analytics.fct_orders",
        conn_id="snowflake_default",
        signal_store=signal_store,
        dialect="snowflake",
        dq_checks=[
            DqSqlCheck(
                name="no_null_order_ids",
                sql="SELECT * FROM {table} WHERE order_id IS NULL",
            ),
            DqSqlCheck(
                name="valid_segments",
                sql="SELECT * FROM {table} WHERE segment NOT IN ('enterprise', 'smb', 'mid-market')",
            ),
            DqSqlCheck(
                name="freshness",
                sql="SELECT 1 WHERE (SELECT MAX(updated_at) FROM {table}) < DATEADD(hour, -2, CURRENT_TIMESTAMP())",
            ),
        ],
    )
