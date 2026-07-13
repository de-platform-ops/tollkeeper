"""Example: PostgreSQL upsert wrapped with Tollkeeper.

Root node (no upstream sensors). Tollkeeper adds DQ checks and signal
emission so downstream DAGs can depend on this table's quality.
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
    dag_id="example_postgres_sql",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    pg_op = SQLExecuteQueryOperator(
        task_id="upsert_users",
        conn_id="postgres_default",
        sql=(
            "INSERT INTO public.dim_users (id, name, email, updated_at) "
            "SELECT id, name, email, NOW() "
            "FROM public.stg_users "
            "ON CONFLICT (id) DO UPDATE SET "
            "  name = EXCLUDED.name, "
            "  email = EXCLUDED.email, "
            "  updated_at = EXCLUDED.updated_at"
        ),
    )

    # No sources -> no upstream sensors. This table is a root node.
    tg = tollkeeper_sql_task_group(
        sql_operator=pg_op,
        table="public.dim_users",
        conn_id="postgres_default",
        signal_store=signal_store,
        sources=[],
        dq_checks=[
            DqSqlCheck(
                name="no_null_ids",
                sql="SELECT * FROM {table} WHERE id IS NULL",
            ),
            DqSqlCheck(
                name="no_null_emails",
                sql="SELECT * FROM {table} WHERE email IS NULL",
            ),
            DqSqlCheck(
                name="unique_emails",
                sql=("SELECT email, COUNT(*) as cnt FROM {table} GROUP BY email HAVING COUNT(*) > 1"),
            ),
        ],
    )
