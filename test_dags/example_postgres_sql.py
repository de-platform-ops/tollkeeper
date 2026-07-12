"""Example: PostgreSQL SQL operator wrapped with Tollkeeper.

Demonstrates the simplest case: a single SQLExecuteQueryOperator with
no upstream dependencies (no sensors). Tollkeeper adds DQ auditing and
signal emission so downstream DAGs can depend on this table's quality.
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


class NullColumnCheck(BaseCheck):
    """Verify a column has no nulls by querying the database."""

    def __init__(self, table: str, column: str) -> None:
        self._table = table
        self._column = column

    def run(self, version_ref: str, *, conn=None) -> CheckResult:
        # Real: cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL")
        return CheckResult(
            check_name=self.name,
            passed=True,
            details=f"Placeholder: would check nulls in {self._table}.{self._column}",
        )


backend = SqlPassthroughBackend()
signal_store = SqliteSignalStore("/tmp/tollkeeper_signals.db")

with DAG(
    dag_id="example_postgres_sql",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    pg_op = SQLExecuteQueryOperator(
        task_id="upsert_users_inner",
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

    # No sources= means no upstream sensors. This table is a root node.
    # Tollkeeper still audits and emits a signal for downstream consumers.
    tg = tollkeeper_task_group(
        sql_operator=pg_op,
        table="public.dim_users",
        backend=backend,
        checks=[
            NullColumnCheck("public.dim_users", "id"),
            NullColumnCheck("public.dim_users", "email"),
        ],
        signal_store=signal_store,
        sources=[],
        engine="local",
    )
