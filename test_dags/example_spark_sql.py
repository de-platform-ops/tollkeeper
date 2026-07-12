"""Example: Spark SQL operator wrapped with Tollkeeper.

Demonstrates how a SparkSqlOperator DAG gains upstream signal gating,
DQ auditing, and downstream signal emission by importing tollkeeper.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow_tollkeeper.compat import DAG

from airflow_tollkeeper import register_defaults, tollkeeper_task_group
from tollkeeper.backends.sql_passthrough import SqlPassthroughBackend
from tollkeeper.checks.base import BaseCheck, CheckResult
from tollkeeper.signals.sqlite import SqliteSignalStore

register_defaults()

# -- DQ check that runs against the DB via the engine connection --------


class RowCountSqlCheck(BaseCheck):
    """Verify the target table has rows after the write."""

    def __init__(self, table: str, min_rows: int = 1) -> None:
        self._table = table
        self._min_rows = min_rows

    def run(self, version_ref: str, *, conn=None) -> CheckResult:
        # conn is the resolved Airflow connection for the DQ engine.
        # In a real implementation, open a JDBC/DBAPI cursor here.
        # This example shows the structure; actual query execution
        # depends on your Spark/Hive setup.
        return CheckResult(
            check_name=self.name,
            passed=True,
            details=f"Placeholder: would query {self._table} via {conn}",
        )


# -- DAG ----------------------------------------------------------------

backend = SqlPassthroughBackend()
signal_store = SqliteSignalStore("/tmp/tollkeeper_signals.db")

# ---- BEFORE tollkeeper (plain Airflow) ----
#
# spark_etl = SparkSqlOperator(
#     task_id="spark_etl",
#     sql="SELECT * FROM raw_events WHERE event_date = '{{ ds }}'",
#     conn_id="spark_default",
#     dag=dag,
# )
#
# ---- AFTER tollkeeper ----

with DAG(
    dag_id="example_spark_sql",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    spark_op = SparkSqlOperator(
        task_id="spark_etl_inner",
        sql="INSERT INTO clean_events SELECT * FROM raw_events WHERE event_date = '{{ ds }}'",
        conn_id="spark_default",
    )

    tg = tollkeeper_task_group(
        sql_operator=spark_op,
        table="clean_events",
        backend=backend,
        checks=[RowCountSqlCheck("clean_events", min_rows=1)],
        signal_store=signal_store,
        sources=["raw_events"],
        engine="spark",
        engine_conn_id="spark_default",
        execution_ctx={"ds": "{{ ds }}"},
    )
