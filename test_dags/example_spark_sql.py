"""Example: Spark SQL operator wrapped with Tollkeeper.

Demonstrates how a SparkSqlOperator DAG gains upstream signal gating,
DQ auditing via SQL checks, and downstream signal emission.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator

from airflow_tollkeeper import DqSqlCheck, register_defaults, tollkeeper_sql_task_group
from airflow_tollkeeper.compat import DAG
from tollkeeper.signals.sqlite import SqliteSignalStore

register_defaults()

signal_store = SqliteSignalStore("/tmp/tollkeeper_signals.db")

# ---- BEFORE tollkeeper (plain Airflow) ----
#
# spark_etl = SparkSqlOperator(
#     task_id="spark_etl",
#     sql="INSERT INTO clean_events SELECT * FROM raw_events WHERE event_date = '{{ ds }}'",
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
        task_id="spark_etl",
        sql="INSERT INTO clean_events SELECT * FROM raw_events WHERE event_date = '{{ ds }}'",
        conn_id="spark_default",
    )

    tg = tollkeeper_sql_task_group(
        sql_operator=spark_op,
        table="clean_events",
        conn_id="spark_default",
        signal_store=signal_store,
        sources=["raw_events"],
        execution_ctx={"ds": "{{ ds }}"},
        dq_checks=[
            DqSqlCheck(
                name="no_null_event_ids",
                sql="SELECT * FROM {table} WHERE event_id IS NULL",
            ),
            DqSqlCheck(
                name="min_row_count",
                sql="SELECT 1 WHERE (SELECT COUNT(*) FROM {table}) < 1",
            ),
        ],
    )
