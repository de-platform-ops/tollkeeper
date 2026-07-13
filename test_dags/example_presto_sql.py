"""Example: Presto multi-table dependency chain with Tollkeeper.

Two tollkeeper task groups form a pipeline: stg_sales feeds fct_sales.
The second group's sensor automatically waits for the first group's signal.
Each stage has its own DQ checks running as separate Airflow tasks.
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
    dag_id="example_presto_multi_table",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    # Stage 1: raw_sales -> stg_sales
    stg_op = SQLExecuteQueryOperator(
        task_id="load_stg_sales",
        conn_id="presto_default",
        sql=(
            "INSERT INTO hive.staging.stg_sales "
            "SELECT id, amount, region, sale_date "
            "FROM hive.raw.raw_sales "
            "WHERE sale_date = DATE '{{ ds }}'"
        ),
    )

    stg_tg = tollkeeper_sql_task_group(
        sql_operator=stg_op,
        table="hive.staging.stg_sales",
        conn_id="presto_default",
        signal_store=signal_store,
        sources=["hive.raw.raw_sales"],
        execution_ctx={"ds": "{{ ds }}"},
        dq_checks=[
            DqSqlCheck(
                name="no_null_ids",
                sql="SELECT * FROM {table} WHERE id IS NULL",
            ),
            DqSqlCheck(
                name="positive_amounts",
                sql="SELECT * FROM {table} WHERE amount <= 0",
            ),
        ],
    )

    # Stage 2: stg_sales -> fct_sales
    # The sensor inside this group waits for stg_sales signal from Stage 1
    fct_op = SQLExecuteQueryOperator(
        task_id="build_fct_sales",
        conn_id="presto_default",
        sql=(
            "INSERT INTO hive.analytics.fct_sales "
            "SELECT region, sale_date, SUM(amount) as total, COUNT(*) as cnt "
            "FROM hive.staging.stg_sales "
            "GROUP BY region, sale_date"
        ),
    )

    fct_tg = tollkeeper_sql_task_group(
        sql_operator=fct_op,
        table="hive.analytics.fct_sales",
        conn_id="presto_default",
        signal_store=signal_store,
        sources=["hive.staging.stg_sales"],
        execution_ctx={"ds": "{{ ds }}"},
        dq_checks=[
            DqSqlCheck(
                name="no_zero_counts",
                sql="SELECT * FROM {table} WHERE cnt = 0",
            ),
            DqSqlCheck(
                name="revenue_sanity",
                sql="SELECT * FROM {table} WHERE total > 10000000",
            ),
        ],
    )

    stg_tg >> fct_tg
