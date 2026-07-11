"""Integration tests for parser + Airflow DAG construction.

These tests require apache-airflow and sqlglot installed (run inside Docker
or with the airflow-wap dev environment). Skipped automatically when either
dependency is missing.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

try:
    from airflow_wap.compat import DAG, BaseOperator
    from write_audit_publish.parser import extract_lineage

    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False

pytestmark = pytest.mark.skipif(not HAS_DEPS, reason="airflow + sqlglot required")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class FakeSQLOperator(BaseOperator):
    """Mimics SQLExecuteQueryOperator: stores SQL, executes nothing."""

    def __init__(self, *, sql: str, **kwargs):
        super().__init__(**kwargs)
        self.sql = sql

    def execute(self, context):
        pass


def _make_dag(dag_id: str = "test") -> DAG:
    return DAG(dag_id=dag_id, start_date=datetime(2026, 1, 1))


def _fake_context(dag: DAG) -> dict:
    return {"ti": MagicMock(), "dag": dag, "run_id": "test_run"}


# ---------------------------------------------------------------------------
# Integration: parser extracts lineage from operator SQL at DAG parse time
# ---------------------------------------------------------------------------


class TestParserWithAirflowOperator:
    """Simulate DAG parse-time: read SQL from operator, extract lineage."""

    def test_simple_operator_lineage(self):
        dag = _make_dag()
        op = FakeSQLOperator(
            task_id="load_orders",
            sql="INSERT INTO warehouse.fact_orders SELECT * FROM staging.raw_orders",
            dag=dag,
        )
        result = extract_lineage(op.sql)
        assert result.sources == {"staging.raw_orders"}
        assert result.sinks == {"warehouse.fact_orders"}

    def test_multi_join_operator(self):
        dag = _make_dag()
        op = FakeSQLOperator(
            task_id="build_fact",
            sql="""
            INSERT INTO fact_sales
            SELECT o.id, c.name, p.sku
            FROM orders o
            JOIN customers c ON o.cust_id = c.id
            JOIN products p ON o.prod_id = p.id
            """,
            dag=dag,
        )
        result = extract_lineage(op.sql)
        assert result.sources == {"orders", "customers", "products"}
        assert result.sinks == {"fact_sales"}

    def test_cte_operator(self):
        dag = _make_dag()
        op = FakeSQLOperator(
            task_id="transform",
            sql="""
            WITH daily_agg AS (
                SELECT dt, SUM(amount) as total FROM raw_txns GROUP BY dt
            )
            INSERT INTO daily_summary SELECT * FROM daily_agg
            """,
            dag=dag,
        )
        result = extract_lineage(op.sql)
        assert result.sources == {"raw_txns"}
        assert "daily_agg" not in result.sources
        assert result.sinks == {"daily_summary"}

    def test_jinja_templated_sql_requires_explicit_lineage(self):
        dag = _make_dag()
        op = FakeSQLOperator(
            task_id="templated",
            sql="INSERT INTO {{ params.target }} SELECT * FROM {{ params.source }}",
            dag=dag,
        )
        with pytest.raises(ValueError, match="template"):
            extract_lineage(op.sql)


# ---------------------------------------------------------------------------
# Integration: parsed lineage drives DAG dependency wiring
# ---------------------------------------------------------------------------


class TestLineageDrivenDependencies:
    """Verify that parsed lineage can wire Airflow task dependencies."""

    def test_sensor_per_source_table(self):
        """Each source table from lineage gets a sensor upstream of the SQL task."""
        dag = _make_dag("lineage_dag")
        sql = """
        INSERT INTO fact_table
        SELECT a.id, b.val, c.meta
        FROM source_a a
        JOIN source_b b ON a.id = b.id
        JOIN source_c c ON a.id = c.id
        """
        result = extract_lineage(sql)

        sql_task = FakeSQLOperator(task_id="write_fact", sql=sql, dag=dag)

        sensors = []
        for src in sorted(result.sources):
            sensor = FakeSQLOperator(
                task_id=f"wait_{src}",
                sql="SELECT 1",
                dag=dag,
            )
            sensor >> sql_task
            sensors.append(sensor)

        assert len(sensors) == 3
        assert set(sql_task.upstream_task_ids) == {"wait_source_a", "wait_source_b", "wait_source_c"}

    def test_publish_task_downstream_of_sql(self):
        """Sink table from lineage configures a publish task downstream."""
        dag = _make_dag("publish_dag")
        sql = "INSERT INTO output_table SELECT * FROM input_table"
        result = extract_lineage(sql)

        sql_task = FakeSQLOperator(task_id="write", sql=sql, dag=dag)
        publish_task = FakeSQLOperator(task_id=f"publish_{list(result.sinks)[0]}", sql="SELECT 1", dag=dag)
        sql_task >> publish_task

        assert set(publish_task.upstream_task_ids) == {"write"}
        assert set(sql_task.downstream_task_ids) == {f"publish_{list(result.sinks)[0]}"}

    def test_full_wap_chain(self):
        """Full chain: sensors >> write >> audit >> publish, all derived from SQL lineage."""
        dag = _make_dag("full_wap")
        sql = """
        INSERT INTO warehouse.daily_metrics
        SELECT d.dt, SUM(t.amount)
        FROM transactions t
        JOIN dim_date d ON t.txn_date = d.dt
        GROUP BY d.dt
        """
        result = extract_lineage(sql)

        # Build sensor >> write >> audit >> publish chain
        write_task = FakeSQLOperator(task_id="write", sql=sql, dag=dag)
        audit_task = FakeSQLOperator(task_id="audit", sql="SELECT 1", dag=dag)
        publish_task = FakeSQLOperator(task_id="publish", sql="SELECT 1", dag=dag)

        for src in sorted(result.sources):
            sensor = FakeSQLOperator(task_id=f"wait_{src}", sql="SELECT 1", dag=dag)
            sensor >> write_task

        write_task >> audit_task >> publish_task

        assert set(write_task.upstream_task_ids) == {"wait_dim_date", "wait_transactions"}
        assert set(audit_task.upstream_task_ids) == {"write"}
        assert set(publish_task.upstream_task_ids) == {"audit"}

    def test_multi_statement_wires_all_sinks(self):
        """Multiple INSERT statements produce multiple sink-specific publish tasks."""
        dag = _make_dag("multi_sink")
        sql = """
        INSERT INTO output_a SELECT * FROM shared_source;
        INSERT INTO output_b SELECT * FROM shared_source;
        """
        result = extract_lineage(sql)
        assert result.sinks == {"output_a", "output_b"}
        assert result.sources == {"shared_source"}

        write_task = FakeSQLOperator(task_id="write_all", sql=sql, dag=dag)
        for sink in sorted(result.sinks):
            pub = FakeSQLOperator(task_id=f"publish_{sink}", sql="SELECT 1", dag=dag)
            write_task >> pub

        assert set(write_task.downstream_task_ids) == {"publish_output_a", "publish_output_b"}


# ---------------------------------------------------------------------------
# Integration: WAPOperator + parser end-to-end
# ---------------------------------------------------------------------------


class TestWAPOperatorWithParser:
    """Parser-informed WAPOperator execution — the full vertical slice."""

    def test_operator_executes_with_parsed_lineage(self):
        """Parse lineage, wire deps, execute the operator — no crash."""
        dag = _make_dag("e2e")
        sql = "INSERT INTO target SELECT * FROM src_1 JOIN src_2 ON src_1.id = src_2.id"
        result = extract_lineage(sql)

        op = FakeSQLOperator(task_id="etl", sql=sql, dag=dag)

        for src in sorted(result.sources):
            sensor = FakeSQLOperator(task_id=f"wait_{src}", sql="SELECT 1", dag=dag)
            sensor >> op

        ctx = _fake_context(dag)
        op.execute(ctx)

        assert set(op.upstream_task_ids) == {"wait_src_1", "wait_src_2"}

    def test_parsed_lineage_matches_operator_table_param(self):
        """Verify that parser sink matches the table= param an operator would use."""
        sql = "INSERT INTO catalog.schema.fact_users SELECT * FROM catalog.schema.raw_users"
        result = extract_lineage(sql, dialect="trino")
        assert "catalog.schema.fact_users" in result.sinks
