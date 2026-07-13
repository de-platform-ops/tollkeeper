from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from airflow_tollkeeper.compat import DAG
from airflow_tollkeeper.dq_operator import DqSqlCheck, TollkeeperDqOperator
from airflow_tollkeeper.signal_operator import TollkeeperSignalEmitter
from airflow_tollkeeper.task_group import tollkeeper_sql_task_group
from tollkeeper.signals.base import DqResult
from tollkeeper.signals.sqlite import SqliteSignalStore

from .conftest import FakeOperator


@pytest.fixture()
def dag():
    return DAG(dag_id="test_dag", start_date=datetime(2026, 1, 1))


@pytest.fixture()
def signal_store(tmp_path):
    return SqliteSignalStore(str(tmp_path / "signals.db"))


@pytest.fixture()
def fake_context():
    ti = MagicMock()
    return {"ti": ti, "ds": "2026-01-01"}


class TestTollkeeperDqOperator:
    @patch("airflow.hooks.base.BaseHook")
    def test_passing_check_stores_result(self, mock_basehook, dag, signal_store, fake_context):
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = []
        mock_basehook.get_hook.return_value = mock_hook

        op = TollkeeperDqOperator(
            task_id="dq_no_nulls",
            check_name="no_nulls",
            check_sql="SELECT * FROM orders WHERE id IS NULL",
            table="orders",
            conn_id="test_conn",
            signal_store=signal_store,
            dag=dag,
        )
        result = op.execute(fake_context)

        assert result is True
        results = signal_store.get_dq_results("orders")
        assert len(results) == 1
        assert results[0].passed is True
        assert results[0].check_name == "no_nulls"

    @patch("airflow.hooks.base.BaseHook")
    def test_failing_check_stores_result(self, mock_basehook, dag, signal_store, fake_context):
        mock_hook = MagicMock()
        mock_hook.get_records.return_value = [("row1",), ("row2",)]
        mock_basehook.get_hook.return_value = mock_hook

        op = TollkeeperDqOperator(
            task_id="dq_no_nulls",
            check_name="no_nulls",
            check_sql="SELECT * FROM orders WHERE id IS NULL",
            table="orders",
            conn_id="test_conn",
            signal_store=signal_store,
            dag=dag,
        )
        result = op.execute(fake_context)

        assert result is False
        results = signal_store.get_dq_results("orders")
        assert len(results) == 1
        assert results[0].passed is False
        assert "2 violations" in results[0].details


class TestTollkeeperSignalEmitter:
    def test_all_checks_passed_emits_signal(self, dag, signal_store, fake_context):
        signal_store.write_dq_result(DqResult(table_name="orders", check_name="no_nulls", passed=True))
        signal_store.write_dq_result(DqResult(table_name="orders", check_name="row_count", passed=True))

        op = TollkeeperSignalEmitter(
            task_id="signal_orders",
            table="orders",
            signal_store=signal_store,
            expected_checks=["no_nulls", "row_count"],
            dag=dag,
        )
        result = op.execute(fake_context)

        assert result == "passed"
        signal = signal_store.check("orders")
        assert signal is not None
        assert signal.status == "passed"

    def test_failed_check_raises_on_stop(self, dag, signal_store, fake_context):
        signal_store.write_dq_result(DqResult(table_name="orders", check_name="no_nulls", passed=True))
        signal_store.write_dq_result(DqResult(table_name="orders", check_name="row_count", passed=False))

        op = TollkeeperSignalEmitter(
            task_id="signal_orders",
            table="orders",
            signal_store=signal_store,
            expected_checks=["no_nulls", "row_count"],
            on_failure="stop",
            dag=dag,
        )
        with pytest.raises(RuntimeError, match="row_count"):
            op.execute(fake_context)

        assert signal_store.check("orders") is None

    def test_failed_check_continues(self, dag, signal_store, fake_context):
        signal_store.write_dq_result(DqResult(table_name="orders", check_name="no_nulls", passed=False))

        op = TollkeeperSignalEmitter(
            task_id="signal_orders",
            table="orders",
            signal_store=signal_store,
            expected_checks=["no_nulls"],
            on_failure="continue",
            dag=dag,
        )
        result = op.execute(fake_context)
        assert result == "failed"
        assert signal_store.check("orders") is None

    def test_missing_check_raises(self, dag, signal_store, fake_context):
        signal_store.write_dq_result(DqResult(table_name="orders", check_name="no_nulls", passed=True))

        op = TollkeeperSignalEmitter(
            task_id="signal_orders",
            table="orders",
            signal_store=signal_store,
            expected_checks=["no_nulls", "row_count"],
            dag=dag,
        )
        with pytest.raises(RuntimeError, match="Missing DQ results"):
            op.execute(fake_context)


class TestTollkeeperSqlTaskGroup:
    def test_creates_correct_task_structure(self, dag, signal_store):
        inner = FakeOperator(task_id="sql_inner", dag=dag)
        checks = [
            DqSqlCheck(name="no_nulls", sql="SELECT * FROM {table} WHERE id IS NULL"),
            DqSqlCheck(name="row_count", sql="SELECT 1 WHERE (SELECT COUNT(*) FROM {table}) < 1"),
        ]

        tg = tollkeeper_sql_task_group(
            sql_operator=inner,
            table="orders",
            dq_checks=checks,
            signal_store=signal_store,
            conn_id="test_conn",
            sources=["raw_orders"],
            dag=dag,
        )

        task_ids = {t.task_id for t in tg}
        assert "wait_raw_orders" in task_ids
        assert "dq_no_nulls" in task_ids
        assert "dq_row_count" in task_ids
        assert "signal_orders" in task_ids

    def test_no_sources_no_sensors(self, dag, signal_store):
        inner = FakeOperator(task_id="sql_inner", dag=dag)
        checks = [DqSqlCheck(name="no_nulls", sql="SELECT * FROM {table} WHERE id IS NULL")]

        tg = tollkeeper_sql_task_group(
            sql_operator=inner,
            table="orders",
            dq_checks=checks,
            signal_store=signal_store,
            conn_id="test_conn",
            sources=[],
            dag=dag,
        )

        task_ids = {t.task_id for t in tg}
        assert not any(tid.startswith("wait_") for tid in task_ids)
        assert "dq_no_nulls" in task_ids
        assert "signal_orders" in task_ids

    def test_circular_dependency_raises(self, dag, signal_store):
        inner = FakeOperator(task_id="sql_inner", dag=dag)
        checks = [DqSqlCheck(name="chk", sql="SELECT 1")]

        with pytest.raises(ValueError, match="Circular"):
            tollkeeper_sql_task_group(
                sql_operator=inner,
                table="orders",
                dq_checks=checks,
                signal_store=signal_store,
                conn_id="test_conn",
                sources=["orders"],
                dag=dag,
            )
