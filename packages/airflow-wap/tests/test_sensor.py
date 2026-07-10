from __future__ import annotations

from datetime import datetime

from airflow_wap import WAPSensor
from airflow_wap.compat import DAG
from write_audit_publish.signals.base import Signal
from write_audit_publish.signals.sqlite import SqliteSignalStore


class TestWAPSensor:
    def test_poke_returns_false_when_no_signal(self, tmp_path):
        store = SqliteSignalStore(str(tmp_path / "signals.db"))
        dag = DAG(dag_id="test_dag", start_date=datetime(2026, 1, 1))

        sensor = WAPSensor(
            task_id="wait_for_sales",
            table="sales",
            signal_store=store,
            dag=dag,
        )
        assert sensor.poke({}) is False

    def test_poke_returns_true_when_signal_exists(self, tmp_path):
        store = SqliteSignalStore(str(tmp_path / "signals.db"))
        store.write(Signal(table_name="sales", status="passed"))
        dag = DAG(dag_id="test_dag", start_date=datetime(2026, 1, 1))

        sensor = WAPSensor(
            task_id="wait_for_sales",
            table="sales",
            signal_store=store,
            dag=dag,
        )
        assert sensor.poke({}) is True

    def test_poke_with_execution_ctx(self, tmp_path):
        store = SqliteSignalStore(str(tmp_path / "signals.db"))
        ctx = {"ds": "2026-07-10"}
        store.write(Signal(table_name="sales", execution_ctx=ctx, status="passed"))
        dag = DAG(dag_id="test_dag", start_date=datetime(2026, 1, 1))

        sensor = WAPSensor(
            task_id="wait_for_sales",
            table="sales",
            signal_store=store,
            execution_ctx=ctx,
            dag=dag,
        )
        assert sensor.poke({}) is True

    def test_poke_wrong_ctx_returns_false(self, tmp_path):
        store = SqliteSignalStore(str(tmp_path / "signals.db"))
        store.write(Signal(table_name="sales", execution_ctx={"ds": "2026-07-09"}, status="passed"))
        dag = DAG(dag_id="test_dag", start_date=datetime(2026, 1, 1))

        sensor = WAPSensor(
            task_id="wait_for_sales",
            table="sales",
            signal_store=store,
            execution_ctx={"ds": "2026-07-10"},
            dag=dag,
        )
        assert sensor.poke({}) is False
