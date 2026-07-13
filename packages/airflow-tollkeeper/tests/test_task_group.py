from __future__ import annotations

import pytest

from airflow_tollkeeper import TollkeeperOperator, TollkeeperSensor
from airflow_tollkeeper.task_group import tollkeeper_task_group
from tollkeeper.signals.sqlite import SqliteSignalStore

from .conftest import (
    FakeBackend,
    FakeOperator,
    PassingCheck,
    register_fake_strategy,
    unregister_fake_strategy,
)


@pytest.fixture(autouse=True)
def _strategy():
    register_fake_strategy()
    yield
    unregister_fake_strategy()


@pytest.fixture()
def backend():
    return FakeBackend()


@pytest.fixture()
def signal_store(tmp_path):
    store = SqliteSignalStore(str(tmp_path / "signals.db"))
    yield store
    store.close()


class TestTollkeeperTaskGroupHappyPath:
    def test_creates_task_group_with_sensors_and_operator(self, dag, backend, signal_store):
        inner = FakeOperator(task_id="insert_sales", dag=dag)

        with dag:
            tg = tollkeeper_task_group(
                sql_operator=inner,
                table="analytics.sales",
                backend=backend,
                checks=[PassingCheck()],
                signal_store=signal_store,
                sources=["raw.orders", "raw.customers"],
                engine="local",
            )

        children = list(tg.children.values())
        sensor_tasks = [t for t in children if isinstance(t, TollkeeperSensor)]
        tollkeeper_tasks = [t for t in children if isinstance(t, TollkeeperOperator)]

        assert len(sensor_tasks) == 2
        assert len(tollkeeper_tasks) == 1
        sensor_tables = {s.table for s in sensor_tasks}
        assert sensor_tables == {"raw.orders", "raw.customers"}

    def test_default_group_id_from_table(self, dag, backend, signal_store):
        inner = FakeOperator(task_id="insert_sales", dag=dag)

        with dag:
            tg = tollkeeper_task_group(
                sql_operator=inner,
                table="sales",
                backend=backend,
                checks=[PassingCheck()],
                signal_store=signal_store,
                sources=["raw.orders"],
                engine="local",
            )

        assert tg.group_id == "tollkeeper_sales"

    def test_custom_group_id(self, dag, backend, signal_store):
        inner = FakeOperator(task_id="insert_sales", dag=dag)

        with dag:
            tg = tollkeeper_task_group(
                group_id="my_custom_group",
                sql_operator=inner,
                table="sales",
                backend=backend,
                checks=[PassingCheck()],
                signal_store=signal_store,
                sources=["raw.orders"],
                engine="local",
            )

        assert tg.group_id == "my_custom_group"

    def test_sensors_upstream_of_operator(self, dag, backend, signal_store):
        inner = FakeOperator(task_id="insert_sales", dag=dag)

        with dag:
            tg = tollkeeper_task_group(
                sql_operator=inner,
                table="sales",
                backend=backend,
                checks=[PassingCheck()],
                signal_store=signal_store,
                sources=["raw.orders"],
                engine="local",
            )

        children = list(tg.children.values())
        tk_op = [t for t in children if isinstance(t, TollkeeperOperator)][0]
        sensor = [t for t in children if isinstance(t, TollkeeperSensor)][0]

        assert sensor in tk_op.upstream_list


class TestTollkeeperTaskGroupAutoParseSQL:
    def test_parses_sql_for_sources(self, dag, backend, signal_store):
        inner = FakeOperator(task_id="insert_sales", dag=dag)

        with dag:
            tg = tollkeeper_task_group(
                sql_operator=inner,
                table="analytics.sales",
                backend=backend,
                checks=[PassingCheck()],
                signal_store=signal_store,
                sql="INSERT INTO analytics.sales SELECT * FROM raw.orders JOIN raw.customers ON 1=1",
                engine="local",
            )

        children = list(tg.children.values())
        sensor_tasks = [t for t in children if isinstance(t, TollkeeperSensor)]
        sensor_tables = {s.table for s in sensor_tasks}
        assert sensor_tables == {"raw.orders", "raw.customers"}

    def test_extracts_sql_from_operator_attribute(self, dag, backend, signal_store):
        inner = FakeOperator(task_id="insert_sales", dag=dag)
        inner.sql = "INSERT INTO analytics.sales SELECT * FROM raw.events"

        with dag:
            tg = tollkeeper_task_group(
                sql_operator=inner,
                table="analytics.sales",
                backend=backend,
                checks=[PassingCheck()],
                signal_store=signal_store,
                engine="local",
            )

        children = list(tg.children.values())
        sensor_tasks = [t for t in children if isinstance(t, TollkeeperSensor)]
        assert len(sensor_tasks) == 1
        assert sensor_tasks[0].table == "raw.events"

    def test_explicit_sources_override_sql(self, dag, backend, signal_store):
        inner = FakeOperator(task_id="insert_sales", dag=dag)

        with dag:
            tg = tollkeeper_task_group(
                sql_operator=inner,
                table="analytics.sales",
                backend=backend,
                checks=[PassingCheck()],
                signal_store=signal_store,
                sql="INSERT INTO analytics.sales SELECT * FROM raw.orders",
                sources=["override.table_a"],
                engine="local",
            )

        children = list(tg.children.values())
        sensor_tasks = [t for t in children if isinstance(t, TollkeeperSensor)]
        assert len(sensor_tasks) == 1
        assert sensor_tasks[0].table == "override.table_a"

    def test_sink_mismatch_from_operator_sql_raises(self, dag, backend, signal_store):
        inner = FakeOperator(task_id="insert_sales", dag=dag)
        inner.sql = "INSERT INTO wrong_table SELECT * FROM raw.orders"

        with pytest.raises(ValueError, match="sink.*does not match.*table"):
            with dag:
                tollkeeper_task_group(
                    sql_operator=inner,
                    table="analytics.sales",
                    backend=backend,
                    checks=[PassingCheck()],
                    signal_store=signal_store,
                    engine="local",
                )


class TestTollkeeperTaskGroupEdgeCases:
    def test_no_sources_creates_operator_only(self, dag, backend, signal_store):
        inner = FakeOperator(task_id="insert_sales", dag=dag)

        with dag:
            tg = tollkeeper_task_group(
                sql_operator=inner,
                table="sales",
                backend=backend,
                checks=[PassingCheck()],
                signal_store=signal_store,
                engine="local",
            )

        children = list(tg.children.values())
        sensor_tasks = [t for t in children if isinstance(t, TollkeeperSensor)]
        tollkeeper_tasks = [t for t in children if isinstance(t, TollkeeperOperator)]
        assert len(sensor_tasks) == 0
        assert len(tollkeeper_tasks) == 1

    def test_jinja_sql_on_operator_falls_through_to_no_sources(self, dag, backend, signal_store):
        inner = FakeOperator(task_id="insert_sales", dag=dag)
        inner.sql = "INSERT INTO sales SELECT * FROM {{ params.source_table }}"

        with dag:
            tg = tollkeeper_task_group(
                sql_operator=inner,
                table="sales",
                backend=backend,
                checks=[PassingCheck()],
                signal_store=signal_store,
                engine="local",
            )

        children = list(tg.children.values())
        sensor_tasks = [t for t in children if isinstance(t, TollkeeperSensor)]
        assert len(sensor_tasks) == 0

    def test_circular_dependency_raises(self, dag, backend, signal_store):
        inner = FakeOperator(task_id="insert_sales", dag=dag)

        with pytest.raises(ValueError, match="(?i)circular"):
            with dag:
                tollkeeper_task_group(
                    sql_operator=inner,
                    table="sales",
                    backend=backend,
                    checks=[PassingCheck()],
                    signal_store=signal_store,
                    sources=["sales", "raw.orders"],
                    engine="local",
                )

    def test_sink_mismatch_raises(self, dag, backend, signal_store):
        inner = FakeOperator(task_id="insert_sales", dag=dag)

        with pytest.raises(ValueError, match="sink.*does not match.*table"):
            with dag:
                tollkeeper_task_group(
                    sql_operator=inner,
                    table="analytics.sales",
                    backend=backend,
                    checks=[PassingCheck()],
                    signal_store=signal_store,
                    sql="INSERT INTO wrong_table SELECT * FROM raw.orders",
                    engine="local",
                )

    def test_execution_ctx_passed_to_sensors(self, dag, backend, signal_store):
        inner = FakeOperator(task_id="insert_sales", dag=dag)
        ctx = {"ds": "{{ ds }}"}

        with dag:
            tg = tollkeeper_task_group(
                sql_operator=inner,
                table="sales",
                backend=backend,
                checks=[PassingCheck()],
                signal_store=signal_store,
                sources=["raw.orders"],
                execution_ctx=ctx,
                engine="local",
            )

        children = list(tg.children.values())
        sensor = [t for t in children if isinstance(t, TollkeeperSensor)][0]
        assert sensor.execution_ctx == {"ds": "{{ ds }}"}

    def test_duplicate_sources_are_deduped(self, dag, backend, signal_store):
        inner = FakeOperator(task_id="insert_sales", dag=dag)

        with dag:
            tg = tollkeeper_task_group(
                sql_operator=inner,
                table="sales",
                backend=backend,
                checks=[PassingCheck()],
                signal_store=signal_store,
                sources=["raw.orders", "raw.orders"],
                engine="local",
            )

        children = list(tg.children.values())
        sensor_tasks = [t for t in children if isinstance(t, TollkeeperSensor)]
        assert len(sensor_tasks) == 1

    def test_dotted_table_names_use_double_underscore_slug(self, dag, backend, signal_store):
        inner = FakeOperator(task_id="insert_sales", dag=dag)

        with dag:
            tg = tollkeeper_task_group(
                sql_operator=inner,
                table="analytics.sales",
                backend=backend,
                checks=[PassingCheck()],
                signal_store=signal_store,
                sources=["raw.orders"],
                engine="local",
            )

        assert tg.group_id == "tollkeeper_analytics__sales"
        children = list(tg.children.values())
        sensor = [t for t in children if isinstance(t, TollkeeperSensor)][0]
        assert sensor.task_id.endswith("wait_raw__orders")


class TestTollkeeperSensorTemplateFields:
    def test_execution_ctx_is_template_field(self):
        assert "execution_ctx" in TollkeeperSensor.template_fields
