from __future__ import annotations

import pytest

from airflow_tollkeeper import TollkeeperOperator
from airflow_tollkeeper.engine import LOCAL_ENGINE, resolve_engine
from tollkeeper.core import AuditFailedError

from .conftest import (
    FakeBackend,
    FakeOperator,
    FailingCheck,
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
def fake_context(dag):
    from unittest.mock import MagicMock

    ti = MagicMock()
    return {"ti": ti, "dag": dag, "run_id": "test_run"}


class TestResolveEngine:
    def test_local_returns_sentinel(self):
        assert resolve_engine(engine="local") is LOCAL_ENGINE

    def test_none_raises(self):
        with pytest.raises(ValueError, match="engine or engine_conn_id is required"):
            resolve_engine()

    def test_both_none_raises(self):
        with pytest.raises(ValueError, match="engine or engine_conn_id is required"):
            resolve_engine(engine=None, engine_conn_id=None)


class TestTollkeeperOperatorPassthrough:
    def test_unknown_operator_passes_through(self, dag, fake_context):
        unregister_fake_strategy()
        inner = FakeOperator(task_id="inner", dag=dag)
        backend = FakeBackend()

        tk = TollkeeperOperator(
            task_id="tollkeeper_task",
            operator=inner,
            table="test_table",
            backend=backend,
            checks=[PassingCheck()],
            engine="local",
            dag=dag,
        )
        result = tk.execute(fake_context)

        assert inner.executed
        assert result is None
        assert len(backend.created) == 0


class TestTollkeeperOperatorLifecycle:
    def test_happy_path_publishes(self, dag, fake_context):
        inner = FakeOperator(task_id="inner", dag=dag)
        backend = FakeBackend()

        tk = TollkeeperOperator(
            task_id="tollkeeper_task",
            operator=inner,
            table="test_table",
            backend=backend,
            checks=[PassingCheck()],
            engine="local",
            dag=dag,
        )
        result = tk.execute(fake_context)

        assert inner.executed
        assert len(backend.published) == 1
        assert backend.published[0][0] == "test_table"
        assert result == "v_0"
        fake_context["ti"].xcom_push.assert_called_once_with(key="tollkeeper_version_ref", value="v_0")

    def test_failed_check_rolls_back(self, dag, fake_context):
        inner = FakeOperator(task_id="inner", dag=dag)
        backend = FakeBackend()

        tk = TollkeeperOperator(
            task_id="tollkeeper_task",
            operator=inner,
            table="test_table",
            backend=backend,
            checks=[FailingCheck()],
            engine="local",
            dag=dag,
        )

        with pytest.raises(AuditFailedError):
            tk.execute(fake_context)

        assert len(backend.published) == 0
        assert len(backend.rolled_back) >= 1

    def test_operator_failure_rolls_back(self, dag, fake_context):
        inner = FakeOperator(task_id="inner", fail=True, dag=dag)
        backend = FakeBackend()

        tk = TollkeeperOperator(
            task_id="tollkeeper_task",
            operator=inner,
            table="test_table",
            backend=backend,
            checks=[PassingCheck()],
            engine="local",
            dag=dag,
        )

        with pytest.raises(RuntimeError, match="FakeOperator failure"):
            tk.execute(fake_context)

        assert len(backend.published) == 0
        assert len(backend.rolled_back) >= 1

    def test_strategy_redirect_called(self, dag, fake_context):
        inner = FakeOperator(task_id="inner", dag=dag)
        backend = FakeBackend()

        tk = TollkeeperOperator(
            task_id="tollkeeper_task",
            operator=inner,
            table="test_table",
            backend=backend,
            checks=[PassingCheck()],
            engine="local",
            dag=dag,
        )
        tk.execute(fake_context)

        assert inner.last_target is None  # restore() clears it

    def test_engine_required(self, dag, fake_context):
        inner = FakeOperator(task_id="inner", dag=dag)
        backend = FakeBackend()

        tk = TollkeeperOperator(
            task_id="tollkeeper_task",
            operator=inner,
            table="test_table",
            backend=backend,
            checks=[PassingCheck()],
            dag=dag,
        )

        with pytest.raises(ValueError, match="engine or engine_conn_id is required"):
            tk.execute(fake_context)


class TestStrategyRegistry:
    def test_registered_operator_returns_strategy(self):
        from airflow_tollkeeper.strategy import strategy_registry

        from .conftest import FakeOperator, FakeStrategy

        strategy = strategy_registry.get(FakeOperator)
        assert isinstance(strategy, FakeStrategy)

    def test_unregistered_operator_returns_none(self):
        from airflow_tollkeeper.strategy import strategy_registry

        strategy = strategy_registry.get(type("UnknownOp", (), {}))
        assert strategy is None
