from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest
from airflow_wap.compat import BaseOperator, DAG

from airflow_wap.strategy import WAPStrategy, strategy_registry
from write_audit_publish.backends.base import Backend
from write_audit_publish.checks.base import BaseCheck, CheckResult


class FakeBackend(Backend):
    def __init__(self) -> None:
        self.created: list[str] = []
        self.published: list[tuple[str, str]] = []
        self.rolled_back: list[tuple[str, str]] = []

    def create_version(self, table: str) -> str:
        ref = f"v_{len(self.created)}"
        self.created.append(table)
        return ref

    def publish_version(self, table: str, version_ref: str) -> None:
        self.published.append((table, version_ref))

    def rollback_version(self, table: str, version_ref: str) -> None:
        self.rolled_back.append((table, version_ref))


class FakeOperator(BaseOperator):
    def __init__(self, *, fail: bool = False, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.fail = fail
        self.executed = False
        self.last_target: str | None = None

    def execute(self, context: Any) -> None:
        if self.fail:
            raise RuntimeError("FakeOperator failure")
        self.executed = True


class FakeStrategy(WAPStrategy):
    def __init__(self) -> None:
        self._original_task_id: str | None = None

    def redirect(self, operator: BaseOperator, version_ref: str) -> None:
        self._original_task_id = operator.task_id
        if isinstance(operator, FakeOperator):
            operator.last_target = version_ref

    def restore(self, operator: BaseOperator) -> None:
        if isinstance(operator, FakeOperator):
            operator.last_target = None


class PassingCheck(BaseCheck):
    def run(self, version_ref: str, *, conn: Any | None = None) -> CheckResult:
        return CheckResult(check_name="PassingCheck", passed=True)


class FailingCheck(BaseCheck):
    def run(self, version_ref: str, *, conn: Any | None = None) -> CheckResult:
        return CheckResult(check_name="FailingCheck", passed=False, details="always fails")


def register_fake_strategy() -> None:
    strategy_registry.register(FakeOperator, FakeStrategy)


def unregister_fake_strategy() -> None:
    strategy_registry._strategies.pop(FakeOperator, None)


@pytest.fixture()
def dag():
    return DAG(dag_id="test_dag", start_date=datetime(2026, 1, 1))
