from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow_tollkeeper.compat import BaseOperator


class TollkeeperStrategy(ABC):
    """Defines how to redirect an operator's writes to a Tollkeeper staging version."""

    @abstractmethod
    def redirect(self, operator: BaseOperator, version_ref: str) -> None:
        """Mutate operator config to write to the staging version."""

    @abstractmethod
    def restore(self, operator: BaseOperator) -> None:
        """Undo redirect after execution."""


class StrategyRegistry:
    """Maps operator types to Tollkeeper strategies. O(1) lookup by operator class."""

    def __init__(self) -> None:
        self._strategies: dict[type[BaseOperator], type[TollkeeperStrategy]] = {}

    def register(self, operator_cls: type[BaseOperator], strategy_cls: type[TollkeeperStrategy]) -> None:
        self._strategies[operator_cls] = strategy_cls

    def get(self, operator_cls: type[BaseOperator]) -> TollkeeperStrategy | None:
        strategy_cls = self._strategies.get(operator_cls)
        return strategy_cls() if strategy_cls else None


class PassThroughStrategy(TollkeeperStrategy):
    """No-op strategy for operators that write directly to the target table.

    SQL operators (SQLExecuteQueryOperator, SparkSqlOperator) don't need
    query rewriting. The operator runs its original SQL unchanged while
    Tollkeeper still enforces the audit/signal lifecycle around it.
    """

    def redirect(self, operator: BaseOperator, version_ref: str) -> None:
        pass

    def restore(self, operator: BaseOperator) -> None:
        pass


strategy_registry = StrategyRegistry()


def register_defaults() -> list[str]:
    """Register PassThroughStrategy for known SQL operators.

    Returns the list of operator class names that were registered.
    Safe to call if providers are not installed.
    """
    registered: list[str] = []
    _operators = [
        "airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator",
        "airflow.providers.apache.spark.operators.spark_sql.SparkSqlOperator",
    ]
    for fqn in _operators:
        module_path, cls_name = fqn.rsplit(".", 1)
        try:
            import importlib

            mod = importlib.import_module(module_path)
            op_cls = getattr(mod, cls_name)
            strategy_registry.register(op_cls, PassThroughStrategy)
            registered.append(cls_name)
        except (ImportError, AttributeError):
            pass
    return registered
