from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow_wap.compat import BaseOperator


class WAPStrategy(ABC):
    """Defines how to redirect an operator's writes to a WAP staging version."""

    @abstractmethod
    def redirect(self, operator: BaseOperator, version_ref: str) -> None:
        """Mutate operator config to write to the staging version."""

    @abstractmethod
    def restore(self, operator: BaseOperator) -> None:
        """Undo redirect after execution."""


class StrategyRegistry:
    """Maps operator types to WAP strategies. O(1) lookup by operator class."""

    def __init__(self) -> None:
        self._strategies: dict[type[BaseOperator], type[WAPStrategy]] = {}

    def register(self, operator_cls: type[BaseOperator], strategy_cls: type[WAPStrategy]) -> None:
        self._strategies[operator_cls] = strategy_cls

    def get(self, operator_cls: type[BaseOperator]) -> WAPStrategy | None:
        strategy_cls = self._strategies.get(operator_cls)
        return strategy_cls() if strategy_cls else None


strategy_registry = StrategyRegistry()
