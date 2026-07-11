from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow_tollkeeper.compat import BaseSensorOperator

if TYPE_CHECKING:
    from tollkeeper.signals.base import SignalStore


class TollkeeperSensor(BaseSensorOperator):
    """Pokes a SignalStore until a table's audit signal appears.

    Use this to gate downstream tasks on upstream Tollkeeper publish completion,
    decoupling downstream from the upstream task's Airflow state.
    """

    template_fields = ("execution_ctx",)

    def __init__(
        self,
        *,
        table: str,
        signal_store: SignalStore,
        execution_ctx: dict | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.table = table
        self.signal_store = signal_store
        self.execution_ctx = execution_ctx

    def poke(self, context: Any) -> bool:
        signal = self.signal_store.check(self.table, self.execution_ctx)
        return signal is not None
