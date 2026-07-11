from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow_wap.compat import BaseSensorOperator

if TYPE_CHECKING:
    from write_audit_publish.signals.base import SignalStore


class WAPSensor(BaseSensorOperator):
    """Pokes a SignalStore until a table's audit signal appears.

    Use this to gate downstream tasks on upstream WAP publish completion,
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
