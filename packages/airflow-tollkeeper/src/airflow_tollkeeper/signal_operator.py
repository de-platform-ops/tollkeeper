from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from airflow_tollkeeper.compat import BaseOperator

if TYPE_CHECKING:
    from tollkeeper.signals.base import SignalStore


class TollkeeperSignalEmitter(BaseOperator):
    """Reads DQ results and emits a signal if all checks passed.

    Queries ``tollkeeper_dq_results`` for the target table. If every check
    passed, writes a success signal to the signal store. Otherwise raises
    ``AirflowFailException`` (or continues, depending on ``on_failure``).
    """

    template_fields = ("execution_ctx",)

    def __init__(
        self,
        *,
        table: str,
        signal_store: SignalStore,
        expected_checks: list[str],
        execution_ctx: dict | None = None,
        on_failure: str = "stop",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.table = table
        self.signal_store = signal_store
        self.expected_checks = expected_checks
        self.execution_ctx = execution_ctx
        self.on_failure = on_failure

    def execute(self, context: Any) -> str:
        results = self.signal_store.get_dq_results(self.table, self.execution_ctx)
        results_by_name = {r.check_name: r for r in results}

        missing = [name for name in self.expected_checks if name not in results_by_name]
        if missing:
            raise RuntimeError(f"Missing DQ results for {self.table}: {missing}")

        failed = [r for r in results if not r.passed]

        if failed:
            names = ", ".join(r.check_name for r in failed)
            msg = f"DQ checks failed for {self.table}: {names}"
            self.log.error(msg)
            if self.on_failure == "stop":
                raise RuntimeError(msg)
            return "failed"

        from tollkeeper.signals.base import Signal

        self.signal_store.write(
            Signal(
                table_name=self.table,
                execution_ctx=self.execution_ctx or {},
                status="passed",
                execution_ts=datetime.now(),
                check_summary=f"{len(results)} checks passed",
            )
        )
        self.log.info("All %d DQ checks passed for '%s', signal emitted", len(results), self.table)
        return "passed"
