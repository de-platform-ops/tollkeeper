from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow_tollkeeper.compat import BaseOperator

from airflow_tollkeeper.engine import LOCAL_ENGINE, resolve_engine
from airflow_tollkeeper.strategy import strategy_registry

if TYPE_CHECKING:
    from tollkeeper.backends.base import Backend
    from tollkeeper.checks.base import BaseCheck
    from tollkeeper.signals.base import SignalStore


class TollkeeperOperator(BaseOperator):
    """Wraps any Airflow operator in a Write-Audit-Publish lifecycle.

    The wrapped operator's writes are redirected to a staging version via a
    registered ``TollkeeperStrategy``. DQ checks run on a remote engine (or in-process
    with ``engine="local"``). On success, the version is published and a signal
    is emitted. On failure, the version is rolled back.

    Operators without a registered strategy raise ``TypeError``. Use
    ``PassThroughStrategy`` for operators that need no SQL rewriting.
    """

    def __init__(
        self,
        *,
        operator: BaseOperator,
        table: str,
        backend: Backend,
        checks: list[BaseCheck],
        engine: str | None = None,
        engine_conn_id: str | None = None,
        signal_store: SignalStore | None = None,
        on_failure: str = "stop",
        execution_ctx: dict | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.operator = operator
        self.table = table
        self.backend = backend
        self.checks = checks
        self.engine = engine
        self.engine_conn_id = engine_conn_id
        self.signal_store = signal_store
        self.on_failure = on_failure
        self.execution_ctx = execution_ctx

    def execute(self, context: Any) -> str | None:
        strategy = strategy_registry.get(type(self.operator))

        if strategy is None:
            raise TypeError(
                f"No TollkeeperStrategy registered for {type(self.operator).__name__}. "
                f"Register a strategy via strategy_registry.register() or use PassThroughStrategy."
            )

        engine_conn = resolve_engine(self.engine, self.engine_conn_id)

        from tollkeeper.core import Tollkeeper

        tk = Tollkeeper(self.backend, signal_store=self.signal_store)
        with tk.table(self.table) as session:
            strategy.redirect(self.operator, session.ref)
            try:
                self.operator.execute(context)
            finally:
                strategy.restore(self.operator)

            conn_arg = engine_conn if engine_conn is not LOCAL_ENGINE else None
            session.audit(
                self.checks,
                on_failure=self.on_failure,
                execution_ctx=self.execution_ctx,
                conn=conn_arg,
            )
            session.publish()

        context["ti"].xcom_push(key="tollkeeper_version_ref", value=session.ref)
        return session.ref
