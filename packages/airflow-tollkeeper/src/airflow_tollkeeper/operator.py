from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow_wap.compat import BaseOperator

from airflow_wap.engine import LOCAL_ENGINE, resolve_engine
from airflow_wap.strategy import strategy_registry

if TYPE_CHECKING:
    from write_audit_publish.backends.base import Backend
    from write_audit_publish.checks.base import BaseCheck
    from write_audit_publish.signals.base import SignalStore


class WAPOperator(BaseOperator):
    """Wraps any Airflow operator in a Write-Audit-Publish lifecycle.

    The wrapped operator's writes are redirected to a staging version via a
    registered ``WAPStrategy``. DQ checks run on a remote engine (or in-process
    with ``engine="local"``). On success, the version is published and a signal
    is emitted. On failure, the version is rolled back.

    Operators without a registered strategy pass through unchanged (no WAP
    lifecycle, checks are skipped).
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
            self.operator.execute(context)
            return None

        engine_conn = resolve_engine(self.engine, self.engine_conn_id)

        from write_audit_publish.core import WAP

        wap = WAP(self.backend, signal_store=self.signal_store)
        with wap.table(self.table) as session:
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

        context["ti"].xcom_push(key="wap_version_ref", value=session.ref)
        return session.ref
