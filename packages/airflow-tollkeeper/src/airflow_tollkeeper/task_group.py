from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from airflow_wap.compat import TaskGroup
from airflow_wap.operator import WAPOperator
from airflow_wap.sensor import WAPSensor

if TYPE_CHECKING:
    from airflow_wap.compat import BaseOperator, DAG
    from write_audit_publish.backends.base import Backend
    from write_audit_publish.checks.base import BaseCheck
    from write_audit_publish.signals.base import SignalStore

logger = logging.getLogger(__name__)


def _slugify(name: str) -> str:
    return name.replace(".", "__")


def _resolve_sources(
    *,
    sources: list[str] | None,
    sql: str | None,
    sql_operator: BaseOperator,
    table: str,
    dialect: str | None,
) -> list[str]:
    if sources is not None:
        return list(dict.fromkeys(sources))

    raw_sql = sql or getattr(sql_operator, "sql", None)
    if raw_sql is None:
        return []

    from write_audit_publish.parser import extract_lineage

    try:
        result = extract_lineage(raw_sql, dialect=dialect)
    except ValueError:
        logger.warning("Could not parse SQL for lineage, creating TaskGroup with no sensors", exc_info=True)
        return []

    if result.sinks and table not in result.sinks:
        raise ValueError(
            f"Parsed sink {result.sinks} does not match table '{table}'. Check the SQL or pass sources= explicitly."
        )

    return list(result.sources)


def wap_task_group(
    *,
    sql_operator: BaseOperator,
    table: str,
    backend: Backend,
    checks: list[BaseCheck],
    signal_store: SignalStore,
    group_id: str | None = None,
    sql: str | None = None,
    dialect: str | None = None,
    sources: list[str] | None = None,
    engine: str | None = None,
    engine_conn_id: str | None = None,
    execution_ctx: dict | None = None,
    on_failure: str = "stop",
    dag: DAG | None = None,
) -> TaskGroup:
    resolved_sources = _resolve_sources(
        sources=sources,
        sql=sql,
        sql_operator=sql_operator,
        table=table,
        dialect=dialect,
    )

    if table in resolved_sources:
        raise ValueError(f"Circular dependency: table '{table}' appears in its own sources {resolved_sources}")

    table_slug = _slugify(table)
    gid = group_id or f"wap_{table_slug}"
    tg_kwargs: dict[str, Any] = {"group_id": gid}
    if dag is not None:
        tg_kwargs["dag"] = dag

    with TaskGroup(**tg_kwargs) as tg:
        sensors = []
        for src in sorted(resolved_sources):
            sensor_id = f"wait_{_slugify(src)}"
            sensor = WAPSensor(
                task_id=sensor_id,
                table=src,
                signal_store=signal_store,
                execution_ctx=execution_ctx,
            )
            sensors.append(sensor)

        wap_op = WAPOperator(
            task_id=f"wap_{table_slug}",
            operator=sql_operator,
            table=table,
            backend=backend,
            checks=checks,
            signal_store=signal_store,
            engine=engine,
            engine_conn_id=engine_conn_id,
            execution_ctx=execution_ctx,
            on_failure=on_failure,
        )

        for sensor in sensors:
            sensor >> wap_op

    return tg
