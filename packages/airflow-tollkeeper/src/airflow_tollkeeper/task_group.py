from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from airflow_tollkeeper.compat import TaskGroup
from airflow_tollkeeper.dq_operator import DqSqlCheck, TollkeeperDqOperator
from airflow_tollkeeper.operator import TollkeeperOperator
from airflow_tollkeeper.sensor import TollkeeperSensor
from airflow_tollkeeper.signal_operator import TollkeeperSignalEmitter

if TYPE_CHECKING:
    from airflow_tollkeeper.compat import BaseOperator, DAG
    from tollkeeper.backends.base import Backend
    from tollkeeper.checks.base import BaseCheck
    from tollkeeper.signals.base import SignalStore

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

    from tollkeeper.parser import extract_lineage

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


def tollkeeper_task_group(
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
    gid = group_id or f"tollkeeper_{table_slug}"
    tg_kwargs: dict[str, Any] = {"group_id": gid}
    if dag is not None:
        tg_kwargs["dag"] = dag

    with TaskGroup(**tg_kwargs) as tg:
        sensors = []
        for src in sorted(resolved_sources):
            sensor_id = f"wait_{_slugify(src)}"
            sensor = TollkeeperSensor(
                task_id=sensor_id,
                table=src,
                signal_store=signal_store,
                execution_ctx=execution_ctx,
            )
            sensors.append(sensor)

        tk_op = TollkeeperOperator(
            task_id=f"tollkeeper_{table_slug}",
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
            sensor >> tk_op

    return tg


def tollkeeper_sql_task_group(
    *,
    sql_operator: BaseOperator,
    table: str,
    dq_checks: list[DqSqlCheck],
    signal_store: SignalStore,
    conn_id: str,
    group_id: str | None = None,
    sql: str | None = None,
    dialect: str | None = None,
    sources: list[str] | None = None,
    execution_ctx: dict | None = None,
    on_failure: str = "stop",
    dag: DAG | None = None,
) -> TaskGroup:
    """Build a task group for SQL passthrough operators with DB-native DQ checks.

    Creates the flow::

        [upstream sensors] >> sql_operator >> [dq checks] >> signal_emitter

    Each DQ check is a separate Airflow task that runs a validation SQL query
    and stores the result in ``tollkeeper_dq_results``. The signal emitter
    reads all results and writes a signal if every check passed.
    """
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
    gid = group_id or f"tollkeeper_{table_slug}"
    tg_kwargs: dict[str, Any] = {"group_id": gid}
    if dag is not None:
        tg_kwargs["dag"] = dag

    with TaskGroup(**tg_kwargs) as tg:
        sensors = []
        for src in sorted(resolved_sources):
            sensor_id = f"wait_{_slugify(src)}"
            sensor = TollkeeperSensor(
                task_id=sensor_id,
                table=src,
                signal_store=signal_store,
                execution_ctx=execution_ctx,
            )
            sensors.append(sensor)

        dq_ops = []
        for check in dq_checks:
            formatted_sql = check.sql.format(table=table)
            dq_op = TollkeeperDqOperator(
                task_id=f"dq_{_slugify(check.name)}",
                check_name=check.name,
                check_sql=formatted_sql,
                table=table,
                conn_id=conn_id,
                signal_store=signal_store,
                execution_ctx=execution_ctx,
            )
            dq_ops.append(dq_op)

        signal_emitter = TollkeeperSignalEmitter(
            task_id=f"signal_{table_slug}",
            table=table,
            signal_store=signal_store,
            expected_checks=[c.name for c in dq_checks],
            execution_ctx=execution_ctx,
            on_failure=on_failure,
        )

        for sensor in sensors:
            sensor >> sql_operator

        for dq_op in dq_ops:
            sql_operator >> dq_op >> signal_emitter

    return tg
