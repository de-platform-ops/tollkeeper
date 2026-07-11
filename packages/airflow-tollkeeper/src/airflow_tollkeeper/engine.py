from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class LocalEngine:
    """Sentinel: run checks in-process (test mode only)."""


LOCAL_ENGINE = LocalEngine()


def resolve_engine(engine: str | None = None, engine_conn_id: str | None = None) -> Any:
    """Resolve an engine keyword or connection ID to a usable connection.

    Args:
        engine: Engine keyword (e.g. ``"spark"``, ``"local"``).
            Keywords other than ``"local"`` resolve to an Airflow connection
            named ``tollkeeper_engine_{keyword}``.
        engine_conn_id: Explicit Airflow connection ID, overrides ``engine``.

    Returns:
        An Airflow ``Connection`` object, or ``LOCAL_ENGINE`` for test mode.

    Raises:
        ValueError: If neither ``engine`` nor ``engine_conn_id`` is provided.
        airflow.exceptions.AirflowNotFoundException: If the resolved connection
            ID does not exist in Airflow's connection store.
    """
    if engine_conn_id:
        from airflow.hooks.base import BaseHook

        return BaseHook.get_connection(engine_conn_id)

    if engine is None:
        raise ValueError("engine or engine_conn_id is required. Use engine='local' for in-process test mode.")

    if engine == "local":
        return LOCAL_ENGINE

    from airflow.hooks.base import BaseHook

    conn_id = f"tollkeeper_engine_{engine}"
    return BaseHook.get_connection(conn_id)
