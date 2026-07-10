"""Airflow 2.x / 3.x import compatibility."""

from __future__ import annotations

try:
    from airflow.sdk import BaseOperator, DAG
except ImportError:
    from airflow.models import BaseOperator, DAG  # type: ignore[assignment,no-redef]

try:
    from airflow.sdk import BaseSensorOperator
except ImportError:
    from airflow.sensors.base import BaseSensorOperator  # type: ignore[assignment,no-redef]

__all__ = ["BaseOperator", "BaseSensorOperator", "DAG"]
