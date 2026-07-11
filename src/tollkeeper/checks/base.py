from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class CheckResult:
    """Result of a single DQ check execution.

    Attributes:
        check_name: Name of the check that produced this result.
        passed: Whether the check passed.
        details: Human-readable details (e.g. ``"3 nulls in 'id'"``).
    """

    check_name: str
    passed: bool
    details: str = ""


class BaseCheck(ABC):
    """Abstract base for data quality checks.

    Subclass this to create checks using any engine (Polars, Pandas, Presto, etc.).
    The ``run`` method receives a version reference (e.g. a file path or table name)
    and returns a ``CheckResult``.
    """

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def run(self, version_ref: str, *, conn: Any | None = None) -> CheckResult:
        """Execute the check against the given version reference.

        Args:
            version_ref: Backend-specific reference to the staged data.
            conn: Optional engine connection for remote check execution.
                When ``None``, checks run in-process.

        Returns:
            A ``CheckResult`` indicating pass/fail with details.
        """
