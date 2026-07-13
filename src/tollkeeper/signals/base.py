from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable


@dataclass
class DqResult:
    """Result of a single DQ check execution stored in the signal store."""

    table_name: str
    check_name: str
    passed: bool
    details: str = ""
    execution_ctx: dict = field(default_factory=dict)
    executed_at: datetime = field(default_factory=datetime.now)


@dataclass
class Signal:
    """A record indicating a table's data has been audited."""

    table_name: str
    execution_ctx: dict = field(default_factory=dict)
    status: str = "passed"
    execution_ts: datetime = field(default_factory=datetime.now)
    check_summary: str = ""
    metadata: dict = field(default_factory=dict)


class SignalStore(ABC):
    """Abstract base for signal stores that track audit completion."""

    def __init__(self, *, on_delete_callback: Callable[[str, str, dict], None] | None = None) -> None:
        self._on_delete_callback = on_delete_callback

    @abstractmethod
    def write(self, signal: Signal) -> None:
        """UPSERT signal row. Creates or overwrites."""

    @abstractmethod
    def delete(self, table: str, execution_ctx: dict | None = None) -> None:
        """Delete signal and process downstream cascade/notify."""

    @abstractmethod
    def check(self, table: str, execution_ctx: dict | None = None) -> Signal | None:
        """Return signal if it exists, else None."""

    def wait(
        self,
        table: str,
        execution_ctx: dict | None = None,
        *,
        timeout_s: float = 300,
        poll_s: float = 10,
    ) -> Signal:
        """Block until signal appears or timeout."""
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            signal = self.check(table, execution_ctx)
            if signal is not None:
                return signal
            time.sleep(poll_s)
        raise TimeoutError(f"No signal for {table} within {timeout_s}s")

    @abstractmethod
    def write_dq_result(self, result: DqResult) -> None:
        """UPSERT a DQ check result."""

    @abstractmethod
    def get_dq_results(self, table: str, execution_ctx: dict | None = None) -> list[DqResult]:
        """Return all DQ results for a table/execution."""

    @abstractmethod
    def delete_dq_results(self, table: str, execution_ctx: dict | None = None) -> None:
        """Delete all DQ results for a table/execution (before re-running checks)."""

    @abstractmethod
    def register_dep(
        self,
        upstream_table: str,
        downstream_table: str,
        cascade_policy: str = "notify",
        upstream_ctx: dict | None = None,
        downstream_ctx: dict | None = None,
    ) -> None:
        """Declare that downstream depends on upstream."""

    @abstractmethod
    def get_downstream(self, table: str, execution_ctx: dict | None = None) -> list[tuple[str, dict, str]]:
        """Return (table, ctx, cascade_policy) for all downstream dependents."""

    def close(self) -> None:
        """Release underlying resources. Override in subclasses that hold connections."""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
