from tollkeeper.backends.base import Backend
from tollkeeper.backends.csv import CsvBackend
from tollkeeper.checks.base import BaseCheck, CheckResult
from tollkeeper.core import Tollkeeper, AuditFailedError, CheckReport, TollkeeperSession
from tollkeeper.signals.base import Signal, SignalStore
from tollkeeper.signals.dbapi import DbApiSignalStore
from tollkeeper.signals.sqlite import SqliteSignalStore

__all__ = [
    "Tollkeeper",
    "TollkeeperSession",
    "CheckReport",
    "AuditFailedError",
    "Backend",
    "BaseCheck",
    "CheckResult",
    "CsvBackend",
    "DbApiSignalStore",
    "Signal",
    "SignalStore",
    "SqliteSignalStore",
]

try:
    from tollkeeper.backends.iceberg import IcebergBackend

    __all__ += ["IcebergBackend"]
except ImportError:
    pass

try:
    from tollkeeper.checks.polars import ExpressionCheck, NullCheck, RowCountCheck, SqlCheck, UniqueCheck

    __all__ += ["ExpressionCheck", "NullCheck", "RowCountCheck", "SqlCheck", "UniqueCheck"]
except ImportError:
    pass

try:
    from tollkeeper.parser import ParseResult, extract_lineage

    __all__ += ["ParseResult", "extract_lineage"]
except ImportError:
    pass
