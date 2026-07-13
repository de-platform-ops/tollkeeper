# Getting Started

## Installation

```bash
pip install tollkeeper                # core only, zero deps
pip install "tollkeeper[polars]"       # + Polars DQ checks
pip install "tollkeeper[iceberg]"      # + PyIceberg backend
pip install "tollkeeper[sql]"          # + sqlglot lineage parser
pip install "tollkeeper[all]"          # everything
```

For Airflow integration:

```bash
pip install airflow-tollkeeper
```

Requires Python 3.11+.

## Basic CSV example

Write-audit-publish with `CsvBackend`:

```python
import shutil
from pathlib import Path

from tollkeeper import Tollkeeper, CsvBackend, NullCheck, RowCountCheck

backend = CsvBackend(staging_dir=Path("/tmp/tollkeeper"), publish_dir=Path("/data/output"))

(Tollkeeper(backend)
    .table("sales")
    .write(lambda ref: shutil.copy("upstream_output.csv", ref))
    .audit([NullCheck("id"), RowCountCheck(min_rows=100)])
    .publish())
```

If any check fails, the staged file is rolled back automatically. Production is never touched.

## Context manager usage

```python
with Tollkeeper(backend).table("sales") as session:
    session.write(lambda ref: shutil.copy("upstream_output.csv", ref))
    session.audit([NullCheck("id")])
    session.publish()
# Auto-rollback on exception or if publish() was never called
```

## Signal store basics

Coordinate across pipelines by tracking table readiness:

```python
from tollkeeper import Tollkeeper, SqliteSignalStore

signal_store = SqliteSignalStore("/tmp/signals.db")
tk = Tollkeeper(backend, signal_store=signal_store)

# A signal is emitted automatically after a successful audit + publish.
# Downstream pipelines can check readiness:
signal = signal_store.check("sales", {"ds": "2026-01-15"})
```

`DbApiSignalStore` works the same way over any DB-API 2.0 connection (Postgres, MySQL, etc.) instead of SQLite.

## Next steps

See the [API Reference](api.md) for the full surface: backends, checks, signal stores, and the SQL lineage parser.
