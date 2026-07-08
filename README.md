# write-audit-publish

[![CI](https://github.com/srchilukoori/write-audit-publish/actions/workflows/ci.yml/badge.svg)](https://github.com/srchilukoori/write-audit-publish/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Isolate pipeline writes into versioned staging, gate promotion on data quality checks, and publish to production only when checks pass.

## Why

Data pipelines that write directly to production tables are fragile. A bad upstream transformation can corrupt production data before anyone notices. The write-audit-publish (WAP) pattern solves this by staging writes in isolation, running data quality checks, and only promoting to production when all checks pass.

`write-audit-publish` codifies this pattern into a Python library with a fluent API and pluggable backends.

## Install

```bash
pip install write-audit-publish
```

Requires Python 3.11+.

## Quick start

```python
import shutil
from pathlib import Path

from write_audit_publish import WAP, CsvBackend, NullCheck, RowCountCheck

backend = CsvBackend(staging_dir=Path("/tmp/wap"), publish_dir=Path("/data/output"))

(WAP(backend)
    .table("sales")
    .write(lambda ref: shutil.copy("upstream_output.csv", ref))
    .audit([NullCheck("id"), RowCountCheck(min_rows=100)])
    .publish())
```

If any check fails, the staged file is rolled back automatically. Production is never touched.

## How it works

```
Upstream CSV ──► staging copy (renamed) ──► DQ checks ──► publish to final path
                                                       ──► or rollback (delete staging)
```

1. **write**: Your callback copies upstream data into the staging location
2. **audit**: DQ checks run against the staged data using the configured engine (Polars, SQL, etc.)
3. **publish**: If checks pass, staging is promoted to the final destination
4. **rollback**: If checks fail (hard mode), staging is deleted automatically

## Data quality checks

Five built-in checks using Polars as the DQ engine:

| Check | Constructor | Passes when |
|-------|-------------|-------------|
| `NullCheck` | `NullCheck("column")` | No nulls in column |
| `RowCountCheck` | `RowCountCheck(min_rows=100)` | Row count >= threshold |
| `ExpressionCheck` | `ExpressionCheck("name", pl.col("age") > 0)` | All rows satisfy the Polars expression |
| `SqlCheck` | `SqlCheck("name", "age > 0 AND score >= 0")` | All rows satisfy the SQL WHERE condition |
| `UniqueCheck` | `UniqueCheck(["region", "date"])` | No duplicate groups on the given columns |

### Custom checks

Subclass `BaseCheck` to create checks with any engine (Pandas, Presto, etc.):

```python
from write_audit_publish import BaseCheck, CheckResult

class SchemaCheck(BaseCheck):
    def __init__(self, expected_columns: list[str]) -> None:
        self._expected = expected_columns

    def run(self, version_ref: str) -> CheckResult:
        import polars as pl
        df = pl.read_csv(version_ref)
        missing = set(self._expected) - set(df.columns)
        return CheckResult(
            check_name=self.name,
            passed=len(missing) == 0,
            details=f"missing columns: {missing}" if missing else "all columns present",
        )
```

### Soft failures

Pass `on_failure="continue"` to publish despite failed checks, with an optional notification callback:

```python
(WAP(backend)
    .table("sales")
    .write(lambda ref: shutil.copy(src, ref))
    .audit(
        [RowCountCheck(min_rows=1000)],
        on_failure="continue",
        on_notify=lambda table, ref, failed: log.warning(f"{table}@{ref}: {failed}"),
    )
    .publish())
```

## API

### `WAP(backend)`

Entry point. Takes a `Backend` instance.

- `.table(name)` creates an isolated staging version and returns a `WAPSession`.

### `WAPSession`

Returned by `.table()`. Supports fluent chaining:

| Method | Description |
|--------|-------------|
| `.write(fn)` | Calls `fn(version_ref)` so your code writes to the staged version |
| `.audit(checks, *, on_failure="stop", on_notify=None)` | Runs checks against the staged version |
| `.publish()` | Promotes the staged version to production |
| `.rollback()` | Discards the staged version |
| `.ref` | The version reference string |
| `.report` | `CheckReport` with `.passed`, `.failed`, `.results` |

### `Backend` (ABC)

Implement for your storage layer:

| Method | Purpose |
|--------|---------|
| `create_version(table) -> str` | Create isolated staging version, return a reference |
| `publish_version(table, ref)` | Promote staged version to production |
| `rollback_version(table, ref)` | Discard staged version |

### `BaseCheck` (ABC)

Implement `run(version_ref) -> CheckResult` for each data quality check. The DQ engine is decoupled from the storage backend.

## Backends

| Backend | Status | Description |
|---------|--------|-------------|
| `CsvBackend` | Available | Local CSV files with staging/publish directories |
| Iceberg | Planned | Branch-based versioning with pointer-swap publish |
| Delta Lake | Planned | |

## Development

```bash
git clone https://github.com/srchilukoori/write-audit-publish.git
cd write-audit-publish
uv sync --group dev --group docs
uv run pytest tests/ -v
uv run ruff check src/ tests/
uv run mkdocs serve        # local docs at http://127.0.0.1:8000
```

## License

[Apache License 2.0](LICENSE)
