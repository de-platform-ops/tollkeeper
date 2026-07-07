# write-audit-publish

Isolate pipeline writes into versioned branches, gate promotion on data quality checks, and publish to production with a pointer swap.

## Why

Data pipelines that copy staging tables to production are slow, expensive, and hard to revert. Modern table formats (Iceberg, Delta Lake) support branch-based versioning where publishing is a pointer swap (milliseconds, zero data movement) and reverting is another pointer swap.

`write-audit-publish` codifies this pattern into a Python library with a fluent API.

## Install

```bash
pip install write-audit-publish
```

Requires Python 3.11+.

## Usage

```python
from write_audit_publish import WAP
from write_audit_publish.backends.iceberg import IcebergBackend
from write_audit_publish.checks.base import BaseCheck, CheckResult

# Define a check
class RowCountCheck(BaseCheck):
    def __init__(self, min_rows: int) -> None:
        self._min_rows = min_rows

    def run(self, version_ref: str) -> CheckResult:
        count = query_count(version_ref)  # your logic
        return CheckResult(
            check_name=self.name,
            passed=count >= self._min_rows,
            details=f"got {count}, expected >= {self._min_rows}",
        )

# Write, audit, publish
backend = IcebergBackend(catalog)

(WAP(backend)
    .table("db.my_table")
    .write(lambda ref: spark.sql(f"INSERT INTO db.my_table.branch_{ref} ..."))
    .audit([RowCountCheck(min_rows=1000)])
    .publish())
```

If any check fails, the version is rolled back automatically. Production is never touched.

### Soft failures

Pass `on_failure="continue"` to publish despite failed checks (with an optional notification callback):

```python
(WAP(backend)
    .table("db.my_table")
    .write(lambda ref: load_data(ref))
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

- `.table(name)` creates an isolated version and returns a `WAPSession`.

### `WAPSession`

Returned by `.table()`. Chain methods:

| Method | Description |
|--------|-------------|
| `.write(fn)` | Calls `fn(version_ref)` so your code writes to the staged version |
| `.audit(checks, *, on_failure="stop", on_notify=None)` | Runs checks against the staged version |
| `.publish()` | Promotes the staged version to production |
| `.rollback()` | Discards the staged version |
| `.ref` | The version reference string (branch name, snapshot ID) |
| `.report` | `CheckReport` with `.passed`, `.failed`, `.results` |

### `Backend` (ABC)

Implement for your table format:

| Method | Purpose |
|--------|---------|
| `create_version(table) -> str` | Create isolated staging version, return a ref |
| `publish_version(table, ref)` | Promote staged version to production |
| `rollback_version(table, ref)` | Discard staged version |

### `BaseCheck` (ABC)

Implement `run(version_ref) -> CheckResult` for each data quality check.

## Backends

| Backend | Status |
|---------|--------|
| Iceberg | Scaffold (in progress) |
| Delta Lake | Planned |
| Snowflake | Planned |

## Development

```bash
git clone https://github.com/sadha/write-audit-publish.git
cd write-audit-publish
uv sync --group dev
uv run pytest tests/ -v
uv run ruff check src/ tests/
```

## License

MIT
