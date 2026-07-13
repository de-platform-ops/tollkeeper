# Data Quality Checks

Tollkeeper runs DQ checks against staged data before publishing. If any check fails, the staged version is rolled back and production stays untouched.

## Built-in checks (Polars)

Install the `polars` extra:

```bash
pip install tollkeeper[polars]
```

Five checks ship with tollkeeper:

### NullCheck

Fails if a column contains any null values.

```python
from tollkeeper import NullCheck

NullCheck("customer_id")
# Result: "3 nulls in 'customer_id'" -> FAIL
```

### RowCountCheck

Fails if the row count is below a threshold.

```python
from tollkeeper import RowCountCheck

RowCountCheck(min_rows=100)
# Result: "47 rows, minimum 100" -> FAIL
```

### UniqueCheck

Fails if any combination of columns has duplicates.

```python
from tollkeeper import UniqueCheck

UniqueCheck(["region", "date"])
# Result: "2 duplicate groups on ['region', 'date']" -> FAIL
```

### ExpressionCheck

Fails if any row does not satisfy a Polars expression.

```python
import polars as pl
from tollkeeper import ExpressionCheck

ExpressionCheck("positive_revenue", pl.col("revenue") > 0)
# Result: "5 rows violate 'positive_revenue'" -> FAIL
```

### SqlCheck

Fails if any row does not satisfy a SQL WHERE condition. Uses Polars' in-memory SQL engine.

```python
from tollkeeper import SqlCheck

SqlCheck("valid_age", "age > 0 AND age < 150")
# Result: "1 rows violate 'valid_age'" -> FAIL
```

The staged data is registered as a table named `data` in the SQL context.

## Using checks in a pipeline

```python
from tollkeeper import Tollkeeper, CsvBackend, NullCheck, RowCountCheck, UniqueCheck

backend = CsvBackend("/data/staging", "/data/prod")

(
    Tollkeeper(backend)
    .table("orders")
    .audit([
        NullCheck("order_id"),
        RowCountCheck(min_rows=1),
        UniqueCheck(["order_id"]),
    ])
    .publish()
)
```

## Failure modes

### Hard failure (default)

All checks run, then if any failed, the staged version is rolled back and `AuditFailedError` is raised:

```python
from tollkeeper import AuditFailedError

try:
    Tollkeeper(backend).table("orders").audit(checks, on_failure="stop").publish()
except AuditFailedError as e:
    print(f"Table: {e.table}")
    print(f"Version: {e.version_ref}")
    for result in e.failed_checks:
        print(f"  {result.check_name}: {result.details}")
```

### Soft failure

Publish proceeds despite failures. Use `on_notify` to handle the failures:

```python
def alert(table, version_ref, failed_checks):
    for check in failed_checks:
        send_alert(f"{table}: {check.check_name} failed - {check.details}")

(
    Tollkeeper(backend)
    .table("orders")
    .audit(checks, on_failure="continue", on_notify=alert)
    .publish()
)
```

## Check report

After auditing, the session's report shows what passed and what failed:

```python
session = Tollkeeper(backend).table("orders").audit(checks)

session.report.passed    # list of CheckResult where passed=True
session.report.failed    # list of CheckResult where passed=False
session.report.results   # all results
```

## Remote check execution

Pass a connection to run checks against a remote engine (e.g., Trino, Presto):

```python
Tollkeeper(backend).table("orders").audit(checks, conn=trino_connection)
```

The `conn` is forwarded to each check's `run()` method. Built-in Polars checks ignore it, but custom checks can use it.

## Writing a custom check

Subclass `BaseCheck` and implement `run()`:

```python
from tollkeeper.checks.base import BaseCheck, CheckResult

class FreshnessCheck(BaseCheck):
    """Fails if the most recent row is older than max_age_hours."""

    def __init__(self, timestamp_col: str, max_age_hours: int = 24) -> None:
        self._col = timestamp_col
        self._max_age = max_age_hours

    def run(self, version_ref, *, conn=None):
        import polars as pl
        from datetime import datetime, timedelta

        df = pl.scan_csv(version_ref).collect()
        newest = df[self._col].cast(pl.Datetime).max()
        cutoff = datetime.now() - timedelta(hours=self._max_age)
        fresh = newest >= cutoff
        return CheckResult(
            check_name=self.name,
            passed=fresh,
            details=f"newest row: {newest}, cutoff: {cutoff}",
        )
```

The `name` property defaults to the class name. Override it for a custom display name.
