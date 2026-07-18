# Backends

A backend manages the lifecycle of a versioned table: creating an isolated staging area, promoting it to production, or rolling it back. Tollkeeper ships three backends and an ABC for writing your own.

## Backend ABC

Every backend implements three methods:

| Method | Purpose |
|--------|---------|
| `create_version(table)` | Create an isolated staging area. Returns an opaque version reference (file path, branch name, etc.). |
| `publish_version(table, version_ref)` | Promote staged data to production. |
| `rollback_version(table, version_ref)` | Discard staged data without publishing. |

```python
from tollkeeper.backends.base import Backend

class MyBackend(Backend):
    def create_version(self, table: str) -> str: ...
    def publish_version(self, table: str, version_ref: str) -> None: ...
    def rollback_version(self, table: str, version_ref: str) -> None: ...
```

## CsvBackend

Local CSV files with staging and publish directories. Zero dependencies.

```python
from pathlib import Path
from tollkeeper import Tollkeeper, CsvBackend

backend = CsvBackend(
    staging_dir=Path("/tmp/tollkeeper"),
    publish_dir=Path("/data/output"),
)
```

**How it works:**

1. `create_version` creates a temp file in `staging_dir` (e.g. `sales.tollkeeper-a1b2c3d4.csv`).
2. Your write callback writes data to that path.
3. `publish_version` atomically moves the file to `publish_dir/sales.csv`.
4. `rollback_version` deletes the staging file.

Cross-device moves fall back to copy-then-delete when `os.replace` raises `OSError`.

### Cleanup orphaned staging files

If a pipeline crashes mid-write, staging files can accumulate. Clean them up:

```python
removed = backend.cleanup_staging(max_age_seconds=3600)
```

This removes any `*.tollkeeper-*.csv` file in `staging_dir` older than the threshold.

## IcebergBackend

Branch-based isolation for Apache Iceberg tables via PyIceberg. Install with `tollkeeper[iceberg]`.

```python
from pyiceberg.catalog.sql import SqlCatalog
from tollkeeper import Tollkeeper, IcebergBackend

catalog = SqlCatalog("default", warehouse="/tmp/warehouse", uri="sqlite:///catalog.db")
backend = IcebergBackend(catalog)
```

**How it works:**

1. `create_version` creates a branch (`tollkeeper-<hash>`) from the current snapshot.
2. Your write callback appends data to that branch (pass the branch name to PyIceberg's `branch=` parameter).
3. `publish_version` fast-forwards `main` to the branch snapshot, then drops the branch.
4. `rollback_version` drops the branch. Production is untouched.

```python
(Tollkeeper(backend)
    .table("db.sales")
    .write(lambda ref: iceberg_table.append(df, branch=ref))
    .audit([NullCheck("amount")])
    .publish())
```

The table must have at least one snapshot before Tollkeeper can branch from it.

## SqlPassthroughBackend

For standard SQL tables with no physical versioning. Writes go directly to the production table. Tollkeeper still enforces the audit and signal lifecycle, but isolation is orchestration-level only: downstream sensors gate on the signal, not a physical branch.

```python
from tollkeeper import Tollkeeper
from tollkeeper.backends.sql_passthrough import SqlPassthroughBackend

backend = SqlPassthroughBackend()
```

**How it works:**

1. `create_version` returns the table name as-is (no staging area).
2. `publish_version` is a no-op (data is already written).
3. `rollback_version` logs a warning. The data cannot be un-written. The DAG halts, preventing downstream signal emission.

Use this when your storage layer (e.g. Hive, BigQuery) does not support branching but you still want DQ gating and signal coordination.

## Choosing a backend

| Backend | Isolation | Rollback | Dependencies | Use when |
|---------|-----------|----------|--------------|----------|
| `CsvBackend` | File-level | Delete staging file | None | Local development, file-based pipelines |
| `IcebergBackend` | Branch-level | Drop branch | `pyiceberg` | Iceberg tables with pointer-swap publish |
| `SqlPassthroughBackend` | None (orchestration only) | Cannot undo writes | None | SQL tables without branching support |

## Writing a custom backend

Subclass `Backend` and implement the three methods. The version reference returned by `create_version` is opaque to Tollkeeper. It gets passed to your write callback, then back to `publish_version` or `rollback_version`.

```python
from tollkeeper.backends.base import Backend

class DeltaBackend(Backend):
    def __init__(self, table_path: str) -> None:
        self._path = table_path

    def create_version(self, table: str) -> str:
        # Create a Delta version/branch/staging area
        ...

    def publish_version(self, table: str, version_ref: str) -> None:
        # Promote to production
        ...

    def rollback_version(self, table: str, version_ref: str) -> None:
        # Discard the staged version
        ...
```

Pass your backend to `Tollkeeper(backend)` and the rest of the API works unchanged.
