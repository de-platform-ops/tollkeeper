# Airflow Integration

`airflow-tollkeeper` wraps Airflow tasks in Tollkeeper's write-audit-publish lifecycle:
stage a write, run DQ checks, publish on pass, roll back on fail, emit a readiness signal.

## Installation

```bash
pip install airflow-tollkeeper
```

Requires `tollkeeper` core (pulled in as a dependency) and Airflow 2.x or 3.x.

## Quick start

The fastest path is `tollkeeper_sql_task_group` with a `SQLExecuteQueryOperator`. It builds:

```
[upstream sensors] >> sql_operator >> [dq checks] >> signal_emitter
```

```python
from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from airflow_tollkeeper import DqSqlCheck, register_defaults, tollkeeper_sql_task_group
from airflow_tollkeeper.compat import DAG
from tollkeeper.signals.sqlite import SqliteSignalStore

register_defaults()

signal_store = SqliteSignalStore("/tmp/tollkeeper_signals.db")

with DAG(
    dag_id="example_postgres_sql",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    pg_op = SQLExecuteQueryOperator(
        task_id="upsert_users",
        conn_id="postgres_default",
        sql=(
            "INSERT INTO public.dim_users (id, name, email, updated_at) "
            "SELECT id, name, email, NOW() FROM public.stg_users "
            "ON CONFLICT (id) DO UPDATE SET "
            "  name = EXCLUDED.name, email = EXCLUDED.email, updated_at = EXCLUDED.updated_at"
        ),
    )

    tg = tollkeeper_sql_task_group(
        sql_operator=pg_op,
        table="public.dim_users",
        conn_id="postgres_default",
        signal_store=signal_store,
        sources=[],  # root node, no upstream sensors
        dq_checks=[
            DqSqlCheck(name="no_null_ids", sql="SELECT * FROM {table} WHERE id IS NULL"),
            DqSqlCheck(name="no_null_emails", sql="SELECT * FROM {table} WHERE email IS NULL"),
            DqSqlCheck(
                name="unique_emails",
                sql="SELECT email, COUNT(*) c FROM {table} GROUP BY email HAVING COUNT(*) > 1",
            ),
        ],
    )
```

`register_defaults()` registers `PassThroughStrategy` for `SQLExecuteQueryOperator` and
`SparkSqlOperator` so Tollkeeper knows how to wrap them without rewriting their SQL. Call it once
at DAG-file import time. More examples in `test_dags/`.

## Components

| Component | What it does |
|---|---|
| `TollkeeperOperator` | Wraps any operator in the WAP lifecycle (redirect → execute → audit → publish/rollback) |
| `TollkeeperSensor` | Pokes a `SignalStore` until an upstream table's signal appears |
| `TollkeeperDqOperator` | Runs one SQL-based DQ check, writes the result to the signal store |
| `TollkeeperSignalEmitter` | Reads DQ results for a table, emits a signal if all passed |
| `tollkeeper_task_group` | TaskGroup builder for the generic `TollkeeperOperator` path (any operator + strategy) |
| `tollkeeper_sql_task_group` | TaskGroup builder for SQL operators, DB-native DQ checks, no strategy needed |
| `TollkeeperStrategy` | ABC defining how to redirect/restore an operator's write target |
| `PassThroughStrategy` | No-op strategy for operators that already write straight to the target table |
| `StrategyRegistry` | O(1) map of operator class → strategy class |
| `register_defaults()` | Registers `PassThroughStrategy` for `SQLExecuteQueryOperator` and `SparkSqlOperator` |

## TollkeeperOperator

Wraps one operator. On execute: look up a strategy for the operator's class, redirect its writes
to a staging ref, run it, restore, audit, publish (or roll back on failure), emit a signal.

| Parameter | Type | Notes |
|---|---|---|
| `operator` | `BaseOperator` | The operator to wrap. Its class must have a registered strategy |
| `table` | `str` | Target table name |
| `backend` | `Backend` | Tollkeeper backend (CSV, Iceberg, SQL passthrough, ...) |
| `checks` | `list[BaseCheck]` | DQ checks run via `tollkeeper.core.Tollkeeper` |
| `engine` | `str \| None` | `"local"` for in-process checks, or a connection-backed engine |
| `engine_conn_id` | `str \| None` | Airflow connection id for the check engine |
| `signal_store` | `SignalStore \| None` | Where readiness signals are written |
| `on_failure` | `str` | `"stop"` (default, raises) or continue past a failed audit |
| `execution_ctx` | `dict \| None` | Partition/execution context stamped on signals and DQ results |

Operators without a registered strategy raise `TypeError` at execute time. Use this path for
non-SQL operators (Spark, custom Python) that need real write redirection:

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow_tollkeeper import TollkeeperOperator, TollkeeperStrategy, strategy_registry
from tollkeeper.backends.iceberg import IcebergBackend


class SparkSubmitStagingStrategy(TollkeeperStrategy):
    """Points --output-table at the staging ref, restores after execute."""

    def redirect(self, operator, version_ref):
        operator.application_args = [*operator.application_args, "--output-table", version_ref]

    def restore(self, operator):
        operator.application_args = operator.application_args[:-2]


strategy_registry.register(SparkSubmitOperator, SparkSubmitStagingStrategy)

spark_op = SparkSubmitOperator(task_id="build_features", application="job.py", application_args=[])

tk_op = TollkeeperOperator(
    task_id="tollkeeper_features",
    operator=spark_op,
    table="analytics.features",
    backend=IcebergBackend(catalog="prod"),
    checks=[...],
    signal_store=signal_store,
)
```

## SQL task group (`tollkeeper_sql_task_group`)

The simpler path when the write is already SQL run through an Airflow DB-API connection. No
strategy or backend needed. The SQL operator runs unchanged, DQ checks run as separate tasks
against the same connection, and a signal is emitted only if every check passes.

| Parameter | Type | Notes |
|---|---|---|
| `sql_operator` | `BaseOperator` | e.g. `SQLExecuteQueryOperator`. runs unmodified |
| `table` | `str` | Target table name |
| `dq_checks` | `list[DqSqlCheck]` | Each becomes its own `TollkeeperDqOperator` task |
| `signal_store` | `SignalStore` | Stores DQ results and the final signal |
| `conn_id` | `str` | Airflow connection used to run each check's SQL |
| `sources` | `list[str] \| None` | Explicit upstream tables; omit to auto-resolve from SQL |
| `sql` | `str \| None` | SQL to parse for lineage if `sql_operator.sql` isn't set |
| `dialect` | `str \| None` | SQL dialect for the lineage parser |
| `execution_ctx` | `dict \| None` | Stamped on DQ results and the signal |
| `on_failure` | `str` | `"stop"` (default) or continue past a failed check |
| `group_id` | `str \| None` | Defaults to `tollkeeper_<table_slug>` |

`DqSqlCheck(name, sql)`. `sql` must contain a `{table}` placeholder and return violation rows.
Zero rows returned means the check passes.

## Generic task group (`tollkeeper_task_group`)

Same shape (`[sensors] >> tollkeeper_op`) but built on `TollkeeperOperator` instead of a bare SQL
operator, so it takes `backend`, `checks`, `engine`, and `engine_conn_id` instead of `dq_checks`
and `conn_id`. Use it whenever the wrapped operator isn't a DB-API SQL operator. Spark, a Python
callable, anything needing a real staging redirect via a `TollkeeperStrategy`.

| | `tollkeeper_sql_task_group` | `tollkeeper_task_group` |
|---|---|---|
| Wrapped operator | SQL DB-API operator, runs as-is | Any operator, needs a strategy |
| Write redirection | None (direct write, DQ checks gate the signal) | Via `TollkeeperStrategy.redirect/restore` |
| DQ checks | `DqSqlCheck` SQL run through `conn_id` | `BaseCheck` list run through `engine`/`backend` |
| Backend | Not needed | Required |

## Strategies

`TollkeeperStrategy` is the ABC every wrapped operator needs a registered implementation for:

```python
class TollkeeperStrategy(ABC):
    def redirect(self, operator: BaseOperator, version_ref: str) -> None: ...
    def restore(self, operator: BaseOperator) -> None: ...
```

`redirect` mutates the operator so its write lands on the staging ref instead of the real table.
`restore` undoes that after `execute()` runs, so retries and reuse see the operator's original
config.

`PassThroughStrategy` is a no-op `redirect`/`restore` pair for operators that already write to the
target table unconditionally (plain SQL). `register_defaults()` registers it for
`SQLExecuteQueryOperator` and `SparkSqlOperator` if those providers are importable; missing
providers are skipped silently.

Write a custom strategy when the operator needs real staging redirection. swap a table name,
S3 path, or `--output` arg on `redirect`, put it back on `restore`:

```python
class MyOperatorStrategy(TollkeeperStrategy):
    def redirect(self, operator, version_ref):
        self._original_path = operator.output_path
        operator.output_path = version_ref

    def restore(self, operator):
        operator.output_path = self._original_path


strategy_registry.register(MyOperator, MyOperatorStrategy)
```

## Auto-lineage

Both task group builders accept `sources`. If you don't pass it, sources are resolved from SQL via
`tollkeeper.parser.extract_lineage()`. pulled from `sql_operator.sql` (or the `sql=` override) and
parsed with `dialect=` if given. If the parsed sinks don't match `table`, a `ValueError` is raised
so a lineage mismatch fails fast at DAG-parse time instead of silently sensing the wrong table. If
parsing fails, a warning is logged and the group is built with no sensors. treat that as a signal
to pass `sources=` explicitly.

## Signal coordination

`TollkeeperSensor` pokes `signal_store.check(table, execution_ctx)` until it returns non-`None`.
Task groups auto-wire one sensor per resolved source, so a DAG only needs to share a
`SignalStore` instance (or point at the same SQLite file / DB-API connection) with its upstream.

Signals are emitted after a successful publish:

- `tollkeeper_task_group` / `TollkeeperOperator`: signal written inside `Tollkeeper.table(...)`'s
  context manager, immediately after `session.publish()`.
- `tollkeeper_sql_task_group`: `TollkeeperSignalEmitter` writes it only once every `DqSqlCheck` in
  `expected_checks` has a passing result.

Cross-DAG example. `dag_b` waits on a table `dag_a` publishes, same signal store:

```python
# dag_a.py
signal_store = SqliteSignalStore("/tmp/tollkeeper_signals.db")
tollkeeper_sql_task_group(sql_operator=..., table="public.dim_users", signal_store=signal_store, sources=[], ...)

# dag_b.py
signal_store = SqliteSignalStore("/tmp/tollkeeper_signals.db")  # same file
tollkeeper_sql_task_group(sql_operator=..., table="public.fact_orders", signal_store=signal_store, sources=["public.dim_users"], ...)
```

`dag_b`'s task group auto-creates a `wait_public__dim_users` sensor ahead of its own SQL task.
