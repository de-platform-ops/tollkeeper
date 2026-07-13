# SQL Passthrough Strategy

Tollkeeper's SQL passthrough strategy lets you add data quality gating and
signal-based orchestration to standard SQL operators (PostgreSQL, MySQL,
Trino, Snowflake, Spark SQL) without requiring a table format that supports
physical versioning.

## How it works

Standard SQL tables have no branches or snapshots. Writes go directly to the
production table. Tollkeeper wraps your SQL operator with orchestration-level
isolation: upstream signals gate execution, DQ checks validate the write, and
the signal is only emitted when every check passes. A failed check halts the
pipeline at the audit boundary.

```
[upstream sensors] >> [your SQL operator] >> [DQ check tasks] >> [signal emitter]
```

Each stage is a visible Airflow task. DQ checks run as real SQL queries
against the target database, not in-process Python checks.

## Task group structure

`tollkeeper_sql_task_group()` builds the following Airflow tasks:

```
    tollkeeper_task_group("orders")
   +-----------------------------------------------------------+
   |                                                           |
   |  wait_raw_events ─┐                                      |
   |  (TollkeeperSensor)├──> upsert_orders ──┬─> dq_no_nulls  |
   |  wait_dim_products ┘    (your SQL op)    ├─> dq_min_rows  |
   |                                          └─> dq_freshness |
   |                                                  |        |
   |                                           signal_orders   |
   |                                                           |
   +-----------------------------------------------------------+
```

| Task | What it does |
|------|-------------|
| `wait_*` sensors | Poll the signal store until each upstream table's signal exists. Skipped for root nodes (no upstream). |
| Your SQL operator | Runs your unmodified SQL. Tollkeeper does not rewrite the query. |
| `dq_*` checks | Each `DqSqlCheck` becomes a separate Airflow task. Runs a validation query via the same database connection. Zero result rows = pass. |
| `signal_*` emitter | Reads all DQ results for this table. If every check passed, writes a `Signal` to the store. If any failed, raises `AirflowException` (configurable). |

## Components

### SqlPassthroughBackend

A no-op backend for tables that do not support physical versioning.

| Method | Behavior |
|--------|----------|
| `create_version(table)` | Returns the table name unchanged |
| `publish_version(table, ref)` | No-op |
| `rollback_version(table, ref)` | Logs a warning (data is already written) |

### PassThroughStrategy

A no-op strategy registered for `SQLExecuteQueryOperator` and
`SparkSqlOperator`. Both `redirect()` and `restore()` are no-ops. Your SQL
runs unmodified.

Call `register_defaults()` once at DAG-module level to register it.

### DqSqlCheck

A dataclass holding a check name and a SQL template. The SQL must return
violation rows (zero rows = pass). Use `{table}` as a placeholder for the
target table name.

```python
DqSqlCheck(
    name="no_null_ids",
    sql="SELECT * FROM {table} WHERE id IS NULL",
)
```

### TollkeeperDqOperator

An Airflow operator that runs a single DQ check. It calls
`hook.get_records(check_sql)` using your Airflow connection and writes the
result to the `tollkeeper_dq_results` table in the signal store.

### TollkeeperSignalEmitter

Reads all DQ results for the target table. If every expected check passed,
writes a `Signal(status="passed")` to the signal store. If any check failed
or is missing, raises `AirflowException` (when `on_failure="stop"`).

## Usage

### Minimal example (root node, no upstream)

```python
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow_tollkeeper import DqSqlCheck, register_defaults, tollkeeper_sql_task_group
from airflow_tollkeeper.compat import DAG
from tollkeeper.signals.sqlite import SqliteSignalStore

register_defaults()
signal_store = SqliteSignalStore("/tmp/tollkeeper_signals.db")

with DAG(dag_id="example", start_date=datetime(2025, 1, 1), schedule=None) as dag:
    op = SQLExecuteQueryOperator(
        task_id="upsert_users",
        conn_id="postgres_default",
        sql="INSERT INTO dim_users SELECT * FROM stg_users ON CONFLICT (id) DO UPDATE ...",
    )

    tg = tollkeeper_sql_task_group(
        sql_operator=op,
        table="dim_users",
        conn_id="postgres_default",
        signal_store=signal_store,
        sources=[],          # root node, no upstream sensors
        dq_checks=[
            DqSqlCheck(name="no_null_ids", sql="SELECT * FROM {table} WHERE id IS NULL"),
            DqSqlCheck(name="no_null_emails", sql="SELECT * FROM {table} WHERE email IS NULL"),
        ],
    )
```

### With upstream dependencies

```python
tg = tollkeeper_sql_task_group(
    sql_operator=op,
    table="fct_sales",
    conn_id="trino_default",
    signal_store=signal_store,
    sources=["stg_sales", "dim_products"],   # sensors auto-created
    dq_checks=[
        DqSqlCheck(name="no_negative_revenue", sql="SELECT * FROM {table} WHERE revenue < 0"),
    ],
)
```

Tollkeeper creates a `TollkeeperSensor` for each source. The sensors poll
the signal store until the upstream table's signal exists before allowing
your SQL operator to run.

### Multi-table chains

When one task group's output is another's input, the signal store links them
automatically:

```python
stg_tg = tollkeeper_sql_task_group(
    sql_operator=stg_op, table="stg_sales", sources=["raw_sales"], ...
)
fct_tg = tollkeeper_sql_task_group(
    sql_operator=fct_op, table="fct_sales", sources=["stg_sales"], ...
)
stg_tg >> fct_tg
```

`fct_sales`'s sensor waits for `stg_sales`'s signal before running.

## Safety model

For standard SQL, data is written to the production table before DQ checks
run. The protection is at the signal boundary, not the write boundary:

1. If DQ checks **pass**: signal is emitted, downstream proceeds.
2. If DQ checks **fail**: no signal is emitted, downstream tasks that depend
   on this table's signal never run. The pipeline halts.

```
  Iceberg/Delta                             SQL Passthrough
  =============                             ===============

  Write to branch (isolated)                Write to prod table
          |                                         |
     Audit branch                              Audit prod table
          |                                         |
    Pass? ── Fail?                           Pass? ── Fail?
      |        |                               |        |
  Fast-forward  Drop branch                Emit signal  No signal
  + emit signal (no damage)                (data stays) (pipeline halts)
```

Iceberg/Delta isolates the write physically. SQL passthrough isolates
downstream consumption via signals. Both prevent bad data from propagating
through the pipeline.

## Parameters reference

`tollkeeper_sql_task_group()` accepts:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `sql_operator` | `BaseOperator` | Yes | Your SQL operator instance |
| `table` | `str` | Yes | Target table name |
| `dq_checks` | `list[DqSqlCheck]` | Yes | DQ validation queries |
| `signal_store` | `SignalStore` | Yes | Where signals and DQ results are stored |
| `conn_id` | `str` | Yes | Airflow connection for running DQ queries |
| `sources` | `list[str]` | No | Upstream tables. Sensors created for each. Auto-parsed from SQL if omitted. |
| `dialect` | `str` | No | SQL dialect for auto-parsing (e.g. `"snowflake"`, `"trino"`) |
| `execution_ctx` | `dict` | No | Execution context passed to signal store (e.g. `{"ds": "2025-01-01"}`) |
| `on_failure` | `str` | No | `"stop"` (default) raises on DQ failure. `"continue"` skips the signal silently. |
| `group_id` | `str` | No | Override the task group ID (default: `tollkeeper_{table}`) |
| `dag` | `DAG` | No | Explicit DAG reference (usually inferred from context) |
