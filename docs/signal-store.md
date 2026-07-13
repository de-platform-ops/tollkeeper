# Signal Store

Tollkeeper's signal store tracks which tables have passed their audit. Pipelines that depend on a table can poll or wait for its signal before proceeding.

## Setup

Two implementations ship with tollkeeper:

```python
from tollkeeper import SqliteSignalStore, DbApiSignalStore

# SQLite (zero config, good for single-machine pipelines)
store = SqliteSignalStore("/var/data/signals.db")

# DB-API 2.0 (PostgreSQL, MySQL, any PEP 249 driver)
import psycopg2
conn = psycopg2.connect("dbname=pipeline")
store = DbApiSignalStore(conn, paramstyle="format")  # PostgreSQL uses %s
```

`SqliteSignalStore` defaults to `:memory:` if no path is given.

`DbApiSignalStore` accepts `paramstyle="qmark"` (SQLite, default) or `paramstyle="format"` (PostgreSQL, MySQL).

Both create the required tables on first use.

## Wiring into Tollkeeper

Pass the store to the `Tollkeeper` constructor. When an audit passes, tollkeeper writes a signal automatically.

```python
from tollkeeper import Tollkeeper, CsvBackend, SqliteSignalStore

store = SqliteSignalStore("signals.db")
backend = CsvBackend("/data/staging", "/data/prod")

(
    Tollkeeper(backend, signal_store=store)
    .table("orders")
    .audit([RowCountCheck(min_rows=1)])
    .publish()
)

# Signal written automatically on audit pass
assert store.check("orders") is not None
```

If the audit fails, no signal is written.

## Reading and waiting for signals

```python
# Non-blocking check
signal = store.check("orders")
if signal:
    print(f"orders passed at {signal.execution_ts}")

# Blocking wait (raises TimeoutError after 300s by default)
signal = store.wait("orders", timeout_s=60, poll_s=5)
```

## Execution context

Partition signals by execution context (date, region, etc.) so daily runs don't overwrite each other:

```python
ctx = {"ds": "2026-07-13"}

(
    Tollkeeper(backend, signal_store=store)
    .table("orders")
    .audit([RowCountCheck(min_rows=1)], execution_ctx=ctx)
    .publish()
)

# Only returns the signal for this specific date
store.check("orders", {"ds": "2026-07-13"})

# Different date returns None
store.check("orders", {"ds": "2026-07-12"})  # None
```

## Dependencies and cascading deletes

Register that one table depends on another. When the upstream signal is deleted (e.g., a re-run), downstream signals are automatically invalidated.

```python
store.register_dep("raw_events", "dim_users", cascade_policy="cascade")
store.register_dep("dim_users", "fact_sessions", cascade_policy="cascade")

# Write signals for all three
store.write(Signal(table_name="raw_events"))
store.write(Signal(table_name="dim_users"))
store.write(Signal(table_name="fact_sessions"))

# Deleting raw_events cascades through the chain
store.delete("raw_events")
assert store.check("dim_users") is None
assert store.check("fact_sessions") is None
```

Use `cascade_policy="notify"` to get a callback instead of automatic deletion:

```python
def on_upstream_invalidated(upstream, downstream, ctx):
    print(f"{upstream} was invalidated, {downstream} may be stale")

store = SqliteSignalStore("signals.db", on_delete_callback=on_upstream_invalidated)
store.register_dep("raw_events", "dim_users", cascade_policy="notify")
```

## DQ result storage

The signal store also persists individual check results for auditing and debugging:

```python
from tollkeeper.signals.base import DqResult

result = DqResult(table_name="orders", check_name="NullCheck", passed=True, details="0 nulls in 'id'")
store.write_dq_result(result)

results = store.get_dq_results("orders")
for r in results:
    print(f"{r.check_name}: {'PASS' if r.passed else 'FAIL'} - {r.details}")
```

## Resource management

Signal stores hold database connections. Close them when done:

```python
# Explicit close
store = SqliteSignalStore("signals.db")
# ... use store ...
store.close()

# Context manager (preferred)
with SqliteSignalStore("signals.db") as store:
    Tollkeeper(backend, signal_store=store).table("orders").audit(checks).publish()
# connection closed automatically
```

## Custom signal store

Implement the `SignalStore` ABC to back signals with Redis, DynamoDB, or anything else:

```python
from tollkeeper.signals.base import SignalStore, Signal, DqResult

class RedisSignalStore(SignalStore):
    def write(self, signal: Signal) -> None: ...
    def delete(self, table: str, execution_ctx: dict | None = None) -> None: ...
    def check(self, table: str, execution_ctx: dict | None = None) -> Signal | None: ...
    def write_dq_result(self, result: DqResult) -> None: ...
    def get_dq_results(self, table: str, execution_ctx: dict | None = None) -> list[DqResult]: ...
    def delete_dq_results(self, table: str, execution_ctx: dict | None = None) -> None: ...
    def register_dep(self, upstream_table: str, downstream_table: str, ...) -> None: ...
    def get_downstream(self, table: str, execution_ctx: dict | None = None) -> list[tuple[str, dict, str]]: ...
    def close(self) -> None: ...
```

The `wait()` method is inherited and works with any implementation that provides `check()`.
