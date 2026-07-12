# SQL PassThrough Strategy: How It Works

## The Problem

Standard SQL tables (PostgreSQL, MySQL, Trino, etc.) have no physical
versioning. Unlike Iceberg or Delta, there is no "branch" to write to
and promote. Writes go directly to the production table.

Tollkeeper still provides value here through **orchestration-level isolation**:
upstream signals gate downstream execution, DQ checks validate the data
before the signal is emitted, and a failed audit prevents the signal,
halting the pipeline.

## Architecture: What Tollkeeper Wraps Around Your Operator

```
                         tollkeeper_task_group("orders")
    +--------------------------------------------------------------------+
    |                                                                    |
    |  +------------------+     +-------------------+                    |
    |  | wait_raw_events  |---->|                   |                    |
    |  | (TollkeeperSensor|     |                   |                    |
    |  |  polls signal    |     |  tollkeeper_orders |                    |
    |  |  store for       |     | (TollkeeperOperator|                    |
    |  |  "raw_events")   |     |  wraps your       |                    |
    |  +------------------+     |  SQLExecuteQuery)  |                    |
    |                           |                   |                    |
    |  +------------------+     |                   |                    |
    |  | wait_dim_products|---->|                   |                    |
    |  | (TollkeeperSensor|     +-------------------+                    |
    |  |  polls signal    |                                              |
    |  |  store for       |                                              |
    |  |  "dim_products") |                                              |
    |  +------------------+                                              |
    |                                                                    |
    +--------------------------------------------------------------------+
```

## Execution Flow Inside TollkeeperOperator

```
TollkeeperOperator.execute(context)
    |
    |  1. STRATEGY LOOKUP
    |     strategy = strategy_registry.get(SQLExecuteQueryOperator)
    |     |
    |     +-- Not found? --> raise TypeError
    |     +-- Found PassThroughStrategy
    |
    |  2. RESOLVE DQ ENGINE
    |     engine_conn = resolve_engine(engine, engine_conn_id)
    |     |
    |     +-- engine="local"  --> LOCAL_ENGINE (in-process, for tests)
    |     +-- engine="spark"  --> BaseHook.get_connection("tollkeeper_engine_spark")
    |     +-- engine_conn_id  --> BaseHook.get_connection(engine_conn_id)
    |
    |  3. CREATE SESSION (context manager)
    |     tk = Tollkeeper(backend, signal_store)
    |     session = tk.table("orders")
    |     |
    |     +-- backend.create_version("orders")
    |     |   |
    |     |   +-- SqlPassthroughBackend: returns "orders" (the table name itself)
    |     |   +-- IcebergBackend: returns "orders__tk_abc123" (a branch)
    |     |   +-- CsvBackend: returns "/staging/orders.tollkeeper-abc.csv"
    |     |
    |     +-- session.ref = "orders"  (for SqlPassthrough)
    |
    |  4. REDIRECT (no-op for PassThrough)
    |     strategy.redirect(operator, session.ref)
    |     |
    |     +-- PassThroughStrategy: pass  (SQL runs unchanged)
    |     +-- IcebergStrategy: rewrites SQL to target the branch table
    |
    |  5. EXECUTE THE WRAPPED OPERATOR
    |     operator.execute(context)
    |     |
    |     +-- Your SQLExecuteQueryOperator runs its SQL against the DB
    |     +-- Data is written directly to "orders" (no staging for SQL)
    |
    |  6. RESTORE (no-op for PassThrough)
    |     strategy.restore(operator)
    |
    |  7. AUDIT (DQ checks)
    |     session.audit(checks, on_failure, execution_ctx)
    |     |
    |     +-- Deletes any stale signal for "orders"
    |     +-- Runs each BaseCheck against session.ref
    |     |
    |     +-- All passed?
    |     |   +-- YES: Writes Signal(table="orders", status="passed") to store
    |     |   +-- NO + on_failure="stop":
    |     |   |     backend.rollback_version()  (logs warning for SQL)
    |     |   |     raises AuditFailedError
    |     |   |     signal is NOT written --> downstream sensors hang/timeout
    |     |   +-- NO + on_failure="continue":
    |     |         signal is NOT written --> downstream sensors hang/timeout
    |     |         session continues (caller decides)
    |
    |  8. PUBLISH
    |     session.publish()
    |     |
    |     +-- SqlPassthroughBackend.publish_version(): no-op
    |     +-- IcebergBackend: fast-forward branch to main
    |
    |  9. XCOM
    |     push "tollkeeper_version_ref" = session.ref
```

## Signal Flow Across a Pipeline

```
DAG: daily_pipeline
====================

  [raw_events signal exists?]          [dim_products signal exists?]
         |                                      |
         v                                      v
  +--------------+                       +--------------+
  | wait_raw     |                       | wait_dim     |
  | (Sensor)     |                       | (Sensor)     |
  +--------------+                       +--------------+
         \                                     /
          \                                   /
           +--------> +---------------+ <----+
                      |  tollkeeper   |
                      |  _orders      |
                      | (Operator)    |
                      +---------------+
                             |
                    audit passes? ----NO----> AuditFailedError
                             |                no signal written
                            YES               downstream blocked
                             |
                      signal_store.write(
                        Signal("orders", "passed")
                      )
                             |
                             v
                   downstream sensors for
                   "orders" will now resolve
```

## The Critical Safety Property

For standard SQL (PassThrough), the data IS already written to production
before the audit runs. The protection is:

1. If audit **fails**, no signal is emitted
2. Downstream tasks waiting on this table's signal will **never proceed**
3. The pipeline halts at the audit boundary, not at the write boundary

This is weaker than Iceberg/Delta (where the write is physically isolated),
but it is the best possible guarantee for standard SQL without CDC or
shadow tables.

```
  ICEBERG FLOW                          SQL PASSTHROUGH FLOW
  ============                          ====================

  Write to branch ----+                 Write to prod table ---+
  (isolated)          |                 (NOT isolated)         |
                      v                                        v
              Audit branch                              Audit prod table
                      |                                        |
              Pass? --+-- Fail?                        Pass? --+-- Fail?
              |            |                           |            |
        Fast-forward    Drop branch               Emit signal   NO signal
        to main         (no damage)               (data stays)  (data stays,
        + emit signal                                            pipeline halts)
```
