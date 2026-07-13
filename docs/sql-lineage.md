# SQL Lineage Parser

Tollkeeper's lineage parser extracts source and sink tables from SQL statements using sqlglot. This powers automatic dependency detection in the Airflow integration, but you can use it standalone.

## Setup

Install the `sqlglot` extra:

```bash
pip install tollkeeper[sqlglot]
```

## Basic usage

```python
from tollkeeper import extract_lineage

result = extract_lineage("""
    INSERT INTO analytics.fact_orders
    SELECT o.*, c.name
    FROM raw.orders o
    JOIN raw.customers c ON o.customer_id = c.id
""")

result.sources  # frozenset({'raw.orders', 'raw.customers'})
result.sinks    # frozenset({'analytics.fact_orders'})
```

## Supported statements

| Statement | Sources | Sinks |
|-----------|---------|-------|
| `SELECT ... FROM a JOIN b` | `{a, b}` | `{}` |
| `INSERT INTO t SELECT ... FROM a` | `{a}` | `{t}` |
| `CREATE TABLE t AS SELECT ... FROM a` | `{a}` | `{t}` |
| `MERGE INTO t USING s ON ...` | `{s}` | `{t}` |

Multi-statement SQL is supported. Sources and sinks are accumulated across all statements.

## CTE handling

Common Table Expressions are excluded from sources. Only the real tables they reference are reported:

```python
result = extract_lineage("""
    WITH enriched AS (
        SELECT * FROM raw.events
    )
    INSERT INTO analytics.sessions
    SELECT * FROM enriched
""")

result.sources  # frozenset({'raw.events'}), not {'enriched'}
result.sinks    # frozenset({'analytics.sessions'})
```

## Fully qualified names

Catalog, schema, and table names are preserved:

```python
result = extract_lineage("SELECT * FROM my_catalog.my_schema.my_table")
result.sources  # frozenset({'my_catalog.my_schema.my_table'})
```

## Dialect support

Pass a sqlglot dialect for engine-specific SQL syntax:

```python
result = extract_lineage(
    "SELECT * FROM `project.dataset.table`",
    dialect="bigquery",
)
```

See [sqlglot dialects](https://sqlglot.com/sqlglot/dialects.html) for the full list.

## Error handling

```python
# Empty SQL
extract_lineage("")        # raises ValueError("SQL string is empty")

# Unparseable SQL
extract_lineage("NOT SQL") # raises ValueError("Failed to parse SQL: ...")

# Jinja templates (common in Airflow)
extract_lineage("SELECT * FROM {{ params.table }}")
# raises ValueError("SQL contains Jinja template expressions; provide explicit sources/sinks")
```

Jinja-templated SQL cannot be statically parsed. In the Airflow integration, provide `sources` and `sinks` explicitly when your SQL uses templates.

## Use with Airflow task groups

The parser is used automatically by `TollkeeperTaskGroup` to wire up sensor dependencies:

```python
from airflow_tollkeeper import TollkeeperTaskGroup

group = TollkeeperTaskGroup(
    table="fact_orders",
    sql="INSERT INTO fact_orders SELECT * FROM dim_customers JOIN raw_events ...",
    backend=backend,
    signal_store=store,
)
# Sensors are created for dim_customers and raw_events automatically
```

If the SQL contains Jinja or the parser can't extract what you need, pass sources explicitly:

```python
group = TollkeeperTaskGroup(
    table="fact_orders",
    sql="{{ params.insert_sql }}",
    sources=["dim_customers", "raw_events"],
    backend=backend,
    signal_store=store,
)
```
