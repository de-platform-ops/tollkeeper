# Docker Testing Guide

Run the full test suite and validate example DAGs in a containerized Airflow environment.

## Prerequisites

- Docker and Docker Compose installed
- Repository cloned locally

## Running tests

Build and run the test container:

```bash
docker compose run --build test
```

This runs all 204 tests (152 core + 52 airflow) inside an Airflow 2.9.3 container with both `tollkeeper` and `airflow-tollkeeper` installed.

The container mounts your local source as volumes, so code changes are reflected without rebuilding:

```bash
# After editing src/ or tests/, just re-run:
docker compose run test
```

## Validating example DAGs

The `test_dags/` directory contains five example DAGs that demonstrate the SQL passthrough strategy with different database engines:

| DAG file | Database | Pattern |
|----------|----------|---------|
| `example_postgres_sql.py` | PostgreSQL | Root node, no upstream sensors |
| `example_trino_sql.py` | Trino | Upstream dependency chain |
| `example_presto_sql.py` | Presto | Multi-table with cross-table dependencies |
| `example_snowflake_sql.py` | Snowflake | Root node |
| `example_spark_sql.py` | Spark SQL | Root node with SparkSqlOperator |

To verify DAGs parse correctly in Airflow:

```bash
docker compose run test airflow dags list
```

To check a specific DAG's task structure:

```bash
docker compose run test airflow tasks list example_postgres_sql --tree
```

## Container lifecycle

The image is ~1.5GB. Avoid unnecessary rebuilds:

```bash
# Build once, reuse:
docker compose build test

# Run tests (reuses existing image):
docker compose run test

# Stop containers but keep images:
docker compose stop

# Full cleanup (removes containers and images):
docker compose down --rmi all
```

## What the Dockerfile sets up

- Base image: `apache/airflow:2.9.3-python3.11`
- Installs `tollkeeper[sql]` and `airflow-tollkeeper` in editable mode
- Configures SequentialExecutor with SQLite (lightweight, no external DB needed)
- Points `AIRFLOW__CORE__DAGS_FOLDER` to `test_dags/`
- Runs `airflow db migrate` at build time

## Troubleshooting

**Tests fail on import errors:** Rebuild the image to pick up new dependencies:

```bash
docker compose build --no-cache test
```

**DAGs not loading:** Check that `test_dags/` is mounted. The docker-compose.yml mounts it automatically, but if you run `docker run` directly, add `-v ./test_dags:/opt/tollkeeper/test_dags`.
