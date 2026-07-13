# airflow-tollkeeper

Airflow operator wrapper for [tollkeeper](https://github.com/de-platform-ops/tollkeeper).

Wraps any Airflow operator in a Write-Audit-Publish lifecycle: stage writes, run DQ checks on a remote engine, publish or rollback.
