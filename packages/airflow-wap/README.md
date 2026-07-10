# airflow-wap

Airflow operator wrapper for [write-audit-publish](https://github.com/srchilukoori/write-audit-publish).

Wraps any Airflow operator in a Write-Audit-Publish lifecycle: stage writes, run DQ checks on a remote engine, publish or rollback.
