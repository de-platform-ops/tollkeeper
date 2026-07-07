from __future__ import annotations

from write_audit_publish.backends.base import Backend

from pyiceberg.catalog import Catalog


class IcebergBackend(Backend):
    def __init__(self, catalog: Catalog) -> None:
        self._catalog = catalog

    def create_version(self, table: str) -> str:
        raise NotImplementedError("Iceberg backend not yet implemented")

    def publish_version(self, table: str, version_ref: str) -> None:
        raise NotImplementedError("Iceberg backend not yet implemented")

    def rollback_version(self, table: str, version_ref: str) -> None:
        raise NotImplementedError("Iceberg backend not yet implemented")
