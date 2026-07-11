from __future__ import annotations

from uuid import uuid4

from pyiceberg.catalog import Catalog

from tollkeeper.backends.base import Backend


class IcebergBackend(Backend):
    """Tollkeeper backend for Apache Iceberg tables using branch-based isolation.

    Creates a temporary branch for staging writes, then fast-forwards ``main``
    on publish or drops the branch on rollback.

    Args:
        catalog: A PyIceberg ``Catalog`` instance.

    Example::

        from pyiceberg.catalog.sql import SqlCatalog
        catalog = SqlCatalog("default", warehouse="/tmp/warehouse", uri="sqlite:///catalog.db")
        backend = IcebergBackend(catalog)
        Tollkeeper(backend).table("db.sales").write(lambda ref: table.append(df, branch=ref)).audit([...]).publish()
    """

    def __init__(self, catalog: Catalog) -> None:
        self._catalog = catalog

    def create_version(self, table: str) -> str:
        """Create a Tollkeeper branch on the Iceberg table.

        Args:
            table: Fully qualified Iceberg table identifier (e.g. ``"db.sales"``).

        Returns:
            Branch name (e.g. ``"tollkeeper-a1b2c3d4"``). Pass this as the ``branch``
            parameter when writing via PyIceberg.
        """
        iceberg_table = self._catalog.load_table(table)
        branch_name = f"tollkeeper-{uuid4().hex[:8]}"
        snapshot = iceberg_table.current_snapshot()
        if snapshot is None:
            raise RuntimeError(f"Table {table} has no snapshots — write initial data before using Tollkeeper")
        iceberg_table.manage_snapshots().create_branch(snapshot.snapshot_id, branch_name).commit()
        return branch_name

    def publish_version(self, table: str, version_ref: str) -> None:
        """Fast-forward ``main`` to the branch snapshot, then remove the branch.

        Args:
            table: Fully qualified Iceberg table identifier.
            version_ref: Branch name returned by ``create_version``.
        """
        iceberg_table = self._catalog.load_table(table)
        iceberg_table.manage_snapshots().set_current_snapshot(ref_name=version_ref).commit()
        iceberg_table.manage_snapshots().remove_branch(version_ref).commit()

    def rollback_version(self, table: str, version_ref: str) -> None:
        """Drop the Tollkeeper branch without publishing.

        Args:
            table: Fully qualified Iceberg table identifier.
            version_ref: Branch name returned by ``create_version``.
        """
        iceberg_table = self._catalog.load_table(table)
        try:
            iceberg_table.manage_snapshots().remove_branch(version_ref).commit()
        except Exception:
            pass
