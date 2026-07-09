from __future__ import annotations

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from write_audit_publish.backends.iceberg import IcebergBackend


@pytest.fixture
def catalog(tmp_path):
    cat = SqlCatalog("test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=str(tmp_path / "warehouse"))
    cat.create_namespace("db")
    return cat


@pytest.fixture
def seeded_catalog(catalog):
    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    tbl = catalog.create_table("db.sales", schema=schema)
    tbl.append(pa.table({"id": [1, 2], "name": ["alice", "bob"]}))
    return catalog


class TestIcebergBackend:
    def test_create_version_returns_branch_name(self, seeded_catalog) -> None:
        backend = IcebergBackend(seeded_catalog)
        ref = backend.create_version("db.sales")
        assert ref.startswith("wap-")
        tbl = seeded_catalog.load_table("db.sales")
        assert ref in tbl.refs()

    def test_create_version_fails_on_empty_table(self, catalog) -> None:
        schema = pa.schema([("id", pa.int64())])
        catalog.create_table("db.empty", schema=schema)
        backend = IcebergBackend(catalog)
        with pytest.raises(RuntimeError, match="no snapshots"):
            backend.create_version("db.empty")

    def test_publish_fast_forwards_main(self, seeded_catalog) -> None:
        backend = IcebergBackend(seeded_catalog)
        ref = backend.create_version("db.sales")
        tbl = seeded_catalog.load_table("db.sales")
        tbl.append(pa.table({"id": [3], "name": ["charlie"]}), branch=ref)

        backend.publish_version("db.sales", ref)

        tbl = seeded_catalog.load_table("db.sales")
        assert len(tbl.scan().to_arrow()) == 3
        assert ref not in tbl.refs()

    def test_rollback_removes_branch(self, seeded_catalog) -> None:
        backend = IcebergBackend(seeded_catalog)
        ref = backend.create_version("db.sales")
        tbl = seeded_catalog.load_table("db.sales")
        tbl.append(pa.table({"id": [3], "name": ["charlie"]}), branch=ref)

        backend.rollback_version("db.sales", ref)

        tbl = seeded_catalog.load_table("db.sales")
        assert len(tbl.scan().to_arrow()) == 2
        assert ref not in tbl.refs()

    def test_rollback_missing_branch_is_noop(self, seeded_catalog) -> None:
        backend = IcebergBackend(seeded_catalog)
        backend.rollback_version("db.sales", "wap-nonexistent")

    def test_unique_refs_per_call(self, seeded_catalog) -> None:
        backend = IcebergBackend(seeded_catalog)
        refs = {backend.create_version("db.sales") for _ in range(5)}
        assert len(refs) == 5
