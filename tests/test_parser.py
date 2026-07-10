from __future__ import annotations

import pytest

from write_audit_publish.parser import extract_lineage


# ---------------------------------------------------------------------------
# Positive: basic lineage extraction
# ---------------------------------------------------------------------------


class TestPositiveBasic:
    def test_simple_insert_select(self):
        sql = "INSERT INTO target_table SELECT * FROM source_table"
        result = extract_lineage(sql)
        assert result.sources == {"source_table"}
        assert result.sinks == {"target_table"}

    def test_ctas(self):
        sql = "CREATE TABLE new_table AS SELECT * FROM source_a JOIN source_b ON source_a.id = source_b.id"
        result = extract_lineage(sql)
        assert result.sources == {"source_a", "source_b"}
        assert result.sinks == {"new_table"}

    def test_plain_select_no_sinks(self):
        sql = "SELECT * FROM users"
        result = extract_lineage(sql)
        assert result.sources == {"users"}
        assert result.sinks == set()

    def test_select_literal_no_tables(self):
        result = extract_lineage("SELECT 1")
        assert result.sources == set()
        assert result.sinks == set()

    def test_multi_source_join(self):
        sql = """
        INSERT INTO fact_orders
        SELECT o.*, c.name
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        JOIN products p ON o.product_id = p.id
        """
        result = extract_lineage(sql)
        assert result.sources == {"orders", "customers", "products"}
        assert result.sinks == {"fact_orders"}

    def test_subquery_sources(self):
        sql = """
        INSERT INTO summary
        SELECT dept, COUNT(*)
        FROM (SELECT * FROM employees WHERE active = 1) e
        GROUP BY dept
        """
        result = extract_lineage(sql)
        assert result.sources == {"employees"}
        assert result.sinks == {"summary"}

    def test_multiple_statements(self):
        sql = """
        INSERT INTO table_a SELECT * FROM source_1;
        INSERT INTO table_b SELECT * FROM source_2;
        """
        result = extract_lineage(sql)
        assert result.sources == {"source_1", "source_2"}
        assert result.sinks == {"table_a", "table_b"}

    def test_result_is_frozen(self):
        result = extract_lineage("SELECT * FROM t")
        with pytest.raises(AttributeError):
            result.sources = set()


# ---------------------------------------------------------------------------
# Positive: CTE handling
# ---------------------------------------------------------------------------


class TestPositiveCTEs:
    def test_ctes_excluded_from_sources(self):
        sql = """
        WITH staging AS (
            SELECT id, name FROM raw_data
        ),
        filtered AS (
            SELECT * FROM staging WHERE id > 10
        )
        INSERT INTO clean_data
        SELECT * FROM filtered
        """
        result = extract_lineage(sql)
        assert result.sources == {"raw_data"}
        assert result.sinks == {"clean_data"}
        assert "staging" not in result.sources
        assert "filtered" not in result.sources

    def test_recursive_cte_excluded(self):
        sql = """
        WITH RECURSIVE ancestors AS (
            SELECT id, parent_id, name FROM org WHERE id = 1
            UNION ALL
            SELECT o.id, o.parent_id, o.name
            FROM org o JOIN ancestors a ON o.id = a.parent_id
        )
        INSERT INTO flat_org SELECT * FROM ancestors
        """
        result = extract_lineage(sql)
        assert result.sources == {"org"}
        assert "ancestors" not in result.sources
        assert result.sinks == {"flat_org"}

    def test_cte_name_matches_real_table(self):
        """CTE named same as a real table elsewhere — CTE wins, excluded from sources."""
        sql = """
        WITH users AS (
            SELECT id FROM raw_users
        )
        INSERT INTO output SELECT * FROM users
        """
        result = extract_lineage(sql)
        assert result.sources == {"raw_users"}
        assert "users" not in result.sources


# ---------------------------------------------------------------------------
# Positive: qualified names and dialects
# ---------------------------------------------------------------------------


class TestPositiveQualifiedAndDialects:
    def test_schema_qualified(self):
        sql = "INSERT INTO warehouse.fact_sales SELECT * FROM staging.raw_sales"
        result = extract_lineage(sql)
        assert result.sources == {"staging.raw_sales"}
        assert result.sinks == {"warehouse.fact_sales"}

    def test_catalog_schema_qualified(self):
        sql = "INSERT INTO hive.warehouse.fact_sales SELECT * FROM hive.staging.raw_sales"
        result = extract_lineage(sql)
        assert result.sources == {"hive.staging.raw_sales"}
        assert result.sinks == {"hive.warehouse.fact_sales"}

    def test_spark_insert_overwrite_table(self):
        sql = "INSERT OVERWRITE TABLE output_table SELECT * FROM input_table"
        result = extract_lineage(sql, dialect="spark")
        assert result.sources == {"input_table"}
        assert result.sinks == {"output_table"}

    def test_spark_insert_overwrite(self):
        sql = "INSERT OVERWRITE target SELECT col1 FROM source"
        result = extract_lineage(sql, dialect="spark")
        assert result.sources == {"source"}
        assert result.sinks == {"target"}

    def test_trino_dialect(self):
        sql = "INSERT INTO catalog.schema.target SELECT * FROM catalog.schema.source"
        result = extract_lineage(sql, dialect="trino")
        assert result.sources == {"catalog.schema.source"}
        assert result.sinks == {"catalog.schema.target"}

    def test_snowflake_dialect(self):
        sql = "INSERT INTO db.schema.target SELECT * FROM db.schema.source"
        result = extract_lineage(sql, dialect="snowflake")
        assert result.sources == {"db.schema.source"}
        assert result.sinks == {"db.schema.target"}


# ---------------------------------------------------------------------------
# Positive: complex statement types
# ---------------------------------------------------------------------------


class TestPositiveComplexStatements:
    def test_merge(self):
        sql = """
        MERGE INTO target t
        USING source s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.val = s.val
        WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
        """
        result = extract_lineage(sql)
        assert result.sources == {"source"}
        assert result.sinks == {"target"}

    def test_union_query(self):
        sql = """
        INSERT INTO combined
        SELECT id, name FROM table_a
        UNION ALL
        SELECT id, name FROM table_b
        """
        result = extract_lineage(sql)
        assert result.sources == {"table_a", "table_b"}
        assert result.sinks == {"combined"}

    def test_self_referential_insert(self):
        """Table appears as both source and sink — only counted as sink."""
        sql = "INSERT INTO archive SELECT * FROM archive WHERE created_at < '2024-01-01'"
        result = extract_lineage(sql)
        assert result.sinks == {"archive"}

    def test_correlated_subquery(self):
        sql = """
        INSERT INTO flagged
        SELECT * FROM orders o
        WHERE EXISTS (SELECT 1 FROM blacklist b WHERE b.customer_id = o.customer_id)
        """
        result = extract_lineage(sql)
        assert result.sources == {"orders", "blacklist"}
        assert result.sinks == {"flagged"}

    def test_nested_subqueries(self):
        sql = """
        INSERT INTO output
        SELECT * FROM (
            SELECT * FROM (
                SELECT * FROM (
                    SELECT * FROM deep_source
                ) l3
            ) l2
        ) l1
        """
        result = extract_lineage(sql)
        assert result.sources == {"deep_source"}
        assert result.sinks == {"output"}


# ---------------------------------------------------------------------------
# Negative: validation and error handling
# ---------------------------------------------------------------------------


class TestNegative:
    def test_empty_string(self):
        with pytest.raises(ValueError, match="empty"):
            extract_lineage("")

    def test_whitespace_only(self):
        with pytest.raises(ValueError, match="empty"):
            extract_lineage("   \n\t  ")

    def test_jinja_double_braces(self):
        with pytest.raises(ValueError, match="template"):
            extract_lineage("SELECT * FROM {{ var.value.table_name }}")

    def test_jinja_block_tags(self):
        with pytest.raises(ValueError, match="template"):
            extract_lineage("{% if env == 'prod' %}SELECT 1{% endif %}")

    def test_jinja_mixed_with_valid_sql(self):
        with pytest.raises(ValueError, match="template"):
            extract_lineage("INSERT INTO target SELECT * FROM {{ params.source }}")

    def test_unparseable_sql(self):
        with pytest.raises(ValueError):
            extract_lineage("THIS IS NOT SQL AT ALL }{}{")

    def test_semicolons_only(self):
        with pytest.raises(ValueError, match="no valid statements"):
            extract_lineage(";")


# ---------------------------------------------------------------------------
# Scale: performance with large SQL
# ---------------------------------------------------------------------------


class TestScale:
    def test_many_joins(self):
        """50 source tables joined together."""
        tables = [f"src_{i}" for i in range(50)]
        joins = " ".join(f"JOIN {t} ON {t}.id = src_0.id" for t in tables[1:])
        sql = f"INSERT INTO mega_output SELECT * FROM {tables[0]} {joins}"
        result = extract_lineage(sql)
        assert result.sources == set(tables)
        assert result.sinks == {"mega_output"}

    def test_many_ctes(self):
        """30 chained CTEs — none should leak into sources."""
        ctes = []
        for i in range(30):
            if i == 0:
                ctes.append(f"cte_{i} AS (SELECT * FROM real_source)")
            else:
                ctes.append(f"cte_{i} AS (SELECT * FROM cte_{i - 1})")
        sql = f"WITH {', '.join(ctes)} INSERT INTO sink SELECT * FROM cte_29"
        result = extract_lineage(sql)
        assert result.sources == {"real_source"}
        assert result.sinks == {"sink"}
        for i in range(30):
            assert f"cte_{i}" not in result.sources

    def test_many_statements(self):
        """100 independent INSERT statements."""
        stmts = [f"INSERT INTO sink_{i} SELECT * FROM src_{i}" for i in range(100)]
        sql = ";\n".join(stmts)
        result = extract_lineage(sql)
        assert result.sources == {f"src_{i}" for i in range(100)}
        assert result.sinks == {f"sink_{i}" for i in range(100)}

    def test_deeply_nested_subqueries(self):
        """20 levels of subquery nesting."""
        inner = "SELECT * FROM deep_source"
        for i in range(20):
            inner = f"SELECT * FROM ({inner}) sub_{i}"
        sql = f"INSERT INTO output {inner}"
        result = extract_lineage(sql)
        assert result.sources == {"deep_source"}
        assert result.sinks == {"output"}

    def test_large_sql_string(self):
        """~100KB SQL string with many columns."""
        cols = ", ".join(f"col_{i}" for i in range(3000))
        sql = f"INSERT INTO big_table SELECT {cols} FROM source_table"
        assert len(sql) > 20_000
        result = extract_lineage(sql)
        assert result.sources == {"source_table"}
        assert result.sinks == {"big_table"}
