from __future__ import annotations

import re
from dataclasses import dataclass

import sqlglot
from sqlglot import exp


_JINJA_PATTERN = re.compile(r"\{\{.*?\}\}|\{%.*?%\}")


@dataclass(frozen=True)
class ParseResult:
    sources: frozenset[str]
    sinks: frozenset[str]


def _table_name(table: exp.Table) -> str:
    parts = []
    if table.catalog:
        parts.append(table.catalog)
    if table.db:
        parts.append(table.db)
    parts.append(table.name)
    return ".".join(parts)


def _collect_cte_names(statement: exp.Expression) -> set[str]:
    return {cte.alias for cte in statement.find_all(exp.CTE)}


def extract_lineage(sql: str, *, dialect: str | None = None) -> ParseResult:
    if not sql or not sql.strip():
        raise ValueError("SQL string is empty")

    if _JINJA_PATTERN.search(sql):
        raise ValueError("SQL contains Jinja template expressions; provide explicit sources/sinks")

    try:
        statements = sqlglot.parse(sql, dialect=dialect)
    except sqlglot.errors.ParseError as e:
        raise ValueError(f"Failed to parse SQL: {e}") from e

    if not statements or all(s is None for s in statements):
        raise ValueError("Failed to parse SQL: no valid statements")

    sources: set[str] = set()
    sinks: set[str] = set()

    for statement in statements:
        if statement is None:
            continue

        cte_names = _collect_cte_names(statement)

        if isinstance(statement, (exp.Insert, exp.Create, exp.Merge)):
            target = statement.find(exp.Table)
            if target:
                sinks.add(_table_name(target))

        for table in statement.find_all(exp.Table):
            name = _table_name(table)
            if name not in sinks and table.name not in cte_names:
                sources.add(name)

    return ParseResult(sources=frozenset(sources), sinks=frozenset(sinks))
