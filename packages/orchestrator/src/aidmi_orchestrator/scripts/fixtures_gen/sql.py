"""SQL literal and INSERT statement rendering."""

from __future__ import annotations

from typing import Any


def sql_val(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def format_inserts(
    table: str,
    columns: list[str],
    rows: list[tuple[Any, ...]],
    *,
    quote_columns: bool = False,
) -> str:
    if not rows:
        return ""
    if quote_columns:
        col_list = ", ".join(f'"{c.strip(chr(34))}"' for c in columns)
    else:
        col_list = ", ".join(columns)
    value_lines = []
    for row in rows:
        vals = ", ".join(sql_val(v) for v in row)
        value_lines.append(f"  ({vals})")
    return f"INSERT INTO {table} ({col_list}) VALUES\n" + ",\n".join(value_lines) + ";"


def schema_header(schema: str) -> str:
    return f"CREATE SCHEMA IF NOT EXISTS {schema};\nSET search_path TO {schema};\n"
