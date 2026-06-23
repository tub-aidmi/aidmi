#!/usr/bin/env python3
"""One-off: extract tmp/*.db SQLite pairs into Postgres-compatible fixture SQL.

Run from repo root:
  uv run python scripts/extract_sqlite_fixtures.py
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TMP_DIR = REPO_ROOT / "tmp"
FIXTURES_DIR = (
    REPO_ROOT / "packages/orchestrator/src/aidmi_orchestrator/fixtures"
)

FIXTURE_NAMES = ("master", "wrong_field_names", "messy_data", "missing_relations")

EXCLUDED_TABLES = frozenset({"_migration_log", "_field_mapping", "sqlite_sequence"})

SQLITE_TYPE_MAP = {
    "TEXT": "text",
    "INTEGER": "integer",
    "REAL": "double precision",
    "BLOB": "bytea",
    "NUMERIC": "numeric",
}


def needs_quoting(name: str) -> bool:
    return name != name.lower()


def pg_ident(name: str) -> str:
    if needs_quoting(name):
        return f'"{name}"'
    return name


def pg_literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return repr(value)
    if isinstance(value, bytes):
        return "NULL"
    s = str(value).replace("'", "''")
    return f"'{s}'"


def strip_sql_comments(sql: str) -> str:
    lines = []
    for line in sql.splitlines():
        if "--" in line:
            line = line.split("--", 1)[0]
        stripped = line.rstrip()
        if stripped:
            lines.append(stripped)
    return " ".join(lines)


def convert_sqlite_ddl(sql: str, *, quote_mixed_case: bool) -> str:
    out = strip_sql_comments(sql).strip().rstrip(";")

    for sqlite_t, pg_t in SQLITE_TYPE_MAP.items():
        out = re.sub(rf"\b{sqlite_t}\b", pg_t, out, flags=re.IGNORECASE)

    out = re.sub(
        r"datetime\s*\(\s*'now'\s*\)",
        "CURRENT_TIMESTAMP",
        out,
        flags=re.IGNORECASE,
    )
    out = re.sub(
        r"DEFAULT\s*\(\s*CURRENT_TIMESTAMP\s*\)",
        "DEFAULT CURRENT_TIMESTAMP",
        out,
        flags=re.IGNORECASE,
    )
    out = re.sub(r"\bINTEGER PRIMARY KEY AUTOINCREMENT\b", "integer PRIMARY KEY", out, flags=re.IGNORECASE)

    if quote_mixed_case:
        out = _quote_ddl_identifiers(out)

    out = re.sub(r"\s+", " ", out)
    return out + ";"


def _quote_ddl_identifiers(sql: str) -> str:
    def quote_ref(match: re.Match) -> str:
        table = match.group(1)
        col = match.group(2) if match.lastindex >= 2 else None
        if col:
            return f"REFERENCES {pg_ident(table)}({pg_ident(col)})"
        return f"REFERENCES {pg_ident(table)}"

    sql = re.sub(
        r"REFERENCES\s+(\w+)\s*\(\s*(\w+)\s*\)",
        quote_ref,
        sql,
        flags=re.IGNORECASE,
    )

    def quote_check_col(match: re.Match) -> str:
        col = match.group(1)
        return f"CHECK ({pg_ident(col)} IN"

    sql = re.sub(r"CHECK\s*\(\s*(\w+)\s+IN\b", quote_check_col, sql, flags=re.IGNORECASE)

    create_match = re.match(r"CREATE TABLE\s+(\S+)\s*\((.*)\)\s*;?\s*$", sql, re.DOTALL | re.IGNORECASE)
    if not create_match:
        return sql

    table_raw = create_match.group(1).strip('"')
    body = create_match.group(2)
    lines = _split_column_defs(body)
    converted_lines = []
    for line in lines:
        converted_lines.append(_quote_column_def_line(line))

    table = pg_ident(table_raw)
    inner = ",\n    ".join(converted_lines)
    return f"CREATE TABLE {table} (\n    {inner}\n)"


def _split_column_defs(body: str) -> list[str]:
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            part = "".join(current).strip()
            if part:
                parts.append(part)
            current = []
            continue
        current.append(ch)
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def _quote_column_def_line(line: str) -> str:
    stripped = line.strip()
    upper = stripped.upper()
    if upper.startswith(("PRIMARY KEY", "UNIQUE ", "CHECK ", "FOREIGN KEY", "CONSTRAINT")):
        return stripped

    match = re.match(r"^(\w+)\s+(.*)$", stripped, re.DOTALL)
    if not match:
        return stripped
    col, rest = match.group(1), match.group(2)
    return f"{pg_ident(col)} {rest}"


def table_needs_quoting(conn: sqlite3.Connection, table: str) -> bool:
    if needs_quoting(table):
        return True
    cols = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    return any(needs_quoting(row[1]) for row in cols)


def list_tables(conn: sqlite3.Connection, *, exclude_internal: bool) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    tables = [r[0] for r in rows]
    if exclude_internal:
        tables = [t for t in tables if t not in EXCLUDED_TABLES]
    return tables


def build_create_table(conn: sqlite3.Connection, table: str) -> str:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    if not row or not row[0]:
        raise ValueError(f"no DDL for table {table!r}")
    quote = table_needs_quoting(conn, table)
    ddl = convert_sqlite_ddl(row[0], quote_mixed_case=quote)
    if quote and not ddl.startswith(f'CREATE TABLE "{table}"'):
        ddl = re.sub(
            rf'^CREATE TABLE\s+{re.escape(table)}\s*\(',
            f'CREATE TABLE "{table}" (',
            ddl,
            count=1,
            flags=re.IGNORECASE,
        )
    return ddl


def build_inserts(conn: sqlite3.Connection, table: str) -> list[str]:
    cols = [row[1] for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()]
    if not cols:
        return []
    col_list = ", ".join(pg_ident(c) for c in cols)
    table_ident = pg_ident(table)
    rows = conn.execute(f'SELECT * FROM "{table}"').fetchall()
    if not rows:
        return []
    statements: list[str] = []
    batch_size = 100
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        value_groups = []
        for row in batch:
            values = ", ".join(pg_literal(v) for v in row)
            value_groups.append(f"({values})")
        statements.append(
            f"INSERT INTO {table_ident} ({col_list}) VALUES\n  "
            + ",\n  ".join(value_groups)
            + ";"
        )
    return statements


def extract_source(fixture: str) -> str:
    db_path = TMP_DIR / f"{fixture}_source.db"
    schema = f"fixture_{fixture}_src"
    conn = sqlite3.connect(db_path)
    try:
        lines = [
            f"CREATE SCHEMA IF NOT EXISTS {schema};",
            f"SET search_path TO {schema};",
            "",
        ]
        for table in list_tables(conn, exclude_internal=True):
            lines.append(build_create_table(conn, table))
            lines.append("")
            inserts = build_inserts(conn, table)
            if inserts:
                lines.extend(inserts)
                lines.append("")
        return "\n".join(lines).rstrip() + "\n"
    finally:
        conn.close()


def order_tables_by_fk(conn: sqlite3.Connection, tables: list[str]) -> list[str]:
    deps: dict[str, set[str]] = {t: set() for t in tables}
    for table in tables:
        ddl = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()[0]
        for match in re.finditer(
            r"REFERENCES\s+(\w+)\s*\(",
            ddl,
            flags=re.IGNORECASE,
        ):
            ref = match.group(1)
            if ref in deps and ref != table:
                deps[table].add(ref)

    ordered: list[str] = []
    remaining = set(tables)
    while remaining:
        ready = sorted(t for t in remaining if not deps[t] - set(ordered))
        if not ready:
            ordered.extend(sorted(remaining))
            break
        for t in ready:
            ordered.append(t)
            remaining.remove(t)
    return ordered


def extract_destination(fixture: str) -> str:
    db_path = TMP_DIR / f"{fixture}_destination.db"
    conn = sqlite3.connect(db_path)
    try:
        lines = [
            "-- Target schema definition (structure only — no data)",
            "-- Postgres-compatible DDL generated from SQLite destination.db",
            "",
        ]
        tables = list_tables(conn, exclude_internal=True)
        for table in order_tables_by_fk(conn, tables):
            lines.append(build_create_table(conn, table))
            lines.append("")
        return "\n".join(lines).rstrip() + "\n"
    finally:
        conn.close()


def main() -> None:
    for name in FIXTURE_NAMES:
        src_db = TMP_DIR / f"{name}_source.db"
        dst_db = TMP_DIR / f"{name}_destination.db"
        if not src_db.exists() or not dst_db.exists():
            raise SystemExit(f"missing db pair for fixture {name!r}")

        out_dir = FIXTURES_DIR / name
        out_dir.mkdir(parents=True, exist_ok=True)

        source_sql = extract_source(name)
        dest_sql = extract_destination(name)

        (out_dir / "source.sql").write_text(source_sql, encoding="utf-8")
        (out_dir / "destination.sql").write_text(dest_sql, encoding="utf-8")

        src_conn = sqlite3.connect(src_db)
        try:
            tables = list_tables(src_conn, exclude_internal=True)
            counts = {
                t: src_conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
                for t in tables
            }
        finally:
            src_conn.close()

        print(f"{name}:")
        print(f"  wrote {out_dir / 'source.sql'}")
        print(f"  wrote {out_dir / 'destination.sql'}")
        for t, n in counts.items():
            print(f"  {t}: {n} rows")


if __name__ == "__main__":
    main()
