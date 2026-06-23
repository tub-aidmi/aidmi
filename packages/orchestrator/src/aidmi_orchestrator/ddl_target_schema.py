"""Parse Postgres CREATE TABLE DDL into TargetSchema."""
from __future__ import annotations

import re

from aidmi_orchestrator.domain import TargetColumn, TargetSchema, TargetTable

CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:\"([^\"]+)\"|(\w+))\s*\((.*)\)\s*;",
    re.IGNORECASE | re.DOTALL,
)

SQL_TYPE_RE = re.compile(
    r"^(text|integer|bigint|smallint|boolean|date|"
    r"timestamp(?:\s+with\s+time\s+zone|\s+without\s+time\s+zone)?|"
    r"timestamptz|double\s+precision|real|numeric(?:\(\d+(?:,\d+)?\))?)\b",
    re.IGNORECASE,
)

CHECK_ENUM_RE = re.compile(
    r"CHECK\s*\(\s*\"?(\w+)\"?\s+IN\s*\((.*?)\)\s*\)",
    re.IGNORECASE | re.DOTALL,
)

STRING_LITERAL_RE = re.compile(r"'((?:''|[^'])*)'")


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


def _parse_enum_values(check_body: str) -> list[str]:
    return [m.group(1).replace("''", "'") for m in STRING_LITERAL_RE.finditer(check_body)]


def _extract_sql_type(rest: str) -> tuple[str, str]:
    match = SQL_TYPE_RE.match(rest.strip())
    if not match:
        first = rest.split()[0] if rest.split() else "text"
        return first.lower(), rest[len(first) :].strip()
    sql_type = re.sub(r"\s+", " ", match.group(1).lower())
    if sql_type == "timestamp with time zone":
        sql_type = "timestamptz"
    return sql_type, rest[match.end() :].strip()


def _parse_column_def(defn: str) -> TargetColumn | list[str] | None:
    stripped = defn.strip()
    upper = stripped.upper()

    if upper.startswith("PRIMARY KEY"):
        inner = stripped[stripped.index("(") + 1 : stripped.rindex(")")]
        return [c.strip().strip('"') for c in inner.split(",")]

    if upper.startswith(("CONSTRAINT", "FOREIGN KEY", "UNIQUE ", "CHECK ")):
        return None

    name_match = re.match(r'^"([^"]+)"\s+(.*)$', stripped, re.DOTALL) or re.match(
        r"^(\w+)\s+(.*)$", stripped, re.DOTALL
    )
    if not name_match:
        return None

    name, rest = name_match.group(1), name_match.group(2)
    sql_type, constraints = _extract_sql_type(rest)

    is_pk = bool(re.search(r"\bPRIMARY\s+KEY\b", constraints, re.IGNORECASE))
    not_null = bool(re.search(r"\bNOT\s+NULL\b", constraints, re.IGNORECASE))
    nullable = not (is_pk or not_null)

    enum_values: list[str] | None = None
    check_match = CHECK_ENUM_RE.search(constraints)
    if check_match and check_match.group(1) == name:
        enum_values = _parse_enum_values(check_match.group(2))

    return TargetColumn(
        name=name,
        sql_type=sql_type,
        nullable=nullable,
        enum_values=enum_values or None,
    )


def parse_create_table(ddl: str) -> TargetTable:
    match = CREATE_TABLE_RE.search(ddl.strip())
    if not match:
        raise ValueError("no CREATE TABLE statement found")

    table_name = match.group(1) or match.group(2)
    body = match.group(3)
    primary_key: list[str] = []
    columns: list[TargetColumn] = []

    for part in _split_column_defs(body):
        parsed = _parse_column_def(part)
        if parsed is None:
            continue
        if isinstance(parsed, list):
            primary_key = parsed
            continue
        columns.append(parsed)
        if re.search(r"\bPRIMARY\s+KEY\b", part, re.IGNORECASE):
            primary_key.append(parsed.name)

    return TargetTable(
        name=table_name,
        columns=columns,
        primary_key=primary_key or None,
    )


def parse_ddl_file(sql: str) -> TargetSchema:
    tables: list[TargetTable] = []
    for statement in _iter_create_statements(sql):
        tables.append(parse_create_table(statement))
    if not tables:
        raise ValueError("no CREATE TABLE statements found in DDL")
    return TargetSchema(tables=tables)


def _iter_create_statements(sql: str) -> list[str]:
    statements: list[str] = []
    for line in sql.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        if stripped.upper().startswith("CREATE TABLE"):
            statements.append(stripped)
    return statements
