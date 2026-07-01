from __future__ import annotations

import re

import sqlglot
from sqlglot.errors import ParseError

_JINJA_SOURCE_RE = re.compile(
    r"\{\{\s*source\s*\(\s*['\"][^'\"]+['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}"
)
_JINJA_ANY_RE = re.compile(r"\{\{.*?\}\}", re.DOTALL)
_DEPENDS_ON_RE = re.compile(r"^\s*--\s*depends_on:.*$", re.MULTILINE)


def strip_jinja(sql: str) -> str:
    s = _DEPENDS_ON_RE.sub("", sql)
    s = _JINJA_SOURCE_RE.sub(lambda m: f'"{m.group(1)}"', s)
    s = _JINJA_ANY_RE.sub("", s)
    return s.strip()


def validate_model_sql(sql: str) -> list[str]:
    stripped = strip_jinja(sql)
    if not stripped:
        return ["model SQL is empty after removing dbt Jinja"]
    try:
        statements = sqlglot.parse(stripped, dialect="postgres")
    except ParseError as exc:
        return [f"SQL parse error: {exc}"]
    if not any(statements):
        return ["no parseable SQL statement found"]
    return []


def validate_models(sql_by_table: dict[str, str]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for table, sql in sql_by_table.items():
        errs = validate_model_sql(sql)
        if errs:
            out[table] = errs
    return out
