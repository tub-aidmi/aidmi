"""Sanitize and validate LLM-produced dbt SQL before writing to disk."""

from __future__ import annotations

import re

_MARKDOWN_FENCE_RE = re.compile(
    r"^\s*```(?:sql)?\s*\n?(.*?)\n?\s*```\s*$",
    re.IGNORECASE | re.DOTALL,
)
_WRAPPER_RE = re.compile(
    r"^\s*normalize_dbt_sql\s*\(\s*['\"]{3}(.*?)['\"]{3}\s*\)\s*$",
    re.IGNORECASE | re.DOTALL,
)
_TRAILING_WRAPPER_CLOSE_RE = re.compile(r"['\"]{3}\s*\)\s*$")
_VALID_START_RE = re.compile(
    r"(?:^\s*--[^\n]*\n\s*)*(?:\{\{\s*config\s*\(|SELECT\b|WITH\b)",
    re.IGNORECASE | re.MULTILINE,
)
_INVENTED_FUNCTION_RE = re.compile(
    r"^\s*normalize_dbt_sql\s*\(", re.IGNORECASE | re.MULTILINE
)
_CONFIG_RE = re.compile(r"\{\{\s*config\s*\(\s*materialized\s*=", re.IGNORECASE)


def sanitize_dbt_sql(sql: str) -> str:
    """Strip markdown fences, wrapper functions, and leading garbage from model SQL."""
    text = sql.lstrip()
    if not text.rstrip():
        return text.rstrip()

    fence = _MARKDOWN_FENCE_RE.match(text.rstrip())
    if fence:
        text = fence.group(1).lstrip()
        if sql.endswith("\n"):
            text = text.rstrip() + "\n"

    wrapper = _WRAPPER_RE.match(text.rstrip())
    if wrapper:
        text = wrapper.group(1).lstrip()
        if sql.endswith("\n"):
            text = text.rstrip() + "\n"

    text = _TRAILING_WRAPPER_CLOSE_RE.sub("", text.rstrip()).rstrip()
    if sql.endswith("\n") and text:
        text = text + "\n"

    match = _VALID_START_RE.search(text)
    if match and match.start() > 0:
        text = text[match.start() :].lstrip()
        if sql.endswith("\n") and text and not text.endswith("\n"):
            text = text.rstrip() + "\n"

    return text


def validate_dbt_sql(sql: str) -> str | None:
    """Return an error message if SQL fails post-sanitize checks, else None."""
    if not sql.strip():
        return "model SQL is empty"

    if _INVENTED_FUNCTION_RE.search(sql):
        return (
            "model SQL must not use invented wrapper functions (e.g. normalize_dbt_sql)"
        )

    if not _CONFIG_RE.search(sql):
        return "model SQL must contain {{ config(materialized="

    return None
