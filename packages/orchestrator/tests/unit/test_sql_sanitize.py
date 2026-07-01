"""Tests for dbt SQL sanitization and validation."""
from __future__ import annotations

from aidmi_orchestrator.strategy.sql_sanitize import sanitize_dbt_sql, validate_dbt_sql

_CLEAN = """{{ config(materialized='table') }}

SELECT 1 AS "Id"
FROM {{ source('src', 't') }}
"""

_WRAPPED = """normalize_dbt_sql('''{{ config(materialized='table') }}

SELECT kunden_nr AS "Id"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}''')"""

_GARBAGE_PREFIX = """ごはん{{ config(materialized='table') }}

SELECT asset.id AS "Id"
FROM {{ source('src', 'asset') }} AS asset
"""


def test_sanitize_leaves_clean_sql_unchanged():
    assert sanitize_dbt_sql(_CLEAN) == _CLEAN


def test_sanitize_strips_normalize_dbt_sql_wrapper():
    out = sanitize_dbt_sql(_WRAPPED)
    assert "normalize_dbt_sql" not in out
    assert out.startswith("{{ config(materialized='table') }}")


def test_sanitize_strips_garbage_prefix():
    out = sanitize_dbt_sql(_GARBAGE_PREFIX)
    assert not out.startswith("ごはん")
    assert out.startswith("{{ config(materialized='table') }}")


def test_sanitize_strips_markdown_fence():
    fenced = f"```sql\n{_CLEAN}\n```"
    assert sanitize_dbt_sql(fenced).rstrip() == _CLEAN.rstrip()


def test_validate_accepts_clean_sql():
    assert validate_dbt_sql(_CLEAN) is None


def test_validate_rejects_invented_function():
    err = validate_dbt_sql("normalize_dbt_sql('''SELECT 1''')")
    assert err is not None
    assert "invented" in err


def test_validate_rejects_missing_config():
    err = validate_dbt_sql("SELECT 1")
    assert err is not None
    assert "config" in err
