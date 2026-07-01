from aidmi_orchestrator.strategy.validation import (
    strip_jinja, validate_model_sql, validate_models,
)

VALID = """\
-- depends_on: {{ source('fixture_messy_data_src', 'Account') }}
{{ config(materialized='table') }}
SELECT
    "Id" AS "Id",
    COALESCE(TRIM("Name"), 'Unknown') AS "Name"
FROM {{ source('fixture_messy_data_src', 'Account') }}
"""

LEAKED_FILENAME = """\
{{ config(materialized='table') }}
SELECT *
FROM normalize_opportunity.sql
"""

DANGLING_PAREN = """\
{{ config(materialized='table') }}
SELECT "Id" AS "Id",
    CASE WHEN "x" THEN 1 END AS "y"
FROM {{ source('s', 'Contact') }}
);
"""

def test_strip_jinja_replaces_source_with_relation():
    out = strip_jinja(VALID)
    assert "{{" not in out
    assert '"Account"' in out
    assert "depends_on" not in out

def test_valid_model_passes():
    assert validate_model_sql(VALID) == []

def test_dangling_paren_flagged():
    assert validate_model_sql(DANGLING_PAREN) != []

def test_empty_after_strip_flagged():
    assert validate_model_sql("{{ config(materialized='table') }}") != []

def test_validate_models_returns_only_failing():
    result = validate_models({"Account": VALID, "Opportunity": DANGLING_PAREN})
    assert "Account" not in result
    assert "Opportunity" in result
