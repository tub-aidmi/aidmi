"""Modular transformation guidelines."""
from __future__ import annotations

import re

from aidmi_orchestrator.domain import (
    ColumnInfo, SourceSummary, TableInfo, TargetColumn, TargetSchema, TargetTable,
)
from aidmi_orchestrator.strategy.base import build_context_prompt
from aidmi_orchestrator.strategy.guidelines.compose import (
    writer_system_prompt,
)
from aidmi_orchestrator.strategy.guidelines.dbt import DBT_PROJECT_GUIDELINES
from aidmi_orchestrator.strategy.guidelines.planning import PLANNING_GUIDELINES
from aidmi_orchestrator.strategy.guidelines.postgres import POSTGRES_SQL_GUIDELINES
from aidmi_orchestrator.strategy.guidelines.query_tool import QUERY_TOOL_GUIDELINES
from aidmi_orchestrator.strategy.guidelines.transformation import TRANSFORMATION_GUIDELINES

_FIXTURE_SPECIFIC = re.compile(
    r"fixture_master|KD-M|CUST-M|Gewonnen|Entscheider|master_kunden",
    re.IGNORECASE,
)


def _assert_fixture_agnostic(text: str) -> None:
    assert not _FIXTURE_SPECIFIC.search(text), f"fixture-specific content found: {text[:200]}"


def test_guideline_modules_non_empty_and_fixture_agnostic() -> None:
    for name, text in [
        ("postgres", POSTGRES_SQL_GUIDELINES),
        ("dbt", DBT_PROJECT_GUIDELINES),
        ("transformation", TRANSFORMATION_GUIDELINES),
        ("planning", PLANNING_GUIDELINES),
        ("query_tool", QUERY_TOOL_GUIDELINES),
    ]:
        assert text.strip(), f"{name} guidelines empty"
        _assert_fixture_agnostic(text)


def test_writer_system_prompt_includes_quoted_aliases_and_source() -> None:
    prompt = writer_system_prompt("You are a writer.")
    assert 'AS "ColumnName"' in prompt or 'AS "Custom_Field__c"' in prompt
    assert "source(" in prompt
    assert "TRY_CAST" in prompt


def test_build_context_prompt_includes_transformation_guidelines() -> None:
    summary = SourceSummary(tables=[TableInfo(
        db_schema="src_raw",
        name="contacts",
        columns=[ColumnInfo(name="id", sql_type="integer", nullable=False)],
        row_count=1,
        sample_rows=[],
    )])
    target = TargetSchema(tables=[TargetTable(
        name="users",
        columns=[TargetColumn(name="user_id", sql_type="integer")],
    )])
    prompt = build_context_prompt(summary, target, "metadata_only")
    assert "# Transformation guidelines" in prompt
    assert "Cross-table keys" in prompt


def test_retry_user_prompt_includes_correction_reminder() -> None:
    from aidmi_orchestrator.strategy.structured_common import retry_user_prompt

    prompt = retry_user_prompt("users", "CTX", "SELECT 1", "syntax error")
    assert "double-quoted alias" in prompt
    assert "AS <type>" in prompt or "AS DOUBLE PRECISION" in prompt
