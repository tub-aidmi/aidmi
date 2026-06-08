"""structured_common: shared per-table structured agent machinery."""
from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from pydantic_ai.models.test import TestModel

from aidmi_orchestrator.domain import (
    ColumnInfo, SourceSummary, TableInfo, TargetColumn, TargetSchema, TargetTable,
)
from aidmi_orchestrator.strategy.structured_common import (
    TableMapping, make_table_agent, generate_table_mapping, manifest_from_mappings,
    per_table_user_prompt, WRITER_SYSTEM_PROMPT,
)


def small_source_summary() -> SourceSummary:
    return SourceSummary(tables=[TableInfo(
        db_schema="src_t_raw", name="contacts",
        columns=[ColumnInfo(name="id", sql_type="integer", nullable=False),
                 ColumnInfo(name="email", sql_type="text", nullable=True)],
        row_count=4,
        sample_rows=[{"id": 1, "email": "a@x.de"}],
    )])


def small_target_schema() -> TargetSchema:
    return TargetSchema(tables=[TargetTable(
        name="users",
        columns=[TargetColumn(name="user_id", sql_type="integer"),
                 TargetColumn(name="email", sql_type="text", nullable=True)],
    )])


def fake_api(tmp_path, make_llm):
    return SimpleNamespace(
        source_summary=small_source_summary(),
        target_schema=small_target_schema(),
        dbt_project_path=tmp_path,
        staging_raw_dataset="src_t_raw",
        staging_out_dataset="src_t_out",
        make_llm=make_llm,
        trace=MagicMock(),
        query_postgres=MagicMock(return_value=[]),
    )


MAPPING_ARGS = dict(
    target_table="users",
    dbt_sql="{{ config(materialized='table') }}\nSELECT 1 AS user_id",
    column_notes=[{"target_column": "user_id", "source_columns": ["contacts.id"], "explanation": "direct"}],
    reasoning="trivial",
)


def test_generate_table_mapping_returns_structured_output() -> None:
    agent = make_table_agent(TestModel(custom_output_args=MAPPING_ARGS))
    mapping = asyncio.run(generate_table_mapping(agent, "users", "context here"))
    assert isinstance(mapping, TableMapping)
    assert mapping.target_table == "users"
    assert "SELECT 1" in mapping.dbt_sql


def test_per_table_user_prompt_mentions_table_and_context() -> None:
    prompt = per_table_user_prompt("users", "THE_CONTEXT")
    assert "`users`" in prompt
    assert "THE_CONTEXT" in prompt


def test_writer_system_prompt_keeps_dbt_rules() -> None:
    assert "{{ config(materialized='table') }}" in WRITER_SYSTEM_PROMPT
    assert "source(" in WRITER_SYSTEM_PROMPT


def test_manifest_from_mappings_builds_notes() -> None:
    mapping = TableMapping(**MAPPING_ARGS)
    manifest = manifest_from_mappings(
        [mapping], source_table_names=["contacts"],
        strategy_name="x", strategy_config={"k": 1},
    )
    assert manifest.strategy_name == "x"
    assert manifest.tables[0].target_table == "users"
    assert manifest.tables[0].source_tables == ["contacts"]
    assert manifest.tables[0].column_notes[0].target_column == "user_id"
