"""Shared building blocks for structured per-table mapping strategies."""
from __future__ import annotations
from typing import Any

from pydantic import BaseModel, Field
from pydantic_ai import Agent, Tool

from aidmi_orchestrator.domain import ColumnNote, MappingManifest, TableMappingNote
from aidmi_orchestrator.strategy.guidelines.compose import (
    retry_correction_reminder,
    writer_system_prompt,
)

_WRITER_ROLE = """\
You are a senior data engineer producing dbt SQL.

You receive a description of a source database (schemas, columns, optionally sample rows) and a target table specification. Your job is to write ONE dbt model — a SELECT statement that transforms the source data into the target schema.

Return a structured TableMapping with:
- target_table: name of the model
- dbt_sql: full SELECT statement
- column_notes: per-target-column source + brief explanation
- reasoning: 1-3 sentences justifying your choices
"""

WRITER_SYSTEM_PROMPT = writer_system_prompt(_WRITER_ROLE)


def per_table_user_prompt(target_table_name: str, context_prompt: str) -> str:
    return (
        f"Target table: `{target_table_name}`.\n\n"
        f"{context_prompt}\n\n"
        f"Produce the dbt model for `{target_table_name}`."
    )


def retry_user_prompt(
    target_table_name: str, context_prompt: str, previous_sql: str, error_message: str,
) -> str:
    return (
        f"Your previous dbt model for `{target_table_name}` failed.\n\n"
        f"Previous SQL:\n```sql\n{previous_sql}\n```\n\n"
        f"dbt error:\n{error_message}\n\n"
        f"{retry_correction_reminder()}\n\n"
        f"{context_prompt}\n\n"
        f"Produce a corrected dbt model for `{target_table_name}` "
        f"using valid PostgreSQL only."
    )


class ColumnNoteOut(BaseModel):
    target_column: str
    source_columns: list[str] = Field(default_factory=list)
    explanation: str = ""


class TableMapping(BaseModel):
    target_table: str
    dbt_sql: str
    column_notes: list[ColumnNoteOut]
    reasoning: str = ""


def make_table_agent(
    model: Any,
    *,
    api: Any = None,
    enable_query_tool: bool = False,
    max_query_tool_rows: int = 100,
    system_prompt: str = WRITER_SYSTEM_PROMPT,
) -> Agent:
    tools: list[Tool] = []
    if enable_query_tool:
        from aidmi_orchestrator.strategy.write_tools_freeform.tools import make_query_postgres
        tools.append(Tool(make_query_postgres(api, max_query_tool_rows), name="query_postgres"))
    return Agent(model, output_type=TableMapping, system_prompt=system_prompt, tools=tools)


async def generate_table_mapping(agent: Agent, target_table_name: str, context: str) -> TableMapping:
    result = await agent.run(per_table_user_prompt(target_table_name, context))
    return result.output


def manifest_from_mappings(
    mappings: list[TableMapping],
    source_table_names: list[str],
    strategy_name: str,
    strategy_config: dict[str, Any],
) -> MappingManifest:
    return MappingManifest(
        tables=[
            TableMappingNote(
                target_table=m.target_table,
                source_tables=source_table_names,
                column_notes=[ColumnNote(**c.model_dump()) for c in m.column_notes],
                reasoning=m.reasoning,
            )
            for m in mappings
        ],
        strategy_name=strategy_name,
        strategy_config=strategy_config,
    )
