"""StructuredPerTable: one PydanticAI agent per target table, parallel via asyncio.gather."""
from __future__ import annotations
import asyncio
from typing import Literal
from pydantic import BaseModel, Field
from pydantic_ai import Agent

from aidmi_orchestrator.domain import (
    ModelSpec, StrategyResult, MappingManifest, TableMappingNote, ColumnNote,
)
from aidmi_orchestrator.strategy.base import (
    build_context_prompt, write_proposal,
)
from aidmi_orchestrator.strategy.structured_per_table.prompts import (
    SYSTEM_PROMPT, per_table_user_prompt,
)


class StructuredPerTableConfig(BaseModel):
    writer_model: ModelSpec
    context_mode: Literal["metadata_only", "metadata_plus_samples", "live_query_tool"] = "metadata_plus_samples"
    samples_per_table: int = 3
    max_query_tool_rows: int = 100


class _ColumnNoteOut(BaseModel):
    target_column: str
    source_columns: list[str] = Field(default_factory=list)
    explanation: str = ""


class _TableMapping(BaseModel):
    target_table: str
    dbt_sql: str
    column_notes: list[_ColumnNoteOut]
    reasoning: str = ""


class StructuredPerTable:
    name = "structured_per_table"

    def __init__(self, config: StructuredPerTableConfig):
        self.config = config

    async def generate(self, api) -> StrategyResult:
        if api.target_schema is None:
            raise ValueError("structured_per_table requires a target_schema (no free-design mode)")

        traced_model = api.make_llm(self.config.writer_model, role="writer")
        context = build_context_prompt(
            api.source_summary, api.target_schema, self.config.context_mode,
            samples_per_table=self.config.samples_per_table,
        )

        async def one_table(target_table_name: str) -> _TableMapping:
            agent = Agent(
                traced_model,
                output_type=_TableMapping,
                system_prompt=SYSTEM_PROMPT,
            )
            user_prompt = per_table_user_prompt(target_table_name, context)
            result = await agent.run(user_prompt)
            return result.output

        target_table_names = [t.name for t in api.target_schema.tables]
        mappings = await asyncio.gather(*(one_table(n) for n in target_table_names))

        sql_by_table = {m.target_table: m.dbt_sql for m in mappings}
        source_tables = sorted(
            {(t.db_schema, t.name) for t in api.source_summary.tables}
        )
        write_proposal(api.dbt_project_path, sql_by_table, source_tables)

        manifest = MappingManifest(
            tables=[
                TableMappingNote(
                    target_table=m.target_table,
                    source_tables=[t.name for t in api.source_summary.tables],
                    column_notes=[
                        ColumnNote(**c.model_dump()) for c in m.column_notes
                    ],
                    reasoning=m.reasoning,
                )
                for m in mappings
            ],
            strategy_name=self.name,
            strategy_config=self.config.model_dump(),
        )
        return StrategyResult(
            target_tables_written=list(sql_by_table),
            target_schema=api.target_schema,
            manifest=manifest,
            self_reported_status="complete",
        )
