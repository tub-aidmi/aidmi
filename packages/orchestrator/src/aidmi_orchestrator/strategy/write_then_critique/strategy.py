"""WriteThenCritique: per-table writers + one global critic, bounded revision rounds."""
from __future__ import annotations
from datetime import datetime
from typing import Literal
from pydantic import BaseModel, Field
from pydantic_ai import Agent

from aidmi_orchestrator.domain import ModelSpec, StrategyResult
from aidmi_orchestrator.strategy.base import build_context_prompt, run_coroutines, write_proposal
from aidmi_orchestrator.strategy.structured_common import (
    generate_table_mapping, make_table_agent, manifest_from_mappings, TableMapping,
)
from aidmi_orchestrator.strategy.write_then_critique.critique import (
    CritiqueReport, run_critique_rounds,
)
from aidmi_orchestrator.strategy.write_then_critique.prompts import (
    CRITIC_SYSTEM_PROMPT, critique_user_prompt, render_proposal, revision_user_prompt,
)
from aidmi_orchestrator.trace import StrategyEvent


class WriteThenCritiqueConfig(BaseModel):
    writer_model: ModelSpec
    critic_model: ModelSpec | None = None
    max_critique_rounds: int = Field(default=2, ge=1)
    context_mode: Literal["metadata_only", "metadata_plus_samples", "live_query_tool"] = "metadata_plus_samples"
    samples_per_table: int = 3
    max_query_tool_rows: int = 100
    serial_llm_calls: bool = False


class WriteThenCritique:
    name = "write_then_critique"

    def __init__(self, config: WriteThenCritiqueConfig):
        self.config = config

    async def generate(self, api) -> StrategyResult:
        if api.target_schema is None:
            raise ValueError("write_then_critique requires a target_schema")

        writer = api.make_llm(self.config.writer_model, role="writer")
        critic = api.make_llm(self.config.critic_model or self.config.writer_model, role="critic")
        context = build_context_prompt(
            api.source_summary, api.target_schema, self.config.context_mode,
            samples_per_table=self.config.samples_per_table,
        )
        writer_agent = make_table_agent(
            writer, api=api,
            enable_query_tool=(self.config.context_mode == "live_query_tool"),
            max_query_tool_rows=self.config.max_query_tool_rows,
        )
        critic_agent = Agent(critic, output_type=CritiqueReport, system_prompt=CRITIC_SYSTEM_PROMPT)

        target_table_names = [t.name for t in api.target_schema.tables]
        initial = await run_coroutines(
            [generate_table_mapping(writer_agent, n, context) for n in target_table_names],
            serial=self.config.serial_llm_calls,
        )
        mappings = {m.target_table: m for m in initial}

        async def critique(current: dict[str, TableMapping]) -> CritiqueReport:
            result = await critic_agent.run(
                critique_user_prompt(context, render_proposal(current))
            )
            api.trace.record(StrategyEvent(
                timestamp=datetime.utcnow(), label="critique_round_complete",
                data={"verdicts": [v.model_dump() for v in result.output.verdicts]},
            ))
            return result.output

        async def revise(name: str, previous: TableMapping, comments: str) -> TableMapping:
            result = await writer_agent.run(
                revision_user_prompt(name, context, previous.dbt_sql, comments)
            )
            return result.output

        mappings, approved = await run_critique_rounds(
            mappings, critique, revise,
            max_rounds=self.config.max_critique_rounds,
            serial=self.config.serial_llm_calls,
        )

        sql_by_table = {name: m.dbt_sql for name, m in mappings.items()}
        source_tables = sorted({(t.db_schema, t.name) for t in api.source_summary.tables})
        write_proposal(api.dbt_project_path, sql_by_table, source_tables, api.source_schema)

        manifest = manifest_from_mappings(
            list(mappings.values()),
            source_table_names=[t.name for t in api.source_summary.tables],
            strategy_name=self.name,
            strategy_config=self.config.model_dump(),
        )
        return StrategyResult(
            target_tables_written=list(sql_by_table),
            target_schema=api.target_schema,
            manifest=manifest,
            self_reported_status="complete" if approved else "partial",
        )
