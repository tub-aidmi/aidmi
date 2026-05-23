"""WriteToolsFreeform: a single agent with file-write tools (optionally with
query + run_dbt for self-correction)."""
from __future__ import annotations
from typing import Literal
from pydantic import BaseModel
from pydantic_ai import Agent, Tool, UsageLimits

from aidmi_pipeline.sources_yaml import ensure_sources_yaml_raw_schema

from aidmi_orchestrator.domain import ModelSpec, StrategyResult
from aidmi_orchestrator.strategy.base import (
    build_context_prompt,
)
from aidmi_orchestrator.strategy.write_tools_freeform.prompts import (
    SYSTEM_PROMPT, initial_user_prompt,
)
from aidmi_orchestrator.strategy.write_tools_freeform.tools import (
    make_write_file, make_read_file, make_query_postgres, make_run_dbt,
)


class WriteToolsFreeformConfig(BaseModel):
    writer_model: ModelSpec
    context_mode: Literal["metadata_only", "metadata_plus_samples", "live_query_tool"] = "metadata_plus_samples"
    samples_per_table: int = 3
    max_query_tool_rows: int = 100
    max_tool_turns: int = 20
    enable_self_correction: bool = False
    max_self_correction_passes: int = 3


class WriteToolsFreeform:
    name = "write_tools_freeform"

    def __init__(self, config: WriteToolsFreeformConfig):
        self.config = config

    async def generate(self, api) -> StrategyResult:
        traced_model = api.make_llm(self.config.writer_model, role="writer")
        tools: list[Tool] = [
            Tool(make_write_file(api), name="write_file"),
            Tool(make_read_file(api), name="read_file"),
        ]
        if self.config.context_mode == "live_query_tool":
            tools.append(Tool(
                make_query_postgres(api, self.config.max_query_tool_rows),
                name="query_postgres",
            ))
        if self.config.enable_self_correction:
            tools.append(Tool(make_run_dbt(api, self.config.max_self_correction_passes), name="run_dbt"))

        agent = Agent(
            traced_model,
            tools=tools,
            system_prompt=SYSTEM_PROMPT,
        )
        context = build_context_prompt(
            api.source_summary, api.target_schema, self.config.context_mode,
            samples_per_table=self.config.samples_per_table,
        )
        await agent.run(initial_user_prompt(context), usage_limits=UsageLimits(request_limit=self.config.max_tool_turns))

        models_dir = api.dbt_project_path / "models"
        ensure_sources_yaml_raw_schema(models_dir, api.staging_raw_dataset)
        produced = [
            p.stem for p in models_dir.glob("*.sql")
        ] if models_dir.exists() else []

        status = "complete" if produced else "gave_up"
        return StrategyResult(
            target_tables_written=produced,
            target_schema=api.target_schema,
            manifest=None,
            self_reported_status=status,
        )
