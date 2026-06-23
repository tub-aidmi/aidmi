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
    build_initial_user_prompt,
    build_system_prompt,
)
from aidmi_orchestrator.strategy.write_tools_freeform.self_correction import (
    run_post_agent_dbt_loop,
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

        usage_limits = UsageLimits(request_limit=self.config.max_tool_turns)
        agent = Agent(
            traced_model,
            tools=tools,
            system_prompt=build_system_prompt(
                enable_self_correction=self.config.enable_self_correction,
            ),
        )
        context = build_context_prompt(
            api.source_summary, api.target_schema, self.config.context_mode,
            samples_per_table=self.config.samples_per_table,
        )
        await agent.run(
            build_initial_user_prompt(
                context,
                enable_self_correction=self.config.enable_self_correction,
            ),
            usage_limits=usage_limits,
        )

        models_dir = api.dbt_project_path / "models"
        ensure_sources_yaml_raw_schema(models_dir, api.source_schema)
        produced = [
            p.stem for p in models_dir.glob("*.sql")
        ] if models_dir.exists() else []

        dbt_ok = True
        if self.config.enable_self_correction and produced:
            dbt_ok = await run_post_agent_dbt_loop(
                api,
                agent,
                usage_limits,
                max_passes=self.config.max_self_correction_passes,
            )

        if not produced:
            status = "gave_up"
        elif self.config.enable_self_correction and not dbt_ok:
            status = "partial"
        else:
            status = "complete"
        return StrategyResult(
            target_tables_written=produced,
            target_schema=api.target_schema,
            manifest=None,
            self_reported_status=status,
        )
