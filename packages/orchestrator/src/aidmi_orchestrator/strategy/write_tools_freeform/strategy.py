"""WriteToolsFreeform: a single agent with file-write tools (optionally with
query + run_dbt for self-correction)."""
from __future__ import annotations
from typing import Literal
from pydantic import BaseModel
from pydantic_ai import Agent, Tool, UsageLimits
from pydantic_ai.exceptions import UnexpectedModelBehavior, UsageLimitExceeded

from aidmi_pipeline.sources_yaml import ensure_sources_yaml_raw_schema

from aidmi_orchestrator.domain import ModelSpec, StrategyResult
from aidmi_orchestrator.progress import log_message
from aidmi_orchestrator.strategy.base import (
    build_context_prompt,
    discover_model_sql_files,
)
from aidmi_orchestrator.strategy.llm_run import google_run_kwargs
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
    inline_run_dbt_tool: bool = False
    max_self_correction_passes: int = 3
    fixer_model: ModelSpec | None = None
    validation_gate: Literal["none", "static", "static+explain"] = "none"


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
        if self.config.enable_self_correction and self.config.inline_run_dbt_tool:
            tools.append(Tool(make_run_dbt(api, self.config.max_self_correction_passes), name="run_dbt"))

        usage_limits = UsageLimits(request_limit=self.config.max_tool_turns)
        agent = Agent(
            traced_model,
            tools=tools,
            system_prompt=build_system_prompt(
                enable_self_correction=self.config.enable_self_correction,
                inline_run_dbt_tool=self.config.inline_run_dbt_tool,
            ),
        )
        context = build_context_prompt(
            api.source_summary, api.target_schema, self.config.context_mode,
            samples_per_table=self.config.samples_per_table,
        )
        log_message(
            f"running writer agent ({self.config.writer_model.model_name}, "
            f"context={self.config.context_mode}, max_turns={self.config.max_tool_turns})",
            scope=self.name,
        )
        run_kwargs = google_run_kwargs(self.config.writer_model)
        try:
            await agent.run(
                build_initial_user_prompt(
                    context,
                    enable_self_correction=self.config.enable_self_correction,
                    inline_run_dbt_tool=self.config.inline_run_dbt_tool,
                ),
                usage_limits=usage_limits,
                **run_kwargs,
            )
        except (UnexpectedModelBehavior, UsageLimitExceeded) as e:
            log_message(f"writer agent stopped early: {e}", scope=self.name)

        models_dir = api.dbt_project_path / "models"
        ensure_sources_yaml_raw_schema(models_dir, api.source_schema)
        produced = [p.stem for p in discover_model_sql_files(api.dbt_project_path)]
        log_message(f"agent finished: {len(produced)} model(s) written", scope=self.name)

        fixer_agent = None
        fixer_run_kwargs = None
        if self.config.fixer_model is not None:
            fixer_llm = api.make_llm(self.config.fixer_model, role="fixer")
            fixer_agent = Agent(
                fixer_llm,
                tools=tools,
                system_prompt=build_system_prompt(
                    enable_self_correction=self.config.enable_self_correction,
                    inline_run_dbt_tool=self.config.inline_run_dbt_tool,
                ),
            )
            fixer_run_kwargs = google_run_kwargs(self.config.fixer_model)

        dbt_ok = True
        if self.config.enable_self_correction and produced:
            log_message(
                f"starting post-agent dbt self-correction (max {self.config.max_self_correction_passes} passes)",
                scope=self.name,
            )
            dbt_ok = await run_post_agent_dbt_loop(
                api,
                agent,
                usage_limits,
                max_passes=self.config.max_self_correction_passes,
                run_kwargs=run_kwargs,
                fixer_agent=fixer_agent,
                fixer_run_kwargs=fixer_run_kwargs,
                validation_gate=self.config.validation_gate,
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
