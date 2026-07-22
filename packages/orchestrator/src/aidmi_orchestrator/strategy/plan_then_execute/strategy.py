"""PlanThenExecute: one global planner call, then per-table writers following the plan."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field
from pydantic_ai import Agent

from aidmi_orchestrator.domain import ModelSpec, StrategyResult
from aidmi_orchestrator.strategy.base import (
    build_context_prompt,
    run_coroutines,
    write_proposal,
)
from aidmi_orchestrator.strategy.plan_then_execute.prompts import (
    PLANNER_SYSTEM_PROMPT,
    executor_user_prompt,
    planner_user_prompt,
)
from aidmi_orchestrator.strategy.self_correction import run_dbt_self_correction
from aidmi_orchestrator.strategy.structured_common import (
    make_table_agent,
    manifest_from_mappings,
)
from aidmi_orchestrator.trace import StrategyEvent


class PlannedColumn(BaseModel):
    target_column: str
    source_columns: list[str] = Field(default_factory=list)
    transform_hint: str = ""


class PlannedTable(BaseModel):
    target_table: str
    source_tables: list[str] = Field(default_factory=list)
    join_keys: list[str] = Field(default_factory=list)
    columns: list[PlannedColumn] = Field(default_factory=list)
    notes: str = ""


class MappingPlan(BaseModel):
    tables: list[PlannedTable] = Field(default_factory=list)
    overview: str = ""


def plan_slice_text(plan: MappingPlan, target_table_name: str) -> str:
    lines = [f"Overview: {plan.overview}"] if plan.overview else []
    slice_ = next((t for t in plan.tables if t.target_table == target_table_name), None)
    if slice_ is None:
        lines.append(
            f"(The plan has no specific plan for `{target_table_name}` — map it yourself, consistent with the overview.)"
        )
        return "\n".join(lines)
    lines.append(f"Source tables: {', '.join(slice_.source_tables) or '(none listed)'}")
    if slice_.join_keys:
        lines.append(f"Join keys: {', '.join(slice_.join_keys)}")
    for c in slice_.columns:
        hint = f" ({c.transform_hint})" if c.transform_hint else ""
        lines.append(
            f"- {c.target_column} <- {', '.join(c.source_columns) or '(unspecified)'}{hint}"
        )
    if slice_.notes:
        lines.append(f"Notes: {slice_.notes}")
    return "\n".join(lines)


class PlanThenExecuteConfig(BaseModel):
    planner_model: ModelSpec
    writer_model: ModelSpec | None = None
    context_mode: Literal[
        "metadata_only", "metadata_plus_samples", "live_query_tool"
    ] = "metadata_plus_samples"
    samples_per_table: int = 3
    max_query_tool_rows: int = 100
    serial_llm_calls: bool = False
    enable_self_correction: bool = False
    max_self_correction_passes: int = 3


class PlanThenExecute:
    name = "plan_then_execute"

    def __init__(self, config: PlanThenExecuteConfig):
        self.config = config

    async def generate(self, api) -> StrategyResult:
        if api.target_schema is None:
            raise ValueError("plan_then_execute requires a target_schema")

        planner = api.make_llm(self.config.planner_model, role="planner")
        writer = api.make_llm(
            self.config.writer_model or self.config.planner_model, role="writer"
        )
        context = build_context_prompt(
            api.source_summary,
            api.target_schema,
            self.config.context_mode,
            samples_per_table=self.config.samples_per_table,
        )

        planner_agent = Agent(
            planner, output_type=MappingPlan, system_prompt=PLANNER_SYSTEM_PROMPT
        )
        plan = (await planner_agent.run(planner_user_prompt(context))).output
        api.trace.record(
            StrategyEvent(
                timestamp=datetime.utcnow(),
                label="plan_complete",
                data={
                    "overview": plan.overview,
                    "tables_planned": [t.target_table for t in plan.tables],
                },
            )
        )

        writer_agent = make_table_agent(
            writer,
            api=api,
            enable_query_tool=(self.config.context_mode == "live_query_tool"),
            max_query_tool_rows=self.config.max_query_tool_rows,
        )

        async def one_table(name: str):
            result = await writer_agent.run(
                executor_user_prompt(name, context, plan_slice_text(plan, name))
            )
            return result.output.model_copy(update={"target_table": name})

        target_table_names = [t.name for t in api.target_schema.tables]
        mappings = await run_coroutines(
            [one_table(n) for n in target_table_names],
            serial=self.config.serial_llm_calls,
        )

        mappings = [
            m.model_copy(
                update={"reasoning": f"{plan.overview}\n{m.reasoning}".strip()}
            )
            for m in mappings
        ]

        sql_by_table = {m.target_table: m.dbt_sql for m in mappings}
        source_tables = sorted(
            {(t.db_schema, t.name) for t in api.source_summary.tables}
        )
        write_proposal(
            api.dbt_project_path, sql_by_table, source_tables, api.source_schema
        )

        mappings_by_table = {m.target_table: m for m in mappings}

        dbt_ok = True
        if self.config.enable_self_correction:
            dbt_ok = await run_dbt_self_correction(
                api,
                writer_agent,
                mappings_by_table,
                context,
                dbt_project_path=api.dbt_project_path,
                source_tables=source_tables,
                source_schema=api.source_schema,
                max_passes=self.config.max_self_correction_passes,
                serial=self.config.serial_llm_calls,
            )

        manifest = manifest_from_mappings(
            list(mappings_by_table.values()),
            source_table_names=[t.name for t in api.source_summary.tables],
            strategy_name=self.name,
            strategy_config=self.config.model_dump(),
        )
        return StrategyResult(
            target_tables_written=list(sql_by_table),
            target_schema=api.target_schema,
            manifest=manifest,
            self_reported_status="complete" if dbt_ok else "partial",
        )
