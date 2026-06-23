"""PlanWriteCritique: plan, write, dbt self-correction, then critique with data validation."""
from __future__ import annotations

import sys
from datetime import datetime

from pydantic import BaseModel, Field
from pydantic_ai import Agent, Tool

from aidmi_orchestrator.domain import ModelSpec, StrategyResult
from aidmi_orchestrator.strategy.base import build_context_prompt, run_coroutines, write_proposal
from aidmi_orchestrator.strategy.plan_then_execute.prompts import executor_user_prompt
from aidmi_orchestrator.strategy.plan_then_execute.strategy import MappingPlan, plan_slice_text
from aidmi_orchestrator.strategy.plan_write_critique.loops import (
    retry_failing_tables_with_progress,
    run_critique_with_dbt_loop,
)
from aidmi_orchestrator.strategy.plan_write_critique.prompts import (
    CRITIC_SYSTEM_PROMPT_WITH_QUERY_TOOL,
    critique_user_prompt,
    planner_user_prompt,
    PLANNER_SYSTEM_PROMPT,
)
from aidmi_orchestrator.strategy.structured_common import (
    make_table_agent,
    manifest_from_mappings,
    retry_user_prompt,
    TableMapping,
)
from aidmi_orchestrator.strategy.write_then_critique.critique import CritiqueReport
from aidmi_orchestrator.strategy.write_then_critique.prompts import (
    render_proposal,
    revision_user_prompt,
)
from aidmi_orchestrator.strategy.write_tools_freeform.tools import make_query_postgres
from aidmi_orchestrator.trace import StrategyEvent


def log_progress(message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[plan_write_critique] {ts} {message}", file=sys.stderr, flush=True)


class PlanWriteCritiqueConfig(BaseModel):
    planner_model: ModelSpec
    writer_model: ModelSpec | None = None
    critic_model: ModelSpec | None = None
    max_dbt_correction_initial: int = Field(default=3, ge=0)
    max_critique_rounds: int = Field(default=2, ge=1)
    max_dbt_correction_per_critique: int = Field(default=2, ge=0)
    samples_per_table: int = 3
    max_query_tool_rows: int = 100
    serial_llm_calls: bool = False


class PlanWriteCritique:
    name = "plan_write_critique"

    def __init__(self, config: PlanWriteCritiqueConfig):
        self.config = config

    async def generate(self, api) -> StrategyResult:
        if api.target_schema is None:
            raise ValueError("plan_write_critique requires a target_schema")

        log_progress("=== PHASE 1: PLANNING ===")
        log_progress("Initializing models (planner, writer, critic)")

        planner = api.make_llm(self.config.planner_model, role="planner")
        writer_spec = self.config.writer_model or self.config.planner_model
        critic_spec = self.config.critic_model or self.config.writer_model or self.config.planner_model
        writer = api.make_llm(writer_spec, role="writer")
        critic = api.make_llm(critic_spec, role="critic")

        context = build_context_prompt(
            api.source_summary,
            api.target_schema,
            mode="live_query_tool",
            samples_per_table=self.config.samples_per_table,
        )

        log_progress("Calling planner model to create mapping plan...")
        planner_agent = Agent(planner, output_type=MappingPlan, system_prompt=PLANNER_SYSTEM_PROMPT)
        plan = (await planner_agent.run(planner_user_prompt(context))).output
        log_progress(f"Plan complete: {len(plan.tables)} tables planned")
        api.trace.record(StrategyEvent(
            timestamp=datetime.utcnow(),
            label="plan_complete",
            data={"overview": plan.overview, "tables_planned": [t.target_table for t in plan.tables]},
        ))

        log_progress("=== PHASE 2: INITIAL WRITE ===")
        writer_agent = make_table_agent(
            writer,
            api=api,
            enable_query_tool=True,
            max_query_tool_rows=self.config.max_query_tool_rows,
        )

        async def one_table(name: str) -> TableMapping:
            result = await writer_agent.run(
                executor_user_prompt(name, context, plan_slice_text(plan, name))
            )
            return result.output.model_copy(
                update={
                    "target_table": name,
                    "reasoning": f"{plan.overview}\n{result.output.reasoning}".strip(),
                }
            )

        target_table_names = [t.name for t in api.target_schema.tables]
        mode = "serial" if self.config.serial_llm_calls else "parallel"
        log_progress(f"Writing {len(target_table_names)} tables in {mode}...")
        initial = await run_coroutines(
            [one_table(n) for n in target_table_names],
            serial=self.config.serial_llm_calls,
        )
        mappings = {m.target_table: m for m in initial}
        log_progress(f"Initial write complete: {len(mappings)} tables generated")

        source_tables = sorted({(t.db_schema, t.name) for t in api.source_summary.tables})

        log_progress("=== PHASE 3: INITIAL DBT SELF-CORRECTION ===")
        sql_by_table = {name: m.dbt_sql for name, m in mappings.items()}
        write_proposal(api.dbt_project_path, sql_by_table, source_tables, api.source_schema)

        async def regenerate(table_name: str, error: str) -> None:
            log_progress(f"  Regenerating table: {table_name}")
            result = await writer_agent.run(
                retry_user_prompt(table_name, context, mappings[table_name].dbt_sql, error)
            )
            fixed = result.output.model_copy(update={"target_table": table_name})
            mappings[table_name] = fixed
            (api.dbt_project_path / "models" / f"{table_name}.sql").write_text(
                fixed.dbt_sql, encoding="utf-8"
            )

        initial_dbt_ok: bool | None = None
        if self.config.max_dbt_correction_initial > 0:
            log_progress(
                f"Running initial dbt self-correction "
                f"(max {self.config.max_dbt_correction_initial} passes)..."
            )
            initial_dbt_ok = await retry_failing_tables_with_progress(
                api.run_dbt,
                regenerate,
                max_passes=self.config.max_dbt_correction_initial,
                serial=self.config.serial_llm_calls,
                progress_callback=lambda pass_num, total: log_progress(
                    f"  dbt correction pass {pass_num}/{total}"
                ),
            )
            log_progress(
                f"Initial dbt correction {'PASSED' if initial_dbt_ok else 'FAILED or incomplete'}"
            )
        else:
            log_progress("Skipping initial dbt correction (max_dbt_correction_initial=0)")

        if initial_dbt_ok is False:
            log_progress("Stopping — initial dbt self-correction did not succeed")
            api.trace.record(StrategyEvent(
                timestamp=datetime.utcnow(),
                label="initial_dbt_correction_complete",
                data={"success": False},
            ))
            sql_by_table = {name: m.dbt_sql for name, m in mappings.items()}
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
                self_reported_status="gave_up",
            )

        api.trace.record(StrategyEvent(
            timestamp=datetime.utcnow(),
            label="initial_dbt_correction_complete",
            data={"success": initial_dbt_ok if initial_dbt_ok is not None else True},
        ))

        critic_agent = Agent(
            critic,
            output_type=CritiqueReport,
            system_prompt=CRITIC_SYSTEM_PROMPT_WITH_QUERY_TOOL,
            tools=[Tool(make_query_postgres(api, self.config.max_query_tool_rows), name="query_postgres")],
        )

        async def critique(current: dict[str, TableMapping]) -> CritiqueReport:
            result = await critic_agent.run(
                critique_user_prompt(context, render_proposal(current), api.out_schema)
            )
            api.trace.record(StrategyEvent(
                timestamp=datetime.utcnow(),
                label="critique_round_complete",
                data={"verdicts": [v.model_dump() for v in result.output.verdicts]},
            ))
            return result.output

        async def revise(name: str, previous: TableMapping, comments: str) -> TableMapping:
            result = await writer_agent.run(
                revision_user_prompt(name, context, previous.dbt_sql, comments)
            )
            return result.output.model_copy(update={"target_table": name})

        async def run_dbt_correction(current_mappings: dict[str, TableMapping]) -> bool:
            sql = {name: m.dbt_sql for name, m in current_mappings.items()}
            write_proposal(api.dbt_project_path, sql, source_tables, api.source_schema)

            async def regenerate_inner(table_name: str, error: str) -> None:
                log_progress(f"    Regenerating table: {table_name}")
                result = await writer_agent.run(
                    retry_user_prompt(
                        table_name, context, current_mappings[table_name].dbt_sql, error
                    )
                )
                fixed = result.output.model_copy(update={"target_table": table_name})
                current_mappings[table_name] = fixed
                (api.dbt_project_path / "models" / f"{table_name}.sql").write_text(
                    fixed.dbt_sql, encoding="utf-8"
                )

            if self.config.max_dbt_correction_per_critique <= 0:
                return True

            return await retry_failing_tables_with_progress(
                api.run_dbt,
                regenerate_inner,
                max_passes=self.config.max_dbt_correction_per_critique,
                serial=self.config.serial_llm_calls,
                progress_callback=lambda pass_num, total: log_progress(
                    f"    dbt correction pass {pass_num}/{total}"
                ),
            )

        mappings, approved = await run_critique_with_dbt_loop(
            mappings,
            critique,
            revise,
            run_dbt_correction,
            max_critique_rounds=self.config.max_critique_rounds,
            serial=self.config.serial_llm_calls,
            log_progress=log_progress,
        )

        sql_by_table = {name: m.dbt_sql for name, m in mappings.items()}
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
