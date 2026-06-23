"""StructuredPerTable: one PydanticAI agent per target table."""
from __future__ import annotations
from typing import Literal
from pydantic import BaseModel

from aidmi_orchestrator.domain import ModelSpec, StrategyResult
from aidmi_orchestrator.strategy.base import build_context_prompt, run_coroutines, write_proposal
from aidmi_orchestrator.strategy.structured_common import (
    generate_table_mapping, make_table_agent, manifest_from_mappings,
    retry_user_prompt,
)
from aidmi_orchestrator.strategy.structured_per_table.self_correction import (
    retry_failing_tables,
)


class StructuredPerTableConfig(BaseModel):
    writer_model: ModelSpec
    context_mode: Literal["metadata_only", "metadata_plus_samples", "live_query_tool"] = "metadata_plus_samples"
    samples_per_table: int = 3
    max_query_tool_rows: int = 100
    enable_self_correction: bool = False
    max_self_correction_passes: int = 3
    serial_llm_calls: bool = False


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
        agent = make_table_agent(
            traced_model,
            api=api,
            enable_query_tool=(self.config.context_mode == "live_query_tool"),
            max_query_tool_rows=self.config.max_query_tool_rows,
        )

        target_table_names = [t.name for t in api.target_schema.tables]
        mappings = await run_coroutines(
            [generate_table_mapping(agent, n, context) for n in target_table_names],
            serial=self.config.serial_llm_calls,
        )

        sql_by_table = {m.target_table: m.dbt_sql for m in mappings}
        source_tables = sorted(
            {(t.db_schema, t.name) for t in api.source_summary.tables}
        )
        write_proposal(api.dbt_project_path, sql_by_table, source_tables, api.source_schema)

        mappings_by_table = {m.target_table: m for m in mappings}

        dbt_ok = True
        if self.config.enable_self_correction:
            async def regenerate(table_name: str, error_message: str) -> None:
                previous = mappings_by_table.get(table_name)
                prompt = retry_user_prompt(
                    table_name, context,
                    previous.dbt_sql if previous else "", error_message,
                )
                run = await agent.run(prompt)
                fixed = run.output.model_copy(update={"target_table": table_name})
                mappings_by_table[table_name] = fixed
                write_proposal(
                    api.dbt_project_path,
                    {name: m.dbt_sql for name, m in mappings_by_table.items()},
                    source_tables,
                    api.source_schema,
                )

            dbt_ok = await retry_failing_tables(
                api.run_dbt, regenerate,
                max_passes=self.config.max_self_correction_passes,
                serial=self.config.serial_llm_calls,
                all_table_names=list(mappings_by_table),
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
