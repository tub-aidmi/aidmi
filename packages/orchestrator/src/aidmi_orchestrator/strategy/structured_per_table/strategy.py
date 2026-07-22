"""StructuredPerTable: one PydanticAI agent per target table."""

from __future__ import annotations
from typing import Literal
from pydantic import BaseModel

from aidmi_orchestrator.domain import ModelSpec, StrategyResult
from aidmi_orchestrator.strategy.base import (
    build_context_prompt,
    run_coroutines,
    write_proposal,
)
from aidmi_orchestrator.strategy.structured_common import (
    generate_table_mapping_safe,
    make_table_agent,
    manifest_from_mappings,
    resolve_structured_status,
)
from aidmi_orchestrator.strategy.llm_run import google_run_kwargs
from aidmi_orchestrator.strategy.self_correction import run_dbt_self_correction


class StructuredPerTableConfig(BaseModel):
    writer_model: ModelSpec
    context_mode: Literal[
        "metadata_only", "metadata_plus_samples", "live_query_tool"
    ] = "metadata_plus_samples"
    samples_per_table: int = 3
    max_query_tool_rows: int = 100
    enable_self_correction: bool = False
    max_self_correction_passes: int = 3
    serial_llm_calls: bool = False
    fixer_model: ModelSpec | None = None
    validation_gate: Literal["none", "static", "static+explain"] = "none"


class StructuredPerTable:
    name = "structured_per_table"

    def __init__(self, config: StructuredPerTableConfig):
        self.config = config

    async def generate(self, api) -> StrategyResult:
        if api.target_schema is None:
            raise ValueError(
                "structured_per_table requires a target_schema (no free-design mode)"
            )

        traced_model = api.make_llm(self.config.writer_model, role="writer")
        context = build_context_prompt(
            api.source_summary,
            api.target_schema,
            self.config.context_mode,
            samples_per_table=self.config.samples_per_table,
        )
        agent = make_table_agent(
            traced_model,
            api=api,
            enable_query_tool=(self.config.context_mode == "live_query_tool"),
            max_query_tool_rows=self.config.max_query_tool_rows,
        )

        writer_run_kwargs = google_run_kwargs(self.config.writer_model)
        target_table_names = [t.name for t in api.target_schema.tables]
        mappings = await run_coroutines(
            [
                generate_table_mapping_safe(
                    agent, n, context, run_kwargs=writer_run_kwargs
                )
                for n in target_table_names
            ],
            serial=self.config.serial_llm_calls,
        )

        sql_by_table = {m.target_table: m.dbt_sql for m in mappings}
        source_tables = sorted(
            {(t.db_schema, t.name) for t in api.source_summary.tables}
        )
        write_proposal(
            api.dbt_project_path, sql_by_table, source_tables, api.source_schema
        )

        mappings_by_table = {m.target_table: m for m in mappings}

        fixer_agent = None
        fixer_run_kwargs = None
        if self.config.fixer_model is not None:
            fixer_llm = api.make_llm(self.config.fixer_model, role="fixer")
            fixer_agent = make_table_agent(
                fixer_llm,
                api=api,
                enable_query_tool=(self.config.context_mode == "live_query_tool"),
                max_query_tool_rows=self.config.max_query_tool_rows,
            )
            fixer_run_kwargs = google_run_kwargs(self.config.fixer_model)

        dbt_ok = True
        if self.config.enable_self_correction:
            dbt_ok = await run_dbt_self_correction(
                api,
                agent,
                mappings_by_table,
                context,
                dbt_project_path=api.dbt_project_path,
                source_tables=source_tables,
                source_schema=api.source_schema,
                max_passes=self.config.max_self_correction_passes,
                serial=self.config.serial_llm_calls,
                run_kwargs=writer_run_kwargs,
                fixer_agent=fixer_agent,
                fixer_run_kwargs=fixer_run_kwargs,
                validation_gate=self.config.validation_gate,
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
            self_reported_status=resolve_structured_status(
                list(mappings_by_table.values()), dbt_ok
            ),
        )
