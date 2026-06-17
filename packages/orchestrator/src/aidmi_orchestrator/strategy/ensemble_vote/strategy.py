"""EnsembleVote: N independent candidates per table; a judge picks each winner."""
from __future__ import annotations
from datetime import datetime
from typing import Literal
from pydantic import BaseModel, Field
from pydantic_ai import Agent

from aidmi_orchestrator.domain import ModelSpec, StrategyResult
from aidmi_orchestrator.strategy.base import build_context_prompt, run_coroutines, write_proposal
from aidmi_orchestrator.strategy.structured_common import (
    TableMapping, generate_table_mapping, make_table_agent, manifest_from_mappings,
)
from aidmi_orchestrator.strategy.ensemble_vote.prompts import (
    JUDGE_SYSTEM_PROMPT, judge_user_prompt,
)
from aidmi_orchestrator.trace import StrategyEvent


class JudgeChoice(BaseModel):
    chosen_index: int
    justification: str = ""


def pick_candidate(candidates: list[TableMapping], index: int) -> TableMapping:
    return candidates[max(0, min(index, len(candidates) - 1))]


class EnsembleVoteConfig(BaseModel):
    writer_model: ModelSpec
    judge_model: ModelSpec | None = None
    n_candidates: int = Field(default=3, ge=1)
    context_mode: Literal["metadata_only", "metadata_plus_samples", "live_query_tool"] = "metadata_plus_samples"
    samples_per_table: int = 3
    max_query_tool_rows: int = 100
    serial_llm_calls: bool = False


class EnsembleVote:
    name = "ensemble_vote"

    def __init__(self, config: EnsembleVoteConfig):
        self.config = config

    async def generate(self, api) -> StrategyResult:
        if api.target_schema is None:
            raise ValueError("ensemble_vote requires a target_schema")

        writer = api.make_llm(self.config.writer_model, role="writer")
        judge = api.make_llm(self.config.judge_model or self.config.writer_model, role="judge")
        context = build_context_prompt(
            api.source_summary, api.target_schema, self.config.context_mode,
            samples_per_table=self.config.samples_per_table,
        )
        writer_agent = make_table_agent(
            writer, api=api,
            enable_query_tool=(self.config.context_mode == "live_query_tool"),
            max_query_tool_rows=self.config.max_query_tool_rows,
        )
        judge_agent = Agent(judge, output_type=JudgeChoice, system_prompt=JUDGE_SYSTEM_PROMPT)

        async def one_table(name: str) -> TableMapping:
            candidates = list(await run_coroutines(
                [generate_table_mapping(writer_agent, name, context)
                 for _ in range(self.config.n_candidates)],
                serial=self.config.serial_llm_calls,
            ))
            choice = (await judge_agent.run(
                judge_user_prompt(name, context, [c.dbt_sql for c in candidates])
            )).output
            api.trace.record(StrategyEvent(
                timestamp=datetime.utcnow(), label="candidates_judged",
                data={
                    "target_table": name,
                    "chosen_index": choice.chosen_index,
                    "justification": choice.justification,
                    "candidate_sqls": [c.dbt_sql for c in candidates],
                },
            ))
            winner = pick_candidate(candidates, choice.chosen_index)
            return winner.model_copy(update={
                "target_table": name,
                "reasoning": f"judge: {choice.justification}\n{winner.reasoning}".strip(),
            })

        target_table_names = [t.name for t in api.target_schema.tables]
        mappings = await run_coroutines(
            [one_table(n) for n in target_table_names],
            serial=self.config.serial_llm_calls,
        )

        sql_by_table = {m.target_table: m.dbt_sql for m in mappings}
        source_tables = sorted({(t.db_schema, t.name) for t in api.source_summary.tables})
        write_proposal(api.dbt_project_path, sql_by_table, source_tables, api.staging_raw_dataset)

        manifest = manifest_from_mappings(
            list(mappings),
            source_table_names=[t.name for t in api.source_summary.tables],
            strategy_name=self.name,
            strategy_config=self.config.model_dump(),
        )
        return StrategyResult(
            target_tables_written=list(sql_by_table),
            target_schema=api.target_schema,
            manifest=manifest,
            self_reported_status="complete",
        )
