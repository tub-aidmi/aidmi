"""Opt-in dbt self-correction on additional structured strategies."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from aidmi_orchestrator.domain import ModelSpec
from aidmi_orchestrator.strategy.ensemble_vote.strategy import (
    EnsembleVote,
    EnsembleVoteConfig,
)
from aidmi_orchestrator.strategy.plan_then_execute.strategy import (
    MappingPlan,
    PlannedTable,
    PlanThenExecute,
    PlanThenExecuteConfig,
)
from aidmi_orchestrator.strategy.write_then_critique.critique import (
    CritiqueReport,
    TableVerdict,
)
from aidmi_orchestrator.strategy.write_then_critique.strategy import (
    WriteThenCritique,
    WriteThenCritiqueConfig,
)
from pydantic_ai.models.test import TestModel

from .test_structured_common import MAPPING_ARGS, fake_api, small_target_schema


def _fail(*names: str) -> SimpleNamespace:
    return SimpleNamespace(
        overall_status="error",
        models=[
            SimpleNamespace(model_name=n, status="error", error_message=f"{n} broke")
            for n in names
        ],
    )


_OK = SimpleNamespace(overall_status="success", models=[])


def _plan() -> MappingPlan:
    return MappingPlan(
        overview="test plan",
        tables=[PlannedTable(target_table="users", source_tables=["contacts"])],
    )


def test_plan_then_execute_partial_when_self_correction_fails(tmp_path) -> None:
    writer_model = TestModel(custom_output_args=MAPPING_ARGS)
    planner_model = TestModel(custom_output_args=_plan().model_dump())
    api = fake_api(
        tmp_path,
        make_llm=lambda spec, role: (
            planner_model if role == "planner" else writer_model
        ),
    )
    api.run_dbt = AsyncMock(return_value=_fail("users"))
    strategy = PlanThenExecute(
        PlanThenExecuteConfig(
            planner_model=ModelSpec(provider="litellm", model_name="m"),
            enable_self_correction=True,
            max_self_correction_passes=2,
        )
    )
    result = asyncio.run(strategy.generate(api))
    assert result.self_reported_status == "partial"
    assert api.run_dbt.await_count == 2


def test_ensemble_vote_partial_when_self_correction_fails(tmp_path) -> None:
    writer_model = TestModel(custom_output_args=MAPPING_ARGS)
    judge_model = TestModel(
        custom_output_args={"chosen_index": 0, "justification": "ok"}
    )
    api = fake_api(
        tmp_path,
        make_llm=lambda spec, role: judge_model if role == "judge" else writer_model,
    )
    api.run_dbt = AsyncMock(return_value=_fail("users"))
    strategy = EnsembleVote(
        EnsembleVoteConfig(
            writer_model=ModelSpec(provider="litellm", model_name="m"),
            n_candidates=1,
            enable_self_correction=True,
            max_self_correction_passes=2,
        )
    )
    result = asyncio.run(strategy.generate(api))
    assert result.self_reported_status == "partial"
    assert api.run_dbt.await_count == 2


def test_write_then_critique_partial_when_self_correction_fails(tmp_path) -> None:
    writer_model = TestModel(custom_output_args=MAPPING_ARGS)
    critic_model = TestModel(
        custom_output_args=CritiqueReport(
            verdicts=[
                TableVerdict(target_table="users", verdict="approved", comments="")
            ],
        ).model_dump()
    )
    api = fake_api(
        tmp_path,
        make_llm=lambda spec, role: critic_model if role == "critic" else writer_model,
    )
    api.target_schema = small_target_schema()
    api.run_dbt = AsyncMock(return_value=_fail("users"))
    strategy = WriteThenCritique(
        WriteThenCritiqueConfig(
            writer_model=ModelSpec(provider="litellm", model_name="m"),
            max_critique_rounds=1,
            enable_self_correction=True,
            max_self_correction_passes=2,
        )
    )
    result = asyncio.run(strategy.generate(api))
    assert result.self_reported_status == "partial"
    assert api.run_dbt.await_count == 2
