"""plan_write_critique: plan, write, dbt self-correction, critique with live query."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from aidmi_orchestrator.domain import ModelSpec
from aidmi_orchestrator.strategy.plan_write_critique.loops import (
    retry_failing_tables_with_progress,
    run_critique_with_dbt_loop,
)
from aidmi_orchestrator.strategy.plan_write_critique.strategy import (
    PlanWriteCritique,
    PlanWriteCritiqueConfig,
)
from aidmi_orchestrator.strategy.structured_common import TableMapping
from aidmi_orchestrator.strategy.write_then_critique.critique import (
    CritiqueReport,
    TableVerdict,
)
from pydantic import ValidationError
from pydantic_ai.models.test import TestModel

from .test_structured_common import MAPPING_ARGS, fake_api

PLAN_ARGS = {
    "overview": "Map contacts to users",
    "tables": [
        {
            "target_table": "users",
            "source_tables": ["contacts"],
            "join_keys": [],
            "columns": [
                {
                    "target_column": "user_id",
                    "source_columns": ["id"],
                    "transform_hint": "",
                }
            ],
            "notes": "",
        }
    ],
}


def _mapping(table: str, sql: str = "SELECT 1") -> TableMapping:
    return TableMapping(target_table=table, dbt_sql=sql, column_notes=[])


def _report(**verdicts: str) -> CritiqueReport:
    return CritiqueReport(
        verdicts=[
            TableVerdict(target_table=t, verdict=v, comments=f"note on {t}")
            for t, v in verdicts.items()
        ]
    )


def _fake_api_with_dbt(tmp_path, make_llm, *, dbt_success: bool = True):
    api = fake_api(tmp_path, make_llm=make_llm)

    async def run_dbt():
        return SimpleNamespace(
            overall_status="success" if dbt_success else "error",
            models=[]
            if dbt_success
            else [
                SimpleNamespace(
                    model_name="users", status="error", error_message="syntax error"
                ),
            ],
        )

    api.run_dbt = run_dbt
    return api


def test_config_defaults_and_model_fallbacks() -> None:
    cfg = PlanWriteCritiqueConfig(
        planner_model=ModelSpec(provider="litellm", model_name="planner"),
    )
    assert cfg.writer_model is None
    assert cfg.critic_model is None
    assert cfg.max_dbt_correction_initial == 3
    assert cfg.max_critique_rounds == 2
    assert cfg.max_dbt_correction_per_critique == 2
    assert cfg.context_mode == "live_query_tool"


def test_config_accepts_context_mode() -> None:
    cfg = PlanWriteCritiqueConfig(
        planner_model=ModelSpec(provider="litellm", model_name="planner"),
        context_mode="metadata_only",
    )
    assert cfg.context_mode == "metadata_only"


def test_config_rejects_zero_critique_rounds() -> None:
    with pytest.raises(ValidationError):
        PlanWriteCritiqueConfig(
            planner_model=ModelSpec(provider="litellm", model_name="m"),
            max_critique_rounds=0,
        )


def test_critique_loop_all_approved_first_round() -> None:
    mappings = {"users": _mapping("users")}
    critique = AsyncMock(return_value=_report(users="approved"))
    revise = AsyncMock()
    dbt = AsyncMock(return_value=True)
    final, approved = asyncio.run(
        run_critique_with_dbt_loop(
            mappings,
            critique,
            revise,
            dbt,
            max_critique_rounds=2,
        )
    )
    assert approved is True
    assert final == mappings
    dbt.assert_not_awaited()


def test_critique_loop_revision_dbt_then_approval() -> None:
    mappings = {"users": _mapping("users", "bad")}
    critique = AsyncMock(
        side_effect=[_report(users="needs_revision"), _report(users="approved")]
    )
    revise = AsyncMock(return_value=_mapping("users", "fixed"))
    dbt = AsyncMock(return_value=True)
    final, approved = asyncio.run(
        run_critique_with_dbt_loop(
            mappings,
            critique,
            revise,
            dbt,
            max_critique_rounds=2,
        )
    )
    assert approved is True
    assert final["users"].dbt_sql == "fixed"
    dbt.assert_awaited_once()


def test_critique_loop_exhausted_rounds() -> None:
    mappings = {"users": _mapping("users")}
    critique = AsyncMock(return_value=_report(users="needs_revision"))
    revise = AsyncMock(return_value=_mapping("users"))
    dbt = AsyncMock(return_value=True)
    final, approved = asyncio.run(
        run_critique_with_dbt_loop(
            mappings,
            critique,
            revise,
            dbt,
            max_critique_rounds=2,
        )
    )
    assert approved is False
    assert critique.await_count == 2
    assert dbt.await_count == 2


def test_critique_loop_stops_when_dbt_fails() -> None:
    mappings = {"users": _mapping("users")}
    critique = AsyncMock(return_value=_report(users="needs_revision"))
    revise = AsyncMock(return_value=_mapping("users", "fixed"))
    dbt = AsyncMock(return_value=False)
    final, approved = asyncio.run(
        run_critique_with_dbt_loop(
            mappings,
            critique,
            revise,
            dbt,
            max_critique_rounds=5,
        )
    )
    assert approved is False
    assert critique.await_count == 1
    assert dbt.await_count == 1
    assert final["users"].dbt_sql == "fixed"


def test_critique_loop_critique_crash() -> None:
    mappings = {"users": _mapping("users")}
    critique = AsyncMock(side_effect=RuntimeError("critic down"))
    revise = AsyncMock()
    dbt = AsyncMock(return_value=True)
    final, approved = asyncio.run(
        run_critique_with_dbt_loop(
            mappings,
            critique,
            revise,
            dbt,
            max_critique_rounds=3,
        )
    )
    assert approved is False
    revise.assert_not_awaited()
    dbt.assert_not_awaited()


def test_critique_loop_revise_crash() -> None:
    mappings = {"users": _mapping("users")}
    critique = AsyncMock(return_value=_report(users="needs_revision"))
    revise = AsyncMock(side_effect=RuntimeError("llm down"))
    dbt = AsyncMock(return_value=True)
    final, approved = asyncio.run(
        run_critique_with_dbt_loop(
            mappings,
            critique,
            revise,
            dbt,
            max_critique_rounds=3,
        )
    )
    assert approved is False
    assert final["users"].dbt_sql == "SELECT 1"
    dbt.assert_not_awaited()


def test_retry_failing_tables_with_progress_success() -> None:
    run_dbt = AsyncMock(
        return_value=SimpleNamespace(overall_status="success", models=[])
    )
    regenerate = AsyncMock()
    ok = asyncio.run(
        retry_failing_tables_with_progress(
            run_dbt,
            regenerate,
            max_passes=3,
        )
    )
    assert ok is True
    run_dbt.assert_awaited_once()
    regenerate.assert_not_awaited()


def test_retry_failing_tables_with_progress_regenerates_then_succeeds() -> None:
    run_dbt = AsyncMock(
        side_effect=[
            SimpleNamespace(
                overall_status="error",
                models=[
                    SimpleNamespace(
                        model_name="users", status="error", error_message="bad sql"
                    )
                ],
            ),
            SimpleNamespace(overall_status="success", models=[]),
        ]
    )
    regenerate = AsyncMock()
    progress: list[tuple[int, int]] = []
    ok = asyncio.run(
        retry_failing_tables_with_progress(
            run_dbt,
            regenerate,
            max_passes=3,
            progress_callback=lambda n, total: progress.append((n, total)),
        )
    )
    assert ok is True
    regenerate.assert_awaited_once()
    assert progress == [(1, 3), (2, 3)]


def test_strategy_stops_when_initial_dbt_fails(tmp_path) -> None:
    planner = TestModel(custom_output_args=PLAN_ARGS)
    writer = TestModel(custom_output_args=MAPPING_ARGS)
    critic = TestModel(
        custom_output_args={
            "verdicts": [
                {"target_table": "users", "verdict": "approved", "comments": ""}
            ],
        }
    )

    def make_llm(spec, role):
        if role == "planner":
            return planner
        if role == "critic":
            return critic
        return writer

    api = _fake_api_with_dbt(tmp_path, make_llm=make_llm, dbt_success=False)
    strategy = PlanWriteCritique(
        PlanWriteCritiqueConfig(
            planner_model=ModelSpec(provider="litellm", model_name="planner"),
            max_dbt_correction_initial=2,
            max_dbt_correction_per_critique=0,
        )
    )
    result = asyncio.run(strategy.generate(api))
    assert result.self_reported_status == "gave_up"
    recorded_labels = [
        call.args[0].label
        for call in api.trace.record.call_args_list
        if call.args and hasattr(call.args[0], "label")
    ]
    assert "critique_round_complete" not in recorded_labels
    assert (tmp_path / "models" / "users.sql").exists()


def test_strategy_complete_when_critic_approves(tmp_path) -> None:
    planner = TestModel(custom_output_args=PLAN_ARGS)
    writer = TestModel(custom_output_args=MAPPING_ARGS)
    critic = TestModel(
        custom_output_args={
            "verdicts": [
                {"target_table": "users", "verdict": "approved", "comments": ""}
            ],
        }
    )
    roles: list[str] = []

    def make_llm(spec, role):
        roles.append(role)
        if role == "planner":
            return planner
        if role == "critic":
            return critic
        return writer

    api = _fake_api_with_dbt(tmp_path, make_llm=make_llm, dbt_success=True)
    strategy = PlanWriteCritique(
        PlanWriteCritiqueConfig(
            planner_model=ModelSpec(provider="litellm", model_name="planner"),
            max_dbt_correction_initial=1,
            max_dbt_correction_per_critique=0,
        )
    )
    result = asyncio.run(strategy.generate(api))
    assert result.self_reported_status == "complete"
    assert set(roles) == {"planner", "writer", "critic"}
    assert (tmp_path / "models" / "users.sql").exists()
    assert result.manifest is not None
    api.trace.record.assert_called()


def test_strategy_partial_when_critic_never_approves(tmp_path) -> None:
    planner = TestModel(custom_output_args=PLAN_ARGS)
    writer = TestModel(custom_output_args=MAPPING_ARGS)
    critic = TestModel(
        custom_output_args={
            "verdicts": [
                {
                    "target_table": "users",
                    "verdict": "needs_revision",
                    "comments": "bad data",
                }
            ],
        }
    )

    def make_llm(spec, role):
        if role == "planner":
            return planner
        if role == "critic":
            return critic
        return writer

    api = _fake_api_with_dbt(tmp_path, make_llm=make_llm, dbt_success=True)
    strategy = PlanWriteCritique(
        PlanWriteCritiqueConfig(
            planner_model=ModelSpec(provider="litellm", model_name="planner"),
            max_critique_rounds=2,
            max_dbt_correction_initial=0,
            max_dbt_correction_per_critique=0,
        )
    )
    result = asyncio.run(strategy.generate(api))
    assert result.self_reported_status == "partial"


def test_strategy_requires_target_schema(tmp_path) -> None:
    api = fake_api(tmp_path, make_llm=lambda spec, role: MagicMock())
    api.target_schema = None
    strategy = PlanWriteCritique(
        PlanWriteCritiqueConfig(
            planner_model=ModelSpec(provider="litellm", model_name="m"),
        )
    )
    with pytest.raises(ValueError, match="target_schema"):
        asyncio.run(strategy.generate(api))
