"""plan_then_execute: one global planner, per-table writers following the plan."""
from __future__ import annotations

import asyncio

from pydantic_ai.models.test import TestModel

from aidmi_orchestrator.domain import ModelSpec
from aidmi_orchestrator.strategy.plan_then_execute.strategy import (
    MappingPlan, PlanThenExecute, PlanThenExecuteConfig, plan_slice_text,
)

from .test_structured_common import MAPPING_ARGS, fake_api

PLAN_ARGS = dict(
    overview="map contacts onto users",
    tables=[{
        "target_table": "users",
        "source_tables": ["contacts"],
        "join_keys": [],
        "columns": [{"target_column": "user_id", "source_columns": ["contacts.id"], "transform_hint": "cast"}],
        "notes": "single table",
    }],
)


def test_generate_runs_planner_then_writers(tmp_path) -> None:
    planner = TestModel(custom_output_args=PLAN_ARGS)
    writer = TestModel(custom_output_args=MAPPING_ARGS)
    roles: list[str] = []

    def make_llm(spec, role):
        roles.append(role)
        return planner if role == "planner" else writer

    api = fake_api(tmp_path, make_llm=make_llm)
    strategy = PlanThenExecute(PlanThenExecuteConfig(
        planner_model=ModelSpec(provider="litellm", model_name="m"),
    ))
    result = asyncio.run(strategy.generate(api))
    assert result.self_reported_status == "complete"
    assert set(roles) == {"planner", "writer"}
    assert (tmp_path / "models" / "users.sql").exists()
    assert result.manifest is not None
    assert "map contacts onto users" in result.manifest.tables[0].reasoning


def test_plan_slice_text_includes_columns_and_overview() -> None:
    plan = MappingPlan(**PLAN_ARGS)
    text = plan_slice_text(plan, "users")
    assert "map contacts onto users" in text
    assert "user_id" in text
    assert "contacts.id" in text


def test_plan_slice_text_handles_missing_table() -> None:
    plan = MappingPlan(**PLAN_ARGS)
    text = plan_slice_text(plan, "ghosts")
    assert "no specific plan" in text
