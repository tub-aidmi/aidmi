"""write_then_critique: global critic over per-table writer proposals."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

from pydantic_ai.models.test import TestModel

from aidmi_orchestrator.domain import ModelSpec
from aidmi_orchestrator.strategy.structured_common import TableMapping
from aidmi_orchestrator.strategy.write_then_critique.critique import (
    CritiqueReport, TableVerdict, run_critique_rounds,
)
from aidmi_orchestrator.strategy.write_then_critique.strategy import (
    WriteThenCritique, WriteThenCritiqueConfig,
)

from .test_structured_common import MAPPING_ARGS, fake_api


def _mapping(table: str, sql: str = "SELECT 1") -> TableMapping:
    return TableMapping(target_table=table, dbt_sql=sql, column_notes=[])


def _report(**verdicts: str) -> CritiqueReport:
    return CritiqueReport(verdicts=[
        TableVerdict(target_table=t, verdict=v, comments=f"note on {t}")
        for t, v in verdicts.items()
    ])


def test_all_approved_first_round() -> None:
    mappings = {"users": _mapping("users")}
    critique = AsyncMock(return_value=_report(users="approved"))
    revise = AsyncMock()
    final, approved = asyncio.run(run_critique_rounds(mappings, critique, revise, max_rounds=2))
    assert approved is True
    assert final == mappings
    critique.assert_awaited_once()
    revise.assert_not_awaited()


def test_revision_then_approval() -> None:
    mappings = {"users": _mapping("users", "bad")}
    critique = AsyncMock(side_effect=[_report(users="needs_revision"), _report(users="approved")])
    revise = AsyncMock(return_value=_mapping("users", "fixed"))
    final, approved = asyncio.run(run_critique_rounds(mappings, critique, revise, max_rounds=2))
    assert approved is True
    assert final["users"].dbt_sql == "fixed"
    revise.assert_awaited_once()
    args = revise.await_args.args
    assert args[0] == "users"
    assert args[2] == "note on users"


def test_rounds_exhausted_returns_not_approved() -> None:
    mappings = {"users": _mapping("users")}
    critique = AsyncMock(return_value=_report(users="needs_revision"))
    revise = AsyncMock(return_value=_mapping("users"))
    final, approved = asyncio.run(run_critique_rounds(mappings, critique, revise, max_rounds=2))
    assert approved is False
    assert critique.await_count == 2
    assert revise.await_count == 2


def test_unknown_table_in_verdict_is_ignored() -> None:
    mappings = {"users": _mapping("users")}
    critique = AsyncMock(return_value=_report(users="approved", ghosts="needs_revision"))
    revise = AsyncMock()
    _, approved = asyncio.run(run_critique_rounds(mappings, critique, revise, max_rounds=1))
    assert approved is True
    revise.assert_not_awaited()


def test_strategy_complete_when_critic_approves(tmp_path) -> None:
    writer = TestModel(custom_output_args=MAPPING_ARGS)
    critic = TestModel(custom_output_args={
        "verdicts": [{"target_table": "users", "verdict": "approved", "comments": ""}],
    })
    roles: list[str] = []

    def make_llm(spec, role):
        roles.append(role)
        return critic if role == "critic" else writer

    api = fake_api(tmp_path, make_llm=make_llm)
    strategy = WriteThenCritique(WriteThenCritiqueConfig(
        writer_model=ModelSpec(provider="litellm", model_name="m"),
    ))
    result = asyncio.run(strategy.generate(api))
    assert result.self_reported_status == "complete"
    assert set(roles) == {"writer", "critic"}
    assert (tmp_path / "models" / "users.sql").exists()
    assert result.manifest is not None


def test_strategy_partial_when_critic_never_approves(tmp_path) -> None:
    writer = TestModel(custom_output_args=MAPPING_ARGS)
    critic = TestModel(custom_output_args={
        "verdicts": [{"target_table": "users", "verdict": "needs_revision", "comments": "wrong cast"}],
    })
    api = fake_api(tmp_path, make_llm=lambda spec, role: critic if role == "critic" else writer)
    strategy = WriteThenCritique(WriteThenCritiqueConfig(
        writer_model=ModelSpec(provider="litellm", model_name="m"),
        max_critique_rounds=2,
    ))
    result = asyncio.run(strategy.generate(api))
    assert result.self_reported_status == "partial"


def test_revise_crash_returns_unapproved() -> None:
    mappings = {"users": _mapping("users")}
    critique = AsyncMock(return_value=_report(users="needs_revision"))
    revise = AsyncMock(side_effect=RuntimeError("llm down"))
    final, approved = asyncio.run(run_critique_rounds(mappings, critique, revise, max_rounds=3))
    assert approved is False
    assert final["users"].dbt_sql == "SELECT 1"
    critique.assert_awaited_once()


def test_config_rejects_zero_rounds() -> None:
    import pytest
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        WriteThenCritiqueConfig(
            writer_model=ModelSpec(provider="litellm", model_name="m"),
            max_critique_rounds=0,
        )
